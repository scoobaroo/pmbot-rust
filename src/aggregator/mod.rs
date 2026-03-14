pub mod candle_builder;

use crate::types::candle::Timeframe;
use crate::types::events::{AggregatorEvent, ExchangeEvent};
use crate::types::market::{AggregatedPrice, Exchange, MarketTick};
use candle_builder::CandleBuilder;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::strategy::volatility::VolatilityTracker;

/// Key: (exchange, symbol)
type FeedKey = (Exchange, String);

pub struct Aggregator {
    latest_ticks: HashMap<FeedKey, MarketTick>,
    volatility_trackers: HashMap<String, VolatilityTracker>,
    candle_builder: CandleBuilder,
    stale_timeout_secs: u64,
    /// Multiplier on Binance volume in VWAP (e.g. 5.0 = 5× weight)
    binance_weight_multiplier: Decimal,
    /// Weight of Chainlink oracle in final blend (0.0–1.0)
    chainlink_blend_weight: Decimal,
}

impl Aggregator {
    pub fn new(
        stale_timeout_secs: u64,
        volatility_window_hours: u64,
        candle_timeframes: Vec<Timeframe>,
        binance_weight_multiplier: f64,
        chainlink_blend_weight: f64,
    ) -> Self {
        let _ = volatility_window_hours; // stored per-tracker on creation
        Self {
            latest_ticks: HashMap::new(),
            volatility_trackers: HashMap::new(),
            candle_builder: CandleBuilder::new(candle_timeframes),
            stale_timeout_secs,
            binance_weight_multiplier: Decimal::from_f64_retain(binance_weight_multiplier)
                .unwrap_or(Decimal::from(5)),
            chainlink_blend_weight: Decimal::from_f64_retain(chainlink_blend_weight)
                .unwrap_or(Decimal::new(1, 1)), // 0.1
        }
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<ExchangeEvent>,
        tx: mpsc::Sender<AggregatorEvent>,
        ml_tx: Option<mpsc::Sender<AggregatorEvent>>,
        shutdown: CancellationToken,
        volatility_window_hours: u64,
    ) {
        info!("Aggregator started");

        let mut stale_check_interval = tokio::time::interval(std::time::Duration::from_secs(
            self.stale_timeout_secs.max(5),
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Aggregator shutting down");
                    return;
                }
                Some(event) = rx.recv() => {
                    match event {
                        ExchangeEvent::Tick(tick) => {
                            let symbol = tick.symbol.clone();
                            let key = (tick.exchange, tick.symbol.clone());
                            self.latest_ticks.insert(key, tick.clone());

                            // Update volatility tracker
                            let tracker = self.volatility_trackers
                                .entry(symbol.clone())
                                .or_insert_with(|| VolatilityTracker::new(volatility_window_hours));
                            tracker.add_price(tick.mid().to_string().parse::<f64>().unwrap_or(0.0), tick.timestamp);

                            // Compute aggregated price
                            if let Some(agg) = self.aggregate(&symbol) {
                                debug!(symbol = %agg.symbol, vwap = %agg.vwap, spread = %agg.spread, vol = agg.volatility, "aggregated");
                                // Forward to ML bridge if configured
                                if let Some(ref ml) = ml_tx {
                                    let _ = ml.send(AggregatorEvent::PriceUpdate(agg.clone())).await;
                                }
                                let _ = tx.send(AggregatorEvent::PriceUpdate(agg)).await;
                            }

                            // Build candles
                            let completed_candles = self.candle_builder.on_tick(&tick);
                            for candle in completed_candles {
                                debug!(symbol = %candle.symbol, timeframe = %candle.timeframe, close = %candle.close, "candle complete");
                                let event = AggregatorEvent::CandleComplete(candle);
                                // Forward to ML bridge if configured
                                if let Some(ref ml) = ml_tx {
                                    let _ = ml.send(event.clone()).await;
                                }
                                let _ = tx.send(event).await;
                            }
                        }
                        ExchangeEvent::Connected(ex) => {
                            info!(exchange = %ex, "feed connected");
                        }
                        ExchangeEvent::Disconnected(ex) => {
                            warn!(exchange = %ex, "feed disconnected");
                        }
                    }
                }
                _ = stale_check_interval.tick() => {
                    self.check_stale_feeds(&tx).await;
                }
            }
        }
    }

    fn aggregate(&self, symbol: &str) -> Option<AggregatedPrice> {
        let ticks: Vec<&MarketTick> = self
            .latest_ticks
            .values()
            .filter(|t| t.symbol == symbol)
            .collect();

        if ticks.is_empty() {
            return None;
        }

        // Separate Chainlink ticks from exchange ticks
        let exchange_ticks: Vec<&&MarketTick> = ticks
            .iter()
            .filter(|t| t.exchange != Exchange::Chainlink)
            .collect();
        let chainlink_tick = ticks.iter().find(|t| t.exchange == Exchange::Chainlink);

        let oracle_price = chainlink_tick.map(|t| t.mid());

        // Exchange VWAP (excluding Chainlink — it has zero volume)
        // Binance volume is multiplied by binance_weight_multiplier so its price
        // dominates the aggregate. This lets the bot see BTC moves 20-40s before
        // Polymarket reprices, opening a structural latency window.
        let exchange_vwap = if exchange_ticks.is_empty() {
            // Only Chainlink available — use its price
            oracle_price.unwrap_or(Decimal::ZERO)
        } else {
            let weighted_volume = |t: &MarketTick| -> Decimal {
                if t.exchange == Exchange::Binance {
                    t.volume_24h * self.binance_weight_multiplier
                } else {
                    t.volume_24h
                }
            };
            let total_volume: Decimal = exchange_ticks.iter().map(|t| weighted_volume(t)).sum();
            if total_volume > Decimal::ZERO {
                exchange_ticks
                    .iter()
                    .map(|t| t.mid() * weighted_volume(t))
                    .sum::<Decimal>()
                    / total_volume
            } else {
                let sum: Decimal = exchange_ticks.iter().map(|t| t.mid()).sum();
                sum / Decimal::from(exchange_ticks.len())
            }
        };

        // Final VWAP: blend Chainlink oracle with configurable weight.
        // Default 0.1 = 10% oracle, 90% exchange VWAP. Keeps the bot responsive
        // to fast exchange price moves rather than lagging behind on-chain oracle.
        let vwap = match oracle_price {
            Some(op) if !exchange_ticks.is_empty() => {
                let cw = self.chainlink_blend_weight;
                exchange_vwap * (Decimal::ONE - cw) + op * cw
            }
            _ => exchange_vwap,
        };

        // Best bid/ask across all feeds (including Chainlink)
        let best_bid_tick = ticks.iter().max_by(|a, b| a.bid.cmp(&b.bid)).unwrap();
        let best_ask_tick = ticks.iter().min_by(|a, b| a.ask.cmp(&b.ask)).unwrap();

        let volatility = self
            .volatility_trackers
            .get(symbol)
            .map(|t| t.annualized_volatility())
            .unwrap_or(0.0);

        // Use the most recent tick timestamp (works for both live and backtest)
        let timestamp = ticks
            .iter()
            .map(|t| t.timestamp)
            .max()
            .unwrap_or_else(Utc::now);

        Some(AggregatedPrice {
            symbol: symbol.to_string(),
            vwap,
            best_bid: best_bid_tick.bid,
            best_ask: best_ask_tick.ask,
            best_bid_exchange: best_bid_tick.exchange,
            best_ask_exchange: best_ask_tick.exchange,
            spread: best_ask_tick.ask - best_bid_tick.bid,
            volatility,
            num_feeds: ticks.len(),
            timestamp,
            oracle_price,
        })
    }

    async fn check_stale_feeds(&self, tx: &mpsc::Sender<AggregatorEvent>) {
        let now = Utc::now();
        let stale_threshold = chrono::Duration::seconds(self.stale_timeout_secs as i64);

        for ((exchange, symbol), tick) in &self.latest_ticks {
            if now - tick.timestamp > stale_threshold {
                warn!(exchange = %exchange, symbol = %symbol, "stale feed detected");
                let _ = tx
                    .send(AggregatorEvent::StaleFeed {
                        exchange: *exchange,
                        symbol: symbol.clone(),
                        last_seen: tick.timestamp,
                    })
                    .await;
            }
        }
    }
}

/// Compute VWAP from a slice of ticks (utility for backtest).
pub fn compute_vwap(ticks: &[MarketTick]) -> Decimal {
    let total_vol: Decimal = ticks.iter().map(|t| t.volume_24h).sum();
    if total_vol == Decimal::ZERO {
        let sum: Decimal = ticks.iter().map(|t| t.mid()).sum();
        return if ticks.is_empty() {
            Decimal::ZERO
        } else {
            sum / Decimal::from(ticks.len())
        };
    }
    ticks
        .iter()
        .map(|t| t.mid() * t.volume_24h)
        .sum::<Decimal>()
        / total_vol
}

pub fn best_bid(ticks: &[MarketTick]) -> Option<(Decimal, Exchange)> {
    ticks
        .iter()
        .max_by(|a, b| a.bid.cmp(&b.bid))
        .map(|t| (t.bid, t.exchange))
}

pub fn best_ask(ticks: &[MarketTick]) -> Option<(Decimal, Exchange)> {
    ticks
        .iter()
        .min_by(|a, b| a.ask.cmp(&b.ask))
        .map(|t| (t.ask, t.exchange))
}

/// Calculate last seen time for a feed.
pub fn last_seen(ticks: &[MarketTick], exchange: Exchange) -> Option<DateTime<Utc>> {
    ticks
        .iter()
        .filter(|t| t.exchange == exchange)
        .map(|t| t.timestamp)
        .max()
}
