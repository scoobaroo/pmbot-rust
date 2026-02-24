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
}

impl Aggregator {
    pub fn new(
        stale_timeout_secs: u64,
        volatility_window_hours: u64,
        candle_timeframes: Vec<Timeframe>,
    ) -> Self {
        let _ = volatility_window_hours; // stored per-tracker on creation
        Self {
            latest_ticks: HashMap::new(),
            volatility_trackers: HashMap::new(),
            candle_builder: CandleBuilder::new(candle_timeframes),
            stale_timeout_secs,
        }
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<ExchangeEvent>,
        tx: mpsc::Sender<AggregatorEvent>,
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
                                let _ = tx.send(AggregatorEvent::PriceUpdate(agg)).await;
                            }

                            // Build candles
                            let completed_candles = self.candle_builder.on_tick(&tick);
                            for candle in completed_candles {
                                debug!(symbol = %candle.symbol, timeframe = %candle.timeframe, close = %candle.close, "candle complete");
                                let _ = tx.send(AggregatorEvent::CandleComplete(candle)).await;
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

        // VWAP: sum(price * volume) / sum(volume)
        let total_volume: Decimal = ticks.iter().map(|t| t.volume_24h).sum();
        let vwap = if total_volume > Decimal::ZERO {
            ticks
                .iter()
                .map(|t| t.mid() * t.volume_24h)
                .sum::<Decimal>()
                / total_volume
        } else {
            // Fallback: simple average of mids
            let sum: Decimal = ticks.iter().map(|t| t.mid()).sum();
            sum / Decimal::from(ticks.len())
        };

        // Best bid/ask across exchanges
        let best_bid_tick = ticks.iter().max_by(|a, b| a.bid.cmp(&b.bid)).unwrap();
        let best_ask_tick = ticks.iter().min_by(|a, b| a.ask.cmp(&b.ask)).unwrap();

        let volatility = self
            .volatility_trackers
            .get(symbol)
            .map(|t| t.annualized_volatility())
            .unwrap_or(0.0);

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
            timestamp: Utc::now(),
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
