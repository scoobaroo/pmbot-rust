use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::kelly;
use crate::strategy::probability;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketDirection, PolymarketMarket};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Default half-spread estimate when no live book data is available.
const DEFAULT_HALF_SPREAD: Decimal = dec!(0.01);

pub struct BlackScholesStrategy {
    markets: HashMap<String, PolymarketMarket>,
    latest_prices: HashMap<String, AggregatedPrice>,
    min_edge_threshold: f64,
    kelly_fraction_cap: f64,
    max_total_exposure_usd: Decimal,
    max_position_usd: Decimal,
    fee_calculator: FeeCalculator,
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,
}

impl BlackScholesStrategy {
    pub fn new(config: &Config) -> Self {
        Self {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            min_edge_threshold: config
                .min_edge_threshold
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.03),
            kelly_fraction_cap: config
                .kelly_fraction_cap
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.5),
            max_total_exposure_usd: config.max_total_exposure_usd,
            max_position_usd: config.max_position_usd,
            fee_calculator: FeeCalculator::new(config.fee_rate_bps),
            book_cache: None,
            ws_book_max_stale_secs: config.ws_book_max_stale_secs,
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    fn evaluate_all_markets(&self) -> Vec<TradeSignal> {
        let mut signals = Vec::new();
        for market in self.markets.values() {
            if let Some(signal) = self.evaluate_market(market) {
                signals.push(signal);
            }
        }
        signals
    }

    /// Look up the live half-spread for a token from the book cache.
    /// Falls back to DEFAULT_HALF_SPREAD if no fresh data.
    fn get_half_spread(&self, token_id: &str) -> Decimal {
        if let Some(ref cache) = self.book_cache {
            // try_read to avoid blocking the strategy event loop
            if let Ok(books) = cache.try_read() {
                if let Some(snap) = books.get(token_id) {
                    if snap.is_fresh(self.ws_book_max_stale_secs) {
                        return snap.half_spread();
                    }
                    debug!(
                        token_id = token_id,
                        "book snapshot stale, using default spread"
                    );
                }
            }
        }
        DEFAULT_HALF_SPREAD
    }

    fn evaluate_market(&self, market: &PolymarketMarket) -> Option<TradeSignal> {
        let price = self.latest_prices.get(&market.underlying_symbol)?;

        let spot = price.vwap.to_string().parse::<f64>().ok()?;
        let strike = market.strike.to_string().parse::<f64>().ok()?;
        let time_to_expiry = probability::time_to_expiry_years(market.expiry);
        let volatility = price.volatility;

        if volatility <= 0.0 || time_to_expiry <= 0.0 {
            return None;
        }

        // Direction-aware probability estimation
        let prob_above =
            probability::prob_above_strike(spot, strike, time_to_expiry, volatility, 0.0);
        let estimated_prob = match market.direction {
            MarketDirection::Bullish => prob_above,
            MarketDirection::Bearish => 1.0 - prob_above,
        };

        let implied_prob = market
            .implied_prob_yes
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.5);

        let raw_edge = kelly::edge(estimated_prob, implied_prob);

        // Determine which token we'd trade and look up its spread
        let token_id = if raw_edge > 0.0 {
            &market.token_id_yes
        } else {
            &market.token_id_no
        };
        let half_spread = self.get_half_spread(token_id);
        let implied_price = if raw_edge > 0.0 {
            market.implied_prob_yes
        } else {
            market.implied_prob_no
        };

        // Deduct trading costs from edge
        let total_cost = self.fee_calculator.total_cost(implied_price, half_spread);
        let total_cost_f64 = total_cost.to_f64().unwrap_or(0.0);
        let net_edge = raw_edge.abs() - total_cost_f64;

        if net_edge < self.min_edge_threshold {
            info!(
                question = %market.question,
                spot = spot,
                strike = strike,
                direction = ?market.direction,
                estimated_prob = format!("{:.1}%", estimated_prob * 100.0),
                implied_prob = format!("{:.1}%", implied_prob * 100.0),
                net_edge = format!("{:.1}%", net_edge * 100.0),
                threshold = format!("{:.1}%", self.min_edge_threshold * 100.0),
                "below edge threshold"
            );
            return None;
        }

        // Use raw_edge sign for direction, but net_edge magnitude
        let edge = if raw_edge > 0.0 { net_edge } else { -net_edge };

        let (side, kelly_frac) = if edge > 0.0 {
            let f = kelly::kelly_fraction(estimated_prob, implied_prob, self.kelly_fraction_cap);
            (Side::Buy, f)
        } else {
            let f = kelly::kelly_fraction(
                1.0 - estimated_prob,
                1.0 - implied_prob,
                self.kelly_fraction_cap,
            );
            (Side::Sell, f)
        };

        if kelly_frac <= 0.0 {
            return None;
        }

        let size_usd = kelly::position_size_usd(
            kelly_frac,
            self.max_total_exposure_usd,
            self.max_position_usd,
        );

        if size_usd <= Decimal::ZERO {
            return None;
        }

        info!(
            question = %market.question,
            spot = spot,
            strike = strike,
            direction = ?market.direction,
            estimated_prob = format!("{:.1}%", estimated_prob * 100.0),
            implied_prob = format!("{:.1}%", implied_prob * 100.0),
            edge = format!("{:.1}%", net_edge * 100.0),
            kelly = format!("{:.1}%", kelly_frac * 100.0),
            size_usd = %size_usd,
            side = %side,
            "opportunity detected"
        );

        let signal_price = match side {
            Side::Buy => market.implied_prob_yes,
            Side::Sell => market.implied_prob_no,
        };

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd,
            confidence: net_edge,
            price: signal_price,
            metadata: SignalMetadata::BlackScholes {
                estimated_prob,
                implied_prob,
                edge: net_edge,
                kelly_fraction: kelly_frac,
            },
            timestamp: Utc::now(),
        })
    }
}

impl Strategy for BlackScholesStrategy {
    fn name(&self) -> &str {
        "black-scholes"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: true,
            candles: false,
            execution_feedback: true,
            polymarket_updates: true,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::PriceUpdate(price) => {
                self.latest_prices.insert(price.symbol.clone(), price);
                self.evaluate_all_markets()
            }
            StrategyEvent::PolymarketUpdate(update) => {
                match update {
                    PolymarketUpdate::MarketsDiscovered(new_markets) => {
                        info!(count = new_markets.len(), "markets discovered");
                        for market in new_markets {
                            self.markets.insert(market.condition_id.clone(), market);
                        }
                    }
                    PolymarketUpdate::PriceUpdate {
                        condition_id,
                        yes_price,
                        no_price,
                    } => {
                        if let Some(market) = self.markets.get_mut(&condition_id) {
                            market.implied_prob_yes = yes_price;
                            market.implied_prob_no = no_price;
                        }
                    }
                }
                Vec::new()
            }
            StrategyEvent::ExecutionFeedback(event) => {
                match &event {
                    crate::types::events::ExecutionEvent::OrderFilled(fill) => {
                        info!(order_id = %fill.order_id, size = %fill.size, price = %fill.price, "order filled");
                    }
                    crate::types::events::ExecutionEvent::OrderFailed { order_id, error } => {
                        warn!(order_id = %order_id, error = %error, "order failed");
                    }
                    _ => {}
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }
}
