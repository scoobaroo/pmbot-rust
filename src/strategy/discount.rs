use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::kelly;
use crate::strategy::probability;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketDirection, MarketType, PolymarketMarket};
use crate::types::order::Side;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Default half-spread when no live book data is available.
const DEFAULT_HALF_SPREAD: Decimal = dec!(0.01);

/// Time-value discount strategy for Polymarket binary contracts.
///
/// Fair value: V = A / (1 + k × D)
///
/// where:
///   A = estimated probability of outcome (expected $1 payout)
///   k = discount rate (annualized)
///   D = time to resolution in years
///
/// Simpler than Black-Scholes for short-duration binary contracts.
/// No continuous hedging assumption. No lognormal return assumption.
/// Hold until resolution — no stop loss, no early exit.
pub struct DiscountStrategy {
    markets: HashMap<String, PolymarketMarket>,
    latest_prices: HashMap<String, AggregatedPrice>,
    discount_rate: f64,
    min_edge_threshold: f64,
    kelly_fraction_cap: f64,
    max_total_exposure_usd: Decimal,
    max_position_usd: Decimal,
    fee_calculator: FeeCalculator,
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,

    // UpDown: captured VWAP at window start, keyed by condition_id → (start_price, window_start_ts)
    updown_start_prices: HashMap<String, (f64, i64)>,
}

impl DiscountStrategy {
    pub fn new(config: &Config) -> Self {
        Self {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            discount_rate: config.discount_rate,
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
            updown_start_prices: HashMap::new(),
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    /// Current time derived from latest price timestamps (backtest-safe).
    fn current_time(&self) -> DateTime<Utc> {
        self.latest_prices
            .values()
            .map(|p| p.timestamp)
            .max()
            .unwrap_or_else(Utc::now)
    }

    /// Look up the live half-spread for a token from the book cache.
    fn get_half_spread(&self, token_id: &str) -> Decimal {
        if let Some(ref cache) = self.book_cache {
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

    /// V = A / (1 + k × D)
    ///
    /// A = estimated probability, k = discount rate, D = time in years.
    fn discounted_value(&self, prob: f64, time_years: f64) -> f64 {
        prob / (1.0 + self.discount_rate * time_years)
    }

    fn evaluate_all_markets(&self) -> Vec<TradeSignal> {
        let mut signals = Vec::new();
        for market in self.markets.values() {
            let signal = match market.market_type {
                MarketType::StrikeAbove => self.evaluate_strike_market(market),
                MarketType::UpDown { .. } => self.evaluate_updown_market(market),
                MarketType::General { .. } => None, // Discount strategy doesn't handle general markets
            };
            if let Some(s) = signal {
                signals.push(s);
            }
        }
        signals
    }

    fn evaluate_strike_market(&self, market: &PolymarketMarket) -> Option<TradeSignal> {
        let price = self.latest_prices.get(&market.underlying_symbol)?;
        let spot = price.vwap.to_f64()?;
        let strike = market.strike.to_f64()?;
        let volatility = price.volatility;
        let now = self.current_time();
        let time_years = probability::time_to_expiry_years(market.expiry, now);

        if volatility <= 0.0 || time_years <= 0.0 {
            return None;
        }

        let prob_above = probability::prob_above_strike(spot, strike, time_years, volatility, 0.0);
        let estimated_prob = match market.direction {
            MarketDirection::Bullish => prob_above,
            MarketDirection::Bearish => 1.0 - prob_above,
        };

        self.make_signal(market, estimated_prob, time_years)
    }

    fn evaluate_updown_market(&self, market: &PolymarketMarket) -> Option<TradeSignal> {
        let (window_start_ts, window_secs) = match market.market_type {
            MarketType::UpDown {
                window_start_ts,
                window_secs,
            } => (window_start_ts, window_secs),
            _ => return None,
        };

        let price = self.latest_prices.get(&market.underlying_symbol)?;
        let spot = price.vwap.to_f64()?;
        let volatility = price.volatility;

        let (start_price, _) = self.updown_start_prices.get(&market.condition_id)?;
        let start_price = *start_price;

        let now = self.current_time().timestamp();
        let window_end = window_start_ts + window_secs as i64;
        let time_remaining_secs = (window_end - now).max(0) as f64;

        if time_remaining_secs <= 0.0 || volatility <= 0.0 {
            return None;
        }

        let time_years = time_remaining_secs / (365.25 * 24.0 * 3600.0);
        let prob_up = probability::prob_price_up(spot, start_price, time_remaining_secs, volatility);

        // UpDown: Bullish = Up, Bearish = Down
        let estimated_prob = match market.direction {
            MarketDirection::Bullish => prob_up,
            MarketDirection::Bearish => 1.0 - prob_up,
        };

        self.make_signal(market, estimated_prob, time_years)
    }

    /// Shared signal generation: discount probability, compute edge, size with Kelly.
    fn make_signal(
        &self,
        market: &PolymarketMarket,
        estimated_prob: f64,
        time_years: f64,
    ) -> Option<TradeSignal> {
        // Core formula: V = A / (1 + k × D)
        let fair_value = self.discounted_value(estimated_prob, time_years);

        let implied_prob = market.implied_prob_yes.to_f64().unwrap_or(0.5);
        let raw_edge = fair_value - implied_prob;

        // Determine trade side and look up spread
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

        // Deduct trading costs
        let total_cost = self.fee_calculator.total_cost(implied_price, half_spread);
        let total_cost_f64 = total_cost.to_f64().unwrap_or(0.0);
        let net_edge = raw_edge.abs() - total_cost_f64;

        if net_edge < self.min_edge_threshold {
            return None;
        }

        let edge = if raw_edge > 0.0 { net_edge } else { -net_edge };

        // Kelly sizing using fair_value as our probability estimate
        let (side, kelly_frac) = if edge > 0.0 {
            let f = kelly::kelly_fraction(fair_value, implied_prob, self.kelly_fraction_cap);
            (Side::Buy, f)
        } else {
            let f = kelly::kelly_fraction(
                1.0 - fair_value,
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

        let signal_price = match side {
            Side::Buy => market.implied_prob_yes,
            Side::Sell => market.implied_prob_no,
        };

        info!(
            question = %market.question,
            estimated_prob = format!("{:.1}%", estimated_prob * 100.0),
            fair_value = format!("{:.1}%", fair_value * 100.0),
            implied_prob = format!("{:.1}%", implied_prob * 100.0),
            discount_rate = self.discount_rate,
            time_years = format!("{:.6}", time_years),
            net_edge = format!("{:.1}%", net_edge * 100.0),
            kelly = format!("{:.1}%", kelly_frac * 100.0),
            size_usd = %size_usd,
            side = %side,
            "discount strategy opportunity"
        );

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd,
            confidence: net_edge,
            price: signal_price,
            metadata: SignalMetadata::Discount {
                estimated_prob,
                fair_value,
                implied_prob,
                discount_rate: self.discount_rate,
                time_to_resolution_secs: time_years * 365.25 * 24.0 * 3600.0,
                edge: net_edge,
                kelly_fraction: kelly_frac,
            },
            timestamp: self.current_time(),
            is_exit: false,
        })
    }

    /// Garbage-collect expired markets and their start prices.
    fn gc_expired_markets(&mut self) {
        let now = self.current_time();
        let expired: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, m)| m.expiry < now)
            .map(|(cid, _)| cid.clone())
            .collect();

        for cid in &expired {
            self.markets.remove(cid);
            self.updown_start_prices.remove(cid);
        }

        if !expired.is_empty() {
            debug!(count = expired.len(), "garbage-collected expired markets");
        }
    }
}

impl Strategy for DiscountStrategy {
    fn name(&self) -> &str {
        "discount"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: true,
            candles: false,
            execution_feedback: true,
            polymarket_updates: true,
            ml_predictions: false,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::PriceUpdate(price) => {
                self.latest_prices.insert(price.symbol.clone(), price);
                self.gc_expired_markets();
                self.evaluate_all_markets()
            }
            StrategyEvent::PolymarketUpdate(update) => {
                match update {
                    PolymarketUpdate::MarketsDiscovered(new_markets) => {
                        info!(count = new_markets.len(), "markets discovered");
                        for market in new_markets {
                            // Capture UpDown start price from VWAP at discovery
                            if let MarketType::UpDown {
                                window_start_ts, ..
                            } = market.market_type
                            {
                                if !self
                                    .updown_start_prices
                                    .contains_key(&market.condition_id)
                                {
                                    if let Some(price) =
                                        self.latest_prices.get(&market.underlying_symbol)
                                    {
                                        let start = price
                                            .oracle_price
                                            .and_then(|d| d.to_f64())
                                            .unwrap_or_else(|| {
                                                price.vwap.to_f64().unwrap_or(0.0)
                                            });
                                        if start > 0.0 {
                                            let source = if price.oracle_price.is_some() {
                                                "oracle"
                                            } else {
                                                "vwap"
                                            };
                                            self.updown_start_prices.insert(
                                                market.condition_id.clone(),
                                                (start, window_start_ts),
                                            );
                                            info!(
                                                symbol = %market.underlying_symbol,
                                                start_price = start,
                                                source,
                                                "captured updown start price"
                                            );
                                        }
                                    }
                                }
                            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discounted_value_no_time() {
        // D = 0 → V = A (no discount)
        let strategy = make_test_strategy(0.1);
        let v = strategy.discounted_value(0.6, 0.0);
        assert!((v - 0.6).abs() < 1e-10);
    }

    #[test]
    fn test_discounted_value_with_time() {
        // V = 0.6 / (1 + 0.1 * 1.0) = 0.6 / 1.1 ≈ 0.5454
        let strategy = make_test_strategy(0.1);
        let v = strategy.discounted_value(0.6, 1.0);
        assert!((v - 0.6 / 1.1).abs() < 1e-10);
    }

    #[test]
    fn test_discounted_value_short_duration() {
        // 5 minutes ≈ 9.51e-6 years
        // V = 0.6 / (1 + 0.1 * 9.51e-6) ≈ 0.6 (negligible discount)
        let strategy = make_test_strategy(0.1);
        let time_years = 300.0 / (365.25 * 24.0 * 3600.0);
        let v = strategy.discounted_value(0.6, time_years);
        assert!(
            (v - 0.6).abs() < 0.001,
            "5min discount should be negligible at k=0.1, got {}",
            v
        );
    }

    #[test]
    fn test_discounted_value_high_rate() {
        // High discount rate matters even for short durations
        let strategy = make_test_strategy(100.0);
        let time_years = 300.0 / (365.25 * 24.0 * 3600.0);
        let v = strategy.discounted_value(0.6, time_years);
        assert!(v < 0.6, "high k should discount noticeably, got {}", v);
    }

    #[test]
    fn test_discount_always_reduces_value() {
        let strategy = make_test_strategy(0.5);
        for prob in [0.3, 0.5, 0.7, 0.9] {
            for time in [0.001, 0.01, 0.1, 1.0] {
                let v = strategy.discounted_value(prob, time);
                assert!(
                    v < prob,
                    "discount must reduce value: prob={}, time={}, v={}",
                    prob,
                    time,
                    v
                );
            }
        }
    }

    fn make_test_strategy(discount_rate: f64) -> DiscountStrategy {
        DiscountStrategy {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            discount_rate,
            min_edge_threshold: 0.03,
            kelly_fraction_cap: 0.5,
            max_total_exposure_usd: dec!(5000),
            max_position_usd: dec!(1000),
            fee_calculator: FeeCalculator::new(156),
            book_cache: None,
            ws_book_max_stale_secs: 30,
            updown_start_prices: HashMap::new(),
        }
    }
}
