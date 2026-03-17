use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketType, PolymarketMarket};
use crate::types::order::Side;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// Default half-spread when no live book data is available.
const DEFAULT_HALF_SPREAD: Decimal = dec!(0.01);

/// Seconds to wait after window start before evaluating (opening range formation).
const DEFAULT_ENTRY_DELAY_SECS: i64 = 60;

/// Minimum seconds before window end to allow entry.
const MIN_TIME_REMAINING_SECS: i64 = 30;

/// Opening Range Breakout strategy for Polymarket 5-minute BTC UpDown markets.
///
/// Based on backtested edge: the first 60 seconds of BTC price action in a
/// 5-minute window predicts the resolution with high accuracy. Larger moves
/// correlate with higher accuracy:
///
///   $10–25 move  → 68% accuracy
///   $25–50 move  → 72% accuracy
///   $50–100 move → 76% accuracy
///   $100+ move   → 99% accuracy
///
/// Strategy: wait 60s, measure |spot - strike|, buy the winning side if
/// empirical accuracy > implied probability + fees. Hold to resolution.
pub struct OrbStrategy {
    markets: HashMap<String, PolymarketMarket>,
    latest_prices: HashMap<String, AggregatedPrice>,
    /// Captured strike price and window start timestamp per condition_id.
    updown_start_prices: HashMap<String, (f64, i64)>,
    /// Markets we've already entered — one entry per market, hold to resolution.
    entered_markets: HashSet<String>,
    fee_calculator: FeeCalculator,
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,
    max_position_usd: Decimal,
    entry_delay_secs: i64,
}

impl OrbStrategy {
    pub fn new(config: &Config) -> Self {
        Self {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            updown_start_prices: HashMap::new(),
            entered_markets: HashSet::new(),
            fee_calculator: FeeCalculator::new(config.fee_rate_bps),
            book_cache: None,
            ws_book_max_stale_secs: config.ws_book_max_stale_secs,
            max_position_usd: config.max_position_usd,
            entry_delay_secs: DEFAULT_ENTRY_DELAY_SECS,
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    /// Backtest-safe current time from latest price timestamps.
    fn current_time(&self) -> DateTime<Utc> {
        self.latest_prices
            .values()
            .map(|p| p.timestamp)
            .max()
            .unwrap_or_else(Utc::now)
    }

    /// Look up live half-spread from book cache.
    fn get_half_spread(&self, token_id: &str) -> Decimal {
        if let Some(ref cache) = self.book_cache {
            if let Ok(books) = cache.try_read() {
                if let Some(snap) = books.get(token_id) {
                    if snap.is_fresh(self.ws_book_max_stale_secs) {
                        return snap.half_spread();
                    }
                }
            }
        }
        DEFAULT_HALF_SPREAD
    }

    /// Empirical accuracy from backtested ORB tiers.
    /// Returns None if the move is too small to have edge after fees.
    fn empirical_accuracy(btc_move_abs: f64) -> Option<f64> {
        if btc_move_abs >= 100.0 {
            Some(0.99)
        } else if btc_move_abs >= 50.0 {
            Some(0.76)
        } else if btc_move_abs >= 25.0 {
            Some(0.72)
        } else if btc_move_abs >= 10.0 {
            Some(0.68)
        } else {
            None
        }
    }

    fn evaluate_all_markets(&self) -> Vec<TradeSignal> {
        let mut signals = Vec::new();
        for market in self.markets.values() {
            if let Some(s) = self.evaluate_updown_market(market) {
                signals.push(s);
            }
        }
        signals
    }

    fn evaluate_updown_market(&self, market: &PolymarketMarket) -> Option<TradeSignal> {
        let (window_start_ts, window_secs) = match market.market_type {
            MarketType::UpDown {
                window_start_ts,
                window_secs,
            } => (window_start_ts, window_secs),
            _ => return None,
        };

        // One entry per market
        if self.entered_markets.contains(&market.condition_id) {
            return None;
        }

        let price = self.latest_prices.get(&market.underlying_symbol)?;
        let spot = price
            .oracle_price
            .and_then(|d| d.to_f64())
            .unwrap_or_else(|| price.vwap.to_f64().unwrap_or(0.0));

        if spot <= 0.0 {
            return None;
        }

        let (start_price, _) = self.updown_start_prices.get(&market.condition_id)?;
        let start_price = *start_price;

        let now = self.current_time().timestamp();
        let elapsed = now - window_start_ts;
        let window_end = window_start_ts + window_secs as i64;
        let time_remaining_secs = (window_end - now).max(0) as f64;

        // Wait for opening range to form
        if elapsed < self.entry_delay_secs {
            return None;
        }

        // Too close to expiry
        if time_remaining_secs < MIN_TIME_REMAINING_SECS as f64 {
            return None;
        }

        // Measure the breakout magnitude
        let btc_move = spot - start_price;
        let btc_move_abs = btc_move.abs();

        let accuracy = Self::empirical_accuracy(btc_move_abs)?;

        // Direction: if BTC moved up → buy Up token, if down → buy Down token
        let (side, token_id, implied_price) = if btc_move > 0.0 {
            // BTC moved up → buy YES (Up token)
            (Side::Buy, &market.token_id_yes, market.implied_prob_yes)
        } else {
            // BTC moved down → buy NO (Down token)
            (Side::Sell, &market.token_id_no, market.implied_prob_no)
        };

        let implied_prob = implied_price.to_f64().unwrap_or(0.5);

        // Deduct trading costs
        let half_spread = self.get_half_spread(token_id);
        let total_cost = self.fee_calculator.total_cost(implied_price, half_spread);
        let total_cost_f64 = total_cost.to_f64().unwrap_or(0.0);

        let edge = accuracy - implied_prob - total_cost_f64;

        if edge <= 0.0 {
            debug!(
                question = %market.question,
                btc_move = format!("{:+.1}", btc_move),
                accuracy = format!("{:.0}%", accuracy * 100.0),
                implied = format!("{:.1}%", implied_prob * 100.0),
                cost = format!("{:.1}%", total_cost_f64 * 100.0),
                edge = format!("{:.1}%", edge * 100.0),
                "ORB: no edge after fees"
            );
            return None;
        }

        info!(
            question = %market.question,
            btc_move = format!("{:+.1}", btc_move),
            accuracy = format!("{:.0}%", accuracy * 100.0),
            implied = format!("{:.1}%", implied_prob * 100.0),
            edge = format!("{:.1}%", edge * 100.0),
            side = %side,
            size_usd = %self.max_position_usd,
            elapsed_secs = elapsed,
            "ORB: breakout signal"
        );

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd: self.max_position_usd,
            confidence: edge,
            price: implied_price,
            metadata: SignalMetadata::Orb {
                btc_move,
                accuracy_tier: accuracy,
                implied_prob,
                edge,
                start_price,
                spot,
                elapsed_secs: elapsed as f64,
                time_remaining_secs,
            },
            timestamp: self.current_time(),
            is_exit: false,
        })
    }

    /// Garbage-collect expired markets.
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
            self.entered_markets.remove(cid);
        }

        if !expired.is_empty() {
            debug!(count = expired.len(), "ORB: garbage-collected expired markets");
        }
    }
}

impl Strategy for OrbStrategy {
    fn name(&self) -> &str {
        "orb"
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
                let signals = self.evaluate_all_markets();
                // Mark entered markets
                for s in &signals {
                    if let TradeTarget::Polymarket(ref m) = s.target {
                        self.entered_markets.insert(m.condition_id.clone());
                    }
                }
                signals
            }
            StrategyEvent::PolymarketUpdate(update) => {
                match update {
                    PolymarketUpdate::MarketsDiscovered(new_markets) => {
                        for market in new_markets {
                            if let MarketType::UpDown {
                                window_start_ts, ..
                            } = market.market_type
                            {
                                if !self
                                    .updown_start_prices
                                    .contains_key(&market.condition_id)
                                {
                                    // Use strike price from market (this is the window start price)
                                    let start = market.strike.to_f64().unwrap_or(0.0);
                                    if start > 0.0 {
                                        self.updown_start_prices.insert(
                                            market.condition_id.clone(),
                                            (start, window_start_ts),
                                        );
                                        info!(
                                            condition_id = %market.condition_id,
                                            strike = start,
                                            "ORB: captured strike for new UpDown market"
                                        );
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
                        info!(
                            order_id = %fill.order_id,
                            price = %fill.price,
                            size = %fill.size,
                            "ORB: order filled"
                        );
                    }
                    crate::types::events::ExecutionEvent::OrderFailed { order_id, error } => {
                        warn!(order_id = %order_id, error = %error, "ORB: order failed");
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
    use crate::types::market::MarketDirection;
    use chrono::Duration;

    fn make_test_strategy() -> OrbStrategy {
        OrbStrategy {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            updown_start_prices: HashMap::new(),
            entered_markets: HashSet::new(),
            fee_calculator: FeeCalculator::new(156), // 1.56%
            book_cache: None,
            ws_book_max_stale_secs: 30,
            max_position_usd: dec!(100),
            entry_delay_secs: 60,
        }
    }

    fn make_updown_market(
        condition_id: &str,
        strike: f64,
        window_start_ts: i64,
        implied_yes: f64,
    ) -> PolymarketMarket {
        PolymarketMarket {
            condition_id: condition_id.to_string(),
            token_id_yes: format!("{}-yes", condition_id),
            token_id_no: format!("{}-no", condition_id),
            question: "Will BTC go up?".to_string(),
            underlying_symbol: "BTC-USD".to_string(),
            strike: Decimal::from_f64_retain(strike).unwrap(),
            expiry: Utc::now() + Duration::minutes(10),
            implied_prob_yes: Decimal::from_f64_retain(implied_yes).unwrap(),
            implied_prob_no: Decimal::from_f64_retain(1.0 - implied_yes).unwrap(),
            direction: MarketDirection::Bullish,
            market_type: MarketType::UpDown {
                window_start_ts,
                window_secs: 300,
            },
        }
    }

    fn make_price(symbol: &str, vwap: f64, ts: DateTime<Utc>) -> AggregatedPrice {
        use crate::types::market::Exchange;
        AggregatedPrice {
            symbol: symbol.to_string(),
            vwap: Decimal::from_f64_retain(vwap).unwrap(),
            best_bid: Decimal::from_f64_retain(vwap - 1.0).unwrap(),
            best_ask: Decimal::from_f64_retain(vwap + 1.0).unwrap(),
            best_bid_exchange: Exchange::Binance,
            best_ask_exchange: Exchange::Binance,
            spread: dec!(2.0),
            volatility: 0.5,
            num_feeds: 1,
            oracle_price: Some(Decimal::from_f64_retain(vwap).unwrap()),
            trade_flow_imbalance: 0.0,
            recent_buy_volume: 0.0,
            recent_sell_volume: 0.0,
            funding_rate: None,
            mark_price: None,
            timestamp: ts,
        }
    }

    #[test]
    fn test_accuracy_tiers() {
        assert_eq!(OrbStrategy::empirical_accuracy(5.0), None);
        assert_eq!(OrbStrategy::empirical_accuracy(10.0), Some(0.68));
        assert_eq!(OrbStrategy::empirical_accuracy(15.0), Some(0.68));
        assert_eq!(OrbStrategy::empirical_accuracy(25.0), Some(0.72));
        assert_eq!(OrbStrategy::empirical_accuracy(50.0), Some(0.76));
        assert_eq!(OrbStrategy::empirical_accuracy(100.0), Some(0.99));
        assert_eq!(OrbStrategy::empirical_accuracy(200.0), Some(0.99));
    }

    #[test]
    fn test_no_signal_before_entry_delay() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 30; // only 30s elapsed

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80050.0, now));

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should not signal before 60s");
    }

    #[test]
    fn test_no_signal_small_move() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 70; // 70s elapsed

        let market = make_updown_market("m1", 80000.0, window_start, 0.52);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Only $5 move — below $10 threshold
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80005.0, now));

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should not signal on <$10 move");
    }

    #[test]
    fn test_signal_on_breakout_up() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 70;

        // $30 move up, market pricing Up at 0.60 (below 72% accuracy)
        let market = make_updown_market("m1", 80000.0, window_start, 0.60);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80030.0, now));

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].side, Side::Buy); // Buy Up token
    }

    #[test]
    fn test_signal_on_breakout_down() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 70;

        // $30 move down, market pricing Down (No) at 0.60 (below 72% accuracy)
        let market = make_updown_market("m1", 80000.0, window_start, 0.40);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 79970.0, now));

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].side, Side::Sell); // Buy Down (No) token
    }

    #[test]
    fn test_no_signal_when_implied_above_accuracy() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 70;

        // $15 move (68% accuracy tier), but market already pricing at 0.70
        let market = make_updown_market("m1", 80000.0, window_start, 0.70);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80015.0, now));

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "no edge when implied > accuracy");
    }

    #[test]
    fn test_one_entry_per_market() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 70;

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80060.0, now));

        // First evaluation → signal
        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);

        // Mark as entered
        strategy.entered_markets.insert("m1".to_string());

        // Second evaluation → no signal (already entered)
        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty());
    }

    #[test]
    fn test_large_move_high_accuracy() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 70;

        // $150 move → 99% accuracy, market at 0.85
        let market = make_updown_market("m1", 80000.0, window_start, 0.85);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80150.0, now));

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);

        if let SignalMetadata::Orb {
            accuracy_tier,
            edge,
            ..
        } = &signals[0].metadata
        {
            assert_eq!(*accuracy_tier, 0.99);
            assert!(*edge > 0.10, "should have >10% edge on $150 move at 0.85 implied");
        } else {
            panic!("expected Orb metadata");
        }
    }
}
