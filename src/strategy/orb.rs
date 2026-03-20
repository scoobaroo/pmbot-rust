use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::candle::Timeframe;
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketType, PolymarketMarket};
use crate::types::order::Side;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, info, warn};

/// Default half-spread when no live book data is available.
const DEFAULT_HALF_SPREAD: Decimal = dec!(0.01);

/// Minimum seconds before window end to allow entry.
const MIN_TIME_REMAINING_SECS: i64 = 30;

/// ATR lookback period (number of 1m candles).
const ATR_PERIOD: usize = 14;

/// Minimum ATR multiple for a move to qualify. Below this, the move is noise.
const MIN_ATR_MULTIPLE: f64 = 0.7;

/// Opening Range Breakout strategy for Polymarket 5-minute BTC UpDown markets.
///
/// Based on backtested edge (4,389 trades): the first 60–120 seconds of BTC price
/// action in a 5-minute window predicts the resolution with high accuracy. Larger
/// moves correlate with higher accuracy:
///
///   $10+ move  → 57% accuracy (needs 120s confirmation)
///   $25+ move  → 68% accuracy (needs 90s)
///   $50+ move  → 76% accuracy (needs 60s)
///   $100+ move → 99% accuracy (needs 60s)
///
/// Filters:
///   - ATR: move must exceed 0.7× ATR(14) on 1m candles to avoid ranging markets
///   - Volume: trade flow imbalance must confirm direction on lower tiers ($10-25)
///
/// Position sizing: half-Kelly based on edge magnitude.
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
    /// Rolling 1m candle true ranges for ATR computation, keyed by symbol.
    true_ranges: HashMap<String, VecDeque<f64>>,
    /// Previous candle close per symbol (needed for true range calculation).
    prev_close: HashMap<String, f64>,
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
            true_ranges: HashMap::new(),
            prev_close: HashMap::new(),
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

    /// Current ATR for a symbol, or None if not enough candles yet.
    fn atr(&self, symbol: &str) -> Option<f64> {
        let trs = self.true_ranges.get(symbol)?;
        if trs.len() < ATR_PERIOD {
            return None;
        }
        let sum: f64 = trs.iter().rev().take(ATR_PERIOD).sum();
        Some(sum / ATR_PERIOD as f64)
    }

    /// Update ATR state from a completed 1m candle.
    fn update_atr(&mut self, symbol: &str, high: f64, low: f64, close: f64) {
        let tr = if let Some(&prev_c) = self.prev_close.get(symbol) {
            // True range: max(high-low, |high-prev_close|, |low-prev_close|)
            (high - low)
                .max((high - prev_c).abs())
                .max((low - prev_c).abs())
        } else {
            high - low
        };
        self.prev_close.insert(symbol.to_string(), close);

        let trs = self
            .true_ranges
            .entry(symbol.to_string())
            .or_insert_with(|| VecDeque::with_capacity(ATR_PERIOD + 1));
        trs.push_back(tr);
        if trs.len() > ATR_PERIOD * 2 {
            trs.pop_front();
        }
    }

    /// Empirical accuracy from backtested ORB tiers (4,389 trades).
    /// Returns None if the move is too small or hasn't had enough confirmation time.
    ///
    /// Uses percentage-based thresholds so it works across BTC and ETH:
    ///   BTC $87K: 0.05% = ~$44, 0.10% = ~$87, 0.15% = ~$131
    ///   ETH $2K:  0.05% = ~$1,  0.10% = ~$2,  0.15% = ~$3
    ///
    /// Two entry windows based on backtest data (72 windows per coin):
    ///
    /// Early window (0-120s):
    ///   0.05% at 60s  → BTC 76%, ETH 71%
    ///   0.07% at 45s  → BTC 83%, ETH 76%
    ///   0.15% at 30s  → BTC 92%, ETH 79%
    ///
    /// Mid window (150-240s):
    ///   0.05% at 150s → BTC 87%, ETH 83%
    ///   0.10% at 180s → BTC 90%, ETH 91%
    ///   0.20% at 180s → BTC 100%, ETH 94%
    fn empirical_accuracy(move_pct: f64, elapsed_secs: i64) -> Option<f64> {
        // === Mid-window momentum (150s+) — highest accuracy entries ===
        if elapsed_secs >= 180 {
            if move_pct >= 0.20 { return Some(0.94); }
            if move_pct >= 0.10 { return Some(0.91); }
            if move_pct >= 0.05 { return Some(0.90); }
        } else if elapsed_secs >= 150 {
            if move_pct >= 0.15 { return Some(0.92); }
            if move_pct >= 0.05 { return Some(0.87); }
        }

        // === Early window — opening range breakout ===
        if move_pct >= 0.15 && elapsed_secs >= 30 {
            Some(0.92)
        } else if move_pct >= 0.07 && elapsed_secs >= 45 {
            Some(0.76)
        } else if move_pct >= 0.05 && elapsed_secs >= 60 {
            Some(0.72)
        } else {
            None
        }
    }

    /// Tiered position sizing: bigger move → bigger bet.
    ///   0.05% early  → 33% of max
    ///   0.07% early  → 50% of max
    ///   0.15%+ early → 100% of max
    ///   0.05%+ mid   → 75% of max
    ///   0.15%+ mid   → 100% of max
    fn tiered_size(&self, move_pct: f64, elapsed_secs: i64) -> Decimal {
        let fraction = if elapsed_secs >= 150 {
            if move_pct >= 0.15 { 1.0 } else { 0.75 }
        } else {
            if move_pct >= 0.15 { 1.0 }
            else if move_pct >= 0.07 { 0.5 }
            else { 0.33 }
        };
        let size = self.max_position_usd.to_f64().unwrap_or(20.0) * fraction;
        Decimal::from_f64_retain(size).unwrap_or(self.max_position_usd)
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

        // Trade 5-minute and 15-minute windows
        if window_secs != 300 && window_secs != 900 {
            return None;
        }

        // BTC and ETH only — accuracy tiers validated on backtest data
        if market.underlying_symbol != "BTC-USD" && market.underlying_symbol != "ETH-USD" {
            return None;
        }

        // One entry per market
        if self.entered_markets.contains(&market.condition_id) {
            return None;
        }

        let price = self.latest_prices.get(&market.underlying_symbol)?;
        let spot = price.vwap.to_f64().unwrap_or(0.0);

        if spot <= 0.0 {
            return None;
        }

        let (start_price, _) = self.updown_start_prices.get(&market.condition_id)?;
        let start_price = *start_price;

        let now = self.current_time().timestamp();
        let elapsed = now - window_start_ts;
        let window_end = window_start_ts + window_secs as i64;
        let time_remaining_secs = (window_end - now).max(0) as f64;

        // Too close to expiry
        if time_remaining_secs < MIN_TIME_REMAINING_SECS as f64 {
            return None;
        }

        // Measure the breakout magnitude (percentage-based for cross-coin support)
        let price_move = spot - start_price;
        let move_abs = price_move.abs();
        let move_pct = if start_price > 0.0 { move_abs / start_price * 100.0 } else { 0.0 };

        // --- ATR filter: skip if move is noise relative to volatility ---
        let atr_val = self.atr(&market.underlying_symbol);
        if let Some(atr) = atr_val {
            if atr > 0.0 && move_abs < atr * MIN_ATR_MULTIPLE {
                debug!(
                    question = %market.question,
                    price_move = format!("{:+.1}", price_move),
                    move_pct = format!("{:.3}%", move_pct),
                    atr = format!("{:.1}", atr),
                    atr_multiple = format!("{:.2}", move_abs / atr),
                    "ORB: move below ATR threshold"
                );
                return None;
            }
        }

        let accuracy = match Self::empirical_accuracy(move_pct, elapsed) {
            Some(a) => a,
            None => {
                debug!(
                    question = %market.question,
                    price_move = format!("{:+.1}", price_move),
                    move_pct = format!("{:.3}%", move_pct),
                    start_price = format!("{:.1}", start_price),
                    spot = format!("{:.1}", spot),
                    elapsed_secs = elapsed,
                    atr = format!("{:.1}", atr_val.unwrap_or(0.0)),
                    "ORB: move too small for any accuracy tier"
                );
                return None;
            }
        };

        // --- Volume filter: require trade flow confirmation on lower tiers ---
        let flow = price.trade_flow_imbalance;
        let flow_confirms = (price_move > 0.0 && flow > 0.0) || (price_move < 0.0 && flow < 0.0);

        if move_pct < 0.07 && !flow_confirms {
            debug!(
                question = %market.question,
                price_move = format!("{:+.1}", price_move),
                move_pct = format!("{:.3}%", move_pct),
                flow = format!("{:.2}", flow),
                "ORB: volume flow doesn't confirm direction"
            );
            return None;
        }

        // Direction: if BTC moved up → buy Up token, if down → buy Down token
        let (side, token_id, implied_price) = if price_move > 0.0 {
            (Side::Buy, &market.token_id_yes, market.implied_prob_yes)
        } else {
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
                price_move = format!("{:+.1}", price_move),
                move_pct = format!("{:.3}%", move_pct),
                accuracy = format!("{:.0}%", accuracy * 100.0),
                implied = format!("{:.1}%", implied_prob * 100.0),
                cost = format!("{:.1}%", total_cost_f64 * 100.0),
                edge = format!("{:.1}%", edge * 100.0),
                "ORB: no edge after fees"
            );
            return None;
        }

        let size_usd = self.tiered_size(move_pct, elapsed);
        let atr_multiple = atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 });

        info!(
            question = %market.question,
            price_move = format!("{:+.1}", price_move),
            move_pct = format!("{:.3}%", move_pct),
            accuracy = format!("{:.0}%", accuracy * 100.0),
            implied = format!("{:.1}%", implied_prob * 100.0),
            edge = format!("{:.1}%", edge * 100.0),
            side = %side,
            size_usd = %size_usd,
            max_usd = %self.max_position_usd,
            atr = format!("{:.1}", atr_val.unwrap_or(0.0)),
            atr_mult = format!("{:.1}x", atr_multiple.unwrap_or(0.0)),
            flow = format!("{:.2}", flow),
            elapsed_secs = elapsed,
            "ORB: breakout signal"
        );

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd,
            confidence: edge,
            price: implied_price,
            metadata: SignalMetadata::Orb {
                btc_move: price_move,
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
            candles: true,
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
            StrategyEvent::CandleComplete(candle) => {
                if candle.timeframe == Timeframe::M1 {
                    let high = candle.high.to_f64().unwrap_or(0.0);
                    let low = candle.low.to_f64().unwrap_or(0.0);
                    let close = candle.close.to_f64().unwrap_or(0.0);
                    self.update_atr(&candle.symbol, high, low, close);

                    if let Some(atr) = self.atr(&candle.symbol) {
                        debug!(
                            symbol = %candle.symbol,
                            atr = format!("{:.1}", atr),
                            "ORB: ATR updated"
                        );
                    }
                }
                Vec::new()
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
                                    let start = self
                                        .latest_prices
                                        .get(&market.underlying_symbol)
                                        .map(|p| p.vwap.to_f64().unwrap_or(0.0))
                                        .unwrap_or(0.0);
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
            fee_calculator: FeeCalculator::new(0),
            book_cache: None,
            ws_book_max_stale_secs: 30,
            max_position_usd: dec!(100),
            true_ranges: HashMap::new(),
            prev_close: HashMap::new(),
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
        make_price_with_flow(symbol, vwap, ts, 0.5)
    }

    fn make_price_with_flow(
        symbol: &str,
        vwap: f64,
        ts: DateTime<Utc>,
        flow: f64,
    ) -> AggregatedPrice {
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
            trade_flow_imbalance: flow,
            recent_buy_volume: 100.0,
            recent_sell_volume: 100.0,
            funding_rate: None,
            mark_price: None,
            binance_mid: Some(Decimal::from_f64_retain(vwap).unwrap()),
            timestamp: ts,
        }
    }

    /// Seed ATR with enough candles so it's available for tests.
    fn seed_atr(strategy: &mut OrbStrategy, symbol: &str, atr_approx: f64) {
        for i in 0..ATR_PERIOD {
            let base = 80000.0 + (i as f64);
            strategy.update_atr(symbol, base + atr_approx, base, base + atr_approx * 0.5);
        }
    }

    #[test]
    fn test_accuracy_tiers() {
        // Too small at any time
        assert_eq!(OrbStrategy::empirical_accuracy(0.03, 120), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.04, 90), None);
        // 0.05% needs 60s
        assert_eq!(OrbStrategy::empirical_accuracy(0.05, 59), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.05, 60), Some(0.72));
        // 0.07% needs 45s
        assert_eq!(OrbStrategy::empirical_accuracy(0.07, 44), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.07, 45), Some(0.76));
        // 0.15% needs 30s
        assert_eq!(OrbStrategy::empirical_accuracy(0.15, 29), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.15, 30), Some(0.92));
        // Mid-window: 0.05% at 150s
        assert_eq!(OrbStrategy::empirical_accuracy(0.05, 150), Some(0.87));
        // Mid-window: 0.10% at 180s
        assert_eq!(OrbStrategy::empirical_accuracy(0.10, 180), Some(0.91));
        // Mid-window: 0.20% at 180s
        assert_eq!(OrbStrategy::empirical_accuracy(0.20, 180), Some(0.94));
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
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should not signal before 60s");
    }

    #[test]
    fn test_no_signal_small_move() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        let market = make_updown_market("m1", 80000.0, window_start, 0.52);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Only 0.006% move — below 0.05% threshold
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80005.0, now));
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should not signal on <0.05% move");
    }

    #[test]
    fn test_signal_on_breakout_up() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // 0.075% move up ($60 on BTC), market pricing Up at 0.55
        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Positive flow confirms up move
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80060.0, now, 0.3),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].side, Side::Buy);
    }

    #[test]
    fn test_signal_on_breakout_down() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // 0.075% move down ($60 on BTC), market pricing Down (No) at 0.55
        let market = make_updown_market("m1", 80000.0, window_start, 0.45);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Negative flow confirms down move
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 79940.0, now, -0.3),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].side, Side::Sell);
    }

    #[test]
    fn test_no_signal_when_implied_above_accuracy() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // $15 move (57% accuracy tier), market already pricing at 0.60
        let market = make_updown_market("m1", 80000.0, window_start, 0.60);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80015.0, now, 0.3),
        );
        seed_atr(&mut strategy, "BTC-USD", 10.0);

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "no edge when implied > accuracy");
    }

    #[test]
    fn test_one_entry_per_market() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80060.0, now, 0.5),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);

        strategy.entered_markets.insert("m1".to_string());

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty());
    }

    #[test]
    fn test_large_move_high_accuracy() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // 0.19% move ($150 on BTC) → 0.92 accuracy at early window, market at 0.85
        let market = make_updown_market("m1", 80000.0, window_start, 0.85);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Large moves (0.07%+) don't need flow confirmation
        strategy
            .latest_prices
            .insert("BTC-USD".to_string(), make_price("BTC-USD", 80150.0, now));
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1);

        if let SignalMetadata::Orb {
            accuracy_tier,
            edge,
            ..
        } = &signals[0].metadata
        {
            assert_eq!(*accuracy_tier, 0.92);
            assert!(*edge > 0.05, "should have >5% edge on 0.19% move at 0.85 implied");
        } else {
            panic!("expected Orb metadata");
        }
    }

    #[test]
    fn test_atr_filter_rejects_noise() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // $12 move but ATR is $20, so move is only 0.6x ATR (below 0.7 threshold)
        let market = make_updown_market("m1", 80000.0, window_start, 0.45);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80012.0, now, 0.3),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0); // ATR=20, move=12 → 0.6x ATR ✗

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should reject move below 0.7x ATR");
    }

    #[test]
    fn test_volume_filter_rejects_counter_flow() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // $30 move up but negative trade flow (sellers dominating)
        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80030.0, now, -0.2), // negative flow on up move
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should reject when flow contradicts direction");
    }

    #[test]
    fn test_volume_filter_skipped_for_large_moves() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // $60 move up with negative flow — large moves skip volume filter
        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80060.0, now, -0.2),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(
            signals.len(),
            1,
            "$50+ moves should bypass volume filter"
        );
    }

    #[test]
    fn test_atr_computation() {
        let mut strategy = make_test_strategy();
        // Not enough candles yet
        assert!(strategy.atr("BTC-USD").is_none());

        // Feed 14 candles with consistent range of $10
        for i in 0..14 {
            let base = 80000.0 + (i as f64);
            strategy.update_atr("BTC-USD", base + 10.0, base, base + 5.0);
        }

        let atr = strategy.atr("BTC-USD").unwrap();
        assert!(
            (atr - 10.0).abs() < 0.5,
            "ATR should be ~10 with $10 ranges, got {}",
            atr
        );
    }

    #[test]
    fn test_no_atr_passes_through() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // No ATR data yet — filter should pass through (not block)
        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80060.0, now, 0.3),
        );
        // No seed_atr — ATR is None

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1, "should still signal when ATR unavailable");
    }
}
