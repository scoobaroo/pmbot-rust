use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::candle::Timeframe;
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketType, PolymarketMarket};
use crate::types::order::Side;
use crate::web::state::{OrbTradeEvent, SharedWebState};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use tracing::{debug, info, warn};

/// Default half-spread when no live book data is available.
const DEFAULT_HALF_SPREAD: Decimal = dec!(0.01);

/// Minimum seconds before window end to allow entry.
const MIN_TIME_REMAINING_SECS: i64 = 30;

/// ATR lookback period (number of 1m candles).
const ATR_PERIOD: usize = 14;

/// Minimum ATR multiple for a move to qualify. Below this, the move is noise.
const MIN_ATR_MULTIPLE: f64 = 0.7;

/// Take profit floor: exit when implied prob reaches this level.
const TAKE_PROFIT_MIN: f64 = 0.75;

/// Take profit ceiling: above this, let it resolve to $1.00 instead of paying exit fees.
const TAKE_PROFIT_MAX: f64 = 0.95;

/// Tracks an open ORB position for early exit logic and data recording.
#[derive(Debug, Clone)]
struct OrbPosition {
    side: Side,
    entry_price: Decimal,
    size_usd: Decimal,
    /// Timestamp of entry signal.
    entry_time: DateTime<Utc>,
    /// Symbol (BTC-USD / ETH-USD).
    symbol: String,
    /// Accuracy tier at entry.
    accuracy: f64,
    /// Computed edge at entry.
    edge: f64,
    /// Trade flow at entry.
    flow: f64,
    /// Move percentage at entry.
    move_pct: f64,
    /// ATR multiple at entry.
    atr_multiple: f64,
    /// Window duration (300 or 900).
    window_secs: u32,
    /// Max favorable excursion — peak book bid seen after entry.
    max_bid_seen: f64,
    /// Max implied prob seen after entry (for comparison with bid).
    max_implied_seen: f64,
}

/// Writes structured CSV data for post-session analysis.
struct OrbDataLogger;

impl OrbDataLogger {
    const TRADES_FILE: &'static str = "data/orb_trades.csv";
    const REJECTIONS_FILE: &'static str = "data/orb_rejections.csv";

    fn log_entry(pos: &OrbPosition, condition_id: &str) {
        let header = "timestamp,condition_id,symbol,side,window_secs,entry_price,size_usd,\
                       accuracy,edge,flow,move_pct,atr_multiple";
        let row = format!(
            "{},{},{},{},{},{:.4},{},{:.3},{:.4},{:.3},{:.4},{:.2}",
            pos.entry_time.to_rfc3339(),
            condition_id,
            pos.symbol,
            pos.side,
            pos.window_secs,
            pos.entry_price,
            pos.size_usd,
            pos.accuracy,
            pos.edge,
            pos.flow,
            pos.move_pct,
            pos.atr_multiple,
        );
        Self::append(Self::TRADES_FILE, header, &row);
    }

    fn log_exit(
        pos: &OrbPosition,
        condition_id: &str,
        exit_type: &str,
        exit_price: f64,
        pnl_pct: f64,
    ) {
        // Append exit columns to a separate line referencing the same condition_id
        let header = "timestamp,condition_id,symbol,exit_type,entry_price,exit_price,\
                       pnl_pct,max_bid_seen,max_implied_seen,hold_secs";
        let hold_secs = (Utc::now() - pos.entry_time).num_seconds();
        let row = format!(
            "{},{},{},{},{:.4},{:.4},{:.2},{:.4},{:.4},{}",
            Utc::now().to_rfc3339(),
            condition_id,
            pos.symbol,
            exit_type,
            pos.entry_price,
            exit_price,
            pnl_pct,
            pos.max_bid_seen,
            pos.max_implied_seen,
            hold_secs,
        );
        Self::append("data/orb_exits.csv", header, &row);
    }

    fn log_rejection(
        condition_id: &str,
        symbol: &str,
        reason: &str,
        move_pct: f64,
        flow: f64,
        elapsed: i64,
        atr_multiple: f64,
    ) {
        let header = "timestamp,condition_id,symbol,reason,move_pct,flow,elapsed_secs,atr_multiple";
        let row = format!(
            "{},{},{},{},{:.4},{:.3},{},{:.2}",
            Utc::now().to_rfc3339(),
            condition_id,
            symbol,
            reason,
            move_pct,
            flow,
            elapsed,
            atr_multiple,
        );
        Self::append(Self::REJECTIONS_FILE, header, &row);
    }

    fn append(path: &str, header: &str, row: &str) {
        let needs_header = !std::path::Path::new(path).exists()
            || std::fs::metadata(path).map(|m| m.len() == 0).unwrap_or(true);
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
            if needs_header {
                let _ = writeln!(f, "{}", header);
            }
            let _ = writeln!(f, "{}", row);
        }
    }
}

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
    /// Open positions by condition_id — used for one-entry-per-market and early exit.
    open_positions: HashMap<String, OrbPosition>,
    fee_calculator: FeeCalculator,
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,
    max_position_usd: Decimal,
    /// Rolling 1m candle true ranges for ATR computation, keyed by symbol.
    true_ranges: HashMap<String, VecDeque<f64>>,
    /// Previous candle close per symbol (needed for true range calculation).
    prev_close: HashMap<String, f64>,
    /// Previous VWAP per symbol — used to confirm momentum is sustained at entry.
    prev_vwap: HashMap<String, f64>,
    /// Optional web state for pushing live trade events to the dashboard.
    web_state: Option<SharedWebState>,
    /// Maps order_id → condition_id so we can link OrderFilled back to position.
    pending_orders: HashMap<String, String>,
}

impl OrbStrategy {
    pub fn new(config: &Config) -> Self {
        Self {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            updown_start_prices: HashMap::new(),
            open_positions: HashMap::new(),
            fee_calculator: FeeCalculator::new(config.fee_rate_bps),
            book_cache: None,
            ws_book_max_stale_secs: config.ws_book_max_stale_secs,
            max_position_usd: config.max_position_usd,
            true_ranges: HashMap::new(),
            prev_close: HashMap::new(),
            prev_vwap: HashMap::new(),
            web_state: None,
            pending_orders: HashMap::new(),
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    pub fn with_web_state(mut self, state: SharedWebState) -> Self {
        self.web_state = Some(state);
        self
    }

    /// Push an ORB trade event to the web dashboard (non-blocking).
    fn push_web_event(&self, event: OrbTradeEvent) {
        if let Some(ref state) = self.web_state {
            let state = state.clone();
            tokio::spawn(async move {
                state.push_orb_event(event).await;
            });
        }
    }

    /// Backtest-safe current time from latest price timestamps.
    fn current_time(&self) -> DateTime<Utc> {
        self.latest_prices
            .values()
            .map(|p| p.timestamp)
            .max()
            .unwrap_or_else(Utc::now)
    }

    /// Look up best bid from orderbook for exit pricing.
    fn get_book_bid(&self, token_id: &str) -> Option<Decimal> {
        let cache = self.book_cache.as_ref()?;
        let books = cache.try_read().ok()?;
        let snap = books.get(token_id)?;
        if !snap.is_fresh(self.ws_book_max_stale_secs) {
            return None;
        }
        if snap.best_bid > Decimal::ZERO {
            Some(snap.best_bid)
        } else {
            None
        }
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

    /// Empirical accuracy from backtested ORB tiers + velocity-based fast entries.
    ///
    /// Two systems:
    /// 1. **Velocity tiers** (< 20s): large moves happening fast = Polymarket hasn't
    ///    repriced yet. Lower accuracy but much better entry prices (55-60¢ vs 75¢+).
    ///    Flow + momentum filters protect against false breakouts.
    ///    Take-profit exits protect against reversals.
    ///
    /// 2. **Confirmed tiers** (20s+): original backtested accuracy levels.
    ///
    /// Velocity = move_pct / elapsed. Fast violent moves are the highest-edge entries
    /// because we're buying before Polymarket market makers adjust.
    ///
    /// Percentage thresholds (cross-coin):
    ///   BTC $70K: 0.06% = ~$42, 0.10% = ~$70, 0.15% = ~$105
    ///   ETH $2K:  0.06% = ~$1.2, 0.10% = ~$2,  0.15% = ~$3
    fn empirical_accuracy(move_pct: f64, elapsed_secs: i64) -> Option<f64> {
        let elapsed = elapsed_secs.max(1); // avoid division by zero

        // === Velocity tiers: fast entries before market reprices ===
        // The edge here isn't accuracy — it's entry price. At 10s the token
        // is still at 50-55¢. By 45s it's at 75¢+. Same move, worse price.
        if elapsed <= 10 {
            // Violent breakout in ≤10s — market hasn't repriced
            if move_pct >= 0.15 { return Some(0.85); }  // massive move, very fast
            if move_pct >= 0.10 { return Some(0.72); }  // strong move, very fast
        } else if elapsed <= 15 {
            if move_pct >= 0.15 { return Some(0.88); }
            if move_pct >= 0.10 { return Some(0.75); }
            if move_pct >= 0.07 { return Some(0.68); }
        } else if elapsed <= 20 {
            if move_pct >= 0.15 { return Some(0.90); }
            if move_pct >= 0.10 { return Some(0.78); }
            if move_pct >= 0.07 { return Some(0.72); }
            if move_pct >= 0.06 { return Some(0.68); }
        }

        // === Mid-window momentum (150s+) — highest accuracy entries ===
        if elapsed_secs >= 180 {
            if move_pct >= 0.20 { return Some(0.94); }
            if move_pct >= 0.10 { return Some(0.91); }
            if move_pct >= 0.05 { return Some(0.90); }
        } else if elapsed_secs >= 150 {
            if move_pct >= 0.15 { return Some(0.92); }
            if move_pct >= 0.05 { return Some(0.87); }
        }

        // === Confirmed window (30-120s) — backtested accuracy ===
        if move_pct >= 0.15 && elapsed_secs >= 30 {
            Some(0.92)
        } else if move_pct >= 0.07 && elapsed_secs >= 45 {
            Some(0.76)
        } else if move_pct >= 0.06 && elapsed_secs >= 50 {
            Some(0.74)
        } else {
            None
        }
    }

    /// Tiered position sizing: bigger move → bigger bet, velocity entries sized down.
    ///
    /// Velocity entries (≤20s): smaller size — accuracy is lower but entry price
    /// is much better (buying at 55¢ vs 75¢). Take-profit exits protect downside.
    ///
    /// Confirmed entries (30s+): standard sizing from backtested tiers.
    /// Mid-window (150s+): larger sizing — highest accuracy.
    fn tiered_size(&self, move_pct: f64, elapsed_secs: i64) -> Decimal {
        let fraction = if elapsed_secs <= 20 {
            // Velocity entries: smaller size, better price
            if move_pct >= 0.15 { 0.50 }      // massive fast move
            else if move_pct >= 0.10 { 0.33 }  // strong fast move
            else { 0.25 }                       // decent fast move
        } else if elapsed_secs >= 150 {
            if move_pct >= 0.15 { 1.0 } else { 0.75 }
        } else {
            if move_pct >= 0.15 { 1.0 }
            else if move_pct >= 0.07 { 0.5 }
            else { 0.25 }
        };
        let size = self.max_position_usd.to_f64().unwrap_or(20.0) * fraction;
        Decimal::from_f64_retain(size).unwrap_or(self.max_position_usd)
    }

    fn evaluate_all_markets(&mut self) -> Vec<TradeSignal> {
        let mut signals = Vec::new();
        let market_list: Vec<PolymarketMarket> = self.markets.values().cloned().collect();
        for market in &market_list {
            if let Some(s) = self.evaluate_updown_market(market) {
                signals.push(s);
            }
        }
        signals
    }

    fn evaluate_updown_market(&mut self, market: &PolymarketMarket) -> Option<TradeSignal> {
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
        let is_eth = market.underlying_symbol == "ETH-USD";
        if market.underlying_symbol != "BTC-USD" && !is_eth {
            return None;
        }

        // One entry per market
        if self.open_positions.contains_key(&market.condition_id) {
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

        // ETH is noisier — require 0.15%+ (~$3.15) minimum vs BTC's 0.05%
        // Backtest: ETH at 0.05% only 67% sustained, at 0.15% = 75.5%, at 0.20% = 80%
        if is_eth && move_pct < 0.15 {
            return None;
        }

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
                OrbDataLogger::log_rejection(
                    &market.condition_id, &market.underlying_symbol,
                    "atr_filter", move_pct, price.trade_flow_imbalance, elapsed, move_abs / atr,
                );
                return None;
            }
        }

        // --- Oracle confirmation: Polymarket resolves against Chainlink, not Binance ---
        // If oracle price is available, verify the move is confirmed on-chain.
        // A Binance spike that Chainlink hasn't confirmed won't resolve in our favor.
        if let Some(oracle) = price.oracle_price {
            let oracle_f64 = oracle.to_f64().unwrap_or(0.0);
            if oracle_f64 > 0.0 {
                let oracle_move = oracle_f64 - start_price;
                if (price_move > 0.0 && oracle_move <= 0.0) || (price_move < 0.0 && oracle_move >= 0.0) {
                    debug!(
                        question = %market.question,
                        exchange_move = format!("{:+.1}", price_move),
                        oracle_move = format!("{:+.1}", oracle_move),
                        "ORB: oracle doesn't confirm exchange direction — skipping"
                    );
                    OrbDataLogger::log_rejection(
                        &market.condition_id, &market.underlying_symbol,
                        "oracle_disagrees", move_pct, price.trade_flow_imbalance, elapsed,
                        atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 }).unwrap_or(0.0),
                    );
                    return None;
                }
            }
        }

        // --- Momentum filter: price must still be moving in breakout direction ---
        // Catches false breakouts where BTC spikes then reverses before we enter.
        if let Some(&prev) = self.prev_vwap.get(&market.underlying_symbol) {
            let prev_move = (prev - start_price).abs();
            if move_abs <= prev_move {
                debug!(
                    question = %market.question,
                    price_move = format!("{:+.1}", price_move),
                    prev_move = format!("{:.1}", prev_move),
                    "ORB: momentum stalling or reversing — skipping"
                );
                OrbDataLogger::log_rejection(
                    &market.condition_id, &market.underlying_symbol,
                    "momentum_stall", move_pct, price.trade_flow_imbalance, elapsed,
                    atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 }).unwrap_or(0.0),
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
                OrbDataLogger::log_rejection(
                    &market.condition_id, &market.underlying_symbol,
                    "move_too_small", move_pct, price.trade_flow_imbalance, elapsed,
                    atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 }).unwrap_or(0.0),
                );
                return None;
            }
        };

        // --- Volume filter: require strong flow confirmation on ALL tiers ---
        // Weak flow = noise/chop. Require both correct direction AND magnitude ≥0.3.
        let flow = price.trade_flow_imbalance;
        let flow_confirms = (price_move > 0.0 && flow > 0.0) || (price_move < 0.0 && flow < 0.0);
        let flow_strong = flow.abs() >= 0.3;

        if !flow_confirms || !flow_strong {
            debug!(
                question = %market.question,
                price_move = format!("{:+.1}", price_move),
                move_pct = format!("{:.3}%", move_pct),
                flow = format!("{:.2}", flow),
                flow_strong,
                "ORB: flow doesn't confirm direction or too weak (<0.3)"
            );
            OrbDataLogger::log_rejection(
                &market.condition_id, &market.underlying_symbol,
                "weak_flow", move_pct, flow, elapsed,
                atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 }).unwrap_or(0.0),
            );
            return None;
        }

        // --- Funding rate signal: confirms or warns against direction ---
        // Negative funding = shorts paying longs = bearish positioning
        // Positive funding = longs paying shorts = bullish positioning
        let funding = price.funding_rate.unwrap_or(0.0);
        let funding_confirms = (price_move > 0.0 && funding > 0.0)
            || (price_move < 0.0 && funding < 0.0);
        let funding_extreme = funding.abs() > 0.0001; // > 0.01% = crowded

        // Adjust accuracy based on funding alignment
        let accuracy = if funding_extreme && !funding_confirms {
            // Extreme funding against our direction = crowded counter-trade
            // Potential squeeze — boost accuracy (we're trading the squeeze)
            let boosted = (accuracy + 0.03).min(0.98);
            debug!(
                question = %market.question,
                funding = format!("{:.6}", funding),
                direction = if price_move > 0.0 { "UP" } else { "DOWN" },
                accuracy_boost = format!("{:.0}% -> {:.0}%", accuracy * 100.0, boosted * 100.0),
                "ORB: extreme counter-funding — squeeze potential, boosting accuracy"
            );
            boosted
        } else if funding_extreme && funding_confirms {
            // Extreme funding in our direction = crowded trade, reversal risk
            // Reduce accuracy to be more cautious
            let reduced = accuracy - 0.03;
            debug!(
                question = %market.question,
                funding = format!("{:.6}", funding),
                direction = if price_move > 0.0 { "UP" } else { "DOWN" },
                accuracy_reduction = format!("{:.0}% -> {:.0}%", accuracy * 100.0, reduced * 100.0),
                "ORB: extreme aligned funding — crowded, reducing accuracy"
            );
            reduced
        } else if funding_confirms {
            // Mild funding confirms our direction — slight boost
            accuracy + 0.01
        } else {
            accuracy
        };

        // Direction: if BTC moved up → buy Up token, if down → buy Down token
        let (side, token_id, implied_price) = if price_move > 0.0 {
            (Side::Buy, &market.token_id_yes, market.implied_prob_yes)
        } else {
            (Side::Sell, &market.token_id_no, market.implied_prob_no)
        };

        // Don't place opposing orders — if we already have a position on this
        // market's underlying in the opposite direction, skip
        if let Some(existing_pos) = self.open_positions.get(&market.condition_id) {
            if existing_pos.side != side {
                debug!(
                    question = %market.question,
                    existing = ?existing_pos.side,
                    new = ?side,
                    "ORB: skipping opposing order"
                );
                return None;
            }
        }

        let implied_prob = implied_price.to_f64().unwrap_or(0.5);

        // Aggressive pricing: bid above implied to sweep the book immediately.
        // Add 5c to implied price (e.g., 0.55 → 0.60) to ensure instant fill.
        let aggressive_price = (implied_price + Decimal::new(5, 2)).min(Decimal::new(95, 2));

        // Max entry price cap — don't buy above 70¢. Guarantees ≥30% return on win.
        if aggressive_price > Decimal::new(70, 2) {
            debug!(
                question = %market.question,
                price = format!("{:.2}", aggressive_price),
                implied = format!("{:.2}", implied_prob),
                "ORB: entry price too high (>70¢) — return not worth risk"
            );
            OrbDataLogger::log_rejection(
                &market.condition_id, &market.underlying_symbol,
                "price_too_high", move_pct, flow, elapsed,
                atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 }).unwrap_or(0.0),
            );
            return None;
        }

        // Deduct trading costs using aggressive price
        let half_spread = self.get_half_spread(token_id);
        let total_cost = self.fee_calculator.total_cost(aggressive_price, half_spread);
        let total_cost_f64 = total_cost.to_f64().unwrap_or(0.0);
        let aggressive_f64 = aggressive_price.to_f64().unwrap_or(0.5);

        let edge = accuracy - aggressive_f64 - total_cost_f64;

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
            OrbDataLogger::log_rejection(
                &market.condition_id, &market.underlying_symbol,
                "no_edge", move_pct, flow, elapsed,
                atr_val.map(|a| if a > 0.0 { move_abs / a } else { 0.0 }).unwrap_or(0.0),
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
            funding = format!("{:.6}", funding),
            funding_confirms,
            elapsed_secs = elapsed,
            "ORB: breakout signal"
        );

        let window_secs_u32 = match market.market_type {
            MarketType::UpDown { window_secs, .. } => window_secs,
            _ => 300,
        };

        // Track position for one-entry-per-market and early exit
        let pos = OrbPosition {
            side,
            entry_price: aggressive_price,
            size_usd,
            entry_time: self.current_time(),
            symbol: market.underlying_symbol.clone(),
            accuracy,
            edge,
            flow,
            move_pct,
            atr_multiple: atr_multiple.unwrap_or(0.0),
            window_secs: window_secs_u32 as u32,
            max_bid_seen: 0.0,
            max_implied_seen: 0.0,
        };
        OrbDataLogger::log_entry(&pos, &market.condition_id);
        // Web dashboard event pushed on OrderFilled, not here — only filled orders count
        self.open_positions.insert(market.condition_id.clone(), pos);

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd,
            confidence: edge,
            price: aggressive_price,
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

    /// Check all open positions for take-profit exits based on orderbook best bid.
    /// Called on every exchange price tick for maximum responsiveness.
    fn check_take_profits(&mut self) -> Vec<TradeSignal> {
        // Phase 1: collect book bids and implied prices for all open positions
        // (avoids borrow conflicts with self.get_book_bid / self.markets)
        let mut position_data: Vec<(String, Option<Decimal>, f64)> = Vec::new(); // (cid, book_bid, implied)
        for (cid, pos) in &self.open_positions {
            let (token_id, implied) = match pos.side {
                Side::Buy => (
                    self.markets.get(cid).map(|m| m.token_id_yes.clone()),
                    self.markets.get(cid).map(|m| m.implied_prob_yes.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
                ),
                Side::Sell => (
                    self.markets.get(cid).map(|m| m.token_id_no.clone()),
                    self.markets.get(cid).map(|m| m.implied_prob_no.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
                ),
            };
            let bid = token_id.and_then(|tid| self.get_book_bid(&tid));
            position_data.push((cid.clone(), bid, implied));
        }

        // Phase 2: update MFE tracking
        for (cid, bid, implied) in &position_data {
            if let Some(pos) = self.open_positions.get_mut(cid) {
                if let Some(best_bid) = bid {
                    let bid_f64 = best_bid.to_f64().unwrap_or(0.0);
                    if bid_f64 > pos.max_bid_seen {
                        pos.max_bid_seen = bid_f64;
                    }
                }
                if *implied > pos.max_implied_seen {
                    pos.max_implied_seen = *implied;
                }
            }
        }

        // Phase 3: generate exit signals
        let now = self.current_time();
        let mut signals = Vec::new();
        let mut to_close = Vec::new();

        for (cid, bid, _) in &position_data {
            let best_bid = match bid {
                Some(b) => *b,
                None => continue,
            };
            let bid_f64 = best_bid.to_f64().unwrap_or(0.0);
            if bid_f64 < TAKE_PROFIT_MIN || bid_f64 > TAKE_PROFIT_MAX {
                continue;
            }

            let pos = match self.open_positions.get(cid) {
                Some(p) => p.clone(),
                None => continue,
            };
            let market = match self.markets.get(cid).cloned() {
                Some(m) => m,
                None => continue,
            };

            let entry_f64 = pos.entry_price.to_f64().unwrap_or(0.5);
            let profit_pct = (bid_f64 - entry_f64) / entry_f64 * 100.0;

            info!(
                condition_id = %cid,
                side = %pos.side,
                entry = format!("{:.2}", entry_f64),
                best_bid = %best_bid,
                profit = format!("{:+.1}%", profit_pct),
                max_bid = format!("{:.2}", pos.max_bid_seen),
                "ORB: take-profit exit at book bid"
            );

            OrbDataLogger::log_exit(&pos, cid, "take_profit", bid_f64, profit_pct);

            signals.push(TradeSignal {
                target: TradeTarget::Polymarket(market),
                side: pos.side,
                size_usd: pos.size_usd,
                confidence: 0.0,
                price: best_bid,
                metadata: SignalMetadata::Orb {
                    btc_move: 0.0,
                    accuracy_tier: pos.accuracy,
                    implied_prob: bid_f64,
                    edge: profit_pct / 100.0,
                    start_price: 0.0,
                    spot: 0.0,
                    elapsed_secs: 0.0,
                    time_remaining_secs: 0.0,
                },
                timestamp: now,
                is_exit: true,
            });

            to_close.push(cid.clone());
        }

        for cid in to_close {
            self.open_positions.remove(&cid);
        }

        signals
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
            // Don't remove open_positions here — wait for MarketResolved
            // event from execution engine which has the actual W/L outcome.
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
                // Save previous VWAP before overwriting — used for momentum confirmation
                if let Some(old) = self.latest_prices.get(&price.symbol) {
                    self.prev_vwap
                        .insert(price.symbol.clone(), old.vwap.to_f64().unwrap_or(0.0));
                }
                self.latest_prices.insert(price.symbol.clone(), price);
                self.gc_expired_markets();
                // Check take-profit exits first (every tick), then new entries
                let mut signals = self.check_take_profits();
                signals.extend(self.evaluate_all_markets());
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
                        Vec::new()
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
                        // Take-profit checks now run on every exchange PriceUpdate tick
                        // via check_take_profits() — not here.
                        Vec::new()
                    }
                }
            }
            StrategyEvent::ExecutionFeedback(event) => {
                match &event {
                    crate::types::events::ExecutionEvent::OrderPlaced(order) => {
                        // Track order_id → condition_id for linking fills to positions
                        self.pending_orders.insert(
                            order.id.clone(),
                            order.condition_id.clone(),
                        );
                    }
                    crate::types::events::ExecutionEvent::OrderFilled(fill) => {
                        info!(
                            order_id = %fill.order_id,
                            price = %fill.price,
                            size = %fill.size,
                            "ORB: order filled"
                        );

                        // Push FILLED event to dashboard using position data
                        if let Some(cid) = self.pending_orders.remove(&fill.order_id) {
                            if let Some(pos) = self.open_positions.get(&cid) {
                                self.push_web_event(OrbTradeEvent {
                                    timestamp: fill.timestamp.to_rfc3339(),
                                    symbol: pos.symbol.clone(),
                                    side: format!("{}", fill.side),
                                    action: "FILLED".to_string(),
                                    price: fill.price.to_f64().unwrap_or(0.0),
                                    size_usd: (fill.price * fill.size).to_f64().unwrap_or(0.0),
                                    move_pct: pos.move_pct,
                                    accuracy: pos.accuracy,
                                    edge: pos.edge,
                                    flow: pos.flow,
                                    window_secs: pos.window_secs,
                                    reason: format!("fee: {:.4}", fill.fee),
                                });
                            }
                        }
                    }
                    crate::types::events::ExecutionEvent::OrderFailed { order_id, error } => {
                        self.pending_orders.remove(order_id);
                        warn!(order_id = %order_id, error = %error, "ORB: order failed");
                    }
                    crate::types::events::ExecutionEvent::MarketResolved { condition_id, pnl, won } => {
                        let result = if *won { "WON" } else { "LOST" };

                        // Look up position data before removing
                        if let Some(pos) = self.open_positions.get(condition_id) {
                            info!(
                                condition_id = %condition_id,
                                symbol = %pos.symbol,
                                side = %pos.side,
                                entry = format!("{:.2}", pos.entry_price),
                                pnl = format!("{:+.2}", pnl),
                                result,
                                max_bid = format!("{:.2}", pos.max_bid_seen),
                                max_implied = format!("{:.2}", pos.max_implied_seen),
                                "ORB: market resolved"
                            );

                            // Log to CSV with actual outcome
                            OrbDataLogger::log_exit(
                                pos, condition_id, result, 0.0, *pnl,
                            );

                            // Push to web dashboard
                            self.push_web_event(OrbTradeEvent {
                                timestamp: Utc::now().to_rfc3339(),
                                symbol: pos.symbol.clone(),
                                side: format!("{}", pos.side),
                                action: result.to_string(),
                                price: pos.entry_price.to_f64().unwrap_or(0.0),
                                size_usd: *pnl,
                                move_pct: pos.move_pct,
                                accuracy: pos.accuracy,
                                edge: *pnl,
                                flow: pos.flow,
                                window_secs: pos.window_secs,
                                reason: format!("P&L: ${:+.2} | entry: {:.0}c", pnl, pos.entry_price.to_f64().unwrap_or(0.0) * 100.0),
                            });
                        } else {
                            info!(
                                condition_id = %condition_id,
                                pnl = format!("{:+.2}", pnl),
                                result,
                                "ORB: market resolved (position already cleaned)"
                            );
                        }

                        // Clean up position
                        self.open_positions.remove(condition_id);
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
            open_positions: HashMap::new(),
            fee_calculator: FeeCalculator::new(0),
            book_cache: None,
            ws_book_max_stale_secs: 30,
            max_position_usd: dec!(100),
            true_ranges: HashMap::new(),
            prev_close: HashMap::new(),
            prev_vwap: HashMap::new(),
            web_state: None,
            pending_orders: HashMap::new(),
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

    fn make_test_position(side: Side, entry_price: f64, size_usd: f64) -> OrbPosition {
        OrbPosition {
            side,
            entry_price: Decimal::from_f64_retain(entry_price).unwrap(),
            size_usd: Decimal::from_f64_retain(size_usd).unwrap(),
            entry_time: Utc::now(),
            symbol: "BTC-USD".to_string(),
            accuracy: 0.76,
            edge: 0.10,
            flow: 0.5,
            move_pct: 0.07,
            atr_multiple: 1.5,
            window_secs: 300,
            max_bid_seen: 0.0,
            max_implied_seen: 0.0,
        }
    }

    /// Set up a book cache with a specific best bid for a token.
    fn seed_book_cache(strategy: &mut OrbStrategy, token_id: &str, best_bid: f64) {
        use crate::polymarket::ws_orderbook::{new_book_cache, BookSnapshot};
        let cache = new_book_cache();
        {
            let mut books = cache.blocking_write();
            books.insert(token_id.to_string(), BookSnapshot {
                best_bid: Decimal::from_f64_retain(best_bid).unwrap(),
                best_ask: Decimal::from_f64_retain(best_bid + 0.02).unwrap(),
                spread: dec!(0.02),
                depth_bid: dec!(500),
                depth_ask: dec!(500),
                timestamp: Utc::now(),
            });
        }
        strategy.book_cache = Some(cache);
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
        // === Velocity tiers (≤20s) ===
        // Too small even at high velocity
        assert_eq!(OrbStrategy::empirical_accuracy(0.05, 5), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.06, 10), None);
        // 0.10% in ≤10s — violent breakout
        assert_eq!(OrbStrategy::empirical_accuracy(0.10, 8), Some(0.72));
        assert_eq!(OrbStrategy::empirical_accuracy(0.15, 5), Some(0.85));
        // 0.07% in ≤15s — strong breakout
        assert_eq!(OrbStrategy::empirical_accuracy(0.07, 12), Some(0.68));
        assert_eq!(OrbStrategy::empirical_accuracy(0.10, 15), Some(0.75));
        assert_eq!(OrbStrategy::empirical_accuracy(0.15, 14), Some(0.88));
        // 0.06% in ≤20s — decent breakout
        assert_eq!(OrbStrategy::empirical_accuracy(0.06, 18), Some(0.68));
        assert_eq!(OrbStrategy::empirical_accuracy(0.07, 20), Some(0.72));
        assert_eq!(OrbStrategy::empirical_accuracy(0.10, 20), Some(0.78));

        // === Confirmed tiers (30s+) ===
        // Too small at any confirmed time
        assert_eq!(OrbStrategy::empirical_accuracy(0.03, 120), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.04, 90), None);
        // 0.05% — no early tier (dropped)
        assert_eq!(OrbStrategy::empirical_accuracy(0.05, 60), None);
        // 0.06% needs 50s
        assert_eq!(OrbStrategy::empirical_accuracy(0.06, 49), None);
        assert_eq!(OrbStrategy::empirical_accuracy(0.06, 50), Some(0.74));
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
    fn test_velocity_entry_fast_breakout() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 8; // only 8 seconds elapsed

        // 0.10% move ($80 on BTC) in 8 seconds — violent breakout
        let market = make_updown_market("m1", 80000.0, window_start, 0.52);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Strong positive flow confirms real buying pressure
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80080.0, now, 0.6),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1, "should fire on 0.10% move in 8s");
        assert_eq!(signals[0].side, Side::Buy);

        // Entry price should be cheap — implied is 0.52, aggressive = 0.57
        let price_f64 = signals[0].price.to_f64().unwrap();
        assert!(price_f64 < 0.65, "velocity entry should get cheap price, got {}", price_f64);
    }

    #[test]
    fn test_velocity_rejects_small_fast_move() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 8; // 8 seconds

        // 0.05% move in 8s — not big enough for velocity tier
        let market = make_updown_market("m1", 80000.0, window_start, 0.52);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80040.0, now, 0.5),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "0.05% in 8s should not trigger velocity entry");
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

        strategy.open_positions.insert("m1".to_string(), make_test_position(Side::Buy, 0.60, 50.0));

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty());
    }

    #[test]
    fn test_large_move_high_accuracy() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // 0.19% move ($150 on BTC) → 0.92 accuracy at early window, market at 0.60
        let market = make_updown_market("m1", 80000.0, window_start, 0.60);
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
            assert!(*edge > 0.10, "should have >10% edge on 0.19% move at 0.60 implied + 0.05 premium");
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
    fn test_volume_filter_rejects_weak_flow_on_large_moves() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        // $60 move up with negative flow — ALL tiers now require strong confirming flow
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
        assert!(
            signals.is_empty(),
            "should reject large move with counter-flow"
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

    #[test]
    fn test_momentum_filter_rejects_stalling_move() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Previous VWAP was further from start than current → move is reversing
        strategy.prev_vwap.insert("BTC-USD".to_string(), 80070.0);
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80060.0, now, 0.5),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert!(signals.is_empty(), "should reject when momentum is stalling");
    }

    #[test]
    fn test_momentum_filter_passes_accelerating_move() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 120;

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy
            .updown_start_prices
            .insert("m1".to_string(), (80000.0, window_start));
        // Previous VWAP was closer to start than current → move is accelerating
        strategy.prev_vwap.insert("BTC-USD".to_string(), 80050.0);
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price_with_flow("BTC-USD", 80060.0, now, 0.5),
        );
        seed_atr(&mut strategy, "BTC-USD", 20.0);

        let signals = strategy.evaluate_all_markets();
        assert_eq!(signals.len(), 1, "should signal when momentum is accelerating");
    }

    // ---- Take-profit exit tests ----

    #[test]
    fn test_take_profit_exit_at_book_bid_080() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 60;

        // Set up a market and an open position (bought YES at 0.60)
        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy.open_positions.insert("m1".to_string(), make_test_position(Side::Buy, 0.60, 50.0));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price("BTC-USD", 80100.0, now),
        );
        // Book bid at 0.80 for the YES token
        seed_book_cache(&mut strategy, "m1-yes", 0.80);

        // Take-profit triggers on exchange PriceUpdate tick (not PolymarketUpdate)
        let signals = strategy.on_event(StrategyEvent::PriceUpdate(
            make_price("BTC-USD", 80100.0, now),
        ));

        // Filter out any entry signals, keep only exits
        let exits: Vec<_> = signals.iter().filter(|s| s.is_exit).collect();
        assert_eq!(exits.len(), 1, "should generate take-profit exit signal");
        assert_eq!(exits[0].side, Side::Buy, "exit keeps same side as entry");
        assert!(exits[0].price > dec!(0.79) && exits[0].price < dec!(0.81), "exit at book bid ~0.80, not implied");
        assert!(!strategy.open_positions.contains_key("m1"), "position should be closed");
    }

    #[test]
    fn test_no_exit_when_book_bid_below_threshold() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 60;

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy.open_positions.insert("m1".to_string(), make_test_position(Side::Buy, 0.60, 50.0));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price("BTC-USD", 80050.0, now),
        );
        // Book bid at 0.65 — below 0.75 take-profit floor
        seed_book_cache(&mut strategy, "m1-yes", 0.65);

        let signals = strategy.on_event(StrategyEvent::PriceUpdate(
            make_price("BTC-USD", 80050.0, now),
        ));
        let exits: Vec<_> = signals.iter().filter(|s| s.is_exit).collect();

        assert!(exits.is_empty(), "should not exit when book bid below threshold");
        assert!(strategy.open_positions.contains_key("m1"), "position should remain open");
    }

    #[test]
    fn test_no_exit_when_book_bid_above_ceiling() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 60;

        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy.open_positions.insert("m1".to_string(), make_test_position(Side::Buy, 0.60, 50.0));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price("BTC-USD", 80200.0, now),
        );
        // Book bid at 0.97 — above 0.95 ceiling, let it resolve to $1.00
        seed_book_cache(&mut strategy, "m1-yes", 0.97);

        let signals = strategy.on_event(StrategyEvent::PriceUpdate(
            make_price("BTC-USD", 80200.0, now),
        ));
        let exits: Vec<_> = signals.iter().filter(|s| s.is_exit).collect();

        assert!(exits.is_empty(), "should not exit above ceiling — let it resolve to $1");
        assert!(strategy.open_positions.contains_key("m1"), "position should remain open");
    }

    #[test]
    fn test_no_exit_without_book_data() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 60;

        // No book cache at all — should not exit even if implied is high
        let market = make_updown_market("m1", 80000.0, window_start, 0.55);
        strategy.markets.insert("m1".to_string(), market);
        strategy.open_positions.insert("m1".to_string(), make_test_position(Side::Buy, 0.60, 50.0));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price("BTC-USD", 80100.0, now),
        );

        let signals = strategy.on_event(StrategyEvent::PriceUpdate(
            make_price("BTC-USD", 80100.0, now),
        ));
        let exits: Vec<_> = signals.iter().filter(|s| s.is_exit).collect();

        assert!(exits.is_empty(), "should not exit without book data — no reliable price");
        assert!(strategy.open_positions.contains_key("m1"), "position should remain open");
    }

    #[test]
    fn test_take_profit_exit_sell_side() {
        let mut strategy = make_test_strategy();
        let now = Utc::now();
        let window_start = now.timestamp() - 60;

        // Holding a NO position (Side::Sell)
        let market = make_updown_market("m1", 80000.0, window_start, 0.45);
        strategy.markets.insert("m1".to_string(), market);
        strategy.open_positions.insert("m1".to_string(), make_test_position(Side::Sell, 0.55, 50.0));
        strategy.latest_prices.insert(
            "BTC-USD".to_string(),
            make_price("BTC-USD", 79800.0, now),
        );
        // Book bid at 0.80 for the NO token
        seed_book_cache(&mut strategy, "m1-no", 0.80);

        // Take-profit triggers on exchange PriceUpdate tick
        let signals = strategy.on_event(StrategyEvent::PriceUpdate(
            make_price("BTC-USD", 79800.0, now),
        ));
        let exits: Vec<_> = signals.iter().filter(|s| s.is_exit).collect();

        assert_eq!(exits.len(), 1, "should exit NO position at take-profit");
        assert_eq!(exits[0].side, Side::Sell, "exit keeps Sell side for NO token");
        assert!(exits[0].price > dec!(0.79) && exits[0].price < dec!(0.81), "exit at book bid ~0.80");
    }
}
