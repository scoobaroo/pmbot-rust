use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::kelly;
use crate::strategy::probability;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::candle::{Candle, Timeframe};
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketDirection, PolymarketMarket};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, VecDeque};
use tracing::{debug, info, warn};

/// Default half-spread estimate when no live book data is available.
const DEFAULT_HALF_SPREAD: Decimal = dec!(0.01);

/// BB band-crossing score decay factor per candle when no new crossing occurs.
const BB_SCORE_DECAY: f64 = 0.7;

// ---------------------------------------------------------------------------
// Internal state structs
// ---------------------------------------------------------------------------

/// Per-symbol Bollinger Bands state (rolling window + crossing score).
struct BbState {
    window: VecDeque<f64>,
    prev_close: Option<f64>,
    last_crossing_score: f64, // +z oversold, -z overbought, decays
}

/// Per-symbol EMA state for MA confluence.
struct MaState {
    fast_ema: f64,
    slow_ema: f64,
    fast_initialized: bool,
    slow_initialized: bool,
    fast_count: usize,
    slow_count: usize,
}

/// Per-symbol position / cooldown tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolPosition {
    Flat,
    Long,
    Short,
}

struct SymbolState {
    position: SymbolPosition,
    cooldown_remaining: usize,
}

/// Tracks an open position for exit logic.
struct OpenPosition {
    condition_id: String,
    side: Side,
    entry_edge: f64,
}

/// Minimum time-to-expiry (in hours) before we flatten the position.
const EXPIRY_FLATTEN_HOURS: f64 = 1.0;

// ---------------------------------------------------------------------------
// UnifiedStrategy
// ---------------------------------------------------------------------------

pub struct UnifiedStrategy {
    // BS core
    markets: HashMap<String, PolymarketMarket>,
    latest_prices: HashMap<String, AggregatedPrice>,
    min_edge_threshold: f64,
    kelly_fraction_cap: f64,
    max_total_exposure_usd: Decimal,
    max_position_usd: Decimal,
    fee_calculator: FeeCalculator,
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,

    // BB confluence
    bb_period: usize,
    bb_num_std: f64,
    bb_timeframe: Timeframe,
    bb_states: HashMap<String, BbState>,

    // MA confluence
    ma_fast_period: usize,
    ma_slow_period: usize,
    ma_timeframe: Timeframe,
    ma_states: HashMap<String, MaState>,

    // Position tracking + cooldown
    cooldown_candles: usize,
    symbol_states: HashMap<String, SymbolState>,

    // Confluence weights
    bb_weight: f64,
    ma_weight: f64,

    // Open positions for exit logic (keyed by condition_id)
    open_positions: HashMap<String, OpenPosition>,
}

impl UnifiedStrategy {
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

            bb_period: config.bb_period,
            bb_num_std: config.bb_num_std,
            bb_timeframe: Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5),
            bb_states: HashMap::new(),

            ma_fast_period: config.ma_fast_period,
            ma_slow_period: config.ma_slow_period,
            ma_timeframe: Timeframe::from_str_loose(&config.ma_timeframe).unwrap_or(Timeframe::M5),
            ma_states: HashMap::new(),

            cooldown_candles: config.bb_cooldown_candles,
            symbol_states: HashMap::new(),

            bb_weight: config.unified_bb_weight,
            ma_weight: config.unified_ma_weight,

            open_positions: HashMap::new(),
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    /// Timeframes this strategy needs candles for (deduplicated).
    pub fn candle_timeframes(&self) -> Vec<Timeframe> {
        if self.bb_timeframe == self.ma_timeframe {
            vec![self.bb_timeframe]
        } else {
            vec![self.bb_timeframe, self.ma_timeframe]
        }
    }

    // -------------------------------------------------------------------
    // Live half-spread lookup (same as BlackScholesStrategy)
    // -------------------------------------------------------------------

    fn get_half_spread(&self, token_id: &str) -> Decimal {
        if let Some(ref cache) = self.book_cache {
            if let Ok(books) = cache.try_read() {
                if let Some(snap) = books.get(token_id) {
                    if snap.is_fresh(self.ws_book_max_stale_secs) {
                        return snap.half_spread();
                    }
                    debug!(token_id, "book snapshot stale, using default spread");
                }
            }
        }
        DEFAULT_HALF_SPREAD
    }

    // -------------------------------------------------------------------
    // Confluence: BB state update
    // -------------------------------------------------------------------

    fn update_bb_state(&mut self, candle: &Candle) {
        if candle.timeframe != self.bb_timeframe {
            return;
        }
        let close = candle.close.to_string().parse::<f64>().unwrap_or(0.0);
        if close <= 0.0 {
            return;
        }

        let state = self
            .bb_states
            .entry(candle.symbol.clone())
            .or_insert(BbState {
                window: VecDeque::with_capacity(self.bb_period),
                prev_close: None,
                last_crossing_score: 0.0,
            });

        // Maintain rolling window
        if state.window.len() == self.bb_period {
            state.window.pop_front();
        }
        state.window.push_back(close);

        // Need full window to compute bands
        if state.window.len() < self.bb_period {
            state.prev_close = Some(close);
            return;
        }

        let sum: f64 = state.window.iter().sum();
        let sma = sum / self.bb_period as f64;
        let variance: f64 =
            state.window.iter().map(|x| (x - sma).powi(2)).sum::<f64>() / self.bb_period as f64;
        let std_dev = variance.sqrt();

        if std_dev < 1e-12 {
            state.prev_close = Some(close);
            state.last_crossing_score *= BB_SCORE_DECAY;
            return;
        }

        let upper = sma + self.bb_num_std * std_dev;
        let lower = sma - self.bb_num_std * std_dev;

        // Check for band crossings
        if let Some(prev) = state.prev_close {
            if close <= lower && prev > lower {
                // Crossed below lower band → oversold → positive z-score
                let z = ((sma - close) / std_dev).clamp(0.0, 3.0) / 3.0; // normalize to [0, 1]
                state.last_crossing_score = z;
            } else if close >= upper && prev < upper {
                // Crossed above upper band → overbought → negative z-score
                let z = ((close - sma) / std_dev).clamp(0.0, 3.0) / 3.0;
                state.last_crossing_score = -z;
            } else {
                // No crossing → decay
                state.last_crossing_score *= BB_SCORE_DECAY;
            }
        }

        state.prev_close = Some(close);
    }

    /// Get BB confluence score for a symbol. Range [-1, +1].
    fn bb_score(&self, symbol: &str) -> f64 {
        self.bb_states
            .get(symbol)
            .map(|s| s.last_crossing_score.clamp(-1.0, 1.0))
            .unwrap_or(0.0)
    }

    // -------------------------------------------------------------------
    // Confluence: MA state update
    // -------------------------------------------------------------------

    fn update_ma_state(&mut self, candle: &Candle) {
        if candle.timeframe != self.ma_timeframe {
            return;
        }
        let close = candle.close.to_string().parse::<f64>().unwrap_or(0.0);
        if close <= 0.0 {
            return;
        }

        let fast_k = 2.0 / (self.ma_fast_period as f64 + 1.0);
        let slow_k = 2.0 / (self.ma_slow_period as f64 + 1.0);

        let state = self
            .ma_states
            .entry(candle.symbol.clone())
            .or_insert(MaState {
                fast_ema: close,
                slow_ema: close,
                fast_initialized: false,
                slow_initialized: false,
                fast_count: 0,
                slow_count: 0,
            });

        state.fast_count += 1;
        state.slow_count += 1;

        if !state.fast_initialized {
            state.fast_ema += (close - state.fast_ema) / state.fast_count as f64;
            if state.fast_count >= self.ma_fast_period {
                state.fast_initialized = true;
            }
        } else {
            state.fast_ema = close * fast_k + state.fast_ema * (1.0 - fast_k);
        }

        if !state.slow_initialized {
            state.slow_ema += (close - state.slow_ema) / state.slow_count as f64;
            if state.slow_count >= self.ma_slow_period {
                state.slow_initialized = true;
            }
        } else {
            state.slow_ema = close * slow_k + state.slow_ema * (1.0 - slow_k);
        }
    }

    /// Get MA confluence score for a symbol. Range [-1, +1].
    /// Positive = bullish (fast > slow), negative = bearish.
    fn ma_score(&self, symbol: &str) -> f64 {
        self.ma_states
            .get(symbol)
            .and_then(|s| {
                if !s.fast_initialized || !s.slow_initialized {
                    return None;
                }
                if s.slow_ema.abs() < 1e-12 {
                    return None;
                }
                let spread_pct = (s.fast_ema - s.slow_ema) / s.slow_ema;
                // Scale: 1% spread → ~0.5 score, cap at ±1.0
                Some((spread_pct * 50.0).clamp(-1.0, 1.0))
            })
            .unwrap_or(0.0)
    }

    // -------------------------------------------------------------------
    // Cooldown management
    // -------------------------------------------------------------------

    fn decrement_cooldowns(&mut self, symbol: &str) {
        if let Some(state) = self.symbol_states.get_mut(symbol) {
            if state.cooldown_remaining > 0 {
                state.cooldown_remaining -= 1;
            }
        }
    }

    // -------------------------------------------------------------------
    // Exit logic: check open positions for exit conditions
    // -------------------------------------------------------------------

    fn check_exits(&self) -> Vec<TradeSignal> {
        let mut signals = Vec::new();
        for pos in self.open_positions.values() {
            let market = match self.markets.get(&pos.condition_id) {
                Some(m) => m,
                None => continue,
            };
            if let Some(signal) = self.check_exit(market, pos) {
                signals.push(signal);
            }
        }
        signals
    }

    fn check_exit(&self, market: &PolymarketMarket, pos: &OpenPosition) -> Option<TradeSignal> {
        let price = self.latest_prices.get(&market.underlying_symbol)?;
        let spot = price.vwap.to_string().parse::<f64>().ok()?;
        let strike = market.strike.to_string().parse::<f64>().ok()?;
        let time_to_expiry = probability::time_to_expiry_years(market.expiry);
        let volatility = price.volatility;

        // 1. Flatten near expiry (< 1 hour)
        let expiry_years = EXPIRY_FLATTEN_HOURS / (365.25 * 24.0);
        if time_to_expiry > 0.0 && time_to_expiry < expiry_years {
            info!(
                question = %market.question,
                time_to_expiry_hrs = format!("{:.2}", time_to_expiry * 365.25 * 24.0),
                "EXIT: flattening near expiry"
            );
            return Some(self.make_exit_signal(market, pos));
        }

        if volatility <= 0.0 || time_to_expiry <= 0.0 {
            return None;
        }

        // Recalculate current edge
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

        // 2. Edge reversal: we're Long but edge is now negative (or vice versa)
        let edge_flipped = match pos.side {
            Side::Buy => raw_edge < -0.02, // edge flipped against us by >2%
            Side::Sell => raw_edge > 0.02,
        };
        if edge_flipped {
            info!(
                question = %market.question,
                current_edge = format!("{:.1}%", raw_edge * 100.0),
                entry_edge = format!("{:.1}%", pos.entry_edge * 100.0),
                side = %pos.side,
                "EXIT: edge reversed"
            );
            return Some(self.make_exit_signal(market, pos));
        }

        // 3. Stop-loss: adjusted edge deeply negative (entry_edge eroded by >50%)
        let bb = self.bb_score(&market.underlying_symbol);
        let ma = self.ma_score(&market.underlying_symbol);
        let multiplier = (1.0 + self.bb_weight * bb + self.ma_weight * ma).clamp(0.0, 2.0);
        let current_adjusted = raw_edge.abs() * multiplier;
        if current_adjusted < pos.entry_edge * -0.5 {
            info!(
                question = %market.question,
                current_adjusted = format!("{:.1}%", current_adjusted * 100.0),
                entry_edge = format!("{:.1}%", pos.entry_edge * 100.0),
                "EXIT: stop-loss triggered"
            );
            return Some(self.make_exit_signal(market, pos));
        }

        None
    }

    fn make_exit_signal(&self, market: &PolymarketMarket, pos: &OpenPosition) -> TradeSignal {
        // Exit = opposite side
        let exit_side = match pos.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let exit_price = match exit_side {
            Side::Buy => market.implied_prob_yes,
            Side::Sell => market.implied_prob_no,
        };
        // Exit with minimum viable size (will be capped by execution engine)
        let size_usd = self.max_position_usd.min(dec!(100));

        TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side: exit_side,
            size_usd,
            confidence: 0.0, // exit, not a new edge
            price: exit_price,
            metadata: SignalMetadata::Unified {
                estimated_prob: 0.0,
                implied_prob: 0.0,
                raw_edge: 0.0,
                adjusted_edge: 0.0,
                confluence_multiplier: 0.0,
                bb_score: 0.0,
                ma_score: 0.0,
                kelly_fraction: 0.0,
            },
            timestamp: Utc::now(),
        }
    }

    // -------------------------------------------------------------------
    // Core: evaluate all markets on price update
    // -------------------------------------------------------------------

    fn evaluate_all_markets(&self) -> Vec<TradeSignal> {
        let mut signals = Vec::new();
        for market in self.markets.values() {
            if let Some(signal) = self.evaluate_market(market) {
                signals.push(signal);
            }
        }
        signals
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

        // Direction-aware BS probability
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

        // Spread + fee deduction
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
        let total_cost = self.fee_calculator.total_cost(implied_price, half_spread);
        let total_cost_f64 = total_cost.to_f64().unwrap_or(0.0);
        let net_edge = raw_edge.abs() - total_cost_f64;

        // --- Confluence modifier ---
        let bb = self.bb_score(&market.underlying_symbol);
        let ma = self.ma_score(&market.underlying_symbol);
        let multiplier = (1.0 + self.bb_weight * bb + self.ma_weight * ma).clamp(0.0, 2.0);
        let adjusted_edge = net_edge * multiplier;

        // --- Gates ---
        // Threshold
        if adjusted_edge < self.min_edge_threshold {
            info!(
                question = %market.question,
                spot,
                strike,
                direction = ?market.direction,
                estimated_prob = format!("{:.1}%", estimated_prob * 100.0),
                implied_prob = format!("{:.1}%", implied_prob * 100.0),
                net_edge = format!("{:.1}%", net_edge * 100.0),
                adjusted_edge = format!("{:.1}%", adjusted_edge * 100.0),
                multiplier = format!("{:.2}", multiplier),
                bb_score = format!("{:.2}", bb),
                ma_score = format!("{:.2}", ma),
                "below adjusted edge threshold"
            );
            return None;
        }

        // Use raw_edge sign for direction
        let edge_signed = if raw_edge > 0.0 {
            adjusted_edge
        } else {
            -adjusted_edge
        };

        let side = if edge_signed > 0.0 {
            Side::Buy
        } else {
            Side::Sell
        };

        // Position gate
        let sym_state = self.symbol_states.get(&market.underlying_symbol);
        if let Some(ss) = sym_state {
            if ss.cooldown_remaining > 0 {
                debug!(
                    symbol = %market.underlying_symbol,
                    cooldown = ss.cooldown_remaining,
                    "cooldown active, skipping signal"
                );
                return None;
            }
            match (side, ss.position) {
                (Side::Buy, SymbolPosition::Long) | (Side::Sell, SymbolPosition::Short) => {
                    return None; // already positioned
                }
                _ => {}
            }
        }

        // Kelly sizing (scale by capped multiplier)
        let (kelly_frac, prob_for_kelly, implied_for_kelly) = if edge_signed > 0.0 {
            (
                kelly::kelly_fraction(estimated_prob, implied_prob, self.kelly_fraction_cap),
                estimated_prob,
                implied_prob,
            )
        } else {
            (
                kelly::kelly_fraction(
                    1.0 - estimated_prob,
                    1.0 - implied_prob,
                    self.kelly_fraction_cap,
                ),
                1.0 - estimated_prob,
                1.0 - implied_prob,
            )
        };
        let _ = (prob_for_kelly, implied_for_kelly); // used only for kelly call above

        if kelly_frac <= 0.0 {
            return None;
        }

        let kelly_scaled = kelly_frac * multiplier.min(1.5);
        let size_usd = kelly::position_size_usd(
            kelly_scaled,
            self.max_total_exposure_usd,
            self.max_position_usd,
        );

        if size_usd <= Decimal::ZERO {
            return None;
        }

        info!(
            question = %market.question,
            spot,
            strike,
            direction = ?market.direction,
            estimated_prob = format!("{:.1}%", estimated_prob * 100.0),
            implied_prob = format!("{:.1}%", implied_prob * 100.0),
            raw_edge = format!("{:.1}%", net_edge * 100.0),
            adjusted_edge = format!("{:.1}%", adjusted_edge * 100.0),
            multiplier = format!("{:.2}", multiplier),
            bb_score = format!("{:.2}", bb),
            ma_score = format!("{:.2}", ma),
            kelly = format!("{:.1}%", kelly_scaled * 100.0),
            size_usd = %size_usd,
            side = %side,
            "unified signal"
        );

        let signal_price = match side {
            Side::Buy => market.implied_prob_yes,
            Side::Sell => market.implied_prob_no,
        };

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd,
            confidence: adjusted_edge,
            price: signal_price,
            metadata: SignalMetadata::Unified {
                estimated_prob,
                implied_prob,
                raw_edge: net_edge,
                adjusted_edge,
                confluence_multiplier: multiplier,
                bb_score: bb,
                ma_score: ma,
                kelly_fraction: kelly_scaled,
            },
            timestamp: Utc::now(),
        })
    }
}

impl Strategy for UnifiedStrategy {
    fn name(&self) -> &str {
        "unified"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: true,
            candles: true,
            execution_feedback: true,
            polymarket_updates: true,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::PriceUpdate(price) => {
                self.latest_prices.insert(price.symbol.clone(), price);

                // Check exits first
                let mut signals = self.check_exits();
                // Remove exited positions
                let exit_condition_ids: Vec<String> = signals
                    .iter()
                    .filter_map(|s| {
                        if let TradeTarget::Polymarket(ref m) = s.target {
                            Some(m.condition_id.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                for cid in &exit_condition_ids {
                    if let Some(pos) = self.open_positions.remove(cid) {
                        // Reset symbol state to Flat
                        if let Some(market) = self.markets.get(cid) {
                            if let Some(ss) = self.symbol_states.get_mut(&market.underlying_symbol)
                            {
                                ss.position = SymbolPosition::Flat;
                                ss.cooldown_remaining = self.cooldown_candles;
                            }
                        }
                        info!(condition_id = %cid, side = %pos.side, "position closed");
                    }
                }

                // Then evaluate new entries
                let entry_signals = self.evaluate_all_markets();
                // Track new positions
                for sig in &entry_signals {
                    if let TradeTarget::Polymarket(ref m) = sig.target {
                        let ss = self
                            .symbol_states
                            .entry(m.underlying_symbol.clone())
                            .or_insert(SymbolState {
                                position: SymbolPosition::Flat,
                                cooldown_remaining: 0,
                            });
                        ss.position = match sig.side {
                            Side::Buy => SymbolPosition::Long,
                            Side::Sell => SymbolPosition::Short,
                        };
                        ss.cooldown_remaining = self.cooldown_candles;

                        self.open_positions.insert(
                            m.condition_id.clone(),
                            OpenPosition {
                                condition_id: m.condition_id.clone(),
                                side: sig.side,
                                entry_edge: sig.confidence,
                            },
                        );
                    }
                }
                signals.extend(entry_signals);
                signals
            }
            StrategyEvent::CandleComplete(candle) => {
                // Decrement cooldown on each candle tick
                let symbol = candle.symbol.clone();
                self.decrement_cooldowns(&symbol);
                self.update_bb_state(&candle);
                self.update_ma_state(&candle);
                Vec::new() // candles never produce signals directly
            }
            StrategyEvent::PolymarketUpdate(update) => {
                match update {
                    PolymarketUpdate::MarketsDiscovered(new_markets) => {
                        info!(count = new_markets.len(), "unified: markets discovered");
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
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::candle::Candle;
    use chrono::TimeZone;

    fn make_config() -> Config {
        Config {
            mode: crate::config::RunMode::Paper,
            strategy: crate::config::StrategyName::Unified,
            backtest_file: String::new(),
            kraken_api_key: String::new(),
            kraken_api_secret: String::new(),
            coinbase_api_key: String::new(),
            coinbase_api_secret: String::new(),
            polymarket_private_key: String::new(),
            polygon_rpc_url: String::new(),
            symbols: vec!["BTC-USD".into()],
            min_edge_threshold: Decimal::new(3, 2), // 0.03
            kelly_fraction_cap: Decimal::new(5, 1), // 0.5
            volatility_window_hours: 24,
            max_position_usd: Decimal::from(1000),
            max_total_exposure_usd: Decimal::from(5000),
            max_drawdown_pct: Decimal::new(10, 2),
            max_orders_per_minute: 10,
            ma_fast_period: 3,
            ma_slow_period: 5,
            ma_timeframe: "5m".into(),
            ma_size_usd: Decimal::from(500),
            bb_period: 5,
            bb_num_std: 2.0,
            bb_timeframe: "5m".into(),
            bb_size_usd: Decimal::from(500),
            bb_cooldown_candles: 3,
            fee_rate_bps: 0, // zero fees in tests for clean edge math
            maker_mode: true,
            heartbeat_interval_secs: 10,
            ws_book_max_stale_secs: 30,
            log_level: "info".into(),
            stale_feed_timeout_secs: 30,
            unified_bb_weight: 0.4,
            unified_ma_weight: 0.4,
        }
    }

    fn make_market(symbol: &str, strike: f64, direction: MarketDirection) -> PolymarketMarket {
        PolymarketMarket {
            condition_id: format!("cond-{}", symbol),
            token_id_yes: format!("yes-{}", symbol),
            token_id_no: format!("no-{}", symbol),
            question: format!("Will {} hit ${}?", symbol, strike),
            underlying_symbol: symbol.to_string(),
            strike: Decimal::from_f64_retain(strike).unwrap(),
            expiry: Utc::now() + chrono::Duration::days(7),
            implied_prob_yes: dec!(0.40), // market says 40% yes
            implied_prob_no: dec!(0.60),
            direction,
        }
    }

    fn make_price(symbol: &str, vwap: f64) -> AggregatedPrice {
        use crate::types::market::Exchange;
        AggregatedPrice {
            symbol: symbol.to_string(),
            vwap: Decimal::from_f64_retain(vwap).unwrap(),
            best_bid: Decimal::from_f64_retain(vwap - 1.0).unwrap(),
            best_ask: Decimal::from_f64_retain(vwap + 1.0).unwrap(),
            best_bid_exchange: Exchange::Kraken,
            best_ask_exchange: Exchange::Coinbase,
            spread: dec!(2.0),
            num_feeds: 3,
            timestamp: Utc::now(),
            volatility: 0.5, // 50% annualized vol
        }
    }

    fn make_candle(symbol: &str, close: f64, ts_secs: i64) -> Candle {
        let close_dec = Decimal::from_f64_retain(close).unwrap();
        Candle {
            symbol: symbol.to_string(),
            timeframe: Timeframe::M5,
            open: close_dec,
            high: close_dec,
            low: close_dec,
            close: close_dec,
            volume: Decimal::from(100),
            tick_count: 10,
            open_time: Utc.timestamp_opt(ts_secs, 0).unwrap(),
            close_time: Utc.timestamp_opt(ts_secs + 300, 0).unwrap(),
        }
    }

    fn inject_market(strat: &mut UnifiedStrategy, market: PolymarketMarket) {
        strat.markets.insert(market.condition_id.clone(), market);
    }

    // -----------------------------------------------------------------------
    // 1. Pure BS when indicators are cold
    // -----------------------------------------------------------------------
    #[test]
    fn test_pure_bs_when_no_candles() {
        let mut strat = UnifiedStrategy::new(&make_config());
        // Strike 90k, spot 100k → high prob above → edge vs 40% implied
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        inject_market(&mut strat, market);

        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));

        // Should produce a signal — no candle data means multiplier = 1.0
        assert!(!signals.is_empty(), "BS should fire without candle data");
        if let SignalMetadata::Unified {
            confluence_multiplier,
            bb_score,
            ma_score,
            ..
        } = &signals[0].metadata
        {
            assert!(
                (*confluence_multiplier - 1.0).abs() < 0.01,
                "multiplier should be ~1.0 when indicators cold, got {}",
                confluence_multiplier
            );
            assert!(bb_score.abs() < 0.01);
            assert!(ma_score.abs() < 0.01);
        } else {
            panic!("expected Unified metadata");
        }
    }

    // -----------------------------------------------------------------------
    // 2. Confluence boosts buy — both BB and MA agree bullish
    // -----------------------------------------------------------------------
    #[test]
    fn test_confluence_boosts_buy() {
        let mut config = make_config();
        config.unified_ma_weight = 0.0; // isolate BB effect
        let mut strat = UnifiedStrategy::new(&config);
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        inject_market(&mut strat, market);

        // Feed BB candles: fill window then drop (oversold crossing → positive bb_score)
        let bb_prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 80.0];
        for (i, &p) in bb_prices.iter().enumerate() {
            strat.on_event(StrategyEvent::CandleComplete(make_candle(
                "BTC-USD",
                p,
                (i as i64) * 300,
            )));
        }

        // Now fire a price update
        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));

        assert!(!signals.is_empty(), "should emit boosted signal");
        if let SignalMetadata::Unified {
            confluence_multiplier,
            bb_score,
            ..
        } = &signals[0].metadata
        {
            assert!(*bb_score > 0.0, "bb_score should be positive (oversold)");
            assert!(
                *confluence_multiplier > 1.0,
                "multiplier should be > 1.0 with positive BB confluence, got {}",
                confluence_multiplier
            );
        } else {
            panic!("expected Unified metadata");
        }
    }

    // -----------------------------------------------------------------------
    // 3. Confluence suppresses conflicting signal (BB overbought vs BS buy)
    // -----------------------------------------------------------------------
    #[test]
    fn test_confluence_suppresses_conflict() {
        let mut config = make_config();
        config.min_edge_threshold = Decimal::new(5, 2); // 0.05
        config.unified_ma_weight = 0.0; // isolate BB effect
        let mut strat = UnifiedStrategy::new(&config);

        // Market with moderate edge
        let mut market = make_market("BTC-USD", 95_000.0, MarketDirection::Bullish);
        market.implied_prob_yes = dec!(0.45);
        market.implied_prob_no = dec!(0.55);
        inject_market(&mut strat, market);

        // Feed BB candles that cross ABOVE upper band → overbought → negative bb_score
        let bb_prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 120.0];
        for (i, &p) in bb_prices.iter().enumerate() {
            strat.on_event(StrategyEvent::CandleComplete(make_candle(
                "BTC-USD",
                p,
                (i as i64) * 300,
            )));
        }

        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));

        // With negative BB score suppressing, multiplier should be < 1.0
        if signals.is_empty() {
            // Suppressed entirely — good
        } else if let SignalMetadata::Unified {
            confluence_multiplier,
            ..
        } = &signals[0].metadata
        {
            assert!(
                *confluence_multiplier < 1.0,
                "multiplier should be < 1.0 on conflict, got {}",
                confluence_multiplier
            );
        }
    }

    // -----------------------------------------------------------------------
    // 4. Cooldown prevents rapid signals
    // -----------------------------------------------------------------------
    #[test]
    fn test_cooldown_prevents_rapid_signals() {
        let mut strat = UnifiedStrategy::new(&make_config());
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        inject_market(&mut strat, market);

        // First price update → signal
        let s1 = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        assert!(!s1.is_empty(), "first signal should fire");

        // Immediately another price update → cooldown should block
        let s2 = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        assert!(s2.is_empty(), "cooldown should prevent second signal");

        // Feed candles to tick down cooldown (cooldown_candles = 3)
        for i in 0..3 {
            strat.on_event(StrategyEvent::CandleComplete(make_candle(
                "BTC-USD",
                100.0,
                (i as i64) * 300,
            )));
        }

        // Reset position to Flat so we can get another signal
        if let Some(ss) = strat.symbol_states.get_mut("BTC-USD") {
            ss.position = SymbolPosition::Flat;
        }

        // After cooldown → signal should fire again
        let s3 = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        assert!(!s3.is_empty(), "signal should fire after cooldown expires");
    }

    // -----------------------------------------------------------------------
    // 5. No duplicate long
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_duplicate_long() {
        let mut strat = UnifiedStrategy::new(&make_config());
        strat.cooldown_candles = 0; // disable cooldown to isolate position check
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        inject_market(&mut strat, market);

        let s1 = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        assert!(!s1.is_empty());
        assert_eq!(s1[0].side, Side::Buy);

        // Second update — already Long, no Buy
        let s2 = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        assert!(s2.is_empty(), "no duplicate buy when already long");
    }

    // -----------------------------------------------------------------------
    // 6. Polymarket-only signals
    // -----------------------------------------------------------------------
    #[test]
    fn test_polymarket_only() {
        let mut strat = UnifiedStrategy::new(&make_config());
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        inject_market(&mut strat, market);

        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));

        for sig in &signals {
            assert!(
                matches!(sig.target, TradeTarget::Polymarket(_)),
                "all signals must target Polymarket"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 7. BB score decays over neutral candles
    // -----------------------------------------------------------------------
    #[test]
    fn test_bb_score_decays() {
        let mut strat = UnifiedStrategy::new(&make_config());

        // Trigger a crossing: fill window then drop
        let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 80.0];
        for (i, &p) in prices.iter().enumerate() {
            strat.on_event(StrategyEvent::CandleComplete(make_candle(
                "BTC-USD",
                p,
                (i as i64) * 300,
            )));
        }

        let initial = strat.bb_score("BTC-USD");
        assert!(
            initial > 0.0,
            "should have positive score after oversold crossing"
        );

        // Feed neutral candles (within bands) — enough for decay to be significant
        for i in 0..10 {
            strat.on_event(StrategyEvent::CandleComplete(make_candle(
                "BTC-USD",
                90.0,
                ((i + 10) as i64) * 300,
            )));
        }

        let decayed = strat.bb_score("BTC-USD");
        assert!(
            decayed < initial * 0.5,
            "score should decay significantly: initial={}, decayed={}",
            initial,
            decayed
        );
    }

    // -----------------------------------------------------------------------
    // 8. Market discovery via PolymarketUpdate
    // -----------------------------------------------------------------------
    #[test]
    fn test_market_discovery() {
        let mut strat = UnifiedStrategy::new(&make_config());
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);

        // Discover via event
        strat.on_event(StrategyEvent::PolymarketUpdate(
            PolymarketUpdate::MarketsDiscovered(vec![market]),
        ));
        assert_eq!(strat.markets.len(), 1, "market should be stored");

        // Price update should evaluate it
        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        assert!(
            !signals.is_empty(),
            "discovered market should produce signal"
        );
    }

    // -----------------------------------------------------------------------
    // 9. Bearish direction uses 1-prob
    // -----------------------------------------------------------------------
    #[test]
    fn test_bearish_direction() {
        let mut strat = UnifiedStrategy::new(&make_config());
        // Bearish market: "Will BTC drop below 110k?"
        // spot 100k, strike 110k → prob_above(100k, 110k) < 0.5
        // Bearish → estimated_prob = 1 - prob_above → high probability
        let mut market = make_market("BTC-USD", 110_000.0, MarketDirection::Bearish);
        market.implied_prob_yes = dec!(0.40); // market underprices YES
        market.implied_prob_no = dec!(0.60);
        inject_market(&mut strat, market);

        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));

        assert!(!signals.is_empty(), "bearish market should produce signal");
        // Since estimated_prob (bearish) > implied 0.40 → Buy
        assert_eq!(signals[0].side, Side::Buy);
    }

    // -----------------------------------------------------------------------
    // 10. Fee deduction reduces edge before confluence
    // -----------------------------------------------------------------------
    #[test]
    fn test_fee_deduction() {
        let mut config = make_config();
        config.fee_rate_bps = 156; // real fee rate
        let mut strat = UnifiedStrategy::new(&config);

        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        inject_market(&mut strat, market);

        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));

        if let Some(sig) = signals.first() {
            if let SignalMetadata::Unified {
                raw_edge,
                adjusted_edge,
                ..
            } = &sig.metadata
            {
                // raw_edge should already be net of fees (smaller than gross edge)
                // adjusted_edge = raw_edge * multiplier
                assert!(
                    *raw_edge > 0.0,
                    "net edge should still be positive after fees"
                );
                // With multiplier ~1.0, adjusted should be close to raw
                assert!(
                    (*adjusted_edge - *raw_edge).abs() < *raw_edge * 0.5,
                    "adjusted should be close to raw when multiplier ~1.0"
                );
            }
        }
        // The key test: with fees, the threshold is harder to beat
        // A tiny-edge market should be suppressed
        let mut config2 = make_config();
        config2.fee_rate_bps = 156;
        config2.min_edge_threshold = Decimal::new(50, 2); // 0.50 — very high threshold
        let mut strat2 = UnifiedStrategy::new(&config2);
        let mut market2 = make_market("ETH-USD", 3_000.0, MarketDirection::Bullish);
        market2.implied_prob_yes = dec!(0.49); // almost no edge
        market2.implied_prob_no = dec!(0.51);
        inject_market(&mut strat2, market2);

        let signals2 = strat2.on_event(StrategyEvent::PriceUpdate(make_price("ETH-USD", 3_500.0)));
        assert!(
            signals2.is_empty(),
            "tiny edge should be killed by fees + high threshold"
        );
    }

    // -----------------------------------------------------------------------
    // 11. Exit on expiry approach
    // -----------------------------------------------------------------------
    #[test]
    fn test_exit_near_expiry() {
        let mut strat = UnifiedStrategy::new(&make_config());
        // Market expiring in 30 minutes (< EXPIRY_FLATTEN_HOURS = 1h)
        let mut market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        market.expiry = Utc::now() + chrono::Duration::minutes(30);
        let cid = market.condition_id.clone();
        inject_market(&mut strat, market);

        // Simulate an open position
        strat.open_positions.insert(
            cid.clone(),
            OpenPosition {
                condition_id: cid,
                side: Side::Buy,
                entry_edge: 0.10,
            },
        );

        // Price update should trigger exit
        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        let exits: Vec<_> = signals.iter().filter(|s| s.confidence == 0.0).collect();
        assert!(!exits.is_empty(), "should produce exit signal near expiry");
        assert_eq!(exits[0].side, Side::Sell, "exit for Buy position = Sell");
    }

    // -----------------------------------------------------------------------
    // 12. Exit on edge reversal
    // -----------------------------------------------------------------------
    #[test]
    fn test_exit_edge_reversal() {
        let mut strat = UnifiedStrategy::new(&make_config());
        // Market: "Will BTC hit $120k?" bullish, implied 30%
        // At spot=100k, with high vol BS estimates maybe ~40% → edge = +10% → Buy
        let mut market = make_market("BTC-USD", 120_000.0, MarketDirection::Bullish);
        market.implied_prob_yes = dec!(0.30);
        market.implied_prob_no = dec!(0.70);
        let cid = market.condition_id.clone();
        inject_market(&mut strat, market);

        // Manually create an open position (simulating a previous entry)
        strat.open_positions.insert(
            cid.clone(),
            OpenPosition {
                condition_id: cid.clone(),
                side: Side::Buy,
                entry_edge: 0.10,
            },
        );
        strat.symbol_states.insert(
            "BTC-USD".into(),
            SymbolState {
                position: SymbolPosition::Long,
                cooldown_remaining: 100,
            },
        );

        // Now flip implied to 99% — BS estimates ~40% but market says 99%
        // edge = 0.40 - 0.99 = -0.59, well below -0.02 threshold
        if let Some(m) = strat.markets.get_mut(&cid) {
            m.implied_prob_yes = dec!(0.99);
            m.implied_prob_no = dec!(0.01);
        }

        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        let exits: Vec<_> = signals.iter().filter(|s| s.confidence == 0.0).collect();
        assert!(
            !exits.is_empty(),
            "should exit when edge reverses against position"
        );
        assert_eq!(exits[0].side, Side::Sell, "exit for Buy = Sell");
    }

    // -----------------------------------------------------------------------
    // 13. No exit when position is healthy
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_exit_when_healthy() {
        let mut strat = UnifiedStrategy::new(&make_config());
        let market = make_market("BTC-USD", 90_000.0, MarketDirection::Bullish);
        let cid = market.condition_id.clone();
        inject_market(&mut strat, market);

        // Open a position
        strat.open_positions.insert(
            cid.clone(),
            OpenPosition {
                condition_id: cid,
                side: Side::Buy,
                entry_edge: 0.10,
            },
        );
        // Mark as Long so entry doesn't fire again
        strat.symbol_states.insert(
            "BTC-USD".into(),
            SymbolState {
                position: SymbolPosition::Long,
                cooldown_remaining: 100,
            },
        );

        // Healthy market — edge still positive, far from expiry
        let signals = strat.on_event(StrategyEvent::PriceUpdate(make_price("BTC-USD", 100_000.0)));
        let exits: Vec<_> = signals.iter().filter(|s| s.confidence == 0.0).collect();
        assert!(exits.is_empty(), "no exit when position is healthy");
    }
}
