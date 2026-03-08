use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::kelly;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::events::{
    MlDirection, MlPrediction, PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget,
};
use crate::types::market::{AggregatedPrice, MarketType, PolymarketMarket};
use crate::types::order::Side;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tracing::{debug, info};

/// Hedged LP strategy for 5-minute UpDown markets across BTC, ETH, SOL, XRP.
///
/// Every cycle: scan markets → compute EV → check hedge → size position → execute.
///
/// Core formulas:
///   Hedge profit:    π = 1 − (pᵧ + pₙ)
///   Reward score:    S(s) = ((v − s)² / v²) · b
///   Kelly sizing:    f* = (p·b − q) / b
///   Expected return: E[R] = (Rewards − L_fill) / C_risk
pub struct HedgedLpStrategy {
    // Market state
    markets: HashMap<String, PolymarketMarket>,
    latest_prices: HashMap<String, AggregatedPrice>,

    // Orderbook access
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,

    // Position sizing
    kelly_fraction_cap: f64,
    max_position_usd: Decimal,
    bankroll_usd: Decimal,
    fee_calculator: FeeCalculator,

    // Hedge parameters
    /// Minimum hedge profit π to execute a hedged trade
    min_hedge_profit: f64,

    // Reward model parameters
    /// Base reward rate (b) — estimated reward per dollar of liquidity
    reward_rate: f64,
    /// Estimated fill risk as fraction of position (L_fill / C_risk)
    fill_risk_fraction: f64,

    // ML integration
    latest_ml: HashMap<String, MlPrediction>,

    // Directional parameters
    min_edge_threshold: f64,

    // Track active condition_ids to avoid double-trading
    active_conditions: HashMap<String, DateTime<Utc>>,

    // Start prices for UpDown markets (condition_id → VWAP at first discovery)
    updown_start_prices: HashMap<String, f64>,

    // Rate-limit diagnostic logging (condition_id → last log time)
    last_eval_log: HashMap<String, DateTime<Utc>>,
}

impl HedgedLpStrategy {
    pub fn new(config: &Config) -> Self {
        Self {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            book_cache: None,
            ws_book_max_stale_secs: config.ws_book_max_stale_secs,
            kelly_fraction_cap: config.kelly_fraction_cap.to_f64().unwrap_or(0.5),
            max_position_usd: config.max_position_usd,
            bankroll_usd: config.max_total_exposure_usd,
            fee_calculator: FeeCalculator::new(config.fee_rate_bps),
            min_hedge_profit: f64_env("HEDGE_MIN_PROFIT", 0.005), // 0.5% minimum
            reward_rate: f64_env("HEDGE_REWARD_RATE", 0.02),      // 2% base reward
            fill_risk_fraction: f64_env("HEDGE_FILL_RISK", 0.3),  // 30% fill risk
            latest_ml: HashMap::new(),
            min_edge_threshold: config.min_edge_threshold.to_f64().unwrap_or(0.03),
            active_conditions: HashMap::new(),
            updown_start_prices: HashMap::new(),
            last_eval_log: HashMap::new(),
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    // -----------------------------------------------------------------------
    // Core formulas
    // -----------------------------------------------------------------------

    /// Hedge condition: π = 1 − (pᵧ + pₙ)
    /// If pᵧ + pₙ < 1, buying both YES and NO locks in π profit per share.
    fn hedge_profit(yes_ask: f64, no_ask: f64) -> f64 {
        1.0 - (yes_ask + no_ask)
    }

    /// Reward score: S(s) = ((v − s)² / v²) · b
    /// Estimates liquidity reward based on distance from mid-price.
    /// v = half the price range (0.5 for binary), s = distance from midpoint.
    fn reward_score(distance_from_mid: f64, reward_rate: f64) -> f64 {
        let v = 0.5; // max distance in a 0-1 probability market
        let s = distance_from_mid.abs().min(v);
        ((v - s) * (v - s) / (v * v)) * reward_rate
    }

    /// Expected return: E[R] = (Rewards − L_fill) / C_risk
    /// Only enter if E[R] > 0.
    fn expected_return(reward: f64, fill_risk: f64, capital_at_risk: f64) -> f64 {
        if capital_at_risk <= 0.0 {
            return 0.0;
        }
        (reward - fill_risk) / capital_at_risk
    }

    // -----------------------------------------------------------------------
    // Market evaluation
    // -----------------------------------------------------------------------

    fn evaluate_market(&mut self, market: &PolymarketMarket) -> Vec<TradeSignal> {
        let mut signals = Vec::new();

        // Skip if we already have an active position on this condition
        if self.active_conditions.contains_key(&market.condition_id) {
            return signals;
        }

        // Only UpDown markets
        let (window_start_ts, window_secs) = match market.market_type {
            MarketType::UpDown {
                window_start_ts,
                window_secs,
            } => (window_start_ts, window_secs),
            _ => return signals,
        };

        let now = Utc::now().timestamp();
        let time_remaining = (window_start_ts + window_secs as i64) - now;

        // Skip if <30s remaining or already expired
        if time_remaining < 30 {
            return signals;
        }

        // Get book data for this market
        let (yes_bid, yes_ask, no_bid, no_ask, _book_depth_yes, _book_depth_no) =
            self.get_book_prices(market);

        let yes_ask_f64 = yes_ask.to_f64().unwrap_or(1.0);
        let no_ask_f64 = no_ask.to_f64().unwrap_or(1.0);
        let yes_bid_f64 = yes_bid.to_f64().unwrap_or(0.0);
        let _no_bid_f64 = no_bid.to_f64().unwrap_or(0.0);

        // === CHECK 1: Hedge opportunity ===
        let hedge_pi = Self::hedge_profit(yes_ask_f64, no_ask_f64);

        if hedge_pi >= self.min_hedge_profit {
            // Locked profit! Buy both sides.
            let fee_yes = self.fee_calculator.taker_fee(yes_ask);
            let fee_no = self.fee_calculator.taker_fee(no_ask);
            let total_fee = fee_yes + fee_no;
            let net_pi = hedge_pi - total_fee.to_f64().unwrap_or(0.0);

            if net_pi > 0.0 {
                let size = self.max_position_usd.min(self.bankroll_usd / Decimal::TWO);

                info!(
                    condition_id = %market.condition_id,
                    symbol = %market.underlying_symbol,
                    yes_ask = %yes_ask,
                    no_ask = %no_ask,
                    hedge_profit = format!("{:.4}", hedge_pi),
                    net_profit = format!("{:.4}", net_pi),
                    "HEDGE opportunity — locked profit"
                );

                // Buy YES
                signals.push(TradeSignal {
                    target: TradeTarget::Polymarket(market.clone()),
                    side: Side::Buy,
                    size_usd: size,
                    confidence: 1.0,
                    price: yes_ask,
                    metadata: SignalMetadata::HedgedLp {
                        hedge_profit: hedge_pi,
                        net_hedge_profit: net_pi,
                        reward_score: 0.0,
                        expected_return: net_pi,
                        kelly_fraction: 1.0, // max conviction for arb
                        yes_ask: yes_ask_f64,
                        no_ask: no_ask_f64,
                        is_hedge: true,
                    },
                    timestamp: Utc::now(),
                    is_exit: false,
                });

                // Buy NO (create a paired market for the other side)
                let no_market = PolymarketMarket {
                    token_id_yes: market.token_id_no.clone(),
                    token_id_no: market.token_id_yes.clone(),
                    implied_prob_yes: market.implied_prob_no,
                    implied_prob_no: market.implied_prob_yes,
                    ..market.clone()
                };
                signals.push(TradeSignal {
                    target: TradeTarget::Polymarket(no_market),
                    side: Side::Buy,
                    size_usd: size,
                    confidence: 1.0,
                    price: no_ask,
                    metadata: SignalMetadata::HedgedLp {
                        hedge_profit: hedge_pi,
                        net_hedge_profit: net_pi,
                        reward_score: 0.0,
                        expected_return: net_pi,
                        kelly_fraction: 1.0,
                        yes_ask: yes_ask_f64,
                        no_ask: no_ask_f64,
                        is_hedge: true,
                    },
                    timestamp: Utc::now(),
                    is_exit: false,
                });

                self.active_conditions
                    .insert(market.condition_id.clone(), Utc::now());
                return signals;
            }
        }

        // === CHECK 2: Directional trade with reward model ===
        let implied_prob = market.implied_prob_yes.to_f64().unwrap_or(0.5);
        let estimated_prob = self.estimate_probability(market);

        let edge = kelly::edge(estimated_prob, implied_prob);

        // Reward score: distance from mid
        let mid = (yes_bid_f64 + yes_ask_f64) / 2.0;
        let distance = (implied_prob - mid).abs();
        let reward = Self::reward_score(distance, self.reward_rate);

        // Kelly sizing
        let kelly_f = if edge > 0.0 {
            kelly::kelly_fraction(estimated_prob, implied_prob, self.kelly_fraction_cap)
        } else {
            0.0
        };

        // Expected return: E[R] = (Rewards − L_fill) / C_risk
        let capital_at_risk = if edge > 0.0 {
            implied_prob * (1.0 - estimated_prob) // probability-weighted loss
        } else {
            1.0
        };
        let fill_risk = self.fill_risk_fraction * capital_at_risk;
        let er = Self::expected_return(reward + edge, fill_risk, capital_at_risk);

        // Diagnostic log (rate-limited to 1 per 10s per market)
        let should_log = self
            .last_eval_log
            .get(&market.condition_id)
            .map_or(true, |last| (Utc::now() - *last).num_seconds() >= 10);
        if should_log {
            info!(
                symbol = %market.underlying_symbol,
                implied_prob = format!("{:.3}", implied_prob),
                estimated_prob = format!("{:.3}", estimated_prob),
                edge = format!("{:.4}", edge),
                expected_return = format!("{:.4}", er),
                reward = format!("{:.4}", reward),
                hedge_pi = format!("{:.4}", hedge_pi),
                yes_ask = format!("{:.3}", yes_ask_f64),
                no_ask = format!("{:.3}", no_ask_f64),
                time_remaining_s = time_remaining,
                "EVAL"
            );
            self.last_eval_log
                .insert(market.condition_id.clone(), Utc::now());
        }

        // Gate: only trade if E[R] > 0 and edge exceeds threshold
        if er > 0.0 && edge.abs() >= self.min_edge_threshold {
            let (side, price) = if edge > 0.0 {
                (Side::Buy, yes_ask) // buy YES (underpriced)
            } else {
                (Side::Buy, no_ask) // buy NO (YES overpriced → bet Down)
            };

            let size = kelly::position_size_usd(kelly_f, self.bankroll_usd, self.max_position_usd);

            if size > Decimal::ZERO {
                let trade_market = if edge > 0.0 {
                    market.clone()
                } else {
                    // Swap to NO side
                    PolymarketMarket {
                        token_id_yes: market.token_id_no.clone(),
                        token_id_no: market.token_id_yes.clone(),
                        implied_prob_yes: market.implied_prob_no,
                        implied_prob_no: market.implied_prob_yes,
                        ..market.clone()
                    }
                };

                info!(
                    condition_id = %market.condition_id,
                    symbol = %market.underlying_symbol,
                    direction = if edge > 0.0 { "UP" } else { "DOWN" },
                    edge = format!("{:.4}", edge),
                    reward_score = format!("{:.4}", reward),
                    expected_return = format!("{:.4}", er),
                    kelly = format!("{:.4}", kelly_f),
                    size_usd = %size,
                    "DIRECTIONAL trade — positive EV"
                );

                signals.push(TradeSignal {
                    target: TradeTarget::Polymarket(trade_market),
                    side,
                    size_usd: size,
                    confidence: edge.abs(),
                    price,
                    metadata: SignalMetadata::HedgedLp {
                        hedge_profit: hedge_pi,
                        net_hedge_profit: hedge_pi
                            - self.fee_calculator.taker_fee(yes_ask).to_f64().unwrap_or(0.0)
                            - self.fee_calculator.taker_fee(no_ask).to_f64().unwrap_or(0.0),
                        reward_score: reward,
                        expected_return: er,
                        kelly_fraction: kelly_f,
                        yes_ask: yes_ask_f64,
                        no_ask: no_ask_f64,
                        is_hedge: false,
                    },
                    timestamp: Utc::now(),
                    is_exit: false,
                });

                self.active_conditions
                    .insert(market.condition_id.clone(), Utc::now());
            }
        } else {
            debug!(
                condition_id = %market.condition_id,
                symbol = %market.underlying_symbol,
                edge = format!("{:.4}", edge),
                expected_return = format!("{:.4}", er),
                "SKIP — negative EV or insufficient edge"
            );
        }

        signals
    }

    /// Estimate probability of UP outcome using available signals.
    ///
    /// Signal hierarchy:
    /// 1. Spot price vs start price (strongest for UpDown — actual price movement)
    /// 2. ML prediction (if available and fresh)
    /// 3. Orderbook imbalance (if book cache available)
    fn estimate_probability(&self, market: &PolymarketMarket) -> f64 {
        let implied = market.implied_prob_yes.to_f64().unwrap_or(0.5);

        // For UpDown markets, use spot price vs start price as primary signal
        let mut prob = if let MarketType::UpDown {
            window_start_ts,
            window_secs,
        } = market.market_type
        {
            let start_price = self
                .updown_start_prices
                .get(&market.condition_id)
                .copied()
                .unwrap_or(0.0);
            if let Some(agg) = self.latest_prices.get(&market.underlying_symbol) {
                let spot = agg.vwap.to_f64().unwrap_or(start_price);
                if start_price > 0.0 {
                    let pct_move = (spot - start_price) / start_price;
                    // Map price movement to probability via logistic function
                    // Larger moves → higher conviction
                    let now = Utc::now().timestamp();
                    let time_remaining =
                        (window_start_ts + window_secs as i64 - now).max(1) as f64;
                    let time_fraction = time_remaining / window_secs as f64;

                    // Scale: as time runs out, current direction becomes more certain
                    let conviction = if time_fraction < 0.3 {
                        3.0 // very confident near expiry
                    } else {
                        1.5
                    };

                    // Logistic: 1 / (1 + exp(-k * pct_move))
                    // k scales with conviction
                    let k = conviction * 500.0; // 500 = sensitivity to 0.1% moves
                    1.0 / (1.0 + (-k * pct_move).exp())
                } else {
                    implied
                }
            } else {
                implied
            }
        } else {
            implied
        };

        // Incorporate ML prediction if available
        if let Some(ml) = self.latest_ml.get(&market.underlying_symbol) {
            let ml_stale = (Utc::now() - ml.timestamp).num_seconds() > 300;
            if !ml_stale {
                let ml_prob = match ml.direction {
                    MlDirection::Up => 0.5 + ml.confidence * 0.5,
                    MlDirection::Down => 0.5 - ml.confidence * 0.5,
                };
                // Blend: 70% price-based, 30% ML
                prob = 0.7 * prob + 0.3 * ml_prob;
            }
        }

        // Incorporate orderbook imbalance
        if let Some(ref cache) = self.book_cache {
            if let Ok(books) = cache.try_read() {
                if let Some(snap) = books.get(&market.token_id_yes) {
                    if snap.is_fresh(self.ws_book_max_stale_secs) {
                        let depth_ratio = if snap.depth_ask > Decimal::ZERO {
                            snap.depth_bid.to_f64().unwrap_or(1.0)
                                / snap.depth_ask.to_f64().unwrap_or(1.0)
                        } else {
                            1.0
                        };
                        let book_signal = (depth_ratio - 1.0).clamp(-0.1, 0.1);
                        prob += book_signal * 0.5;
                    }
                }
            }
        }

        prob.clamp(0.01, 0.99)
    }

    /// Get best bid/ask from book cache, falling back to market implied prices.
    fn get_book_prices(
        &self,
        market: &PolymarketMarket,
    ) -> (Decimal, Decimal, Decimal, Decimal, Decimal, Decimal) {
        let mut yes_bid = market.implied_prob_yes;
        let mut yes_ask = market.implied_prob_yes;
        let mut no_bid = market.implied_prob_no;
        let mut no_ask = market.implied_prob_no;
        let mut depth_yes = Decimal::ZERO;
        let mut depth_no = Decimal::ZERO;

        if let Some(ref cache) = self.book_cache {
            if let Ok(books) = cache.try_read() {
                if let Some(snap) = books.get(&market.token_id_yes) {
                    if snap.is_fresh(self.ws_book_max_stale_secs) {
                        yes_bid = snap.best_bid;
                        yes_ask = snap.best_ask;
                        depth_yes = snap.depth_bid + snap.depth_ask;
                    }
                }
                if let Some(snap) = books.get(&market.token_id_no) {
                    if snap.is_fresh(self.ws_book_max_stale_secs) {
                        no_bid = snap.best_bid;
                        no_ask = snap.best_ask;
                        depth_no = snap.depth_bid + snap.depth_ask;
                    }
                }
            }
        }

        (yes_bid, yes_ask, no_bid, no_ask, depth_yes, depth_no)
    }

    /// Garbage-collect expired condition IDs and stale start prices.
    fn gc_active_conditions(&mut self) {
        let cutoff = Utc::now() - chrono::Duration::minutes(10);
        self.active_conditions.retain(|_, ts| *ts > cutoff);
        // Keep start prices only for markets we still track
        self.updown_start_prices
            .retain(|cid, _| self.markets.contains_key(cid));
        // GC markets older than 10 minutes
        self.markets.retain(|_, m| m.expiry > cutoff);
        self.last_eval_log.retain(|cid, _| self.markets.contains_key(cid));
    }
}

impl Strategy for HedgedLpStrategy {
    fn name(&self) -> &str {
        "HedgedLP"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: true,
            candles: false, // pure market-microstructure, no candles needed
            execution_feedback: true,
            polymarket_updates: true,
            ml_predictions: true,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::PriceUpdate(agg) => {
                let symbol = agg.symbol.clone();
                self.latest_prices.insert(symbol.clone(), agg);

                // Re-evaluate all known UpDown markets for this symbol
                let markets_to_eval: Vec<PolymarketMarket> = self
                    .markets
                    .values()
                    .filter(|m| {
                        m.underlying_symbol == symbol
                            && matches!(m.market_type, MarketType::UpDown { .. })
                            && !self.active_conditions.contains_key(&m.condition_id)
                    })
                    .cloned()
                    .collect();

                let mut signals = Vec::new();
                for market in &markets_to_eval {
                    signals.extend(self.evaluate_market(market));
                }
                signals
            }

            StrategyEvent::PolymarketUpdate(update) => {
                match update {
                    PolymarketUpdate::MarketsDiscovered(new_markets) => {
                        // GC stale conditions periodically
                        self.gc_active_conditions();

                        let mut all_signals = Vec::new();

                        for market in &new_markets {
                            // Only process UpDown markets
                            if !matches!(market.market_type, MarketType::UpDown { .. }) {
                                continue;
                            }

                            self.markets
                                .insert(market.condition_id.clone(), market.clone());

                            // Record start price on first discovery
                            if !self.updown_start_prices.contains_key(&market.condition_id) {
                                if let Some(agg) =
                                    self.latest_prices.get(&market.underlying_symbol)
                                {
                                    let vwap = agg.vwap.to_f64().unwrap_or(0.0);
                                    if vwap > 0.0 {
                                        self.updown_start_prices
                                            .insert(market.condition_id.clone(), vwap);
                                    }
                                }
                            }

                            // Evaluate every discovered market
                            let signals = self.evaluate_market(market);
                            all_signals.extend(signals);
                        }

                        if !all_signals.is_empty() {
                            info!(
                                signals = all_signals.len(),
                                "HedgedLP evaluated {} markets, generated {} signals",
                                new_markets.len(),
                                all_signals.len()
                            );
                        }

                        all_signals
                    }
                    PolymarketUpdate::PriceUpdate {
                        condition_id,
                        yes_price,
                        no_price,
                    } => {
                        // Update stored market prices
                        if let Some(market) = self.markets.get_mut(&condition_id) {
                            market.implied_prob_yes = yes_price;
                            market.implied_prob_no = no_price;
                        }

                        // Re-evaluate if we don't have an active position
                        if !self.active_conditions.contains_key(&condition_id) {
                            if let Some(market) = self.markets.get(&condition_id).cloned() {
                                return self.evaluate_market(&market);
                            }
                        }

                        Vec::new()
                    }
                }
            }

            StrategyEvent::MlUpdate(prediction) => {
                self.latest_ml
                    .insert(prediction.symbol.clone(), prediction);
                Vec::new()
            }

            StrategyEvent::ExecutionFeedback(_) => Vec::new(),
            StrategyEvent::CandleComplete(_) => Vec::new(),
        }
    }
}

fn f64_env(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
