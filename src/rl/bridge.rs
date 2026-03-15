use crate::polymarket::trade_poller::TradeActivityCache;
use crate::polymarket::ws_orderbook::BookCache;
use crate::rl::state::{StateBuilder, STATE_DIM};
use crate::types::candle::Timeframe;
use crate::types::events::{
    AggregatorEvent, ExecutionEvent, MlPrediction, PolymarketUpdate, SignalMetadata, TradeSignal,
    TradeTarget,
};
use crate::types::market::{MarketType, PolymarketMarket};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Configuration for the RL bridge.
#[derive(Debug, Clone)]
pub struct RlBridgeConfig {
    pub server_url: String,
    pub timeout_ms: u64,
    pub capital_usd: f64,
    pub log_transitions: bool,
    pub transition_log_path: String,
}

/// Request body for POST /act.
#[derive(Serialize)]
struct ActRequest {
    state: Vec<f64>,
    state_version: u32,
    market_id: String,
    timestamp: i64,
}

/// Response from POST /act.
#[derive(Deserialize)]
struct ActResponse {
    action: u8,
    confidence: f64,
    #[allow(dead_code)]
    action_probs: Vec<f64>,
    latency_ms: f64,
}

/// Request body for POST /feedback.
#[derive(Serialize)]
struct FeedbackRequest {
    market_id: String,
    action: u8,
    fill_price: f64,
    fill_size: f64,
    fee: f64,
    pnl: f64,
    timestamp: i64,
}

/// Response from GET /health.
#[derive(Deserialize)]
struct HealthResp {
    status: String,
    #[allow(dead_code)]
    model: String,
    #[allow(dead_code)]
    ready: bool,
}

/// Transition logged for offline training.
#[derive(Serialize)]
struct Transition {
    state: Vec<f64>,
    action: u8,
    reward: f64,
    next_state: Vec<f64>,
    done: bool,
    market_id: String,
    timestamp: i64,
}

const ACTION_NAMES: [&str; 7] = [
    "strong_buy",
    "medium_buy",
    "small_buy",
    "hold",
    "small_sell",
    "medium_sell",
    "strong_sell",
];

/// Position size fractions for each action (% of capital).
const ACTION_SIZES: [f64; 7] = [0.30, 0.15, 0.05, 0.0, 0.05, 0.15, 0.30];

/// Async RL bridge task that gathers state, queries the Python PPO server,
/// and emits TradeSignals directly to the execution engine.
pub struct RlBridge {
    config: RlBridgeConfig,
    client: reqwest::Client,
    book_cache: Option<BookCache>,
    trade_activity_cache: Option<TradeActivityCache>,
}

impl RlBridge {
    pub fn new(
        config: RlBridgeConfig,
        book_cache: Option<BookCache>,
        trade_activity_cache: Option<TradeActivityCache>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .expect("failed to build reqwest client");

        Self {
            config,
            client,
            book_cache,
            trade_activity_cache,
        }
    }

    /// Main loop: subscribes to broadcast channels, builds state, queries RL server,
    /// emits TradeSignals.
    pub async fn run(
        self,
        mut agg_rx: broadcast::Receiver<AggregatorEvent>,
        mut exec_rx: broadcast::Receiver<ExecutionEvent>,
        mut poly_rx: broadcast::Receiver<PolymarketUpdate>,
        mut ml_rx: broadcast::Receiver<MlPrediction>,
        signal_tx: mpsc::Sender<TradeSignal>,
        shutdown: CancellationToken,
    ) {
        info!(server_url = %self.config.server_url, "RL bridge starting");

        // Wait for server to become available
        self.wait_for_server(&shutdown).await;
        if shutdown.is_cancelled() {
            return;
        }
        info!("RL bridge connected to PPO server");

        let mut state_builder = StateBuilder::new(
            self.config.capital_usd,
            20,  // bb_period
            9,   // ma_fast_period
            21,  // ma_slow_period
        );

        // Active UpDown markets keyed by underlying_symbol
        let mut active_markets: HashMap<String, PolymarketMarket> = HashMap::new();

        // Throttle: only query RL server every N seconds (not every tick)
        let mut last_act_time = std::time::Instant::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(std::time::Instant::now);
        let act_interval = Duration::from_secs(5);

        // Last state for transition logging
        let mut last_state: Option<Vec<f64>> = None;
        let mut last_action: Option<u8> = None;
        let mut last_market_id: Option<String> = None;

        // Open transition log file
        let mut log_file = if self.config.log_transitions {
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.config.transition_log_path)
            {
                Ok(f) => Some(f),
                Err(e) => {
                    warn!(error = %e, path = %self.config.transition_log_path, "failed to open transition log");
                    None
                }
            }
        } else {
            None
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("RL bridge shutting down");
                    return;
                }
                result = agg_rx.recv() => {
                    let event = match result {
                        Ok(e) => e,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(skipped = n, "RL bridge: aggregator broadcast lagged");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    };

                    match event {
                        AggregatorEvent::PriceUpdate(ref price) => {
                            state_builder.on_price_update(price);

                            // Find best active UpDown market for this symbol
                            let market = active_markets.get(&price.symbol).cloned();
                            if let Some(ref m) = market {
                                state_builder.active_market = Some(m.clone());
                            }

                            // Update Polymarket book cache + trade activity (after setting active market)
                            if let Some(token_id) = state_builder.active_market.as_ref().map(|m| m.token_id_yes.clone()) {
                                self.update_book_state(&mut state_builder, &token_id).await;
                                self.update_trade_activity(&mut state_builder, &token_id).await;
                            }

                            // Only act if we have an active market and throttle interval elapsed
                            let now_inst = std::time::Instant::now();
                            if let Some(ref market) = market {
                                if now_inst.duration_since(last_act_time) < act_interval {
                                    continue; // throttle — too soon since last action query
                                }
                                if let Some(state) = state_builder.build(&price.symbol) {
                                    // Log transition from previous step (independent of RL server)
                                    if let (Some(ref prev_state), Some(prev_action), Some(ref prev_market_id)) =
                                        (&last_state, last_action, &last_market_id)
                                    {
                                        if let Some(ref mut f) = log_file {
                                            let t = Transition {
                                                state: prev_state.clone(),
                                                action: prev_action,
                                                reward: 0.0, // reward comes from feedback
                                                next_state: state.clone(),
                                                done: false,
                                                market_id: prev_market_id.clone(),
                                                timestamp: Utc::now().timestamp(),
                                            };
                                            if let Ok(json) = serde_json::to_string(&t) {
                                                let _ = writeln!(f, "{}", json);
                                            }
                                        }
                                    }

                                    // Always record state for transition logging
                                    last_act_time = now_inst;
                                    let market_id = market.condition_id.clone();

                                    // Query RL server for action; default to hold (3) if unavailable
                                    let action = match self.request_action(&state, &market_id).await {
                                        Ok((action, confidence, latency_ms)) => {
                                            // Map action to trade signal
                                            if let Some(signal) = self.action_to_signal(
                                                action,
                                                confidence,
                                                latency_ms,
                                                market,
                                                price,
                                            ) {
                                                info!(
                                                    action = ACTION_NAMES[action as usize],
                                                    confidence = format!("{:.1}%", confidence * 100.0),
                                                    size_usd = %signal.size_usd,
                                                    latency_ms = format!("{:.1}", latency_ms),
                                                    "RL signal"
                                                );
                                                let _ = signal_tx.send(signal).await;
                                            }
                                            action
                                        }
                                        Err(e) => {
                                            debug!(error = %e, "RL action request failed, logging hold");
                                            3 // hold
                                        }
                                    };

                                    last_state = Some(state);
                                    last_action = Some(action);
                                    last_market_id = Some(market_id);
                                }
                            }
                        }
                        AggregatorEvent::CandleComplete(ref candle) => {
                            // Only process M5 candles for BB/MA features
                            if candle.timeframe == Timeframe::M5 {
                                state_builder.on_candle(candle);
                            }
                        }
                        _ => {}
                    }
                }
                result = poly_rx.recv() => {
                    let event = match result {
                        Ok(e) => e,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return,
                    };

                    if let PolymarketUpdate::MarketsDiscovered(markets) = event {
                        for market in markets {
                            // Track latest UpDown market per symbol
                            if matches!(market.market_type, MarketType::UpDown { .. }) {
                                active_markets.insert(market.underlying_symbol.clone(), market);
                            }
                        }
                    }
                }
                result = exec_rx.recv() => {
                    let event = match result {
                        Ok(e) => e,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return,
                    };

                    state_builder.on_execution_event(&event);

                    // Send feedback to RL server for reward computation
                    if let ExecutionEvent::OrderFilled(ref fill) = event {
                        let market_id = last_market_id.clone().unwrap_or_default();
                        let action = last_action.unwrap_or(3);
                        let _ = self
                            .send_feedback(
                                &market_id,
                                action,
                                dec_to_f64(fill.price),
                                dec_to_f64(fill.size),
                                dec_to_f64(fill.fee),
                                0.0, // P&L computed server-side
                            )
                            .await;
                    }
                }
                result = ml_rx.recv() => {
                    let pred = match result {
                        Ok(p) => p,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return,
                    };
                    state_builder.on_ml_prediction(&pred);
                }
            }
        }
    }

    /// Map a discrete action (0-6) to a TradeSignal, or None for HOLD.
    fn action_to_signal(
        &self,
        action: u8,
        confidence: f64,
        latency_ms: f64,
        market: &PolymarketMarket,
        price: &crate::types::market::AggregatedPrice,
    ) -> Option<TradeSignal> {
        if action == 3 {
            return None; // HOLD
        }

        let size_frac = ACTION_SIZES[action as usize];
        let size_usd = Decimal::from_f64(self.config.capital_usd * size_frac)
            .unwrap_or(Decimal::ZERO);

        if size_usd <= Decimal::ZERO {
            return None;
        }

        // Actions 0-2 = Buy (YES token), 4-6 = Sell (NO token)
        let side = if action < 3 { Side::Buy } else { Side::Sell };

        let entry_price = if side == Side::Buy {
            market.implied_prob_yes
        } else {
            market.implied_prob_no
        };

        Some(TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd,
            confidence,
            price: entry_price,
            metadata: SignalMetadata::Rl {
                action,
                action_name: ACTION_NAMES[action as usize].to_string(),
                confidence,
                state_dim: STATE_DIM,
                rl_latency_ms: latency_ms,
            },
            timestamp: price.timestamp,
            is_exit: false,
        })
    }

    /// Update book state from BookCache.
    async fn update_book_state(&self, state: &mut StateBuilder, token_id: &str) {
        if let Some(ref cache) = self.book_cache {
            let books = cache.read().await;
            if let Some(snap) = books.get(token_id) {
                state.poly_best_bid = dec_to_f64(snap.best_bid);
                state.poly_best_ask = dec_to_f64(snap.best_ask);
                state.poly_depth_bid = dec_to_f64(snap.depth_bid);
                state.poly_depth_ask = dec_to_f64(snap.depth_ask);
            }
        }
    }

    /// Update Polymarket trade activity from TradeActivityCache.
    async fn update_trade_activity(&self, state: &mut StateBuilder, token_id: &str) {
        if let Some(ref cache) = self.trade_activity_cache {
            let activity = cache.read().await;
            if let Some(a) = activity.get(token_id) {
                state.poly_trade_count_5m = a.trade_count_5m as f64;
                state.poly_volume_5m = a.volume_5m;
            }
        }
    }

    /// POST /act — Send state, receive action.
    async fn request_action(
        &self,
        state: &[f64],
        market_id: &str,
    ) -> Result<(u8, f64, f64), reqwest::Error> {
        let url = format!("{}/act", self.config.server_url);
        let req = ActRequest {
            state: state.to_vec(),
            state_version: 1,
            market_id: market_id.to_string(),
            timestamp: Utc::now().timestamp(),
        };

        let resp = self.client.post(&url).json(&req).send().await?;
        let body: ActResponse = resp.json().await?;
        Ok((body.action, body.confidence, body.latency_ms))
    }

    /// POST /feedback — Send execution result.
    async fn send_feedback(
        &self,
        market_id: &str,
        action: u8,
        fill_price: f64,
        fill_size: f64,
        fee: f64,
        pnl: f64,
    ) -> Result<(), reqwest::Error> {
        let url = format!("{}/feedback", self.config.server_url);
        let req = FeedbackRequest {
            market_id: market_id.to_string(),
            action,
            fill_price,
            fill_size,
            fee,
            pnl,
            timestamp: Utc::now().timestamp(),
        };
        let _ = self.client.post(&url).json(&req).send().await?;
        Ok(())
    }

    /// Retry GET /health until RL server is available.
    async fn wait_for_server(&self, shutdown: &CancellationToken) {
        let url = format!("{}/health", self.config.server_url);
        loop {
            match self.client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(health) = resp.json::<HealthResp>().await {
                        if health.status == "ok" {
                            return;
                        }
                    }
                }
                _ => {}
            }
            info!("RL server not available, retrying in 5s...");
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }
}

fn dec_to_f64(d: Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}
