use crate::copy_trade::types::*;
use crate::polymarket::client::{OrderParams, PolymarketClient};
use crate::types::events::PolymarketUpdate;
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const DATA_API_BASE: &str = "https://data-api.polymarket.com";

/// Logged expert demonstration for imitation learning.
#[derive(Serialize)]
struct ExpertTransition {
    state: Vec<f64>,
    expert_action: u8,
    expert_size_usd: f64,
    timestamp: i64,
    market_id: String,
    source_wallet: String,
}

/// Shared latest RL state vector for expert demonstration logging.
pub type SharedRlState = std::sync::Arc<tokio::sync::RwLock<Option<Vec<f64>>>>;

pub fn new_shared_rl_state() -> SharedRlState {
    std::sync::Arc::new(tokio::sync::RwLock::new(None))
}

/// Async copy-trade bridge that polls target wallets and mirrors trades
/// using its own separate Polymarket account. Does NOT route through
/// the main execution engine — complete account isolation.
pub struct CopyTradeBridge {
    config: CopyTradeConfig,
    client: reqwest::Client,
    poly_client: Option<Arc<PolymarketClient>>,
    cache: CopyTraderCache,
    rl_state: Option<SharedRlState>,
}

impl CopyTradeBridge {
    pub fn new(
        config: CopyTradeConfig,
        poly_client: Option<PolymarketClient>,
        cache: CopyTraderCache,
        rl_state: Option<SharedRlState>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client");

        Self {
            config,
            client,
            poly_client: poly_client.map(Arc::new),
            cache,
            rl_state,
        }
    }

    pub async fn run(
        self,
        mut poly_rx: broadcast::Receiver<PolymarketUpdate>,
        shutdown: CancellationToken,
    ) {
        info!(
            targets = ?self.config.targets,
            poll_secs = self.config.poll_interval_secs,
            size_ratio = self.config.size_ratio,
            has_live_client = self.poly_client.is_some(),
            "copy-trade bridge starting (isolated account)"
        );

        // Track last-seen trade timestamp per wallet to detect new trades
        let mut last_seen: HashMap<String, i64> = HashMap::new();

        // Positions refresh counter
        let mut poll_count: u64 = 0;
        let positions_every = 12; // refresh positions every 12 polls (~60s at 5s interval)

        let mut poll_interval =
            tokio::time::interval(Duration::from_secs(self.config.poll_interval_secs));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Expert demo log file
        let mut expert_log = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("data/expert_transitions.jsonl")
        {
            Ok(f) => Some(f),
            Err(e) => {
                warn!(error = %e, "failed to open expert transition log");
                None
            }
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("copy-trade bridge shutting down");
                    return;
                }
                // Drain polymarket broadcast to prevent lag warnings
                result = poly_rx.recv() => {
                    match result {
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
                _ = poll_interval.tick() => {
                    poll_count += 1;

                    for target in &self.config.targets {
                        // Poll recent trades
                        match self.fetch_activity(target).await {
                            Ok(trades) => {
                                let last_ts = last_seen.get(target).copied().unwrap_or(0);

                                // First poll: set baseline timestamp, don't mirror historical trades
                                if last_ts == 0 {
                                    if let Some(max_ts) = trades.iter().map(|t| t.timestamp).max() {
                                        last_seen.insert(target.clone(), max_ts);
                                        info!(
                                            target = target,
                                            baseline_ts = max_ts,
                                            recent_trades = trades.len(),
                                            "copy-trade: set baseline timestamp (skipping historical)"
                                        );
                                    }
                                    continue;
                                }

                                let new_trades: Vec<&TraderTrade> = trades
                                    .iter()
                                    .filter(|t| t.timestamp > last_ts)
                                    .collect();

                                if !new_trades.is_empty() {
                                    if let Some(max_ts) = new_trades.iter().map(|t| t.timestamp).max() {
                                        last_seen.insert(target.clone(), max_ts);
                                    }

                                    for trade in &new_trades {
                                        self.process_trade(
                                            target,
                                            trade,
                                            &mut expert_log,
                                        )
                                        .await;
                                    }

                                    self.update_cache_from_trades(&new_trades).await;
                                }
                            }
                            Err(e) => {
                                warn!(target = target, error = %e, "copy-trade: failed to fetch activity");
                            }
                        }

                        // Periodically refresh positions for win rate
                        if poll_count % positions_every == 0 {
                            if let Ok(positions) = self.fetch_positions(target).await {
                                self.update_cache_from_positions(&positions).await;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn process_trade(
        &self,
        target: &str,
        trade: &TraderTrade,
        expert_log: &mut Option<std::fs::File>,
    ) {
        // Use the token_id directly from the trade data — no market map lookup needed.
        // The activity API returns the exact `asset` (token_id) the trader bought/sold.
        let token_id = &trade.asset;
        if token_id.is_empty() {
            warn!("copy-trade: skipping trade with empty asset/token_id");
            return;
        }

        // Compute mirrored size
        let raw_size = trade.usdc_size * self.config.size_ratio;
        let size_usd = raw_size.min(self.config.max_position_usd).max(1.0);

        let side = match trade.side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => {
                warn!(side = %trade.side, "copy-trade: unknown trade side");
                return;
            }
        };

        let price = Decimal::from_f64(trade.price).unwrap_or(Decimal::ZERO);
        if price <= Decimal::ZERO {
            warn!("copy-trade: skipping trade with zero price");
            return;
        }

        let latency_secs = (Utc::now().timestamp() - trade.timestamp).abs();

        let size_dec = Decimal::from_f64(size_usd).unwrap_or(Decimal::ZERO);
        let min_tokens = Decimal::from(5); // Polymarket minimum order size
        let mut token_size = (size_dec / price).round_dp_with_strategy(
            2,
            rust_decimal::RoundingStrategy::ToZero,
        );
        // Enforce Polymarket minimum of 5 tokens
        if token_size < min_tokens {
            token_size = min_tokens;
        }

        info!(
            target = target,
            market = %trade.title,
            side = %trade.side,
            source_size = format!("${:.2}", trade.usdc_size),
            mirror_size = format!("${:.2}", size_usd),
            price = %price,
            token_size = %token_size,
            latency_secs = latency_secs,
            token_id = %token_id,
            "copy-trade: mirroring trade"
        );

        // Execute directly on copy-trade Polymarket account
        if let Some(ref poly_client) = self.poly_client {
            let params = OrderParams {
                token_id,
                side,
                price: price.round_dp(2),
                size: token_size,
                order_type: "GTC",
                post_only: false,
                fee_rate_bps: Some(1000),
                neg_risk: None,
                expiration: None, // GTC — let it fill or expire naturally
            };

            match poly_client.place_order(&params).await {
                Ok(order_id) => {
                    info!(
                        order_id = %order_id,
                        target = target,
                        market = %trade.title,
                        "copy-trade: order placed on copytrade account"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        target = target,
                        market = %trade.title,
                        "copy-trade: failed to place order"
                    );
                }
            }
        } else {
            info!(
                target = target,
                market = %trade.title,
                side = %trade.side,
                size_usd = format!("${:.2}", size_usd),
                "copy-trade: PAPER mode — would mirror trade (no copytrade key configured)"
            );
        }

        // Log expert demonstration for RL imitation learning
        if let Some(ref rl_state) = self.rl_state {
            if let Some(ref state) = *rl_state.read().await {
                let expert_action = match (trade.side.to_uppercase().as_str(), trade.usdc_size) {
                    ("BUY", s) if s >= 500.0 => 0,  // strong_buy
                    ("BUY", s) if s >= 100.0 => 1,  // medium_buy
                    ("BUY", _) => 2,                  // small_buy
                    ("SELL", s) if s >= 500.0 => 6,  // strong_sell
                    ("SELL", s) if s >= 100.0 => 5,  // medium_sell
                    ("SELL", _) => 4,                  // small_sell
                    _ => 3,                            // hold
                };

                let demo = ExpertTransition {
                    state: state.clone(),
                    expert_action,
                    expert_size_usd: trade.usdc_size,
                    timestamp: trade.timestamp,
                    market_id: trade.condition_id.clone(),
                    source_wallet: target.to_string(),
                };

                if let Some(ref mut f) = expert_log {
                    if let Ok(json) = serde_json::to_string(&demo) {
                        let _ = writeln!(f, "{}", json);
                    }
                }
            }
        }
    }

    async fn update_cache_from_trades(&self, trades: &[&TraderTrade]) {
        let mut cache = self.cache.write().await;

        for trade in trades {
            cache.total_trades_seen += 1;

            let n = cache.total_trades_seen as f64;
            cache.avg_trade_size =
                cache.avg_trade_size * ((n - 1.0) / n) + trade.usdc_size / n;

            cache.last_action = match trade.side.to_uppercase().as_str() {
                "BUY" => 1.0,
                "SELL" => -1.0,
                _ => 0.0,
            };

            if cache.avg_trade_size > 0.0 {
                cache.last_size_normalized = trade.usdc_size / cache.avg_trade_size;
            }
        }
    }

    async fn update_cache_from_positions(&self, positions: &[TraderPosition]) {
        let mut cache = self.cache.write().await;

        let mut net_buy = 0.0_f64;
        let mut net_sell = 0.0_f64;
        let mut wins = 0_u64;
        let total = positions.len() as u64;

        for pos in positions {
            if pos.outcome.to_lowercase().contains("up") || pos.outcome.to_lowercase() == "yes" {
                net_buy += pos.size;
            } else {
                net_sell += pos.size;
            }
            if pos.cash_pnl > 0.0 {
                wins += 1;
            }
        }

        let total_size = net_buy + net_sell;
        cache.position_direction = if total_size > 0.0 {
            (net_buy - net_sell) / total_size
        } else {
            0.0
        };

        if total > 0 {
            cache.recent_win_rate = wins as f64 / total as f64;
            cache.total_wins = wins;
        }
    }

    async fn fetch_activity(&self, wallet: &str) -> Result<Vec<TraderTrade>, reqwest::Error> {
        let url = format!(
            "{}/activity?user={}&type=TRADE&limit=20&sortDirection=DESC",
            DATA_API_BASE, wallet
        );
        let resp = self.client.get(&url).send().await?;
        let trades: Vec<TraderTrade> = resp.json().await?;
        Ok(trades)
    }

    async fn fetch_positions(&self, wallet: &str) -> Result<Vec<TraderPosition>, reqwest::Error> {
        let url = format!(
            "{}/positions?user={}&limit=100&sortDirection=DESC",
            DATA_API_BASE, wallet
        );
        let resp = self.client.get(&url).send().await?;
        let positions: Vec<TraderPosition> = resp.json().await?;
        Ok(positions)
    }
}
