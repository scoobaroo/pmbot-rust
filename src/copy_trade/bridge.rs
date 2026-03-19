use crate::copy_trade::types::*;
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::PolymarketMarket;
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

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

/// Async copy-trade bridge task that polls Polymarket data-api for target
/// trader activity and mirrors their trades.
pub struct CopyTradeBridge {
    config: CopyTradeConfig,
    client: reqwest::Client,
    cache: CopyTraderCache,
    rl_state: Option<SharedRlState>,
}

impl CopyTradeBridge {
    pub fn new(
        config: CopyTradeConfig,
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
            cache,
            rl_state,
        }
    }

    pub async fn run(
        self,
        mut poly_rx: broadcast::Receiver<PolymarketUpdate>,
        signal_tx: mpsc::Sender<TradeSignal>,
        shutdown: CancellationToken,
    ) {
        info!(
            targets = ?self.config.targets,
            poll_secs = self.config.poll_interval_secs,
            size_ratio = self.config.size_ratio,
            "copy-trade bridge starting"
        );

        // condition_id → PolymarketMarket for signal generation
        let mut market_map: HashMap<String, PolymarketMarket> = HashMap::new();

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
                result = poly_rx.recv() => {
                    match result {
                        Ok(PolymarketUpdate::MarketsDiscovered(markets)) => {
                            for m in markets {
                                market_map.insert(m.condition_id.clone(), m);
                            }
                            debug!(markets = market_map.len(), "copy-trade: market map updated");
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(skipped = n, "copy-trade: polymarket broadcast lagged");
                        }
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
                                let new_trades: Vec<&TraderTrade> = trades
                                    .iter()
                                    .filter(|t| t.timestamp > last_ts)
                                    .collect();

                                if !new_trades.is_empty() {
                                    // Update last seen to max timestamp
                                    if let Some(max_ts) = new_trades.iter().map(|t| t.timestamp).max() {
                                        last_seen.insert(target.clone(), max_ts);
                                    }

                                    for trade in &new_trades {
                                        self.process_trade(
                                            target,
                                            trade,
                                            &market_map,
                                            &signal_tx,
                                            &mut expert_log,
                                        )
                                        .await;
                                    }

                                    // Update cache
                                    self.update_cache_from_trades(&new_trades).await;
                                } else if last_ts == 0 && !trades.is_empty() {
                                    // First poll — just record latest timestamp, don't mirror old trades
                                    if let Some(max_ts) = trades.iter().map(|t| t.timestamp).max() {
                                        last_seen.insert(target.clone(), max_ts);
                                        info!(
                                            target = target,
                                            baseline_ts = max_ts,
                                            "copy-trade: set baseline timestamp"
                                        );
                                    }
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
        market_map: &HashMap<String, PolymarketMarket>,
        signal_tx: &mpsc::Sender<TradeSignal>,
        expert_log: &mut Option<std::fs::File>,
    ) {
        let market = match market_map.get(&trade.condition_id) {
            Some(m) => m,
            None => {
                debug!(
                    condition_id = %trade.condition_id,
                    title = %trade.title,
                    "copy-trade: skipping trade in unknown market"
                );
                return;
            }
        };

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
        let latency_ms = (Utc::now().timestamp() - trade.timestamp).abs() as f64 * 1000.0;

        info!(
            target = target,
            market = %trade.title,
            side = %trade.side,
            source_size = format!("${:.2}", trade.usdc_size),
            mirror_size = format!("${:.2}", size_usd),
            price = %price,
            "copy-trade: mirroring trade"
        );

        let signal = TradeSignal {
            target: TradeTarget::Polymarket(market.clone()),
            side,
            size_usd: Decimal::from_f64(size_usd).unwrap_or(Decimal::ZERO),
            confidence: 0.8,
            price,
            metadata: SignalMetadata::CopyTrade {
                source_wallet: target.to_string(),
                source_side: trade.side.clone(),
                source_size_usd: trade.usdc_size,
                mirrored_size_usd: size_usd,
                latency_ms,
            },
            timestamp: Utc::now(),
            is_exit: side == Side::Sell,
        };

        let _ = signal_tx.send(signal).await;

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

            // Update running average trade size
            let n = cache.total_trades_seen as f64;
            cache.avg_trade_size =
                cache.avg_trade_size * ((n - 1.0) / n) + trade.usdc_size / n;

            // Last action
            cache.last_action = match trade.side.to_uppercase().as_str() {
                "BUY" => 1.0,
                "SELL" => -1.0,
                _ => 0.0,
            };

            // Normalized size
            if cache.avg_trade_size > 0.0 {
                cache.last_size_normalized = trade.usdc_size / cache.avg_trade_size;
            }
        }
    }

    async fn update_cache_from_positions(&self, positions: &[TraderPosition]) {
        let mut cache = self.cache.write().await;

        // Compute net direction from positions
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
