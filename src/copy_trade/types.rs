use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Configuration for the copy-trade bridge.
#[derive(Debug, Clone)]
pub struct CopyTradeConfig {
    pub enabled: bool,
    pub targets: Vec<String>,
    pub poll_interval_secs: u64,
    pub size_ratio: f64,
    pub max_position_usd: f64,
    /// Maximum seconds of latency before we skip a trade (stale signal).
    pub max_latency_secs: i64,
    /// Price premium to add on top of their entry to ensure fill (e.g., 0.05 = 5%).
    pub price_premium_pct: f64,
    /// Maximum slippage from their entry before we skip (e.g., 0.15 = 15%).
    pub max_slippage_pct: f64,
}

/// Single trade from GET /activity?type=TRADE.
#[derive(Debug, Clone, Deserialize)]
pub struct TraderTrade {
    pub timestamp: i64,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(default)]
    pub size: f64,
    #[serde(rename = "usdcSize", default)]
    pub usdc_size: f64,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub asset: String,
    #[serde(rename = "outcomeIndex", default)]
    pub outcome_index: u8,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub slug: String,
}

/// Position from GET /positions.
#[derive(Debug, Clone, Deserialize)]
pub struct TraderPosition {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(default)]
    pub size: f64,
    #[serde(rename = "avgPrice", default)]
    pub avg_price: f64,
    #[serde(rename = "cashPnl", default)]
    pub cash_pnl: f64,
    #[serde(rename = "percentPnl", default)]
    pub percent_pnl: f64,
    #[serde(rename = "curPrice", default)]
    pub cur_price: f64,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub asset: String,
    #[serde(default)]
    pub title: String,
}

/// Shared state exposed for RL integration and web dashboard.
#[derive(Debug, Clone, Default)]
pub struct CopyTraderState {
    /// -1.0 sell, 0.0 hold, 1.0 buy
    pub last_action: f64,
    /// trade size / avg trade size
    pub last_size_normalized: f64,
    /// net position direction [-1, 1]
    pub position_direction: f64,
    /// rolling win rate from positions
    pub recent_win_rate: f64,
    /// average trade size (for normalization)
    pub avg_trade_size: f64,
    pub total_trades_seen: u64,
    pub total_wins: u64,
}

pub type CopyTraderCache = Arc<RwLock<CopyTraderState>>;

pub fn new_copy_trader_cache() -> CopyTraderCache {
    Arc::new(RwLock::new(CopyTraderState::default()))
}
