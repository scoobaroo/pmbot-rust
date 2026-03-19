use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cached wallet data for the dashboard.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WalletData {
    pub address: String,
    pub profile: Option<serde_json::Value>,
    pub activity: Vec<serde_json::Value>,
    pub positions: Vec<serde_json::Value>,
    pub closed_positions: Vec<serde_json::Value>,
    pub stats: WalletStats,
    pub last_refresh_ts: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WalletStats {
    pub total_pnl: f64,
    pub win_rate: f64,
    pub avg_trade_size: f64,
    pub trade_count: u64,
    pub open_positions: u64,
    pub total_value: f64,
}

pub struct WebState {
    pub wallets: RwLock<HashMap<String, WalletData>>,
    pub client: reqwest::Client,
}

pub type SharedWebState = Arc<WebState>;

pub fn new_web_state() -> SharedWebState {
    Arc::new(WebState {
        wallets: RwLock::new(HashMap::new()),
        client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build web reqwest client"),
    })
}
