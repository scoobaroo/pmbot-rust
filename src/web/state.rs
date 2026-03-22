use std::collections::{HashMap, VecDeque};
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

/// A single ORB trade event for the dashboard live feed.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OrbTradeEvent {
    pub timestamp: String,
    pub symbol: String,
    pub side: String,
    pub action: String, // "ENTRY" or "EXIT" or "REJECTED"
    pub price: f64,
    pub size_usd: f64,
    pub move_pct: f64,
    pub accuracy: f64,
    pub edge: f64,
    pub flow: f64,
    pub window_secs: u32,
    pub reason: String, // rejection reason or exit type
}

const MAX_ORB_EVENTS: usize = 100;

pub struct WebState {
    pub wallets: RwLock<HashMap<String, WalletData>>,
    pub orb_trades: RwLock<VecDeque<OrbTradeEvent>>,
    pub client: reqwest::Client,
}

impl WebState {
    pub async fn push_orb_event(&self, event: OrbTradeEvent) {
        let mut trades = self.orb_trades.write().await;
        trades.push_front(event);
        if trades.len() > MAX_ORB_EVENTS {
            trades.pop_back();
        }
    }
}

pub type SharedWebState = Arc<WebState>;

pub fn new_web_state() -> SharedWebState {
    Arc::new(WebState {
        wallets: RwLock::new(HashMap::new()),
        orb_trades: RwLock::new(VecDeque::new()),
        client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build web reqwest client"),
    })
}
