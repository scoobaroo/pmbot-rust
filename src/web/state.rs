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
    let mut trades = VecDeque::new();

    // Load historical trades from CSV so they survive restarts
    load_csv_trades(&mut trades, "data/orb_trades.csv", "ENTRY");
    load_csv_exits(&mut trades, "data/orb_exits.csv");

    // Sort by timestamp descending (most recent first)
    let mut sorted: Vec<_> = trades.into_iter().collect();
    sorted.sort_by(|a: &OrbTradeEvent, b: &OrbTradeEvent| b.timestamp.cmp(&a.timestamp));
    sorted.truncate(MAX_ORB_EVENTS);
    let trades: VecDeque<_> = sorted.into_iter().collect();

    Arc::new(WebState {
        wallets: RwLock::new(HashMap::new()),
        orb_trades: RwLock::new(trades),
        client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build web reqwest client"),
    })
}

fn load_csv_trades(trades: &mut VecDeque<OrbTradeEvent>, path: &str, action: &str) {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return,
    };
    for line in content.lines().skip(1) {
        // timestamp,condition_id,symbol,side,window_secs,entry_price,size_usd,accuracy,edge,flow,move_pct,atr_multiple
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 12 { continue; }
        trades.push_back(OrbTradeEvent {
            timestamp: cols[0].to_string(),
            symbol: cols[2].to_string(),
            side: cols[3].to_string(),
            action: action.to_string(),
            price: cols[5].parse().unwrap_or(0.0),
            size_usd: cols[6].parse().unwrap_or(0.0),
            move_pct: cols[10].parse().unwrap_or(0.0),
            accuracy: cols[7].parse().unwrap_or(0.0),
            edge: cols[8].parse().unwrap_or(0.0),
            flow: cols[9].parse().unwrap_or(0.0),
            window_secs: cols[4].parse().unwrap_or(0),
            reason: String::new(),
        });
    }
}

fn load_csv_exits(trades: &mut VecDeque<OrbTradeEvent>, path: &str) {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return,
    };
    for line in content.lines().skip(1) {
        // timestamp,condition_id,symbol,exit_type,entry_price,exit_price,pnl_pct,max_bid_seen,max_implied_seen,hold_secs
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 10 { continue; }
        let exit_type = cols[3];
        let action = if exit_type == "resolution" {
            // Check pnl to determine won/lost — resolution with pnl > 0 = won
            let pnl: f64 = cols[6].parse().unwrap_or(0.0);
            if pnl > 0.0 { "WON" } else { "LOST" }
        } else {
            "EXIT"
        };
        let pnl_pct: f64 = cols[6].parse().unwrap_or(0.0);
        trades.push_back(OrbTradeEvent {
            timestamp: cols[0].to_string(),
            symbol: cols[2].to_string(),
            side: String::new(),
            action: action.to_string(),
            price: cols[4].parse().unwrap_or(0.0),
            size_usd: pnl_pct, // P&L percentage in this field for display
            move_pct: 0.0,
            accuracy: 0.0,
            edge: 0.0,
            flow: 0.0,
            window_secs: 0,
            reason: format!("P&L: {:+.1}% | held {}s | MFE bid:{} impl:{}",
                pnl_pct, cols[9], cols[7], cols[8]),
        });
    }
}
