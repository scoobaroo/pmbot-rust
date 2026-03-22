use crate::web::state::{SharedWebState, WalletData, WalletStats};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, Json};
use serde_json::{json, Value};

const DATA_API_BASE: &str = "https://data-api.polymarket.com";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

/// GET / — serve dashboard HTML
pub async fn dashboard() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

/// GET /api/orb/trades — recent ORB trade events (entries, exits, rejections)
pub async fn orb_trades(State(state): State<SharedWebState>) -> Json<Value> {
    let trades = state.orb_trades.read().await;
    let list: Vec<_> = trades.iter().collect();
    Json(json!(list))
}

/// GET /api/wallets — list all tracked wallets with stats
pub async fn list_wallets(State(state): State<SharedWebState>) -> Json<Value> {
    let wallets = state.wallets.read().await;
    let list: Vec<&WalletData> = wallets.values().collect();
    Json(json!(list))
}

/// GET /api/wallet/:address/activity
pub async fn wallet_activity(
    State(state): State<SharedWebState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let addr = address.to_lowercase();
    let wallets = state.wallets.read().await;
    if let Some(data) = wallets.get(&addr) {
        Ok(Json(json!(data.activity)))
    } else {
        // Fetch live
        drop(wallets);
        match fetch_activity(&state.client, &addr).await {
            Ok(activity) => Ok(Json(json!(activity))),
            Err(_) => Err(StatusCode::BAD_GATEWAY),
        }
    }
}

/// GET /api/wallet/:address/positions
pub async fn wallet_positions(
    State(state): State<SharedWebState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let addr = address.to_lowercase();
    let wallets = state.wallets.read().await;
    if let Some(data) = wallets.get(&addr) {
        Ok(Json(json!(data.positions)))
    } else {
        drop(wallets);
        match fetch_positions(&state.client, &addr).await {
            Ok(positions) => Ok(Json(json!(positions))),
            Err(_) => Err(StatusCode::BAD_GATEWAY),
        }
    }
}

/// GET /api/wallet/:address/stats
pub async fn wallet_stats(
    State(state): State<SharedWebState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let addr = address.to_lowercase();
    let wallets = state.wallets.read().await;
    if let Some(data) = wallets.get(&addr) {
        Ok(Json(json!(data.stats)))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// POST /api/wallet/:address — add wallet to tracking
pub async fn add_wallet(
    State(state): State<SharedWebState>,
    Path(address): Path<String>,
) -> StatusCode {
    let addr = address.to_lowercase();
    let mut wallets = state.wallets.write().await;
    if wallets.contains_key(&addr) {
        return StatusCode::OK;
    }
    wallets.insert(
        addr.clone(),
        WalletData {
            address: addr,
            ..Default::default()
        },
    );
    StatusCode::CREATED
}

/// DELETE /api/wallet/:address — remove wallet from tracking
pub async fn remove_wallet(
    State(state): State<SharedWebState>,
    Path(address): Path<String>,
) -> StatusCode {
    let addr = address.to_lowercase();
    let mut wallets = state.wallets.write().await;
    if wallets.remove(&addr).is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// --- Data fetching helpers ---

pub async fn fetch_activity(
    client: &reqwest::Client,
    wallet: &str,
) -> Result<Vec<Value>, reqwest::Error> {
    let url = format!(
        "{}/activity?user={}&type=TRADE&limit=50&sortDirection=DESC",
        DATA_API_BASE, wallet
    );
    let resp = client.get(&url).send().await?;
    let data: Vec<Value> = resp.json().await?;
    Ok(data)
}

pub async fn fetch_positions(
    client: &reqwest::Client,
    wallet: &str,
) -> Result<Vec<Value>, reqwest::Error> {
    let url = format!(
        "{}/positions?user={}&limit=100&sortDirection=DESC",
        DATA_API_BASE, wallet
    );
    let resp = client.get(&url).send().await?;
    let data: Vec<Value> = resp.json().await?;
    Ok(data)
}

pub async fn fetch_closed_positions(
    client: &reqwest::Client,
    wallet: &str,
) -> Result<Vec<Value>, reqwest::Error> {
    let url = format!(
        "{}/closed-positions?user={}&limit=50&sortDirection=DESC",
        DATA_API_BASE, wallet
    );
    let resp = client.get(&url).send().await?;
    let data: Vec<Value> = resp.json().await?;
    Ok(data)
}

pub async fn fetch_profile(
    client: &reqwest::Client,
    wallet: &str,
) -> Result<Value, reqwest::Error> {
    let url = format!("{}/public-profile?address={}", GAMMA_API_BASE, wallet);
    let resp = client.get(&url).send().await?;
    let data: Value = resp.json().await?;
    Ok(data)
}

/// Refresh all data for a single wallet and compute stats.
pub async fn refresh_wallet(client: &reqwest::Client, data: &mut WalletData) {
    let addr = &data.address;

    if let Ok(activity) = fetch_activity(client, addr).await {
        data.activity = activity;
    }
    if let Ok(positions) = fetch_positions(client, addr).await {
        data.positions = positions;
    }
    if let Ok(closed) = fetch_closed_positions(client, addr).await {
        data.closed_positions = closed;
    }
    if data.profile.is_none() {
        if let Ok(profile) = fetch_profile(client, addr).await {
            data.profile = Some(profile);
        }
    }

    // Compute stats
    data.stats = compute_stats(&data.positions, &data.closed_positions, &data.activity);
    data.last_refresh_ts = chrono::Utc::now().timestamp();
}

fn compute_stats(
    positions: &[Value],
    closed_positions: &[Value],
    activity: &[Value],
) -> WalletStats {
    let mut total_pnl = 0.0_f64;
    let mut wins = 0_u64;
    let mut total_pos = 0_u64;
    let mut total_value = 0.0_f64;

    // From open positions
    for pos in positions {
        let pnl = pos["cashPnl"].as_f64().unwrap_or(0.0);
        total_pnl += pnl;
        total_pos += 1;
        if pnl > 0.0 {
            wins += 1;
        }
        let size = pos["size"].as_f64().unwrap_or(0.0);
        let price = pos["curPrice"].as_f64().unwrap_or(0.0);
        total_value += size * price;
    }

    // From closed positions
    for pos in closed_positions {
        let pnl = pos["realizedPnl"].as_f64().unwrap_or(0.0);
        total_pnl += pnl;
        total_pos += 1;
        if pnl > 0.0 {
            wins += 1;
        }
    }

    let win_rate = if total_pos > 0 {
        wins as f64 / total_pos as f64
    } else {
        0.0
    };

    // Avg trade size from activity
    let mut trade_sum = 0.0_f64;
    let trade_count = activity.len() as u64;
    for trade in activity {
        trade_sum += trade["usdcSize"].as_f64().unwrap_or(0.0);
    }
    let avg_trade_size = if trade_count > 0 {
        trade_sum / trade_count as f64
    } else {
        0.0
    };

    WalletStats {
        total_pnl,
        win_rate,
        avg_trade_size,
        trade_count,
        open_positions: positions.len() as u64,
        total_value,
    }
}
