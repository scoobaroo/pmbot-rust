use crate::web::handlers;
use crate::web::state::{OrbTradeEvent, SharedWebState};
use axum::routing::{delete, get, post};
use axum::Router;
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Start the web dashboard server.
pub async fn run(state: SharedWebState, port: u16, shutdown: CancellationToken) {
    let app = Router::new()
        .route("/", get(handlers::dashboard))
        .route("/api/orb/trades", get(handlers::orb_trades))
        .route("/api/wallets", get(handlers::list_wallets))
        .route("/api/wallet/{address}/activity", get(handlers::wallet_activity))
        .route("/api/wallet/{address}/positions", get(handlers::wallet_positions))
        .route("/api/wallet/{address}/stats", get(handlers::wallet_stats))
        .route("/api/wallet/{address}", post(handlers::add_wallet))
        .route("/api/wallet/{address}", delete(handlers::remove_wallet))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!(port = port, "web dashboard listening");

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(error = %e, port = port, "failed to bind web server");
            return;
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .unwrap_or_else(|e| warn!(error = %e, "web server error"));
}

/// Background task that polls Polymarket for trade resolutions and pushes WON/LOST to dashboard.
pub async fn poll_resolutions(
    state: SharedWebState,
    wallet_address: String,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut seen_redeems: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                let url = format!(
                    "https://data-api.polymarket.com/activity?user={}&limit=50&sortDirection=DESC",
                    wallet_address
                );
                let resp = match state.client.get(&url).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        debug!(error = %e, "failed to fetch activity for resolutions");
                        continue;
                    }
                };
                let data: Vec<serde_json::Value> = match resp.json().await {
                    Ok(d) => d,
                    Err(_) => continue,
                };

                for item in &data {
                    let ttype = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if ttype != "REDEEM" {
                        continue;
                    }

                    let cid = item.get("conditionId").and_then(|v| v.as_str()).unwrap_or("");
                    let tx = item.get("transactionHash").and_then(|v| v.as_str()).unwrap_or("");

                    // Deduplicate by tx hash
                    let key = format!("{}:{}", cid, tx);
                    if seen_redeems.contains(&key) {
                        continue;
                    }
                    seen_redeems.insert(key);

                    let size = item.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let usdc = item.get("usdcSize").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let title = item.get("title").and_then(|v| v.as_str()).unwrap_or("");
                    let ts = item.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);

                    let (action, pnl_display) = if size > 0.0 || usdc > 0.0 {
                        ("WON", format!("${:.2} redeemed", usdc))
                    } else {
                        ("LOST", "resolved worthless".to_string())
                    };

                    let timestamp = chrono::DateTime::from_timestamp(ts, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default();

                    // Determine symbol from title
                    let symbol = if title.contains("Bitcoin") { "BTC-USD" }
                        else if title.contains("Ethereum") { "ETH-USD" }
                        else { "UNKNOWN" };

                    info!(
                        action,
                        title = &title[..title.len().min(45)],
                        usdc = format!("${:.2}", usdc),
                        "ORB resolution from Polymarket API"
                    );

                    state.push_orb_event(OrbTradeEvent {
                        timestamp,
                        symbol: symbol.to_string(),
                        side: String::new(),
                        action: action.to_string(),
                        price: 0.0,
                        size_usd: usdc,
                        move_pct: 0.0,
                        accuracy: 0.0,
                        edge: usdc,
                        flow: 0.0,
                        window_secs: 0,
                        reason: pnl_display,
                    }).await;
                }
            }
        }
    }
}

/// Background task that refreshes wallet data periodically.
pub async fn background_refresh(state: SharedWebState, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                // Collect addresses first to avoid holding the lock during HTTP calls
                let addresses: Vec<String> = {
                    let wallets = state.wallets.read().await;
                    wallets.keys().cloned().collect()
                };

                for addr in &addresses {
                    let mut wallets = state.wallets.write().await;
                    if let Some(data) = wallets.get_mut(addr) {
                        handlers::refresh_wallet(&state.client, data).await;
                    }
                }
            }
        }
    }
}
