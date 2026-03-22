use crate::web::handlers;
use crate::web::state::SharedWebState;
use axum::routing::{delete, get, post};
use axum::Router;
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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
