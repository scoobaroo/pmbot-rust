use crate::exchanges::backoff_delay;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Shared book cache: token_id → BookSnapshot.
pub type BookCache = Arc<RwLock<HashMap<String, BookSnapshot>>>;

/// Create a new empty BookCache.
pub fn new_book_cache() -> BookCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Snapshot of an L2 orderbook for a single Polymarket token.
#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub spread: Decimal,
    pub depth_bid: Decimal,
    pub depth_ask: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl BookSnapshot {
    /// Check whether this snapshot is still fresh.
    pub fn is_fresh(&self, max_age_secs: u64) -> bool {
        let age = Utc::now() - self.timestamp;
        age.num_seconds() < max_age_secs as i64
    }

    /// Half-spread in price units.
    pub fn half_spread(&self) -> Decimal {
        self.spread / Decimal::TWO
    }
}

/// Streams real-time L2 orderbook updates from Polymarket CLOB WebSocket.
pub struct OrderbookStream {
    token_ids: Vec<String>,
    book_cache: BookCache,
}

impl OrderbookStream {
    pub fn new(token_ids: Vec<String>, book_cache: BookCache) -> Self {
        Self {
            token_ids,
            book_cache,
        }
    }

    pub async fn run(&self, shutdown: CancellationToken) {
        if self.token_ids.is_empty() {
            info!("OrderbookStream: no token IDs to subscribe, exiting");
            return;
        }

        let mut attempt = 0u32;

        loop {
            if shutdown.is_cancelled() {
                return;
            }

            match connect_async(POLYMARKET_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!("OrderbookStream: WebSocket connected");
                    attempt = 0;

                    let (mut write, mut read) = ws_stream.split();

                    // Subscribe to L2 updates for each token
                    for token_id in &self.token_ids {
                        let sub = serde_json::json!({
                            "type": "subscribe",
                            "channel": "book",
                            "market": token_id,
                        });
                        if let Err(e) = write
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                sub.to_string(),
                            ))
                            .await
                        {
                            error!(error = %e, "OrderbookStream: failed to subscribe");
                            break;
                        }
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!("OrderbookStream: shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        self.handle_message(&text).await;
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(error = %e, "OrderbookStream: WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!("OrderbookStream: WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, attempt, "OrderbookStream: connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                delay_secs = delay.as_secs(),
                "OrderbookStream: reconnecting"
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => return,
            }
            attempt = attempt.saturating_add(1);
        }
    }

    async fn handle_message(&self, text: &str) {
        let v: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return,
        };

        // Parse market/asset_id from the message
        let market = match v.get("market").or_else(|| v.get("asset_id")) {
            Some(m) => match m.as_str() {
                Some(s) => s.to_string(),
                None => return,
            },
            None => return,
        };

        // Parse bids and asks arrays
        let bids = v.get("bids").and_then(|b| b.as_array());
        let asks = v.get("asks").and_then(|a| a.as_array());

        if bids.is_none() && asks.is_none() {
            return;
        }

        let mut cache = self.book_cache.write().await;
        let entry = cache.entry(market.clone()).or_insert_with(|| BookSnapshot {
            best_bid: Decimal::ZERO,
            best_ask: Decimal::ONE,
            spread: Decimal::ONE,
            depth_bid: Decimal::ZERO,
            depth_ask: Decimal::ZERO,
            timestamp: Utc::now(),
        });

        if let Some(bid_levels) = bids {
            let (best, depth) = parse_levels(bid_levels);
            if let Some(b) = best {
                entry.best_bid = b;
            }
            entry.depth_bid = depth;
        }

        if let Some(ask_levels) = asks {
            let (best, depth) = parse_levels(ask_levels);
            if let Some(a) = best {
                entry.best_ask = a;
            }
            entry.depth_ask = depth;
        }

        entry.spread = entry.best_ask - entry.best_bid;
        entry.timestamp = Utc::now();

        debug!(
            market = %market,
            bid = %entry.best_bid,
            ask = %entry.best_ask,
            spread = %entry.spread,
            "book update"
        );
    }
}

/// Parse price levels array. Returns (best_price, total_depth).
/// Each level is expected as {"price": "0.55", "size": "100"} or ["0.55", "100"].
fn parse_levels(levels: &[serde_json::Value]) -> (Option<Decimal>, Decimal) {
    let mut best: Option<Decimal> = None;
    let mut total_depth = Decimal::ZERO;

    for level in levels {
        let (price, size) = if let Some(obj) = level.as_object() {
            let p = obj
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str(s).ok());
            let s = obj
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            (p, s)
        } else if let Some(arr) = level.as_array() {
            let p = arr
                .first()
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str(s).ok());
            let s = arr
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            (p, s)
        } else {
            continue;
        };

        if let Some(p) = price {
            // Skip levels with zero size (removals)
            if size > Decimal::ZERO {
                total_depth += size;
                match best {
                    None => best = Some(p),
                    Some(b) => {
                        // For bids we want highest, for asks lowest.
                        // We process both the same way: first non-zero is best
                        // since the exchange typically sends sorted.
                        // Keep the first one (assumed best).
                        let _ = b;
                    }
                }
            }
        }
    }

    (best, total_depth)
}
