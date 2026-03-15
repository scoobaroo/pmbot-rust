use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::debug;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const POLL_INTERVAL_SECS: u64 = 10;
const ACTIVITY_WINDOW_SECS: i64 = 300; // 5 minutes

/// Recent trade activity for a Polymarket token.
#[derive(Debug, Clone, Default)]
pub struct TradeActivity {
    pub trade_count_5m: u64,
    pub volume_5m: f64,
    pub last_update: Option<DateTime<Utc>>,
}

/// Shared cache of trade activity per token_id.
pub type TradeActivityCache = Arc<RwLock<HashMap<String, TradeActivity>>>;

/// Create a new empty TradeActivityCache.
pub fn new_trade_activity_cache() -> TradeActivityCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Polls the Polymarket CLOB REST API for recent trades on active tokens.
pub struct TradePoller {
    client: reqwest::Client,
    cache: TradeActivityCache,
}

impl TradePoller {
    pub fn new(cache: TradeActivityCache) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build reqwest client");
        Self { client, cache }
    }

    pub async fn run(
        self,
        mut token_rx: tokio::sync::mpsc::Receiver<Vec<String>>,
        shutdown: CancellationToken,
    ) {
        let mut active_tokens: Vec<String> = Vec::new();
        let mut poll_interval = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                Some(tokens) = token_rx.recv() => {
                    // Update active token list from market scanner
                    for token in tokens {
                        if !active_tokens.contains(&token) {
                            active_tokens.push(token);
                        }
                    }
                    // Keep list manageable — only track most recent 20 tokens
                    if active_tokens.len() > 20 {
                        active_tokens = active_tokens.split_off(active_tokens.len() - 20);
                    }
                }
                _ = poll_interval.tick() => {
                    for token_id in &active_tokens {
                        self.poll_token(token_id).await;
                    }
                }
            }
        }
    }

    async fn poll_token(&self, token_id: &str) {
        let url = format!(
            "{}/trades?asset_id={}&limit=50",
            CLOB_BASE, token_id
        );

        let resp = match self.client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                debug!(error = %e, token_id = token_id, "trade poll failed");
                return;
            }
        };

        let body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                debug!(error = %e, "trade poll parse failed");
                return;
            }
        };

        let now = Utc::now();
        let cutoff = now - chrono::Duration::seconds(ACTIVITY_WINDOW_SECS);

        let mut count = 0u64;
        let mut volume = 0.0f64;

        // Parse trades array — Polymarket CLOB returns [{ "size": "10", "price": "0.55", "timestamp": "..." }, ...]
        if let Some(trades) = body.as_array() {
            for trade in trades {
                // Parse timestamp
                let ts_str = trade.get("match_time").or_else(|| trade.get("timestamp"));
                if let Some(ts_val) = ts_str {
                    let ts = if let Some(s) = ts_val.as_str() {
                        DateTime::parse_from_rfc3339(s)
                            .map(|dt| dt.with_timezone(&Utc))
                            .ok()
                    } else if let Some(ms) = ts_val.as_i64() {
                        DateTime::from_timestamp(ms / 1000, 0)
                    } else {
                        None
                    };

                    if let Some(trade_time) = ts {
                        if trade_time < cutoff {
                            continue;
                        }
                    }
                }

                count += 1;
                if let Some(size_str) = trade.get("size").and_then(|v| v.as_str()) {
                    volume += size_str.parse::<f64>().unwrap_or(0.0);
                }
            }
        }

        let mut cache = self.cache.write().await;
        cache.insert(
            token_id.to_string(),
            TradeActivity {
                trade_count_5m: count,
                volume_5m: volume,
                last_update: Some(now),
            },
        );
    }
}
