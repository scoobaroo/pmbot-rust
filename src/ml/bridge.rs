use crate::polymarket::ws_orderbook::BookCache;
use crate::types::candle::Candle;
use crate::types::events::{AggregatorEvent, MlDirection, MlPrediction};
use crate::types::market::AggregatedPrice;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Configuration for the ML bridge.
#[derive(Debug, Clone)]
pub struct MlBridgeConfig {
    pub server_url: String,
    pub timeout_ms: u64,
    pub active_yes_token_id: Option<String>,
}

/// Request body for POST /candle.
#[derive(Serialize)]
struct CandlePayload {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

/// Response from POST /candle.
#[derive(Deserialize)]
struct CandleResp {
    #[allow(dead_code)]
    buffered: u64,
    ready: bool,
}

/// Response from POST /predict.
#[derive(Deserialize)]
struct PredictResp {
    direction: String,
    confidence: f64,
    model: String,
    #[allow(dead_code)]
    features_used: u64,
    #[allow(dead_code)]
    buffer_size: u64,
    latency_ms: f64,
}

/// Request body for POST /market-context.
#[derive(Serialize)]
struct MarketContextPayload {
    timestamp: i64,
    poly_best_bid: Option<f64>,
    poly_best_ask: Option<f64>,
    poly_spread: Option<f64>,
    poly_depth_bid: Option<f64>,
    poly_depth_ask: Option<f64>,
    oracle_price: Option<f64>,
    vwap: Option<f64>,
    cross_exchange_spread: Option<f64>,
    realtime_volatility: Option<f64>,
}

/// Response from POST /market-context.
#[derive(Deserialize)]
struct MarketContextResp {
    #[allow(dead_code)]
    buffered: u64,
    #[allow(dead_code)]
    last_implied_prob: f64,
}

/// Response from GET /health.
#[derive(Deserialize)]
struct HealthResp {
    status: String,
    #[allow(dead_code)]
    model: String,
    #[allow(dead_code)]
    candle_count: u64,
    #[allow(dead_code)]
    ready: bool,
}

/// Async ML bridge task that forwards candles to the Python prediction server
/// and sends ML predictions back to the strategy runner via mpsc channel.
pub struct MlBridge {
    config: MlBridgeConfig,
    client: reqwest::Client,
    book_cache: Option<BookCache>,
}

impl MlBridge {
    pub fn new(config: MlBridgeConfig, book_cache: Option<BookCache>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .expect("failed to build reqwest client");

        Self { config, client, book_cache }
    }

    /// Main loop: receives CandleComplete events from the aggregator,
    /// forwards candles to the prediction server, and sends predictions
    /// to the strategy runner.
    pub async fn run(
        self,
        mut candle_rx: mpsc::Receiver<AggregatorEvent>,
        prediction_tx: mpsc::Sender<MlPrediction>,
        shutdown: CancellationToken,
    ) {
        info!(server_url = %self.config.server_url, "ML bridge starting");

        // Wait for server to become available (non-blocking retry)
        self.wait_for_server(&shutdown).await;
        if shutdown.is_cancelled() {
            return;
        }

        info!("ML bridge connected to prediction server");

        let mut latest_agg_price: Option<AggregatedPrice> = None;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("ML bridge shutting down");
                    return;
                }
                Some(event) = candle_rx.recv() => {
                    match event {
                        AggregatorEvent::CandleComplete(c) => {
                            // Forward candle to prediction server
                            match self.send_candle(&c).await {
                                Ok(ready) => {
                                    // Send market context (fire-and-forget)
                                    if let Err(e) = self.send_market_context(&latest_agg_price).await {
                                        warn!(error = %e, "ML market context send failed");
                                    }

                                    if ready {
                                        // Buffer is warm — request prediction
                                        match self.request_prediction(&c.symbol).await {
                                            Ok(prediction) => {
                                                debug!(
                                                    symbol = %prediction.symbol,
                                                    direction = ?prediction.direction,
                                                    confidence = prediction.confidence,
                                                    latency_ms = prediction.latency_ms,
                                                    "ML prediction received"
                                                );
                                                if prediction_tx.send(prediction).await.is_err() {
                                                    warn!("ML prediction channel closed");
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "ML prediction request failed");
                                            }
                                        }
                                    } else {
                                        debug!(symbol = %c.symbol, "ML buffer warming up");
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "ML candle send failed");
                                }
                            }
                        }
                        AggregatorEvent::PriceUpdate(agg) => {
                            latest_agg_price = Some(agg);
                        }
                        _ => {} // ignore other events
                    }
                }
            }
        }
    }

    /// Retry GET /health every 5s until the server responds.
    async fn wait_for_server(&self, shutdown: &CancellationToken) {
        let url = format!("{}/health", self.config.server_url);
        loop {
            match self.client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(health) = resp.json::<HealthResp>().await {
                        if health.status == "ok" {
                            return;
                        }
                    }
                }
                _ => {}
            }
            info!("ML server not available, retrying in 5s...");
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }

    /// POST /candle — returns whether the buffer is ready for prediction.
    async fn send_candle(&self, candle: &Candle) -> Result<bool, reqwest::Error> {
        let url = format!("{}/candle", self.config.server_url);
        let payload = CandlePayload {
            timestamp: candle.open_time.timestamp(),
            open: dec_to_f64(candle.open),
            high: dec_to_f64(candle.high),
            low: dec_to_f64(candle.low),
            close: dec_to_f64(candle.close),
            volume: dec_to_f64(candle.volume),
        };

        let resp = self.client.post(&url).json(&payload).send().await?;
        let body: CandleResp = resp.json().await?;
        Ok(body.ready)
    }

    /// POST /predict — returns an MlPrediction.
    async fn request_prediction(&self, symbol: &str) -> Result<MlPrediction, reqwest::Error> {
        let url = format!("{}/predict", self.config.server_url);
        let body = serde_json::json!({ "symbol": symbol });

        let resp = self.client.post(&url).json(&body).send().await?;
        let pred: PredictResp = resp.json().await?;

        let direction = match pred.direction.as_str() {
            "up" => MlDirection::Up,
            _ => MlDirection::Down,
        };

        Ok(MlPrediction {
            symbol: symbol.to_string(),
            direction,
            confidence: pred.confidence,
            model: pred.model,
            latency_ms: pred.latency_ms,
            timestamp: Utc::now(),
        })
    }

    /// POST /market-context — fire-and-forget, sends live market data to prediction server.
    async fn send_market_context(
        &self,
        latest_agg: &Option<AggregatedPrice>,
    ) -> Result<(), reqwest::Error> {
        let url = format!("{}/market-context", self.config.server_url);

        // Read Polymarket book data if available
        let (poly_bid, poly_ask, poly_spread, poly_depth_bid, poly_depth_ask) =
            if let (Some(ref cache), Some(ref token_id)) =
                (&self.book_cache, &self.config.active_yes_token_id)
            {
                let books = cache.read().await;
                if let Some(snap) = books.get(token_id) {
                    (
                        Some(dec_to_f64(snap.best_bid)),
                        Some(dec_to_f64(snap.best_ask)),
                        Some(dec_to_f64(snap.spread)),
                        Some(dec_to_f64(snap.depth_bid)),
                        Some(dec_to_f64(snap.depth_ask)),
                    )
                } else {
                    (None, None, None, None, None)
                }
            } else {
                (None, None, None, None, None)
            };

        // Read aggregated price data
        let (oracle_price, vwap, spread, volatility, timestamp) = match latest_agg {
            Some(ref agg) => (
                agg.oracle_price.map(dec_to_f64),
                Some(dec_to_f64(agg.vwap)),
                Some(dec_to_f64(agg.spread)),
                Some(agg.volatility),
                agg.timestamp.timestamp(),
            ),
            None => (None, None, None, None, chrono::Utc::now().timestamp()),
        };

        let payload = MarketContextPayload {
            timestamp,
            poly_best_bid: poly_bid,
            poly_best_ask: poly_ask,
            poly_spread: poly_spread,
            poly_depth_bid: poly_depth_bid,
            poly_depth_ask: poly_depth_ask,
            oracle_price,
            vwap,
            cross_exchange_spread: spread,
            realtime_volatility: volatility,
        };

        let resp = self.client.post(&url).json(&payload).send().await?;
        let _body: MarketContextResp = resp.json().await?;
        Ok(())
    }
}

/// Convert Decimal to f64 using the existing pattern in the codebase.
fn dec_to_f64(d: rust_decimal::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}
