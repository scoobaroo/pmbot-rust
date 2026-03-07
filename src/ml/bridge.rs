use crate::types::candle::Candle;
use crate::types::events::{AggregatorEvent, MlDirection, MlPrediction};
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
}

impl MlBridge {
    pub fn new(config: MlBridgeConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .expect("failed to build reqwest client");

        Self { config, client }
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

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("ML bridge shutting down");
                    return;
                }
                Some(event) = candle_rx.recv() => {
                    let candle = match event {
                        AggregatorEvent::CandleComplete(c) => c,
                        _ => continue, // only process candles
                    };

                    // Forward candle to prediction server
                    match self.send_candle(&candle).await {
                        Ok(ready) => {
                            if ready {
                                // Buffer is warm — request prediction
                                match self.request_prediction(&candle.symbol).await {
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
                                debug!(symbol = %candle.symbol, "ML buffer warming up");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "ML candle send failed");
                        }
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
}

/// Convert Decimal to f64 using the existing pattern in the codebase.
fn dec_to_f64(d: rust_decimal::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}
