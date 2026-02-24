use crate::exchanges::{backoff_delay, ExchangeFeed};
use crate::types::events::ExchangeEvent;
use crate::types::market::{Exchange, MarketTick};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const KRAKEN_WS_URL: &str = "wss://ws.kraken.com/v2";

pub struct KrakenFeed;

impl ExchangeFeed for KrakenFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Kraken
    }

    async fn run(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<ExchangeEvent>,
        shutdown: CancellationToken,
    ) {
        let mut attempt = 0u32;

        loop {
            if shutdown.is_cancelled() {
                return;
            }

            match connect_async(KRAKEN_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "Kraken", "WebSocket connected");
                    attempt = 0;
                    let _ = tx.send(ExchangeEvent::Connected(Exchange::Kraken)).await;

                    let (mut write, mut read) = ws_stream.split();

                    // Subscribe to ticker for all symbols
                    let kraken_symbols: Vec<String> = symbols
                        .iter()
                        .map(|s| crate::exchanges::common::to_exchange_symbol(Exchange::Kraken, s))
                        .collect();

                    let sub_msg = serde_json::json!({
                        "method": "subscribe",
                        "params": {
                            "channel": "ticker",
                            "symbol": kraken_symbols,
                        }
                    });

                    if let Err(e) = write
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            sub_msg.to_string(),
                        ))
                        .await
                    {
                        error!(exchange = "Kraken", error = %e, "failed to subscribe");
                        let _ = tx.send(ExchangeEvent::Disconnected(Exchange::Kraken)).await;
                        continue;
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "Kraken", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Some(tick) = parse_kraken_ticker(&text) {
                                            debug!(exchange = "Kraken", symbol = %tick.symbol, bid = %tick.bid, ask = %tick.ask, "tick");
                                            let _ = tx.send(ExchangeEvent::Tick(tick)).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "Kraken", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "Kraken", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    let _ = tx.send(ExchangeEvent::Disconnected(Exchange::Kraken)).await;
                }
                Err(e) => {
                    error!(exchange = "Kraken", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "Kraken",
                delay_secs = delay.as_secs(),
                "reconnecting"
            );
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => return,
            }
            attempt = attempt.saturating_add(1);
        }
    }
}

fn parse_kraken_ticker(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Kraken v2 ticker: {"channel":"ticker","type":"update","data":[{...}]}
    if v.get("channel")?.as_str()? != "ticker" {
        return None;
    }
    if v.get("type")?.as_str()? != "update" && v.get("type")?.as_str()? != "snapshot" {
        return None;
    }

    let data = v.get("data")?.as_array()?.first()?;
    let symbol_raw = data.get("symbol")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Kraken, symbol_raw);

    let bid = Decimal::from_str(data.get("bid")?.as_f64()?.to_string().as_str()).ok()?;
    let ask = Decimal::from_str(data.get("ask")?.as_f64()?.to_string().as_str()).ok()?;
    let last = Decimal::from_str(data.get("last")?.as_f64()?.to_string().as_str()).ok()?;
    let volume = Decimal::from_str(data.get("volume")?.as_f64()?.to_string().as_str())
        .unwrap_or(Decimal::ZERO);

    Some(MarketTick {
        exchange: Exchange::Kraken,
        symbol,
        bid,
        ask,
        last,
        volume_24h: volume,
        timestamp: Utc::now(),
    })
}
