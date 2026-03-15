use crate::exchanges::backoff_delay;
use crate::types::events::ExchangeEvent;
use crate::types::market::Exchange;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";

/// Streams real-time funding rate + mark price from Binance USDS-M Futures.
/// Subscribes to `<symbol>@markPrice@1s` — pushes every 1 second.
pub struct BinanceFuturesFeed;

impl BinanceFuturesFeed {
    pub async fn run(
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

            match connect_async(BINANCE_FUTURES_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "BinanceFutures", "WebSocket connected");
                    attempt = 0;

                    let (mut write, mut read) = ws_stream.split();

                    // Subscribe to mark price stream at 1s interval for all symbols
                    let params: Vec<String> = symbols
                        .iter()
                        .map(|s| {
                            let ex_sym = to_futures_symbol(s);
                            format!("{}@markPrice@1s", ex_sym)
                        })
                        .collect();

                    let sub_msg = serde_json::json!({
                        "method": "SUBSCRIBE",
                        "params": params,
                        "id": 1
                    });

                    if let Err(e) = write
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            sub_msg.to_string(),
                        ))
                        .await
                    {
                        error!(exchange = "BinanceFutures", error = %e, "failed to subscribe");
                        continue;
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "BinanceFutures", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Some(event) = parse_mark_price_update(&text) {
                                            let _ = tx.send(event).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "BinanceFutures", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "BinanceFutures", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(exchange = "BinanceFutures", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "BinanceFutures",
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

/// Convert canonical symbol (e.g. "BTC-USD") to Binance futures format ("btcusdt").
fn to_futures_symbol(symbol: &str) -> String {
    symbol
        .replace("-USD", "usdt")
        .replace("-", "")
        .to_lowercase()
}

/// Parse Binance Futures markPriceUpdate event.
/// Payload: { "e": "markPriceUpdate", "s": "BTCUSDT", "p": "42500.50", "i": "42500.00",
///            "P": "42500.25", "r": "0.00010000", "T": 1672531200000 }
fn parse_mark_price_update(text: &str) -> Option<ExchangeEvent> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Skip subscription ack
    if v.get("result").is_some() {
        return None;
    }

    if v.get("e")?.as_str()? != "markPriceUpdate" {
        return None;
    }

    let symbol_raw = v.get("s")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Binance, symbol_raw);
    let rate = Decimal::from_str(v.get("r")?.as_str()?).ok()?;
    let mark_price = Decimal::from_str(v.get("p")?.as_str()?).ok()?;

    // Next funding time in milliseconds
    let next_funding_ms = v.get("T")?.as_i64()?;
    let next_funding_time = DateTime::from_timestamp(next_funding_ms / 1000, 0)
        .unwrap_or_else(|| Utc::now());

    debug!(
        exchange = "BinanceFutures",
        symbol = %symbol,
        funding_rate = %rate,
        mark_price = %mark_price,
        "funding rate update"
    );

    Some(ExchangeEvent::FundingRate {
        symbol,
        rate,
        mark_price,
        next_funding_time,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mark_price_update() {
        let json = r#"{
            "e": "markPriceUpdate",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "p": "42500.50",
            "i": "42500.00",
            "P": "42500.25",
            "r": "0.00015000",
            "T": 1672560000000
        }"#;

        let event = parse_mark_price_update(json).unwrap();
        match event {
            ExchangeEvent::FundingRate {
                symbol,
                rate,
                mark_price,
                ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(rate, Decimal::from_str("0.00015000").unwrap());
                assert_eq!(mark_price, Decimal::from_str("42500.50").unwrap());
            }
            _ => panic!("expected FundingRate"),
        }
    }

    #[test]
    fn test_parse_subscription_ack() {
        let json = r#"{"result":null,"id":1}"#;
        assert!(parse_mark_price_update(json).is_none());
    }

    #[test]
    fn test_to_futures_symbol() {
        assert_eq!(to_futures_symbol("BTC-USD"), "btcusdt");
        assert_eq!(to_futures_symbol("ETH-USD"), "ethusdt");
        assert_eq!(to_futures_symbol("SOL-USD"), "solusdt");
    }
}
