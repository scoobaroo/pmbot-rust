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

const OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub struct OkxFeed;

impl ExchangeFeed for OkxFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Okx
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

            match connect_async(OKX_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "OKX", "WebSocket connected");
                    attempt = 0;
                    let _ = tx.send(ExchangeEvent::Connected(Exchange::Okx)).await;

                    let (mut write, mut read) = ws_stream.split();

                    // Subscribe to tickers for all symbols
                    let args: Vec<serde_json::Value> = symbols
                        .iter()
                        .map(|s| {
                            serde_json::json!({
                                "channel": "tickers",
                                "instId": crate::exchanges::common::to_exchange_symbol(Exchange::Okx, s)
                            })
                        })
                        .collect();

                    let sub_msg = serde_json::json!({
                        "op": "subscribe",
                        "args": args
                    });

                    if let Err(e) = write
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            sub_msg.to_string(),
                        ))
                        .await
                    {
                        error!(exchange = "OKX", error = %e, "failed to subscribe");
                        let _ = tx.send(ExchangeEvent::Disconnected(Exchange::Okx)).await;
                        continue;
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "OKX", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Some(tick) = parse_okx_ticker(&text) {
                                            debug!(exchange = "OKX", symbol = %tick.symbol, bid = %tick.bid, ask = %tick.ask, "tick");
                                            let _ = tx.send(ExchangeEvent::Tick(tick)).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "OKX", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "OKX", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    let _ = tx.send(ExchangeEvent::Disconnected(Exchange::Okx)).await;
                }
                Err(e) => {
                    error!(exchange = "OKX", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "OKX",
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

fn parse_okx_ticker(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Filter out subscription ack: {"event":"subscribe","arg":{...}}
    if v.get("event").is_some() {
        return None;
    }

    // Ticker push: {"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{...}]}
    let arg = v.get("arg")?;
    if arg.get("channel")?.as_str()? != "tickers" {
        return None;
    }

    let data = v.get("data")?.as_array()?.first()?;
    let inst_id = data.get("instId")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Okx, inst_id);

    let bid = Decimal::from_str(data.get("bidPx")?.as_str()?).ok()?;
    let ask = Decimal::from_str(data.get("askPx")?.as_str()?).ok()?;
    let last = Decimal::from_str(data.get("last")?.as_str()?).ok()?;
    let volume = data
        .get("vol24h")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    Some(MarketTick {
        exchange: Exchange::Okx,
        symbol,
        bid,
        ask,
        last,
        volume_24h: volume,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_okx_ticker() {
        let json = r#"{
            "arg": {
                "channel": "tickers",
                "instId": "BTC-USDT"
            },
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "last": "42500.75",
                "lastSz": "0.1",
                "askPx": "42501.00",
                "askSz": "11",
                "bidPx": "42500.50",
                "bidSz": "5",
                "open24h": "42400.75",
                "high24h": "42800.00",
                "low24h": "42100.00",
                "volCcy24h": "51849360.00",
                "vol24h": "1234.56",
                "sodUtc0": "42300.00",
                "sodUtc8": "42350.00",
                "ts": "1672531200000"
            }]
        }"#;

        let tick = parse_okx_ticker(json).unwrap();
        assert_eq!(tick.exchange, Exchange::Okx);
        assert_eq!(tick.symbol, "BTC-USD");
        assert_eq!(tick.bid, Decimal::from_str("42500.50").unwrap());
        assert_eq!(tick.ask, Decimal::from_str("42501.00").unwrap());
        assert_eq!(tick.last, Decimal::from_str("42500.75").unwrap());
        assert_eq!(tick.volume_24h, Decimal::from_str("1234.56").unwrap());
    }

    #[test]
    fn test_parse_okx_subscription_ack() {
        let json = r#"{"event":"subscribe","arg":{"channel":"tickers","instId":"BTC-USDT"}}"#;
        assert!(parse_okx_ticker(json).is_none());
    }

    #[test]
    fn test_parse_okx_non_ticker() {
        let json = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{"instId": "BTC-USDT", "px": "42500.50"}]
        }"#;
        assert!(parse_okx_ticker(json).is_none());
    }
}
