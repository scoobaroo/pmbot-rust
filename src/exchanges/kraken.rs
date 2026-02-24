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
const KRAKEN_REST_URL: &str = "https://api.kraken.com/0/public";

pub struct KrakenFeed {
    http: reqwest::Client,
}

impl Default for KrakenFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl KrakenFeed {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    /// Fetch current ticker via REST API for initial price bootstrap.
    async fn fetch_ticker(&self, pair: &str) -> Option<MarketTick> {
        let url = format!("{}/Ticker?pair={}", KRAKEN_REST_URL, pair);
        let resp = match self.http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(exchange = "Kraken", pair, error = %e, "REST ticker fetch failed");
                return None;
            }
        };

        if !resp.status().is_success() {
            warn!(exchange = "Kraken", pair, status = %resp.status(), "REST ticker non-200");
            return None;
        }

        let text = resp.text().await.ok()?;
        parse_rest_ticker(&text)
    }
}

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
        // Bootstrap: fetch initial prices via REST
        for symbol in &symbols {
            // Kraken REST uses pairs like "XBTUSD" (no slash)
            let kraken_pair =
                crate::exchanges::common::to_exchange_symbol(Exchange::Kraken, symbol)
                    .replace('/', "");
            if let Some(tick) = self.fetch_ticker(&kraken_pair).await {
                info!(exchange = "Kraken", symbol = %tick.symbol, "REST bootstrap tick");
                let _ = tx.send(ExchangeEvent::Tick(tick)).await;
            }
        }

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
                                        if let Some(tick) = parse_ws_ticker(&text) {
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

/// Parse a WebSocket ticker message from Kraken v2 feed.
fn parse_ws_ticker(text: &str) -> Option<MarketTick> {
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

/// Parse a REST ticker response from /0/public/Ticker.
/// Response: {"error":[],"result":{"XXBTZUSD":{"a":["price","wlv","lv"],"b":["price","wlv","lv"],"c":["price","vol"],"v":["today","24h"],...}}}
fn parse_rest_ticker(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    let result = v.get("result")?.as_object()?;
    // Take the first (and usually only) pair from the result
    let (pair_key, pair_data) = result.iter().next()?;

    let symbol = crate::exchanges::common::normalize_symbol(
        Exchange::Kraken,
        &pair_key.replace("XXBT", "XBT/").replace("ZUSD", "USD"),
    );

    // "b": ["price", "whole_lot_volume", "lot_volume"]
    let bid = Decimal::from_str(pair_data.get("b")?.as_array()?.first()?.as_str()?).ok()?;
    // "a": ["price", "whole_lot_volume", "lot_volume"]
    let ask = Decimal::from_str(pair_data.get("a")?.as_array()?.first()?.as_str()?).ok()?;
    // "c": ["price", "lot_volume"]
    let last = Decimal::from_str(pair_data.get("c")?.as_array()?.first()?.as_str()?).ok()?;
    // "v": ["today", "last_24h"]
    let volume = pair_data
        .get("v")
        .and_then(|v| v.as_array())
        .and_then(|a| a.get(1))
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ws_ticker() {
        let json = r#"{
            "channel": "ticker",
            "type": "update",
            "data": [{
                "symbol": "XBT/USD",
                "bid": 42500.50,
                "ask": 42501.00,
                "last": 42500.75,
                "volume": 1234.56,
                "vwap": 42450.00,
                "low": 42100.00,
                "high": 42800.00,
                "change": 100.00,
                "change_pct": 0.24
            }]
        }"#;

        let tick = parse_ws_ticker(json).unwrap();
        assert_eq!(tick.exchange, Exchange::Kraken);
        assert_eq!(tick.symbol, "BTC-USD");
        assert_eq!(tick.bid, Decimal::from_str("42500.5").unwrap());
        assert_eq!(tick.ask, Decimal::from_str("42501").unwrap());
        assert_eq!(tick.last, Decimal::from_str("42500.75").unwrap());
    }

    #[test]
    fn test_parse_ws_subscription_ack() {
        let json = r#"{"channel":"status","type":"update","data":[{"system":"online","version":"2.0.0"}]}"#;
        assert!(parse_ws_ticker(json).is_none());
    }

    #[test]
    fn test_parse_ws_non_ticker() {
        let json = r#"{"channel":"heartbeat"}"#;
        assert!(parse_ws_ticker(json).is_none());
    }

    #[test]
    fn test_parse_rest_ticker() {
        let json = r#"{
            "error": [],
            "result": {
                "XXBTZUSD": {
                    "a": ["42501.00000", "1", "1.000"],
                    "b": ["42500.50000", "1", "1.000"],
                    "c": ["42500.75000", "0.001"],
                    "v": ["1000.00", "1234.56"],
                    "p": ["42450.00", "42400.00"],
                    "t": [1000, 2000],
                    "l": ["42100.00", "42000.00"],
                    "h": ["42800.00", "42900.00"],
                    "o": "42400.00"
                }
            }
        }"#;

        let tick = parse_rest_ticker(json).unwrap();
        assert_eq!(tick.exchange, Exchange::Kraken);
        assert_eq!(tick.symbol, "BTC-USD");
        assert_eq!(tick.bid, Decimal::from_str("42500.50000").unwrap());
        assert_eq!(tick.ask, Decimal::from_str("42501.00000").unwrap());
        assert_eq!(tick.last, Decimal::from_str("42500.75000").unwrap());
        assert_eq!(tick.volume_24h, Decimal::from_str("1234.56").unwrap());
    }
}
