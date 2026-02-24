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

const COINBASE_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";
const COINBASE_REST_URL: &str = "https://api.exchange.coinbase.com";

pub struct CoinbaseFeed {
    http: reqwest::Client,
}

impl Default for CoinbaseFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl CoinbaseFeed {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    /// Fetch current ticker via REST API for initial price bootstrap.
    async fn fetch_ticker(&self, product_id: &str) -> Option<MarketTick> {
        let url = format!("{}/products/{}/ticker", COINBASE_REST_URL, product_id);
        let resp = match self.http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(exchange = "Coinbase", product_id, error = %e, "REST ticker fetch failed");
                return None;
            }
        };

        if !resp.status().is_success() {
            warn!(exchange = "Coinbase", product_id, status = %resp.status(), "REST ticker non-200");
            return None;
        }

        let text = resp.text().await.ok()?;
        parse_rest_ticker(product_id, &text)
    }
}

impl ExchangeFeed for CoinbaseFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Coinbase
    }

    async fn run(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<ExchangeEvent>,
        shutdown: CancellationToken,
    ) {
        // Bootstrap: fetch initial prices via REST
        for symbol in &symbols {
            let product_id =
                crate::exchanges::common::to_exchange_symbol(Exchange::Coinbase, symbol);
            if let Some(tick) = self.fetch_ticker(&product_id).await {
                info!(exchange = "Coinbase", symbol = %tick.symbol, "REST bootstrap tick");
                let _ = tx.send(ExchangeEvent::Tick(tick)).await;
            }
        }

        let mut attempt = 0u32;

        loop {
            if shutdown.is_cancelled() {
                return;
            }

            match connect_async(COINBASE_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "Coinbase", "WebSocket connected");
                    attempt = 0;
                    let _ = tx.send(ExchangeEvent::Connected(Exchange::Coinbase)).await;

                    let (mut write, mut read) = ws_stream.split();

                    let product_ids: Vec<String> = symbols
                        .iter()
                        .map(|s| {
                            crate::exchanges::common::to_exchange_symbol(Exchange::Coinbase, s)
                        })
                        .collect();

                    let sub_msg = serde_json::json!({
                        "type": "subscribe",
                        "product_ids": product_ids,
                        "channels": ["ticker"]
                    });

                    if let Err(e) = write
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            sub_msg.to_string(),
                        ))
                        .await
                    {
                        error!(exchange = "Coinbase", error = %e, "failed to subscribe");
                        let _ = tx
                            .send(ExchangeEvent::Disconnected(Exchange::Coinbase))
                            .await;
                        continue;
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "Coinbase", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Some(tick) = parse_ws_ticker(&text) {
                                            debug!(exchange = "Coinbase", symbol = %tick.symbol, bid = %tick.bid, ask = %tick.ask, "tick");
                                            let _ = tx.send(ExchangeEvent::Tick(tick)).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "Coinbase", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "Coinbase", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    let _ = tx
                        .send(ExchangeEvent::Disconnected(Exchange::Coinbase))
                        .await;
                }
                Err(e) => {
                    error!(exchange = "Coinbase", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "Coinbase",
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

/// Parse a WebSocket ticker message from the public Exchange feed.
fn parse_ws_ticker(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Only process "ticker" type messages; skip "subscriptions" ack and others
    if v.get("type")?.as_str()? != "ticker" {
        return None;
    }

    let product_id = v.get("product_id")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Coinbase, product_id);

    let price = Decimal::from_str(v.get("price")?.as_str()?).ok()?;
    let best_bid = v
        .get("best_bid")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(price);
    let best_ask = v
        .get("best_ask")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(price);
    let volume = v
        .get("volume_24h")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    Some(MarketTick {
        exchange: Exchange::Coinbase,
        symbol,
        bid: best_bid,
        ask: best_ask,
        last: price,
        volume_24h: volume,
        timestamp: Utc::now(),
    })
}

/// Parse a REST ticker response from /products/{id}/ticker.
fn parse_rest_ticker(product_id: &str, text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Coinbase, product_id);

    let price = Decimal::from_str(v.get("price")?.as_str()?).ok()?;
    let bid = v
        .get("bid")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(price);
    let ask = v
        .get("ask")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(price);
    let volume = v
        .get("volume")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    Some(MarketTick {
        exchange: Exchange::Coinbase,
        symbol,
        bid,
        ask,
        last: price,
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
            "type": "ticker",
            "sequence": 37475248783,
            "product_id": "BTC-USD",
            "price": "42500.75",
            "open_24h": "42400.75",
            "volume_24h": "1234.56",
            "low_24h": "42100.00",
            "high_24h": "42800.00",
            "volume_30d": "9788783.60",
            "best_bid": "42500.50",
            "best_bid_size": "0.46688654",
            "best_ask": "42501.00",
            "best_ask_size": "1.56637040",
            "side": "buy",
            "time": "2024-01-15T10:30:00.061769Z",
            "trade_id": 370843401,
            "last_size": "0.01"
        }"#;

        let tick = parse_ws_ticker(json).unwrap();
        assert_eq!(tick.exchange, Exchange::Coinbase);
        assert_eq!(tick.symbol, "BTC-USD");
        assert_eq!(tick.bid, Decimal::from_str("42500.50").unwrap());
        assert_eq!(tick.ask, Decimal::from_str("42501.00").unwrap());
        assert_eq!(tick.last, Decimal::from_str("42500.75").unwrap());
        assert_eq!(tick.volume_24h, Decimal::from_str("1234.56").unwrap());
    }

    #[test]
    fn test_parse_ws_subscription_ack() {
        let json = r#"{
            "type": "subscriptions",
            "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        }"#;
        assert!(parse_ws_ticker(json).is_none());
    }

    #[test]
    fn test_parse_ws_non_ticker() {
        let json = r#"{"type": "heartbeat", "sequence": 123, "product_id": "BTC-USD"}"#;
        assert!(parse_ws_ticker(json).is_none());
    }

    #[test]
    fn test_parse_rest_ticker() {
        let json = r#"{
            "trade_id": 86326522,
            "price": "6268.48",
            "size": "0.00698254",
            "time": "2024-01-15T10:30:00.833Z",
            "bid": "6265.15",
            "ask": "6267.71",
            "volume": "53602.03940154"
        }"#;

        let tick = parse_rest_ticker("BTC-USD", json).unwrap();
        assert_eq!(tick.exchange, Exchange::Coinbase);
        assert_eq!(tick.symbol, "BTC-USD");
        assert_eq!(tick.bid, Decimal::from_str("6265.15").unwrap());
        assert_eq!(tick.ask, Decimal::from_str("6267.71").unwrap());
        assert_eq!(tick.last, Decimal::from_str("6268.48").unwrap());
        assert_eq!(
            tick.volume_24h,
            Decimal::from_str("53602.03940154").unwrap()
        );
    }
}
