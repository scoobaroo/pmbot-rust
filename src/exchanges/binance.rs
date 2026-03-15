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

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws";

pub struct BinanceFeed;

impl ExchangeFeed for BinanceFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
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

            match connect_async(BINANCE_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "Binance", "WebSocket connected");
                    attempt = 0;
                    let _ = tx.send(ExchangeEvent::Connected(Exchange::Binance)).await;

                    let (mut write, mut read) = ws_stream.split();

                    // Subscribe to 24hr ticker + aggregate trades for all symbols
                    let mut params: Vec<String> = Vec::new();
                    for s in &symbols {
                        let ex_sym = crate::exchanges::common::to_exchange_symbol(Exchange::Binance, s);
                        params.push(format!("{}@ticker", ex_sym));
                        params.push(format!("{}@aggTrade", ex_sym));
                    }

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
                        error!(exchange = "Binance", error = %e, "failed to subscribe");
                        let _ = tx
                            .send(ExchangeEvent::Disconnected(Exchange::Binance))
                            .await;
                        continue;
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "Binance", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Some(tick) = parse_binance_ticker(&text) {
                                            debug!(exchange = "Binance", symbol = %tick.symbol, bid = %tick.bid, ask = %tick.ask, "tick");
                                            let _ = tx.send(ExchangeEvent::Tick(tick)).await;
                                        } else if let Some(event) = parse_binance_agg_trade(&text) {
                                            let _ = tx.send(event).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "Binance", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "Binance", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    let _ = tx
                        .send(ExchangeEvent::Disconnected(Exchange::Binance))
                        .await;
                }
                Err(e) => {
                    error!(exchange = "Binance", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "Binance",
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

fn parse_binance_ticker(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Filter out subscription ack: {"result":null,"id":1}
    if v.get("result").is_some() {
        return None;
    }

    // 24hrTicker event: {"e":"24hrTicker","s":"BTCUSDT","b":"42500.50","a":"42501.00","c":"42500.75","v":"1234.56",...}
    if v.get("e")?.as_str()? != "24hrTicker" {
        return None;
    }

    let symbol_raw = v.get("s")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Binance, symbol_raw);

    let bid = Decimal::from_str(v.get("b")?.as_str()?).ok()?;
    let ask = Decimal::from_str(v.get("a")?.as_str()?).ok()?;
    let last = Decimal::from_str(v.get("c")?.as_str()?).ok()?;
    let volume = Decimal::from_str(v.get("v")?.as_str()?).unwrap_or(Decimal::ZERO);

    Some(MarketTick {
        exchange: Exchange::Binance,
        symbol,
        bid,
        ask,
        last,
        volume_24h: volume,
        timestamp: Utc::now(),
    })
}

/// Parse Binance aggregate trade event.
/// `m` = true means the buyer is the maker → this is a SELL (taker sold into the bid).
fn parse_binance_agg_trade(text: &str) -> Option<ExchangeEvent> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    if v.get("e")?.as_str()? != "aggTrade" {
        return None;
    }

    let symbol_raw = v.get("s")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Binance, symbol_raw);
    let price = Decimal::from_str(v.get("p")?.as_str()?).ok()?;
    let quantity = Decimal::from_str(v.get("q")?.as_str()?).ok()?;
    // m=true means buyer is maker → trade was sell-initiated (taker sold)
    let is_buyer_maker = v.get("m")?.as_bool()?;
    let is_buy = !is_buyer_maker;

    Some(ExchangeEvent::AggTrade {
        symbol,
        price,
        quantity,
        is_buy,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_binance_ticker() {
        let json = r#"{
            "e": "24hrTicker",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "p": "100.00",
            "P": "0.24",
            "w": "42000.00",
            "c": "42500.75",
            "Q": "0.001",
            "b": "42500.50",
            "B": "1.200",
            "a": "42501.00",
            "A": "0.800",
            "o": "42400.75",
            "h": "42800.00",
            "l": "42100.00",
            "v": "1234.56",
            "q": "51849360.00",
            "O": 1672444800000,
            "C": 1672531200000,
            "F": 100,
            "L": 200,
            "n": 101
        }"#;

        let tick = parse_binance_ticker(json).unwrap();
        assert_eq!(tick.exchange, Exchange::Binance);
        assert_eq!(tick.symbol, "BTC-USD");
        assert_eq!(tick.bid, Decimal::from_str("42500.50").unwrap());
        assert_eq!(tick.ask, Decimal::from_str("42501.00").unwrap());
        assert_eq!(tick.last, Decimal::from_str("42500.75").unwrap());
        assert_eq!(tick.volume_24h, Decimal::from_str("1234.56").unwrap());
    }

    #[test]
    fn test_parse_binance_subscription_ack() {
        let json = r#"{"result":null,"id":1}"#;
        assert!(parse_binance_ticker(json).is_none());
    }

    #[test]
    fn test_parse_binance_non_ticker() {
        let json = r#"{"e":"aggTrade","E":1672531200000,"s":"BTCUSDT","p":"42500.50"}"#;
        assert!(parse_binance_ticker(json).is_none());
    }

    #[test]
    fn test_parse_binance_agg_trade_buy() {
        let json = r#"{"e":"aggTrade","E":1672531200000,"s":"BTCUSDT","a":123,"p":"42500.50","q":"0.5","f":100,"l":100,"T":1672531200000,"m":false}"#;
        let event = parse_binance_agg_trade(json).unwrap();
        match event {
            ExchangeEvent::AggTrade { symbol, price, quantity, is_buy, .. } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(price, Decimal::from_str("42500.50").unwrap());
                assert_eq!(quantity, Decimal::from_str("0.5").unwrap());
                assert!(is_buy); // m=false → buyer is taker → buy
            }
            _ => panic!("expected AggTrade"),
        }
    }

    #[test]
    fn test_parse_binance_agg_trade_sell() {
        let json = r#"{"e":"aggTrade","E":1672531200000,"s":"BTCUSDT","a":124,"p":"42499.00","q":"1.2","f":101,"l":101,"T":1672531200000,"m":true}"#;
        let event = parse_binance_agg_trade(json).unwrap();
        match event {
            ExchangeEvent::AggTrade { is_buy, .. } => {
                assert!(!is_buy); // m=true → buyer is maker → sell
            }
            _ => panic!("expected AggTrade"),
        }
    }
}
