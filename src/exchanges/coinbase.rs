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

const COINBASE_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

pub struct CoinbaseFeed;

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
                        "channel": "ticker"
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
                                        if let Some(tick) = parse_coinbase_ticker(&text) {
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

fn parse_coinbase_ticker(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    let channel = v.get("channel")?.as_str()?;
    if channel != "ticker" {
        return None;
    }

    let events = v.get("events")?.as_array()?;
    let event = events.first()?;
    let tickers = event.get("tickers")?.as_array()?;
    let ticker = tickers.first()?;

    let product_id = ticker.get("product_id")?.as_str()?;
    let symbol = crate::exchanges::common::normalize_symbol(Exchange::Coinbase, product_id);

    let price = Decimal::from_str(ticker.get("price")?.as_str()?).ok()?;
    let best_bid = ticker
        .get("best_bid")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(price);
    let best_ask = ticker
        .get("best_ask")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(price);
    let volume = ticker
        .get("volume_24_h")
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
