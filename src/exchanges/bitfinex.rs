use crate::exchanges::{backoff_delay, ExchangeFeed};
use crate::types::events::ExchangeEvent;
use crate::types::market::{Exchange, MarketTick};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const BITFINEX_WS_URL: &str = "wss://api-pub.bitfinex.com/ws/2";

pub struct BitfinexFeed;

impl ExchangeFeed for BitfinexFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Bitfinex
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

            match connect_async(BITFINEX_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "Bitfinex", "WebSocket connected");
                    attempt = 0;
                    let _ = tx.send(ExchangeEvent::Connected(Exchange::Bitfinex)).await;

                    let (mut write, mut read) = ws_stream.split();

                    // Bitfinex uses channel IDs; map them after subscription
                    let mut chan_map: HashMap<u64, String> = HashMap::new();

                    // Subscribe to each symbol individually
                    for sym in &symbols {
                        let bfx_sym =
                            crate::exchanges::common::to_exchange_symbol(Exchange::Bitfinex, sym);
                        let sub_msg = serde_json::json!({
                            "event": "subscribe",
                            "channel": "ticker",
                            "symbol": bfx_sym,
                        });
                        if let Err(e) = write
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                sub_msg.to_string(),
                            ))
                            .await
                        {
                            error!(exchange = "Bitfinex", error = %e, "failed to subscribe");
                        }
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "Bitfinex", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        // Handle subscription confirmations and ticker data
                                        if let Some(tick) = parse_bitfinex_message(&text, &mut chan_map) {
                                            debug!(exchange = "Bitfinex", symbol = %tick.symbol, bid = %tick.bid, ask = %tick.ask, "tick");
                                            let _ = tx.send(ExchangeEvent::Tick(tick)).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "Bitfinex", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "Bitfinex", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    let _ = tx
                        .send(ExchangeEvent::Disconnected(Exchange::Bitfinex))
                        .await;
                }
                Err(e) => {
                    error!(exchange = "Bitfinex", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "Bitfinex",
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

fn parse_bitfinex_message(text: &str, chan_map: &mut HashMap<u64, String>) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    // Handle subscription confirmation: {"event":"subscribed","channel":"ticker","chanId":123,"symbol":"tBTCUSD"}
    if let Some(event) = v.get("event").and_then(|e| e.as_str()) {
        if event == "subscribed" {
            if let (Some(chan_id), Some(symbol)) = (
                v.get("chanId").and_then(|c| c.as_u64()),
                v.get("symbol").and_then(|s| s.as_str()),
            ) {
                let normalized =
                    crate::exchanges::common::normalize_symbol(Exchange::Bitfinex, symbol);
                chan_map.insert(chan_id, normalized);
            }
        }
        return None;
    }

    // Ticker update: [CHAN_ID,[BID,BID_SIZE,ASK,ASK_SIZE,DAILY_CHANGE,DAILY_CHANGE_RELATIVE,LAST_PRICE,VOLUME,HIGH,LOW]]
    let arr = v.as_array()?;
    if arr.len() < 2 {
        return None;
    }

    let chan_id = arr[0].as_u64()?;
    let symbol = chan_map.get(&chan_id)?.clone();

    // Skip heartbeat messages: [CHAN_ID,"hb"]
    if arr[1].is_string() {
        return None;
    }

    let data = arr[1].as_array()?;
    if data.len() < 8 {
        return None;
    }

    let bid = dec_from_value(&data[0])?;
    let ask = dec_from_value(&data[2])?;
    let last = dec_from_value(&data[6])?;
    let volume = dec_from_value(&data[7]).unwrap_or(Decimal::ZERO);

    Some(MarketTick {
        exchange: Exchange::Bitfinex,
        symbol,
        bid,
        ask,
        last,
        volume_24h: volume,
        timestamp: Utc::now(),
    })
}

fn dec_from_value(v: &serde_json::Value) -> Option<Decimal> {
    if let Some(f) = v.as_f64() {
        Decimal::from_str(&f.to_string()).ok()
    } else if let Some(s) = v.as_str() {
        Decimal::from_str(s).ok()
    } else {
        None
    }
}
