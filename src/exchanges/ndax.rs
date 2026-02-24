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

const NDAX_WS_URL: &str = "wss://api.ndax.io/WSGateway/";

pub struct NdaxFeed;

impl ExchangeFeed for NdaxFeed {
    fn exchange(&self) -> Exchange {
        Exchange::Ndax
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

            match connect_async(NDAX_WS_URL).await {
                Ok((ws_stream, _)) => {
                    info!(exchange = "NDAX", "WebSocket connected");
                    attempt = 0;
                    let _ = tx.send(ExchangeEvent::Connected(Exchange::Ndax)).await;

                    let (mut write, mut read) = ws_stream.split();

                    // NDAX uses a JSON-RPC style protocol
                    // Subscribe to Level1 market data for each instrument
                    for (i, sym) in symbols.iter().enumerate() {
                        let ndax_sym =
                            crate::exchanges::common::to_exchange_symbol(Exchange::Ndax, sym);
                        // NDAX SubscribeLevel1 frame
                        let sub_msg = serde_json::json!({
                            "m": 0,      // MessageType: Request
                            "i": i + 1,  // Sequence number
                            "n": "SubscribeLevel1",
                            "o": serde_json::json!({
                                "InstrumentId": instrument_id_for_symbol(&ndax_sym),
                            }).to_string(),
                        });

                        if let Err(e) = write
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                sub_msg.to_string(),
                            ))
                            .await
                        {
                            error!(exchange = "NDAX", error = %e, "failed to subscribe");
                        }
                    }

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                info!(exchange = "NDAX", "shutting down");
                                return;
                            }
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Some(tick) = parse_ndax_message(&text) {
                                            debug!(exchange = "NDAX", symbol = %tick.symbol, bid = %tick.bid, ask = %tick.ask, "tick");
                                            let _ = tx.send(ExchangeEvent::Tick(tick)).await;
                                        }
                                    }
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        warn!(exchange = "NDAX", error = %e, "WebSocket error");
                                        break;
                                    }
                                    None => {
                                        warn!(exchange = "NDAX", "WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    let _ = tx.send(ExchangeEvent::Disconnected(Exchange::Ndax)).await;
                }
                Err(e) => {
                    error!(exchange = "NDAX", error = %e, attempt, "connection failed");
                }
            }

            let delay = backoff_delay(attempt);
            warn!(
                exchange = "NDAX",
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

/// Map common symbols to NDAX instrument IDs.
/// In production, these would be fetched from the NDAX REST API.
fn instrument_id_for_symbol(symbol: &str) -> u32 {
    match symbol {
        "BTCUSD" => 1,
        "ETHUSD" => 3,
        "BTCCAD" => 2,
        "ETHCAD" => 4,
        _ => 1, // default to BTC/USD
    }
}

fn parse_ndax_message(text: &str) -> Option<MarketTick> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;

    let msg_type = v.get("n")?.as_str()?;
    if msg_type != "Level1UpdateEvent" && msg_type != "SubscribeLevel1" {
        return None;
    }

    let payload_str = v.get("o")?.as_str()?;
    let payload: serde_json::Value = serde_json::from_str(payload_str).ok()?;

    let bid = dec_from_value(&payload, "BestBid")?;
    let ask = dec_from_value(&payload, "BestOffer")?;
    let last = dec_from_value(&payload, "LastTradedPx").unwrap_or(bid);
    let volume = dec_from_value(&payload, "Rolling24HrVolume").unwrap_or(Decimal::ZERO);

    // Try to get symbol from InstrumentId
    let instrument_id = payload.get("InstrumentId")?.as_u64()? as u32;
    let symbol = symbol_for_instrument_id(instrument_id);

    Some(MarketTick {
        exchange: Exchange::Ndax,
        symbol,
        bid,
        ask,
        last,
        volume_24h: volume,
        timestamp: Utc::now(),
    })
}

fn symbol_for_instrument_id(id: u32) -> String {
    match id {
        1 => "BTC-USD".to_string(),
        2 => "BTC-CAD".to_string(),
        3 => "ETH-USD".to_string(),
        4 => "ETH-CAD".to_string(),
        _ => format!("UNKNOWN-{}", id),
    }
}

fn dec_from_value(payload: &serde_json::Value, key: &str) -> Option<Decimal> {
    let v = payload.get(key)?;
    if let Some(f) = v.as_f64() {
        Decimal::from_str(&f.to_string()).ok()
    } else if let Some(s) = v.as_str() {
        Decimal::from_str(s).ok()
    } else {
        None
    }
}
