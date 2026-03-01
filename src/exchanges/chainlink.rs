use crate::types::events::ExchangeEvent;
use crate::types::market::{Exchange, MarketTick};
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use std::fmt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
struct ChainlinkError(String);

impl fmt::Display for ChainlinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ChainlinkError {}

/// Which RPC endpoint to use for a given feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chain {
    Polygon,
    Ethereum,
}

/// Chainlink price feed contract addresses.
/// Most feeds are on Polygon mainnet; SOL-USD is on Ethereum mainnet
/// (no Chainlink SOL feed exists on Polygon).
const FEEDS: &[(&str, &str, Chain)] = &[
    (
        "BTC-USD",
        "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        Chain::Polygon,
    ),
    (
        "ETH-USD",
        "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        Chain::Polygon,
    ),
    (
        "XRP-USD",
        "0x785ba89291f676b5386652eB12b30cF361020694",
        Chain::Polygon,
    ),
    (
        "SOL-USD",
        "0x4ffC43a60e009B551865A93d232E33Fce9f01507",
        Chain::Ethereum,
    ),
];

/// Selector for `latestRoundData()` — keccak256("latestRoundData()")[:4]
const LATEST_ROUND_DATA_SELECTOR: &str = "0xfeaf968c";

/// Chainlink oracle polling interval.
const POLL_INTERVAL_SECS: u64 = 5;

/// Chainlink has 8 decimal places for USD-denominated feeds.
const CHAINLINK_DECIMALS: u32 = 8;

pub struct ChainlinkFeed {
    polygon_rpc_url: String,
    ethereum_rpc_url: String,
}

impl ChainlinkFeed {
    pub fn new(polygon_rpc_url: &str, ethereum_rpc_url: &str) -> Self {
        Self {
            polygon_rpc_url: polygon_rpc_url.to_string(),
            ethereum_rpc_url: ethereum_rpc_url.to_string(),
        }
    }

    fn rpc_url(&self, chain: Chain) -> &str {
        match chain {
            Chain::Polygon => &self.polygon_rpc_url,
            Chain::Ethereum => &self.ethereum_rpc_url,
        }
    }

    pub async fn run(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<ExchangeEvent>,
        shutdown: CancellationToken,
    ) {
        // Filter feeds to only those matching configured symbols
        let active_feeds: Vec<(&str, &str, Chain)> = FEEDS
            .iter()
            .filter(|(sym, _, _)| symbols.iter().any(|s| s == *sym))
            .copied()
            .collect();

        if active_feeds.is_empty() {
            info!("Chainlink: no matching feeds for configured symbols");
            return;
        }

        let polygon_count = active_feeds
            .iter()
            .filter(|(_, _, c)| *c == Chain::Polygon)
            .count();
        let ethereum_count = active_feeds
            .iter()
            .filter(|(_, _, c)| *c == Chain::Ethereum)
            .count();

        info!(
            polygon_feeds = polygon_count,
            ethereum_feeds = ethereum_count,
            "Chainlink oracle feed started (polling every {}s)",
            POLL_INTERVAL_SECS
        );
        let _ = tx.send(ExchangeEvent::Connected(Exchange::Chainlink)).await;

        let client = reqwest::Client::new();
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(POLL_INTERVAL_SECS));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Chainlink feed shutting down");
                    let _ = tx.send(ExchangeEvent::Disconnected(Exchange::Chainlink)).await;
                    return;
                }
                _ = interval.tick() => {
                    for &(symbol, address, chain) in &active_feeds {
                        let rpc = self.rpc_url(chain);
                        match Self::fetch_price(&client, rpc, address).await {
                            Ok(price) => {
                                let tick = MarketTick {
                                    exchange: Exchange::Chainlink,
                                    symbol: symbol.to_string(),
                                    bid: price,
                                    ask: price,
                                    last: price,
                                    volume_24h: Decimal::ZERO,
                                    timestamp: Utc::now(),
                                };
                                debug!(symbol, price = %price, ?chain, "Chainlink tick");
                                if tx.send(ExchangeEvent::Tick(tick)).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                warn!(symbol, ?chain, error = %e, "Chainlink fetch failed");
                            }
                        }
                    }
                }
            }
        }
    }

    async fn fetch_price(
        client: &reqwest::Client,
        rpc_url: &str,
        contract_address: &str,
    ) -> Result<Decimal, BoxError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": contract_address,
                    "data": LATEST_ROUND_DATA_SELECTOR
                },
                "latest"
            ]
        });

        let resp = client
            .post(rpc_url)
            .json(&body)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            return Err(ChainlinkError(format!("RPC error: {}", err)).into());
        }

        let result = resp
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ChainlinkError("missing result in RPC response".into()))?;

        decode_latest_round_data(result)
    }
}

/// Decode the hex response from `latestRoundData()`.
///
/// Returns: `(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)`
/// Each field is 32 bytes (right-padded for uint80). The `answer` field (bytes 32..64) contains
/// the price with 8 decimals.
fn decode_latest_round_data(hex_result: &str) -> Result<Decimal, BoxError> {
    let hex = hex_result.strip_prefix("0x").unwrap_or(hex_result);

    // We need at least 64 hex chars (32 bytes) for roundId + answer
    if hex.len() < 128 {
        return Err(ChainlinkError(format!(
            "response too short: {} hex chars (need >=128)",
            hex.len()
        ))
        .into());
    }

    // answer is bytes 32..64 → hex chars 64..128
    let answer_hex = &hex[64..128];

    // Parse as i256 (signed) — but prices are always positive
    let answer = i128::from_str_radix(answer_hex, 16)
        .map_err(|e| ChainlinkError(format!("failed to parse answer: {}", e)))?;

    if answer <= 0 {
        return Err(ChainlinkError(format!("invalid Chainlink price: {}", answer)).into());
    }

    // Divide by 10^8 to get USD price
    let divisor = Decimal::from(10u64.pow(CHAINLINK_DECIMALS));
    let price = Decimal::from(answer) / divisor;

    Ok(price)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_chainlink_response() {
        // BTC at $97,123.456789 → answer = 9712345678900
        let hex = "0x\
            0000000000000000000000000000000000000000000000000000000000000001\
            000000000000000000000000000000000000000000000000000008d554ea0434\
            0000000000000000000000000000000000000000000000000000000065d8f000\
            0000000000000000000000000000000000000000000000000000000065d8f000\
            0000000000000000000000000000000000000000000000000000000000000001";

        let price = decode_latest_round_data(hex).unwrap();
        let expected = Decimal::from(9_712_345_678_900i64) / Decimal::from(100_000_000);
        assert_eq!(price, expected);
    }

    #[test]
    fn test_decode_real_btc_price() {
        // BTC at $100,000.12345678 → answer = 10000012345678
        let hex = "0x\
            0000000000000000000000000000000000000000000000000000000000000001\
            000000000000000000000000000000000000000000000000000009184f2f014e\
            0000000000000000000000000000000000000000000000000000000065d8f000\
            0000000000000000000000000000000000000000000000000000000065d8f000\
            0000000000000000000000000000000000000000000000000000000000000001";

        let price = decode_latest_round_data(hex).unwrap();
        let expected = Decimal::from(10_000_012_345_678i64) / Decimal::from(100_000_000);
        assert_eq!(price, expected);
    }

    #[test]
    fn test_decode_too_short() {
        let result = decode_latest_round_data("0x1234");
        assert!(result.is_err());
    }
}
