use crate::types::events::PolymarketUpdate;
use crate::types::market::{MarketDirection, MarketType, PolymarketMarket};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com";

/// Mapping from up/down slug prefix to the exchange symbol we track.
const UPDOWN_ASSETS: &[(&str, &str)] = &[
    ("btc", "BTC-USD"),
    ("eth", "ETH-USD"),
    ("sol", "SOL-USD"),
    ("xrp", "XRP-USD"),
];

pub struct MarketScanner {
    http: Client,
    symbols: Vec<String>,
    poll_interval_secs: u64,
    token_tx: mpsc::Sender<Vec<String>>,
    updown_enabled: bool,
    updown_only: bool,
}

impl MarketScanner {
    pub fn new(
        symbols: Vec<String>,
        token_tx: mpsc::Sender<Vec<String>>,
        updown_enabled: bool,
        updown_only: bool,
    ) -> Self {
        Self {
            http: Client::new(),
            symbols,
            poll_interval_secs: 60,
            token_tx,
            updown_enabled,
            updown_only,
        }
    }

    pub async fn run(self, tx: mpsc::Sender<PolymarketUpdate>, shutdown: CancellationToken) {
        info!(
            updown_enabled = self.updown_enabled,
            updown_only = self.updown_only,
            "Market scanner started"
        );

        let mut strike_interval =
            tokio::time::interval(std::time::Duration::from_secs(self.poll_interval_secs));
        let mut updown_interval = tokio::time::interval(std::time::Duration::from_secs(15));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Market scanner shutting down");
                    return;
                }
                _ = strike_interval.tick(), if !self.updown_only => {
                    self.poll_and_send(
                        self.scan_markets().await,
                        "crypto price markets",
                        &tx,
                    ).await;
                }
                _ = updown_interval.tick(), if self.updown_enabled => {
                    self.poll_and_send(
                        self.scan_updown_markets().await,
                        "up/down markets",
                        &tx,
                    ).await;
                }
            }
        }
    }

    async fn poll_and_send(
        &self,
        result: Result<Vec<PolymarketMarket>, reqwest::Error>,
        label: &str,
        tx: &mpsc::Sender<PolymarketUpdate>,
    ) {
        match result {
            Ok(markets) if !markets.is_empty() => {
                info!(count = markets.len(), "found {}", label);

                let token_ids: Vec<String> = markets
                    .iter()
                    .flat_map(|m| vec![m.token_id_yes.clone(), m.token_id_no.clone()])
                    .collect();

                if let Err(e) = self.token_tx.send(token_ids).await {
                    warn!(error = %e, "failed to send token IDs to orderbook stream");
                }

                let _ = tx.send(PolymarketUpdate::MarketsDiscovered(markets)).await;
            }
            Ok(_) => {
                debug!("no new {} found", label);
            }
            Err(e) => {
                error!(error = %e, "{} scan failed", label);
            }
        }
    }

    async fn scan_markets(&self) -> Result<Vec<PolymarketMarket>, reqwest::Error> {
        let resp = self
            .http
            .get(format!("{}/events", GAMMA_API_URL))
            .query(&[("closed", "false"), ("tag", "crypto"), ("limit", "50")])
            .send()
            .await?;

        let events: Vec<serde_json::Value> = resp.json().await?;
        let mut all_markets = Vec::new();

        for event in &events {
            let nested = match event.get("markets").and_then(|m| m.as_array()) {
                Some(m) => m,
                None => continue,
            };

            for m in nested {
                if let Some(market) = parse_crypto_market(m, &self.symbols) {
                    info!(
                        question = %market.question,
                        symbol = %market.underlying_symbol,
                        strike = %market.strike,
                        direction = ?market.direction,
                        implied_prob = format!("{:.1}%", market.implied_prob_yes.to_f64().unwrap_or(0.0) * 100.0),
                        token_yes = %market.token_id_yes,
                        "discovered market"
                    );
                    all_markets.push(market);
                }
            }
        }

        Ok(all_markets)
    }

    async fn scan_updown_markets(&self) -> Result<Vec<PolymarketMarket>, reqwest::Error> {
        let mut all_markets = Vec::new();

        // Scan both 5-minute and 15-minute UpDown windows
        let windows: &[(u64, &str)] = &[(300, "5m"), (900, "15m")];

        for &(window_secs, label) in windows {
            let window_start = current_window_start(window_secs);

            for &(slug_prefix, symbol) in UPDOWN_ASSETS {
                if !self.symbols.iter().any(|s| s == symbol) {
                    continue;
                }

                let slug = format!("{}-updown-{}-{}", slug_prefix, label, window_start);
                let resp = self
                    .http
                    .get(format!("{}/events", GAMMA_API_URL))
                    .query(&[("slug", slug.as_str())])
                    .send()
                    .await?;

                let events: Vec<serde_json::Value> = resp.json().await?;
                for event in &events {
                    let nested = match event.get("markets").and_then(|m| m.as_array()) {
                        Some(m) => m,
                        None => continue,
                    };
                    for m in nested {
                        if let Some(market) =
                            parse_updown_market(m, symbol, window_start, window_secs)
                        {
                            info!(
                                question = %market.question,
                                symbol = %market.underlying_symbol,
                                window = label,
                                token_up = %market.token_id_yes,
                                implied_up = format!("{:.1}%", market.implied_prob_yes.to_f64().unwrap_or(0.0) * 100.0),
                                "discovered up/down market"
                            );
                            all_markets.push(market);
                        }
                    }
                }
            }
        }

        Ok(all_markets)
    }
}

/// Returns the Unix timestamp of the current window start for the given interval.
fn current_window_start(window_secs: u64) -> i64 {
    let now = Utc::now().timestamp();
    now - (now % window_secs as i64)
}

/// Map full crypto names in questions to ticker symbols.
fn name_to_ticker(text: &str) -> Option<&'static str> {
    let upper = text.to_uppercase();
    if upper.contains("BITCOIN") || upper.contains("BTC") {
        Some("BTC")
    } else if upper.contains("ETHEREUM") || upper.contains("ETHER") || upper.contains("ETH") {
        Some("ETH")
    } else if upper.contains("SOLANA") || upper.contains("SOL") {
        Some("SOL")
    } else if upper.contains("DOGECOIN") || upper.contains("DOGE") {
        Some("DOGE")
    } else if upper.contains("RIPPLE") || upper.contains("XRP") {
        Some("XRP")
    } else {
        None
    }
}

/// Detect market direction from the question text.
fn detect_direction(question: &str) -> MarketDirection {
    let q = question.to_lowercase();
    if q.contains("dip") || q.contains("below") || q.contains("fall") || q.contains("drop") {
        MarketDirection::Bearish
    } else {
        // Default to bullish: "reach", "hit", "above", etc.
        MarketDirection::Bullish
    }
}

fn parse_crypto_market(
    v: &serde_json::Value,
    configured_symbols: &[String],
) -> Option<PolymarketMarket> {
    let question = v.get("question")?.as_str()?;

    // Map question text to a ticker
    let ticker = name_to_ticker(question)?;

    // Check if any configured symbol starts with this ticker (e.g. "BTC-USD" starts with "BTC")
    let matching_symbol = configured_symbols
        .iter()
        .find(|s| s.split('-').next().unwrap_or(s) == ticker)?;

    // Extract strike price from question
    let strike = extract_strike_from_question(question)?;

    let condition_id = v.get("conditionId")?.as_str()?.to_string();

    // Get token IDs from clobTokenIds — may be a JSON string or a direct array
    let (token_yes, token_no) = parse_clob_token_ids(v)?;

    // Parse expiry
    let end_date = v
        .get("endDate")
        .or_else(|| v.get("endDateIso"))
        .and_then(|d| d.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|| Utc::now() + chrono::Duration::days(30));

    // Parse outcome prices — may be a JSON string or a direct array
    let yes_price = parse_outcome_price(v, 0).unwrap_or(Decimal::new(50, 2));
    let no_price = parse_outcome_price(v, 1).unwrap_or(Decimal::ONE - yes_price);

    let direction = detect_direction(question);

    Some(PolymarketMarket {
        condition_id,
        token_id_yes: token_yes,
        token_id_no: token_no,
        question: question.to_string(),
        underlying_symbol: matching_symbol.clone(),
        strike,
        expiry: end_date,
        implied_prob_yes: yes_price,
        implied_prob_no: no_price,
        direction,
        market_type: MarketType::StrikeAbove,
    })
}

/// Parse outcomePrices which may be a JSON string like "[\"0.35\",\"0.65\"]" or an array.
fn parse_outcome_price(v: &serde_json::Value, index: usize) -> Option<Decimal> {
    let prices = v.get("outcomePrices")?;

    if let Some(s) = prices.as_str() {
        // It's a JSON-encoded string
        let parsed: Vec<String> = serde_json::from_str(s).ok()?;
        parsed.get(index).and_then(|p| Decimal::from_str(p).ok())
    } else if let Some(arr) = prices.as_array() {
        arr.get(index)
            .and_then(|p| p.as_str())
            .and_then(|s| Decimal::from_str(s).ok())
    } else {
        None
    }
}

/// Parse clobTokenIds which may be a JSON-encoded string or a direct array.
fn parse_clob_token_ids(v: &serde_json::Value) -> Option<(String, String)> {
    let raw = v.get("clobTokenIds")?;

    let tokens: Vec<String> = if let Some(s) = raw.as_str() {
        // JSON-encoded string like "[\"token1\",\"token2\"]"
        serde_json::from_str(s).ok()?
    } else if let Some(arr) = raw.as_array() {
        arr.iter()
            .filter_map(|t| t.as_str().map(String::from))
            .collect()
    } else {
        return None;
    };

    if tokens.len() < 2 {
        return None;
    }
    Some((tokens[0].clone(), tokens[1].clone()))
}

/// Parse an up/down market from the Gamma API response.
fn parse_updown_market(
    v: &serde_json::Value,
    symbol: &str,
    window_start_ts: i64,
    window_secs: u64,
) -> Option<PolymarketMarket> {
    let condition_id = v.get("conditionId")?.as_str()?.to_string();
    let question = v
        .get("question")
        .and_then(|q| q.as_str())
        .unwrap_or("Up or Down?")
        .to_string();

    // clobTokenIds: [0] = Up, [1] = Down
    let (token_up, token_down) = parse_clob_token_ids(v)?;

    // Parse expiry
    let end_date = v
        .get("endDate")
        .or_else(|| v.get("endDateIso"))
        .and_then(|d| d.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|| {
            DateTime::from_timestamp(window_start_ts + window_secs as i64, 0)
                .unwrap_or_else(Utc::now)
        });

    let up_price = parse_outcome_price(v, 0).unwrap_or(Decimal::new(50, 2));
    let down_price = parse_outcome_price(v, 1).unwrap_or(Decimal::ONE - up_price);

    Some(PolymarketMarket {
        condition_id,
        token_id_yes: token_up,  // "Up" token
        token_id_no: token_down, // "Down" token
        question,
        underlying_symbol: symbol.to_string(),
        strike: Decimal::ZERO, // No strike for up/down
        expiry: end_date,
        implied_prob_yes: up_price,          // P(Up)
        implied_prob_no: down_price,         // P(Down)
        direction: MarketDirection::Bullish, // Up = bullish
        market_type: MarketType::UpDown {
            window_start_ts,
            window_secs,
        },
    })
}

/// Check resolution status for expired markets via Gamma API.
/// Returns Vec<(condition_id, up_won: bool)> for resolved markets only.
pub async fn check_resolutions(
    http: &reqwest::Client,
    condition_ids: &[String],
) -> Vec<(String, bool)> {
    let mut resolved = Vec::new();

    for cid in condition_ids {
        let url = format!("{}/markets/{}", GAMMA_API_URL, cid);
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(condition_id = %cid, error = %e, "failed to query Gamma API for resolution");
                continue;
            }
        };

        let market: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(condition_id = %cid, error = %e, "failed to parse Gamma API response");
                continue;
            }
        };

        // Only process closed markets
        let closed = market
            .get("closed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let end_date = market.get("endDate").and_then(|v| v.as_str()).unwrap_or("unknown");
        if !closed {
            tracing::info!(
                condition_id = %cid,
                closed = closed,
                end_date = end_date,
                "market not yet closed in Gamma API"
            );
            continue;
        }

        // Parse outcomePrices: ["1","0"] = Up won, ["0","1"] = Down won
        let outcome_prices = market.get("outcomePrices");
        let up_won = if let Some(prices) = outcome_prices {
            let parsed: Option<Vec<String>> = if let Some(s) = prices.as_str() {
                serde_json::from_str(s).ok()
            } else {
                prices.as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
            };

            match parsed {
                Some(p) if p.len() >= 2 => p[0] == "1",
                _ => {
                    warn!(condition_id = %cid, "closed market has invalid outcomePrices");
                    continue;
                }
            }
        } else {
            warn!(condition_id = %cid, "closed market missing outcomePrices");
            continue;
        };

        info!(
            condition_id = %cid,
            up_won = up_won,
            "market resolution confirmed via Gamma API"
        );
        resolved.push((cid.clone(), up_won));
    }

    resolved
}

fn extract_strike_from_question(question: &str) -> Option<Decimal> {
    // Match patterns like "$100,000", "$50000", "$1,234.56"
    let mut i = 0;
    let chars: Vec<char> = question.chars().collect();
    while i < chars.len() {
        if chars[i] == '$' {
            i += 1;
            let mut num_str = String::new();
            while i < chars.len()
                && (chars[i].is_ascii_digit() || chars[i] == ',' || chars[i] == '.')
            {
                if chars[i] != ',' {
                    num_str.push(chars[i]);
                }
                i += 1;
            }
            if !num_str.is_empty() {
                if let Ok(d) = Decimal::from_str(&num_str) {
                    return Some(d);
                }
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_strike() {
        assert_eq!(
            extract_strike_from_question("Will BTC be above $100,000 on March 31?"),
            Some(Decimal::from(100000))
        );
        assert_eq!(
            extract_strike_from_question("Will ETH hit $5000?"),
            Some(Decimal::from(5000))
        );
        assert_eq!(extract_strike_from_question("No price here"), None);
    }

    #[test]
    fn test_name_to_ticker() {
        assert_eq!(name_to_ticker("Will Bitcoin reach $100k?"), Some("BTC"));
        assert_eq!(name_to_ticker("Will Ethereum hit $5000?"), Some("ETH"));
        assert_eq!(name_to_ticker("Will Solana reach $200?"), Some("SOL"));
        assert_eq!(name_to_ticker("Will Trump win?"), None);
    }

    #[test]
    fn test_detect_direction() {
        assert_eq!(
            detect_direction("Will Bitcoin reach $100,000?"),
            MarketDirection::Bullish
        );
        assert_eq!(
            detect_direction("Will Bitcoin dip to $50,000?"),
            MarketDirection::Bearish
        );
        assert_eq!(
            detect_direction("Will ETH fall below $2000?"),
            MarketDirection::Bearish
        );
        assert_eq!(
            detect_direction("Will BTC hit $200,000?"),
            MarketDirection::Bullish
        );
    }

    #[test]
    fn test_parse_crypto_market() {
        // clobTokenIds is a JSON-encoded string in the real API
        let market_json = serde_json::json!({
            "question": "Will Bitcoin reach $100,000 by December 31, 2026?",
            "conditionId": "0xdaa4",
            "clobTokenIds": "[\"56078\",\"11291\"]",
            "outcomePrices": "[\"0.35\",\"0.65\"]",
            "outcomes": ["Yes", "No"],
            "endDate": "2027-01-01T05:00:00Z"
        });

        let symbols = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];
        let market = parse_crypto_market(&market_json, &symbols).unwrap();

        assert_eq!(market.underlying_symbol, "BTC-USD");
        assert_eq!(market.strike, Decimal::from(100000));
        assert_eq!(market.token_id_yes, "56078");
        assert_eq!(market.token_id_no, "11291");
        assert_eq!(market.direction, MarketDirection::Bullish);
        assert_eq!(market.implied_prob_yes, Decimal::new(35, 2));
    }

    #[test]
    fn test_parse_crypto_market_bearish() {
        let market_json = serde_json::json!({
            "question": "Will Ethereum dip to $1,500 by March 2026?",
            "conditionId": "0xbeef",
            "clobTokenIds": "[\"token_a\",\"token_b\"]",
            "outcomePrices": "[\"0.20\",\"0.80\"]",
            "endDate": "2026-04-01T00:00:00Z"
        });

        let symbols = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];
        let market = parse_crypto_market(&market_json, &symbols).unwrap();

        assert_eq!(market.underlying_symbol, "ETH-USD");
        assert_eq!(market.direction, MarketDirection::Bearish);
    }

    #[test]
    fn test_parse_updown_market() {
        let market_json = serde_json::json!({
            "conditionId": "0xup1",
            "question": "Will BTC go up in the next 5 minutes?",
            "clobTokenIds": ["up_token_123", "down_token_456"],
            "outcomePrices": ["0.52", "0.48"],
            "outcomes": ["Up", "Down"],
            "endDate": "2026-02-28T12:05:00Z"
        });

        let market = parse_updown_market(&market_json, "BTC-USD", 1772280000, 300).unwrap();
        assert_eq!(market.condition_id, "0xup1");
        assert_eq!(market.underlying_symbol, "BTC-USD");
        assert_eq!(market.token_id_yes, "up_token_123");
        assert_eq!(market.token_id_no, "down_token_456");
        assert_eq!(market.strike, Decimal::ZERO);
        assert_eq!(market.implied_prob_yes, Decimal::new(52, 2));
        assert_eq!(
            market.market_type,
            MarketType::UpDown {
                window_start_ts: 1772280000,
                window_secs: 300,
            }
        );
    }

    #[test]
    fn test_current_5m_window_start() {
        let ts = current_window_start(300);
        assert_eq!(ts % 300, 0, "window start must be aligned to 5 minutes");
        let now = Utc::now().timestamp();
        assert!(now - ts < 300, "window start must be within current window");
        assert!(ts <= now, "window start must be <= now");
    }

    #[test]
    fn test_current_15m_window_start() {
        let ts = current_window_start(900);
        assert_eq!(ts % 900, 0, "window start must be aligned to 15 minutes");
        let now = Utc::now().timestamp();
        assert!(now - ts < 900, "window start must be within current window");
        assert!(ts <= now, "window start must be <= now");
    }

    #[test]
    fn test_parse_rejects_non_crypto() {
        let market_json = serde_json::json!({
            "question": "Will Trump win the election?",
            "conditionId": "0x1234",
            "clobTokenIds": "[\"aaa\",\"bbb\"]",
            "outcomePrices": "[\"0.50\",\"0.50\"]",
            "endDate": "2026-11-01T00:00:00Z"
        });

        let symbols = vec!["BTC-USD".to_string()];
        assert!(parse_crypto_market(&market_json, &symbols).is_none());
    }
}
