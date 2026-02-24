use crate::types::events::PolymarketUpdate;
use crate::types::market::PolymarketMarket;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com";

pub struct MarketScanner {
    http: Client,
    symbols: Vec<String>,
    poll_interval_secs: u64,
}

impl MarketScanner {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            http: Client::new(),
            symbols,
            poll_interval_secs: 60,
        }
    }

    pub async fn run(self, tx: mpsc::Sender<PolymarketUpdate>, shutdown: CancellationToken) {
        info!("Market scanner started");

        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(self.poll_interval_secs));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Market scanner shutting down");
                    return;
                }
                _ = interval.tick() => {
                    match self.scan_markets().await {
                        Ok(markets) if !markets.is_empty() => {
                            info!(count = markets.len(), "found crypto price markets");
                            let _ = tx.send(PolymarketUpdate::MarketsDiscovered(markets)).await;
                        }
                        Ok(_) => {
                            debug!("no new crypto price markets found");
                        }
                        Err(e) => {
                            error!(error = %e, "market scan failed");
                        }
                    }
                }
            }
        }
    }

    async fn scan_markets(&self) -> Result<Vec<PolymarketMarket>, reqwest::Error> {
        let mut all_markets = Vec::new();

        for symbol in &self.symbols {
            let resp = self
                .http
                .get(format!("{}/markets", GAMMA_API_URL))
                .query(&[("closed", "false"), ("limit", "50"), ("tag", "crypto")])
                .send()
                .await?;

            let markets: Vec<serde_json::Value> = resp.json().await?;

            for m in markets {
                if let Some(market) = parse_crypto_market(&m, symbol) {
                    all_markets.push(market);
                }
            }
        }

        Ok(all_markets)
    }
}

fn parse_crypto_market(v: &serde_json::Value, target_symbol: &str) -> Option<PolymarketMarket> {
    let question = v.get("question")?.as_str()?;

    // Match questions like "Will BTC be above $100,000 on March 31?"
    let q_upper = question.to_uppercase();
    let base = target_symbol.split('-').next().unwrap_or(target_symbol);

    if !q_upper.contains(base) {
        return None;
    }

    // Try to extract strike price from question
    let strike = extract_strike_from_question(question)?;

    let condition_id = v.get("conditionId")?.as_str()?.to_string();

    // Get token IDs from outcomes
    let tokens = v.get("tokens")?.as_array()?;
    let (token_yes, token_no) = if tokens.len() >= 2 {
        (
            tokens[0].get("token_id")?.as_str()?.to_string(),
            tokens[1].get("token_id")?.as_str()?.to_string(),
        )
    } else {
        return None;
    };

    // Parse expiry
    let end_date = v
        .get("endDate")
        .or_else(|| v.get("end_date_iso"))
        .and_then(|d| d.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|| Utc::now() + chrono::Duration::days(30));

    // Parse current prices
    let yes_price = v
        .get("outcomePrices")
        .and_then(|p| p.as_str())
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .and_then(|prices| prices.first().and_then(|p| Decimal::from_str(p).ok()))
        .unwrap_or(Decimal::new(50, 2));

    let no_price = Decimal::ONE - yes_price;

    Some(PolymarketMarket {
        condition_id,
        token_id_yes: token_yes,
        token_id_no: token_no,
        question: question.to_string(),
        underlying_symbol: target_symbol.to_string(),
        strike,
        expiry: end_date,
        implied_prob_yes: yes_price,
        implied_prob_no: no_price,
    })
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
}
