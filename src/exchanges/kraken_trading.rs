use crate::types::order::Side;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256, Sha512};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info};

const KRAKEN_API_URL: &str = "https://api.kraken.com";

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, thiserror::Error)]
pub enum KrakenTradingError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {0}")]
    Api(String),
    #[error("Auth error: {0}")]
    Auth(String),
    #[error("Decode error: {0}")]
    Decode(#[from] base64::DecodeError),
}

#[derive(Debug, serde::Deserialize)]
struct KrakenResponse {
    error: Vec<String>,
    result: Option<serde_json::Value>,
}

pub struct KrakenSpotClient {
    api_key: String,
    api_secret: Vec<u8>,
    http: reqwest::Client,
}

impl KrakenSpotClient {
    pub fn new(api_key: String, api_secret: String) -> Result<Self, KrakenTradingError> {
        let decoded_secret = BASE64.decode(&api_secret)?;
        Ok(Self {
            api_key,
            api_secret: decoded_secret,
            http: reqwest::Client::new(),
        })
    }

    /// Place an order on Kraken spot.
    /// Returns the transaction ID (txid) on success.
    pub async fn place_order(
        &self,
        pair: &str,
        side: Side,
        order_type: &str,
        volume: Decimal,
        price: Option<Decimal>,
    ) -> Result<String, KrakenTradingError> {
        let nonce = Self::nonce();
        let side_str = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        let mut params = vec![
            ("nonce".to_string(), nonce.to_string()),
            ("ordertype".to_string(), order_type.to_string()),
            ("type".to_string(), side_str.to_string()),
            ("volume".to_string(), volume.to_string()),
            ("pair".to_string(), pair.to_string()),
        ];

        if let Some(p) = price {
            params.push(("price".to_string(), p.to_string()));
        }

        let path = "/0/private/AddOrder";
        let post_data = serde_urlencoded::to_string(&params)
            .map_err(|e| KrakenTradingError::Api(e.to_string()))?;

        let signature = self.sign(path, nonce, &post_data)?;

        let resp: KrakenResponse = self
            .http
            .post(format!("{}{}", KRAKEN_API_URL, path))
            .header("API-Key", &self.api_key)
            .header("API-Sign", &signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await?
            .json()
            .await?;

        if !resp.error.is_empty() {
            let err = resp.error.join(", ");
            error!(error = %err, pair, "Kraken AddOrder failed");
            return Err(KrakenTradingError::Api(err));
        }

        // Extract txid from result
        let txid = resp
            .result
            .as_ref()
            .and_then(|r| r.get("txid"))
            .and_then(|t| t.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        info!(txid = %txid, pair, side = side_str, volume = %volume, "Kraken order placed");
        Ok(txid)
    }

    /// Cancel an open order by txid.
    pub async fn cancel_order(&self, txid: &str) -> Result<(), KrakenTradingError> {
        let nonce = Self::nonce();

        let params = vec![
            ("nonce".to_string(), nonce.to_string()),
            ("txid".to_string(), txid.to_string()),
        ];

        let path = "/0/private/CancelOrder";
        let post_data = serde_urlencoded::to_string(&params)
            .map_err(|e| KrakenTradingError::Api(e.to_string()))?;

        let signature = self.sign(path, nonce, &post_data)?;

        let resp: KrakenResponse = self
            .http
            .post(format!("{}{}", KRAKEN_API_URL, path))
            .header("API-Key", &self.api_key)
            .header("API-Sign", &signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await?
            .json()
            .await?;

        if !resp.error.is_empty() {
            let err = resp.error.join(", ");
            error!(error = %err, txid, "Kraken CancelOrder failed");
            return Err(KrakenTradingError::Api(err));
        }

        info!(txid, "Kraken order cancelled");
        Ok(())
    }

    /// Kraken HMAC-SHA512 signature.
    /// sig = HMAC-SHA512(secret, path + SHA256(nonce + post_data))
    fn sign(&self, path: &str, nonce: u64, post_data: &str) -> Result<String, KrakenTradingError> {
        let nonce_post = format!("{}{}", nonce, post_data);
        let sha256_digest = Sha256::digest(nonce_post.as_bytes());

        let mut hmac_input = path.as_bytes().to_vec();
        hmac_input.extend_from_slice(&sha256_digest);

        let mut mac = HmacSha512::new_from_slice(&self.api_secret)
            .map_err(|e| KrakenTradingError::Auth(e.to_string()))?;
        mac.update(&hmac_input);
        let result = mac.finalize().into_bytes();

        Ok(BASE64.encode(result))
    }

    fn nonce() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis() as u64
    }

    /// Convert canonical symbol (e.g. "BTC-USD") to Kraken pair format ("XBTUSD").
    pub fn to_kraken_pair(canonical: &str) -> String {
        let parts: Vec<&str> = canonical.split('-').collect();
        if parts.len() != 2 {
            return canonical.to_string();
        }
        let base = match parts[0] {
            "BTC" => "XBT",
            other => other,
        };
        format!("{}{}", base, parts[1])
    }
}
