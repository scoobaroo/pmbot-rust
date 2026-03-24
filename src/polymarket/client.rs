use crate::polymarket::auth::{self, ApiCredentials, AuthError};
use crate::types::order::Side;
use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, info};

const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

#[derive(Error, Debug)]
pub enum PolymarketError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {0}")]
    Api(String),
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),
}

/// Parameters for placing an order on the CLOB.
pub struct OrderParams<'a> {
    pub token_id: &'a str,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    /// "GTC", "GTD", "FOK", or "FAK"
    pub order_type: &'a str,
    /// If true, order is rejected if it would cross the spread (maker only)
    pub post_only: bool,
    /// Optional fee rate override in basis points
    pub fee_rate_bps: Option<u32>,
    /// Whether to use the neg-risk CTF exchange (default: query CLOB)
    pub neg_risk: Option<bool>,
    /// Unix timestamp for GTD order expiration (0 = no expiration)
    pub expiration: Option<i64>,
}

pub struct PolymarketClient {
    http: Client,
    credentials: ApiCredentials,
    signer: PrivateKeySigner,
    /// The signing key address (derived from private key)
    signer_address: Address,
    /// The proxy/funder wallet where USDC is held
    funder_address: Address,
}

impl PolymarketClient {
    pub fn new(credentials: ApiCredentials, signer: PrivateKeySigner) -> Self {
        let signer_address = signer.address();
        // POLY_FUNDER_ADDRESS is the proxy wallet shown on Polymarket UI where USDC lives.
        // If not set, assume EOA mode (signer == funder).
        let funder_address: Address = std::env::var("POLY_FUNDER_ADDRESS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(signer_address);

        if funder_address != signer_address {
            info!(
                signer = %signer_address,
                funder = %funder_address,
                "proxy wallet mode: signer != funder (signatureType=1 POLY_PROXY)"
            );
        }

        Self {
            http: Client::new(),
            credentials,
            signer,
            signer_address,
            funder_address,
        }
    }

    /// Create a client with an explicit funder/proxy address (for separate accounts).
    pub fn new_with_funder(
        credentials: ApiCredentials,
        signer: PrivateKeySigner,
        funder_override: Option<String>,
    ) -> Self {
        let signer_address = signer.address();
        let funder_address: Address = funder_override
            .and_then(|s| if s.is_empty() { None } else { s.parse().ok() })
            .unwrap_or(signer_address);

        if funder_address != signer_address {
            info!(
                signer = %signer_address,
                funder = %funder_address,
                "proxy wallet mode: signer != funder (signatureType=1 POLY_PROXY)"
            );
        }

        Self {
            http: Client::new(),
            credentials,
            signer,
            signer_address,
            funder_address,
        }
    }

    /// Build L2 authentication headers for an API request.
    fn l2_headers(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<Vec<(&'static str, String)>, PolymarketError> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let signature =
            auth::sign_request(&self.credentials.api_secret, &timestamp, method, path, body)?;

        Ok(vec![
            ("POLY_ADDRESS", self.credentials.wallet_address.clone()),
            ("POLY_SIGNATURE", signature),
            ("POLY_TIMESTAMP", timestamp),
            ("POLY_API_KEY", self.credentials.api_key.clone()),
            ("POLY_PASSPHRASE", self.credentials.api_passphrase.clone()),
        ])
    }

    /// Place a limit order on the CLOB with EIP-712 order signing.
    pub async fn place_order(&self, params: &OrderParams<'_>) -> Result<String, PolymarketError> {
        info!(
            token_id = params.token_id,
            side = %params.side,
            price = %params.price,
            size = %params.size,
            order_type = params.order_type,
            post_only = params.post_only,
            "placing order"
        );

        let fee_rate_bps = params.fee_rate_bps.unwrap_or(0);
        let side_u8: u8 = match params.side {
            Side::Buy => 0,
            Side::Sell => 1,
        };
        let side_str = match params.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };

        // Calculate maker/taker amounts (6 decimal places)
        // CLOB requires exact price×size relationship:
        //   BUY: maker (USDC, 2 dec) = floor(size × price), taker (tokens, 4 dec) = size
        //   SELL: maker (tokens, 4 dec) = size, taker (USDC, 2 dec) = floor(size × price)
        // Round size to 4 decimals first, then derive maker from rounded values.
        use rust_decimal::RoundingStrategy;
        // Truncate (floor) to avoid rounding up past what CLOB expects
        let size_rounded = params.size.round_dp_with_strategy(4, RoundingStrategy::ToZero);
        let price_rounded = params.price.round_dp_with_strategy(4, RoundingStrategy::ToZero);
        // Don't round USDC — CLOB computes exact price×size and validates it
        let usdc_amount = size_rounded * price_rounded;

        let (maker_amount, taker_amount) = match params.side {
            Side::Buy => {
                let maker = auth::to_token_decimals(usdc_amount);
                let taker = auth::to_token_decimals(size_rounded);
                (maker, taker)
            }
            Side::Sell => {
                let maker = auth::to_token_decimals(size_rounded);
                let taker = auth::to_token_decimals(usdc_amount);
                (maker, taker)
            }
        };

        // Parse token ID to U256
        let token_id_u256 = params
            .token_id
            .parse::<U256>()
            .map_err(|e| PolymarketError::Api(format!("invalid token ID: {e}")))?;

        // Generate salt matching official SDK pattern: Math.round(Math.random() * Date.now())
        // Must stay within JS Number.MAX_SAFE_INTEGER (2^53) for CLOB server compatibility
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let salt_val: u64 = ((rand::random::<f64>()) * now_ms as f64).round() as u64;
        let salt = U256::from(salt_val);

        // Determine neg_risk: use provided value or query CLOB
        let neg_risk = match params.neg_risk {
            Some(nr) => nr,
            None => self.get_neg_risk(params.token_id).await?,
        };

        debug!(neg_risk = neg_risk, token_id = params.token_id, "signing order");

        // Sign the order with EIP-712
        // Determine if we're using a proxy wallet (funder != signer)
        let is_proxy = self.funder_address != self.signer_address;
        let signature_type: u8 = if is_proxy { 1 } else { 0 }; // 1 = POLY_PROXY, 0 = EOA

        let signature = auth::sign_order(
            &self.signer,
            &auth::OrderSignParams {
                salt,
                maker: self.funder_address, // funder/proxy wallet holds the USDC
                token_id: token_id_u256,
                maker_amount,
                taker_amount,
                fee_rate_bps: U256::from(fee_rate_bps as u64),
                side: side_u8,
                neg_risk,
                signature_type,
                expiration: U256::from(params.expiration.unwrap_or(0) as u64),
            },
        )
        .await?;

        let funder_hex = format!("{}", self.funder_address);
        let signer_hex = format!("{}", self.signer_address);

        // Build the signed order body
        // salt and signatureType are integers; all other numerics are strings
        let mut order_body = serde_json::json!({
            "order": {
                "salt": salt_val,
                "maker": funder_hex,
                "signer": signer_hex,
                "taker": "0x0000000000000000000000000000000000000000",
                "tokenId": params.token_id,
                "makerAmount": maker_amount.to_string(),
                "takerAmount": taker_amount.to_string(),
                "expiration": params.expiration.unwrap_or(0).to_string(),
                "nonce": "0",
                "feeRateBps": fee_rate_bps.to_string(),
                "side": side_str,
                "signatureType": signature_type,
                "signature": signature,
            },
            "owner": self.credentials.api_key,
            "orderType": params.order_type,
            "deferExec": false,
        });

        if params.post_only {
            order_body["postOnly"] = serde_json::json!(true);
        }

        let body_str = serde_json::to_string(&order_body)
            .map_err(|e| PolymarketError::Api(format!("JSON serialize: {e}")))?;

        debug!(body = %body_str, "CLOB order payload");

        // Build L2 auth headers
        let headers = self.l2_headers("POST", "/order", &body_str)?;

        let mut req = self.http.post(format!("{CLOB_BASE_URL}/order"));
        for (name, value) in headers {
            req = req.header(name, value);
        }
        req = req.header("Content-Type", "application/json");

        let resp = req.body(body_str).send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            return Err(PolymarketError::Api(format!(
                "status={}, body={}",
                status, text
            )));
        }

        let v: Value =
            serde_json::from_str(&text).map_err(|e| PolymarketError::Api(e.to_string()))?;

        let order_id = v
            .get("orderID")
            .and_then(|id| id.as_str())
            .unwrap_or("unknown")
            .to_string();

        debug!(order_id = %order_id, "order placed");
        Ok(order_id)
    }

    /// Cancel an existing order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<(), PolymarketError> {
        info!(order_id = order_id, "cancelling order");

        let path = format!("/order/{order_id}");
        let headers = self.l2_headers("DELETE", &path, "")?;

        let mut req = self.http.delete(format!("{CLOB_BASE_URL}{path}"));
        for (name, value) in headers {
            req = req.header(name, value);
        }

        let resp = req.send().await?;

        if !resp.status().is_success() {
            let text = resp.text().await?;
            return Err(PolymarketError::Api(text));
        }

        Ok(())
    }

    /// Cancel all open orders.
    pub async fn cancel_all_orders(&self) -> Result<usize, PolymarketError> {
        let body = serde_json::json!({});
        let body_str = serde_json::to_string(&body)
            .map_err(|e| PolymarketError::Api(format!("JSON serialize: {e}")))?;
        let headers = self.l2_headers("DELETE", "/cancel-all", &body_str)?;

        let mut req = self.http.delete(format!("{CLOB_BASE_URL}/cancel-all"));
        for (name, value) in headers {
            req = req.header(name, value);
        }
        req = req.header("Content-Type", "application/json");

        let resp = req.body(body_str).send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            return Err(PolymarketError::Api(format!("cancel-all: {text}")));
        }

        // Response contains cancelled order IDs
        let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        let count = v.as_array().map(|a| a.len()).unwrap_or(0);
        info!(count = count, "cancelled all open orders");
        Ok(count)
    }

    /// Get current open orders.
    pub async fn get_open_orders(&self) -> Result<Vec<Value>, PolymarketError> {
        let headers = self.l2_headers("GET", "/orders", "")?;

        let mut req = self.http.get(format!("{CLOB_BASE_URL}/orders"));
        for (name, value) in headers {
            req = req.header(name, value);
        }

        let resp = req.send().await?;
        let orders: Vec<Value> = resp.json().await?;
        Ok(orders)
    }

    /// Query whether a token uses the neg-risk CTF exchange.
    pub async fn get_neg_risk(&self, token_id: &str) -> Result<bool, PolymarketError> {
        let resp = self
            .http
            .get(format!("{CLOB_BASE_URL}/neg-risk?token_id={token_id}"))
            .send()
            .await?;

        let v: Value = resp.json().await?;
        Ok(v.get("neg_risk").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    /// Get the orderbook for a specific token.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<Value, PolymarketError> {
        let resp = self
            .http
            .get(format!("{CLOB_BASE_URL}/book?token_id={token_id}"))
            .send()
            .await?;

        let book: Value = resp.json().await?;
        Ok(book)
    }

    /// Send a heartbeat to keep the session alive.
    pub async fn send_heartbeat(&self) -> Result<(), PolymarketError> {
        let headers = self.l2_headers("GET", "/heartbeat", "")?;

        let mut req = self.http.get(format!("{CLOB_BASE_URL}/heartbeat"));
        for (name, value) in headers {
            req = req.header(name, value);
        }

        let resp = req.send().await?;

        if !resp.status().is_success() {
            let text = resp.text().await?;
            return Err(PolymarketError::Api(format!("heartbeat failed: {text}")));
        }

        debug!("heartbeat sent");
        Ok(())
    }
}
