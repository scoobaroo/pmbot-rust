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
}

pub struct PolymarketClient {
    http: Client,
    credentials: ApiCredentials,
    signer: PrivateKeySigner,
    wallet_address: Address,
}

impl PolymarketClient {
    pub fn new(credentials: ApiCredentials, signer: PrivateKeySigner) -> Self {
        let wallet_address = signer.address();
        Self {
            http: Client::new(),
            credentials,
            signer,
            wallet_address,
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
        let (maker_amount, taker_amount) = match params.side {
            Side::Buy => {
                // BUY: pay USDC (size * price), receive tokens (size)
                let maker = auth::to_token_decimals(params.size * params.price);
                let taker = auth::to_token_decimals(params.size);
                (maker, taker)
            }
            Side::Sell => {
                // SELL: pay tokens (size), receive USDC (size * price)
                let maker = auth::to_token_decimals(params.size);
                let taker = auth::to_token_decimals(params.size * params.price);
                (maker, taker)
            }
        };

        // Parse token ID to U256
        let token_id_u256 = params
            .token_id
            .parse::<U256>()
            .map_err(|e| PolymarketError::Api(format!("invalid token ID: {e}")))?;

        // Generate random salt (u64 so it serializes as a JSON integer in the body)
        let salt_val: u64 = rand::random();
        let salt = U256::from(salt_val);

        // Sign the order with EIP-712
        let signature = auth::sign_order(
            &self.signer,
            &auth::OrderSignParams {
                salt,
                maker: self.wallet_address,
                token_id: token_id_u256,
                maker_amount,
                taker_amount,
                fee_rate_bps: U256::from(fee_rate_bps as u64),
                side: side_u8,
                neg_risk: true, // assume neg_risk for all Polymarket markets
            },
        )
        .await?;

        let wallet_hex = format!("{}", self.wallet_address);

        // Build the signed order body
        // salt and signatureType are integers; all other numerics are strings
        let mut order_body = serde_json::json!({
            "order": {
                "salt": salt_val,
                "maker": wallet_hex,
                "signer": wallet_hex,
                "taker": "0x0000000000000000000000000000000000000000",
                "tokenId": params.token_id,
                "makerAmount": maker_amount.to_string(),
                "takerAmount": taker_amount.to_string(),
                "expiration": "0",
                "nonce": "0",
                "feeRateBps": fee_rate_bps.to_string(),
                "side": side_str,
                "signatureType": 0,
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
