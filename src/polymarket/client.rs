use crate::polymarket::auth::ApiCredentials;
use crate::types::order::Side;
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
    Auth(#[from] crate::polymarket::auth::AuthError),
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
}

impl PolymarketClient {
    pub fn new(credentials: ApiCredentials) -> Self {
        Self {
            http: Client::new(),
            credentials,
        }
    }

    /// Place a limit order on the CLOB.
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

        let mut body = serde_json::json!({
            "tokenID": params.token_id,
            "side": match params.side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            },
            "price": params.price.to_string(),
            "size": params.size.to_string(),
            "type": params.order_type,
        });

        if params.post_only {
            body["postOnly"] = serde_json::json!(true);
        }
        if let Some(bps) = params.fee_rate_bps {
            body["feeRateBps"] = serde_json::json!(bps.to_string());
        }

        let resp = self
            .http
            .post(format!("{}/order", CLOB_BASE_URL))
            .header("POLY-ADDRESS", &self.credentials.api_key)
            .header("POLY-SIGNATURE", &self.credentials.api_secret)
            .header("POLY-TIMESTAMP", chrono::Utc::now().timestamp().to_string())
            .header("POLY-NONCE", uuid::Uuid::new_v4().to_string())
            .json(&body)
            .send()
            .await?;

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

        let resp = self
            .http
            .delete(format!("{}/order/{}", CLOB_BASE_URL, order_id))
            .header("POLY-ADDRESS", &self.credentials.api_key)
            .header("POLY-SIGNATURE", &self.credentials.api_secret)
            .header("POLY-TIMESTAMP", chrono::Utc::now().timestamp().to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            let text = resp.text().await?;
            return Err(PolymarketError::Api(text));
        }

        Ok(())
    }

    /// Get current open orders.
    pub async fn get_open_orders(&self) -> Result<Vec<Value>, PolymarketError> {
        let resp = self
            .http
            .get(format!("{}/orders", CLOB_BASE_URL))
            .header("POLY-ADDRESS", &self.credentials.api_key)
            .header("POLY-SIGNATURE", &self.credentials.api_secret)
            .header("POLY-TIMESTAMP", chrono::Utc::now().timestamp().to_string())
            .send()
            .await?;

        let orders: Vec<Value> = resp.json().await?;
        Ok(orders)
    }

    /// Get the orderbook for a specific token.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<Value, PolymarketError> {
        let resp = self
            .http
            .get(format!("{}/book?token_id={}", CLOB_BASE_URL, token_id))
            .send()
            .await?;

        let book: Value = resp.json().await?;
        Ok(book)
    }

    /// Send a heartbeat to keep the session alive.
    pub async fn send_heartbeat(&self) -> Result<(), PolymarketError> {
        let resp = self
            .http
            .post(format!("{}/heartbeat", CLOB_BASE_URL))
            .header("POLY-ADDRESS", &self.credentials.api_key)
            .header("POLY-SIGNATURE", &self.credentials.api_secret)
            .header("POLY-TIMESTAMP", chrono::Utc::now().timestamp().to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            let text = resp.text().await?;
            return Err(PolymarketError::Api(format!("heartbeat failed: {}", text)));
        }

        debug!("heartbeat sent");
        Ok(())
    }
}
