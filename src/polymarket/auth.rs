use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use alloy::sol;
use alloy::sol_types::eip712_domain;
use alloy::sol_types::SolStruct;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;
use tracing::info;

// ── EIP-712 types ──────────────────────────────────────────────────────

sol! {
    /// ClobAuth struct for L1 authentication (API key derivation).
    struct ClobAuth {
        address address;
        string timestamp;
        uint256 nonce;
        string message;
    }
}

sol! {
    /// Order struct for CTF Exchange order signing.
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

// ── Constants ──────────────────────────────────────────────────────────

const CLOB_AUTH_MSG: &str = "This message attests that I control the given wallet";
const POLYGON_CHAIN_ID: u64 = 137;
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

/// Polymarket CTF Exchange on Polygon mainnet.
const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
/// Polymarket Neg Risk CTF Exchange on Polygon mainnet.
const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

// ── Error types ────────────────────────────────────────────────────────

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid private key: {0}")]
    InvalidKey(String),
    #[error("signing failed: {0}")]
    SigningError(String),
    #[error("API error: {0}")]
    ApiError(String),
}

// ── API Credentials ────────────────────────────────────────────────────

/// L2 API credentials derived from L1 EIP-712 signature.
#[derive(Debug, Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    pub wallet_address: String,
}

// ── Signer creation ───────────────────────────────────────────────────

/// Create a PrivateKeySigner from a 0x-prefixed hex private key.
pub fn create_signer(private_key: &str) -> Result<PrivateKeySigner, AuthError> {
    private_key
        .parse::<PrivateKeySigner>()
        .map_err(|e| AuthError::InvalidKey(e.to_string()))
}

// ── L1: Derive API credentials via EIP-712 ────────────────────────────

/// Derive L2 API credentials from a private key via EIP-712 signing.
///
/// 1. Signs a ClobAuth EIP-712 struct with the private key
/// 2. Calls GET /auth/derive-api-key with L1 headers
/// 3. Returns API credentials + the signer (needed for order signing)
pub async fn derive_api_credentials(
    private_key: &str,
) -> Result<(ApiCredentials, PrivateKeySigner), AuthError> {
    let signer = create_signer(private_key)?;
    let address = signer.address();
    let address_hex = format!("{address}"); // checksummed

    let timestamp = chrono::Utc::now().timestamp().to_string();
    let nonce = 0u64;

    // Build ClobAuth EIP-712 typed data
    let auth = ClobAuth {
        address,
        timestamp: timestamp.clone(),
        nonce: U256::from(nonce),
        message: CLOB_AUTH_MSG.to_string(),
    };

    let domain = eip712_domain! {
        name: "ClobAuthDomain",
        version: "1",
        chain_id: POLYGON_CHAIN_ID,
    };

    // Sign the EIP-712 hash
    let hash = auth.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash(&hash)
        .await
        .map_err(|e| AuthError::SigningError(e.to_string()))?;
    let sig_hex = format!("0x{}", hex::encode(sig.as_bytes()));

    info!(address = %address_hex, "deriving API credentials via L1 auth");

    // Call the CLOB auth endpoint
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{CLOB_BASE_URL}/auth/derive-api-key"))
        .header("POLY_ADDRESS", &address_hex)
        .header("POLY_SIGNATURE", &sig_hex)
        .header("POLY_TIMESTAMP", &timestamp)
        .header("POLY_NONCE", nonce.to_string())
        .send()
        .await
        .map_err(|e| AuthError::ApiError(e.to_string()))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| AuthError::ApiError(e.to_string()))?;

    if !status.is_success() {
        // If derive fails, try creating a new key
        if status.as_u16() == 404 || status.as_u16() == 400 {
            info!("derive-api-key failed, trying to create new key");
            return create_api_key(&signer, &address_hex).await;
        }
        return Err(AuthError::ApiError(format!(
            "derive-api-key: status={status}, body={text}"
        )));
    }

    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| AuthError::ApiError(format!("invalid JSON: {e}")))?;

    let api_key = v
        .get("apiKey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let secret = v
        .get("secret")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let passphrase = v
        .get("passphrase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if api_key.is_empty() {
        return Err(AuthError::ApiError("empty apiKey in response".into()));
    }

    info!(address = %address_hex, "API credentials derived successfully");

    let creds = ApiCredentials {
        api_key,
        api_secret: secret,
        api_passphrase: passphrase,
        wallet_address: address_hex,
    };

    Ok((creds, signer))
}

/// Create a new API key via POST /auth/api-key with L1 headers.
async fn create_api_key(
    signer: &PrivateKeySigner,
    address_hex: &str,
) -> Result<(ApiCredentials, PrivateKeySigner), AuthError> {
    let timestamp = chrono::Utc::now().timestamp().to_string();
    let nonce = 0u64;

    let auth = ClobAuth {
        address: signer.address(),
        timestamp: timestamp.clone(),
        nonce: U256::from(nonce),
        message: CLOB_AUTH_MSG.to_string(),
    };

    let domain = eip712_domain! {
        name: "ClobAuthDomain",
        version: "1",
        chain_id: POLYGON_CHAIN_ID,
    };

    let hash = auth.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash(&hash)
        .await
        .map_err(|e| AuthError::SigningError(e.to_string()))?;
    let sig_hex = format!("0x{}", hex::encode(sig.as_bytes()));

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{CLOB_BASE_URL}/auth/api-key"))
        .header("POLY_ADDRESS", address_hex)
        .header("POLY_SIGNATURE", &sig_hex)
        .header("POLY_TIMESTAMP", &timestamp)
        .header("POLY_NONCE", nonce.to_string())
        .send()
        .await
        .map_err(|e| AuthError::ApiError(e.to_string()))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| AuthError::ApiError(e.to_string()))?;

    if !status.is_success() {
        return Err(AuthError::ApiError(format!(
            "create-api-key: status={status}, body={text}"
        )));
    }

    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| AuthError::ApiError(format!("invalid JSON: {e}")))?;

    let api_key = v
        .get("apiKey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let secret = v
        .get("secret")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let passphrase = v
        .get("passphrase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if api_key.is_empty() {
        return Err(AuthError::ApiError(
            "empty apiKey in create response".into(),
        ));
    }

    info!(address = %address_hex, "API key created successfully");

    let creds = ApiCredentials {
        api_key,
        api_secret: secret,
        api_passphrase: passphrase,
        wallet_address: address_hex.to_string(),
    };

    Ok((creds, signer.clone()))
}

// ── L2: HMAC request signing ──────────────────────────────────────────

/// Sign a request with HMAC-SHA256 for L2 API authentication.
///
/// Message format: `timestamp + method + path + body`
/// Uses URL-safe base64 encoding (matching Polymarket's Python client).
pub fn sign_request(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<String, AuthError> {
    let message = format!("{timestamp}{method}{path}{body}");

    let secret_bytes = base64::engine::general_purpose::URL_SAFE
        .decode(secret)
        .or_else(|_| {
            // Fall back to URL_SAFE_NO_PAD if standard URL_SAFE fails
            base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(secret)
        })
        .map_err(|e| AuthError::InvalidKey(format!("base64 decode secret: {e}")))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
        .map_err(|e| AuthError::SigningError(e.to_string()))?;
    mac.update(message.as_bytes());
    let result = mac.finalize();

    Ok(base64::engine::general_purpose::URL_SAFE.encode(result.into_bytes()))
}

// ── Order signing ─────────────────────────────────────────────────────

/// Parameters for signing an order.
pub struct OrderSignParams {
    pub salt: U256,
    pub maker: Address,
    pub token_id: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub fee_rate_bps: U256,
    pub side: u8,
    pub neg_risk: bool,
}

/// Build and sign an order for the Polymarket CTF Exchange.
///
/// Returns the EIP-712 signature as a hex string.
pub async fn sign_order(
    signer: &PrivateKeySigner,
    params: &OrderSignParams,
) -> Result<String, AuthError> {
    let exchange_addr: Address = if params.neg_risk {
        NEG_RISK_CTF_EXCHANGE
    } else {
        CTF_EXCHANGE
    }
    .parse()
    .map_err(|e: alloy::hex::FromHexError| {
        AuthError::InvalidKey(format!("invalid exchange address: {e}"))
    })?;

    let order = Order {
        salt: params.salt,
        maker: params.maker,
        signer: params.maker, // signer == maker for EOA
        taker: Address::ZERO,
        tokenId: params.token_id,
        makerAmount: params.maker_amount,
        takerAmount: params.taker_amount,
        expiration: U256::ZERO,
        nonce: U256::ZERO,
        feeRateBps: params.fee_rate_bps,
        side: params.side,
        signatureType: 0, // EOA
    };

    let domain = eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "1",
        chain_id: POLYGON_CHAIN_ID,
        verifying_contract: exchange_addr,
    };

    let hash = order.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash(&hash)
        .await
        .map_err(|e| AuthError::SigningError(e.to_string()))?;

    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

/// Convert a decimal amount to token decimals (6 decimal places).
pub fn to_token_decimals(amount: rust_decimal::Decimal) -> U256 {
    let scaled = amount * rust_decimal::Decimal::from(1_000_000u64);
    let truncated = scaled.trunc();
    // Convert to u128 first (Decimal can hold values up to ~2^96)
    let val = truncated.to_string().parse::<u128>().unwrap_or(0);
    U256::from(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_signer_from_hex() {
        // Use a well-known test private key (DO NOT use in production)
        let key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let signer = create_signer(key).unwrap();
        let addr = format!("{}", signer.address());
        // This is the well-known Hardhat account #0 address
        assert_eq!(addr, "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
    }

    #[test]
    fn test_to_token_decimals() {
        use rust_decimal::Decimal;
        use std::str::FromStr;

        // 100 tokens → 100_000_000
        let amt = to_token_decimals(Decimal::from(100));
        assert_eq!(amt, U256::from(100_000_000u64));

        // 0.55 → 550_000
        let amt = to_token_decimals(Decimal::from_str("0.55").unwrap());
        assert_eq!(amt, U256::from(550_000u64));

        // 10.5 * 0.6 = 6.3 → 6_300_000
        let amt = to_token_decimals(Decimal::from_str("6.3").unwrap());
        assert_eq!(amt, U256::from(6_300_000u64));
    }

    #[tokio::test]
    async fn test_sign_order_produces_valid_signature() {
        let key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let signer = create_signer(key).unwrap();
        let maker = signer.address();

        let sig = sign_order(
            &signer,
            &OrderSignParams {
                salt: U256::from(12345u64),
                maker,
                token_id: U256::from(1u64),
                maker_amount: U256::from(1_000_000u64),
                taker_amount: U256::from(2_000_000u64),
                fee_rate_bps: U256::from(100u64),
                side: 0,
                neg_risk: true,
            },
        )
        .await
        .unwrap();

        assert!(sig.starts_with("0x"));
        // EIP-712 signature is 65 bytes = 130 hex chars + "0x" prefix
        assert_eq!(sig.len(), 132);
    }
}
