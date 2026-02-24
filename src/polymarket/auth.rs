use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid private key: {0}")]
    InvalidKey(String),
    #[error("signing failed: {0}")]
    SigningError(String),
}

/// L2 API credentials derived from L1 EIP-712 signature.
#[derive(Debug, Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

impl ApiCredentials {
    /// Create from existing L2 credentials (if already derived).
    pub fn new(key: String, secret: String, passphrase: String) -> Self {
        Self {
            api_key: key,
            api_secret: secret,
            api_passphrase: passphrase,
        }
    }
}

/// Derive L2 API credentials from a private key via EIP-712 signing.
/// In production, this calls the Polymarket auth endpoint.
pub async fn derive_api_credentials(_private_key: &str) -> Result<ApiCredentials, AuthError> {
    // The actual flow:
    // 1. Create EIP-712 typed data for auth
    // 2. Sign with private key using alloy
    // 3. POST to Polymarket derive-api-key endpoint
    // 4. Receive API key, secret, passphrase

    info!("API credential derivation would occur here in production");

    // Placeholder — real implementation requires Polymarket API interaction
    Ok(ApiCredentials {
        api_key: String::new(),
        api_secret: String::new(),
        api_passphrase: String::new(),
    })
}

/// Sign a request with HMAC-SHA256 for L2 API authentication.
pub fn sign_request(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<String, AuthError> {
    let message = format!("{}{}{}{}", timestamp, method, path, body);

    let secret_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, secret)
        .map_err(|e| AuthError::InvalidKey(e.to_string()))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
        .map_err(|e| AuthError::SigningError(e.to_string()))?;
    mac.update(message.as_bytes());
    let result = mac.finalize();

    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        result.into_bytes(),
    ))
}
