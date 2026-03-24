//! Auto-redeem winning positions on Polymarket via on-chain `redeemPositions()`.
//!
//! After a market resolves, winning conditional tokens can be redeemed for USDC
//! by calling `redeemPositions()` on the Gnosis Conditional Tokens contract.
//! This module handles that automatically so USDC flows back to the wallet.

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use std::collections::HashSet;
use tracing::{info, warn};

// ── Contract interface ───────────────────────────────────────────────

sol! {
    /// Gnosis Conditional Tokens `redeemPositions` function.
    /// Converts winning conditional tokens → USDC collateral.
    #[sol(rpc)]
    interface IConditionalTokens {
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        ) external;
    }
}

// ── Constants ────────────────────────────────────────────────────────

/// Gnosis Conditional Tokens contract on Polygon mainnet.
const CONDITIONAL_TOKENS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// USDC on Polygon (PoS bridged).
const USDC_POLYGON: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// USDC.e (native) on Polygon — Polymarket uses this one.
const USDCE_POLYGON: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";

/// Default Polygon RPC.
const DEFAULT_POLYGON_RPC: &str = "https://polygon-rpc.com";

/// Binary market index sets: [1, 2] = [YES outcome, NO outcome].
fn binary_index_sets() -> Vec<U256> {
    vec![U256::from(1), U256::from(2)]
}

// ── Redeemer ─────────────────────────────────────────────────────────

/// Handles on-chain redemption of winning Polymarket positions.
pub struct Redeemer {
    rpc_url: String,
    signer: PrivateKeySigner,
    /// Condition IDs we've already redeemed (prevent double-redeem).
    redeemed: HashSet<String>,
}

impl Redeemer {
    /// Create from environment variables.
    /// Returns None if private key is not configured.
    pub fn from_env() -> Option<Self> {
        let pk = std::env::var("POLYMARKET_PRIVATE_KEY").ok()?;
        if pk.is_empty() {
            return None;
        }
        let signer: PrivateKeySigner = pk.parse().ok()?;
        let rpc_url = std::env::var("POLYGON_RPC_URL")
            .unwrap_or_else(|_| DEFAULT_POLYGON_RPC.to_string());

        info!(
            address = %signer.address(),
            rpc = %rpc_url,
            "on-chain redeemer initialized"
        );

        Some(Self {
            rpc_url,
            signer,
            redeemed: HashSet::new(),
        })
    }

    /// Redeem winning tokens for a resolved market.
    /// Tries both USDC variants (PoS bridged and native) since Polymarket
    /// has used both at different times.
    pub async fn redeem(&mut self, condition_id: &str) {
        if self.redeemed.contains(condition_id) {
            return;
        }

        // Parse condition_id as bytes32
        let cid_bytes = match parse_condition_id(condition_id) {
            Some(b) => b,
            None => {
                warn!(condition_id, "invalid condition_id format for redeem");
                return;
            }
        };

        let ct_addr: Address = CONDITIONAL_TOKENS.parse().unwrap();
        let index_sets = binary_index_sets();

        // Try USDC.e (native) first — this is what Polymarket currently uses
        for usdc_addr_str in [USDCE_POLYGON, USDC_POLYGON] {
            let usdc_addr: Address = usdc_addr_str.parse().unwrap();
            match self.try_redeem(ct_addr, usdc_addr, cid_bytes, &index_sets).await {
                Ok(true) => {
                    info!(
                        condition_id,
                        usdc = usdc_addr_str,
                        "redeemed winning tokens on-chain"
                    );
                    self.redeemed.insert(condition_id.to_string());
                    return;
                }
                Ok(false) => {
                    // No tokens to redeem with this USDC variant, try next
                }
                Err(e) => {
                    warn!(
                        condition_id,
                        usdc = usdc_addr_str,
                        error = %e,
                        "redeem call failed"
                    );
                }
            }
        }
    }

    async fn try_redeem(
        &self,
        ct_addr: Address,
        usdc_addr: Address,
        condition_id: FixedBytes<32>,
        index_sets: &[U256],
    ) -> Result<bool, String> {
        let provider = ProviderBuilder::new()
            .wallet(alloy::network::EthereumWallet::from(self.signer.clone()))
            .connect_http(self.rpc_url.parse().map_err(|e| format!("bad RPC URL: {e}"))?);

        let contract = IConditionalTokens::new(ct_addr, &provider);

        let parent_collection = FixedBytes::<32>::ZERO;

        let call = contract.redeemPositions(
            usdc_addr,
            parent_collection,
            condition_id,
            index_sets.to_vec(),
        );

        // Estimate gas first — if it reverts, there's nothing to redeem
        match call.estimate_gas().await {
            Ok(gas) => {
                if gas < 30_000 {
                    // Minimal gas = no-op (no tokens to redeem)
                    return Ok(false);
                }
            }
            Err(_) => {
                // Reverted — no tokens to redeem with this collateral
                return Ok(false);
            }
        }

        // Send the transaction
        let pending = call
            .send()
            .await
            .map_err(|e| format!("send failed: {e}"))?;

        let receipt = pending
            .get_receipt()
            .await
            .map_err(|e| format!("receipt failed: {e}"))?;

        if receipt.status() {
            Ok(true)
        } else {
            Err("transaction reverted".to_string())
        }
    }
}

/// Parse a 0x-prefixed hex condition_id into bytes32.
fn parse_condition_id(cid: &str) -> Option<FixedBytes<32>> {
    let hex_str = cid.strip_prefix("0x").unwrap_or(cid);
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    Some(FixedBytes::from_slice(&bytes))
}
