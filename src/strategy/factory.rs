use crate::config::{Config, StrategyName};
use rust_decimal::prelude::*;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::black_scholes::BlackScholesStrategy;
use crate::strategy::bollinger::BollingerBandsStrategy;
use crate::strategy::discount::DiscountStrategy;
use crate::strategy::hedged_lp::HedgedLpStrategy;
use crate::strategy::ma_crossover::MACrossoverStrategy;
use crate::strategy::orb::OrbStrategy;
use crate::strategy::resolution_sniper::ResolutionSniperStrategy;
use crate::strategy::traits::Strategy;
use crate::strategy::unified::UnifiedStrategy;
use crate::web::state::SharedWebState;

pub fn create_strategy(
    name: &StrategyName,
    config: &Config,
    book_cache: Option<BookCache>,
    web_state: Option<SharedWebState>,
) -> Box<dyn Strategy> {
    match name {
        StrategyName::BlackScholes => {
            let mut s = BlackScholesStrategy::new(config);
            if let Some(bc) = book_cache {
                s = s.with_book_cache(bc);
            }
            Box::new(s)
        }
        StrategyName::Unified => {
            let mut s = UnifiedStrategy::new(config);
            if let Some(bc) = book_cache {
                s = s.with_book_cache(bc);
            }
            Box::new(s)
        }
        StrategyName::HedgedLp => {
            let mut s = HedgedLpStrategy::new(config);
            if let Some(bc) = book_cache {
                s = s.with_book_cache(bc);
            }
            Box::new(s)
        }
        StrategyName::Discount => {
            let mut s = DiscountStrategy::new(config);
            if let Some(bc) = book_cache {
                s = s.with_book_cache(bc);
            }
            Box::new(s)
        }
        StrategyName::MaCrossover => Box::new(MACrossoverStrategy::new(config)),
        StrategyName::BollingerBands => Box::new(BollingerBandsStrategy::new(config)),
        StrategyName::Orb => {
            let mut s = OrbStrategy::new(config);
            if let Some(bc) = book_cache {
                s = s.with_book_cache(bc);
            }
            if let Some(ws) = web_state {
                s = s.with_web_state(ws);
            }
            // Set bankroll: try BANKROLL env, then fetch from Polymarket API, then default
            let bankroll: f64 = std::env::var("BANKROLL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| {
                    // Try to fetch balance from Polymarket activity API
                    let wallet = std::env::var("COPYTRADE_FUNDER_ADDRESS").unwrap_or_default();
                    if !wallet.is_empty() {
                        if let Ok(balance) = fetch_wallet_balance(&wallet) {
                            tracing::info!(balance = format!("${:.0}", balance), "fetched bankroll from Polymarket API");
                            return balance;
                        }
                    }
                    config.max_position_usd.to_f64().unwrap_or(100.0) * 10.0
                });
            s = s.with_bankroll(bankroll);
            Box::new(s)
        }
        StrategyName::Sniper => {
            let mut s = ResolutionSniperStrategy::new(config);
            if let Some(bc) = book_cache {
                s = s.with_book_cache(bc);
            }
            Box::new(s)
        }
    }
}

/// Fetch approximate wallet balance from Polymarket activity API.
/// Sums redeems - buys to estimate current cash + position value.
fn fetch_wallet_balance(wallet: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let mut total_spent = 0.0_f64;
    let mut total_redeemed = 0.0_f64;
    let mut offset = 0;

    loop {
        let url = format!(
            "https://data-api.polymarket.com/activity?user={}&limit=500&offset={}&sortDirection=ASC",
            wallet, offset
        );
        let resp: Vec<serde_json::Value> = client.get(&url).send()?.json()?;
        if resp.is_empty() {
            break;
        }
        for event in &resp {
            let etype = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let usdc = event.get("usdcSize").and_then(|v| v.as_f64()).unwrap_or(0.0);
            match etype {
                "TRADE" => {
                    let side = event.get("side").and_then(|v| v.as_str()).unwrap_or("");
                    if side == "BUY" {
                        total_spent += usdc;
                    }
                }
                "REDEEM" => {
                    total_redeemed += usdc;
                }
                _ => {}
            }
        }
        if resp.len() < 500 {
            break;
        }
        offset += 500;
    }

    // Net P&L = redeemed - spent. Add to assumed starting capital.
    // We don't know the exact starting deposit, so use spent as proxy.
    // If net is positive, balance > initial deposit.
    let net_pnl = total_redeemed - total_spent;

    // Estimate: look at the max spent at any point as the deposit amount
    // Simple heuristic: total_redeemed is the cash we got back
    // Current balance ≈ total_redeemed (remaining after all trades)
    // This is imperfect but better than a hardcoded value
    let balance = if net_pnl > 0.0 {
        // Profitable: assume initial deposit + profits
        total_redeemed.max(500.0) // at least $500
    } else {
        total_redeemed.max(100.0)
    };

    Ok(balance)
}
