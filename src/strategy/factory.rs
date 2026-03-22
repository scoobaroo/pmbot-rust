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
            // Set bankroll from BANKROLL env var (default: max_position_usd * 10)
            let bankroll: f64 = std::env::var("BANKROLL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(config.max_position_usd.to_f64().unwrap_or(100.0) * 10.0);
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
