use crate::config::{Config, StrategyName};
use crate::strategy::black_scholes::BlackScholesStrategy;
use crate::strategy::bollinger::BollingerBandsStrategy;
use crate::strategy::hedged_lp::HedgedLpStrategy;
use crate::strategy::ma_crossover::MACrossoverStrategy;
use crate::strategy::traits::Strategy;
use crate::strategy::unified::UnifiedStrategy;

pub fn create_strategy(config: &Config) -> Box<dyn Strategy> {
    match config.strategy {
        StrategyName::BlackScholes => Box::new(BlackScholesStrategy::new(config)),
        StrategyName::MaCrossover => Box::new(MACrossoverStrategy::new(config)),
        StrategyName::BollingerBands => Box::new(BollingerBandsStrategy::new(config)),
        StrategyName::Unified => Box::new(UnifiedStrategy::new(config)),
        StrategyName::HedgedLp => Box::new(HedgedLpStrategy::new(config)),
    }
}
