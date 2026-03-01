use crate::config::{Config, StrategyName};
use crate::strategy::black_scholes::BlackScholesStrategy;
use crate::strategy::bollinger::BollingerBandsStrategy;
use crate::strategy::ma_crossover::MACrossoverStrategy;
use crate::strategy::traits::Strategy;

pub fn create_strategy(config: &Config) -> Box<dyn Strategy> {
    match config.strategy {
        StrategyName::BlackScholes => Box::new(BlackScholesStrategy::new(config)),
        StrategyName::MaCrossover => Box::new(MACrossoverStrategy::new(config)),
        StrategyName::BollingerBands => Box::new(BollingerBandsStrategy::new(config)),
    }
}
