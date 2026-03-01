use crate::config::Config;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::candle::{Candle, Timeframe};
use crate::types::events::{SignalMetadata, TradeSignal, TradeTarget};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tracing::info;

struct EmaState {
    fast_ema: f64,
    slow_ema: f64,
    fast_initialized: bool,
    slow_initialized: bool,
    fast_count: usize,
    slow_count: usize,
    prev_fast_above_slow: Option<bool>,
}

pub struct MACrossoverStrategy {
    fast_period: usize,
    slow_period: usize,
    timeframe: Timeframe,
    size_usd: Decimal,
    emas: HashMap<String, EmaState>,
}

impl MACrossoverStrategy {
    pub fn new(config: &Config) -> Self {
        let timeframe = Timeframe::from_str_loose(&config.ma_timeframe).unwrap_or(Timeframe::M5);

        Self {
            fast_period: config.ma_fast_period,
            slow_period: config.ma_slow_period,
            timeframe,
            size_usd: config.ma_size_usd,
            emas: HashMap::new(),
        }
    }

    fn process_candle(&mut self, candle: &Candle) -> Vec<TradeSignal> {
        if candle.timeframe != self.timeframe {
            return Vec::new();
        }

        let close = candle.close.to_string().parse::<f64>().unwrap_or(0.0);
        if close <= 0.0 {
            return Vec::new();
        }

        let fast_k = 2.0 / (self.fast_period as f64 + 1.0);
        let slow_k = 2.0 / (self.slow_period as f64 + 1.0);

        let state = self.emas.entry(candle.symbol.clone()).or_insert(EmaState {
            fast_ema: close,
            slow_ema: close,
            fast_initialized: false,
            slow_initialized: false,
            fast_count: 0,
            slow_count: 0,
            prev_fast_above_slow: None,
        });

        // Update EMAs
        state.fast_count += 1;
        state.slow_count += 1;

        if !state.fast_initialized {
            // Seed with SMA until we have enough data
            state.fast_ema = state.fast_ema + (close - state.fast_ema) / state.fast_count as f64;
            if state.fast_count >= self.fast_period {
                state.fast_initialized = true;
            }
        } else {
            state.fast_ema = close * fast_k + state.fast_ema * (1.0 - fast_k);
        }

        if !state.slow_initialized {
            state.slow_ema = state.slow_ema + (close - state.slow_ema) / state.slow_count as f64;
            if state.slow_count >= self.slow_period {
                state.slow_initialized = true;
            }
        } else {
            state.slow_ema = close * slow_k + state.slow_ema * (1.0 - slow_k);
        }

        // Only generate signals once both EMAs are initialized
        if !state.fast_initialized || !state.slow_initialized {
            return Vec::new();
        }

        let fast_above_slow = state.fast_ema > state.slow_ema;

        let signal = match state.prev_fast_above_slow {
            Some(prev) if prev != fast_above_slow => {
                let side = if fast_above_slow {
                    Side::Buy // bullish crossover
                } else {
                    Side::Sell // bearish crossover
                };

                info!(
                    symbol = %candle.symbol,
                    side = %side,
                    fast_ema = state.fast_ema,
                    slow_ema = state.slow_ema,
                    "MA crossover detected"
                );

                Some(TradeSignal {
                    target: TradeTarget::Spot {
                        symbol: candle.symbol.clone(),
                        exchange: None,
                    },
                    side,
                    size_usd: self.size_usd,
                    confidence: (state.fast_ema - state.slow_ema).abs() / state.slow_ema,
                    price: candle.close,
                    metadata: SignalMetadata::MACrossover {
                        fast_ema: state.fast_ema,
                        slow_ema: state.slow_ema,
                        timeframe: self.timeframe.to_string(),
                    },
                    timestamp: Utc::now(),
                })
            }
            _ => None,
        };

        state.prev_fast_above_slow = Some(fast_above_slow);

        signal.into_iter().collect()
    }
}

impl Strategy for MACrossoverStrategy {
    fn name(&self) -> &str {
        "ma-crossover"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: false,
            candles: true,
            execution_feedback: false,
            polymarket_updates: false,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::CandleComplete(candle) => self.process_candle(&candle),
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::candle::Candle;
    use chrono::TimeZone;

    fn make_config() -> Config {
        crate::config::Config {
            mode: crate::config::RunMode::Paper,
            strategy: crate::config::StrategyName::MaCrossover,
            backtest_file: String::new(),
            kraken_api_key: String::new(),
            kraken_api_secret: String::new(),
            coinbase_api_key: String::new(),
            coinbase_api_secret: String::new(),
            polymarket_private_key: String::new(),
            polygon_rpc_url: String::new(),
            symbols: vec!["BTC-USD".into()],
            min_edge_threshold: Decimal::new(3, 2),
            kelly_fraction_cap: Decimal::new(5, 1),
            volatility_window_hours: 24,
            max_position_usd: Decimal::from(1000),
            max_total_exposure_usd: Decimal::from(5000),
            max_drawdown_pct: Decimal::new(10, 2),
            max_orders_per_minute: 10,
            ma_fast_period: 3,
            ma_slow_period: 5,
            ma_timeframe: "5m".into(),
            ma_size_usd: Decimal::from(500),
            bb_period: 20,
            bb_num_std: 2.0,
            bb_timeframe: "5m".into(),
            bb_size_usd: Decimal::from(500),
            bb_cooldown_candles: 3,
            fee_rate_bps: 156,
            maker_mode: true,
            heartbeat_interval_secs: 10,
            ws_book_max_stale_secs: 30,
            log_level: "info".into(),
            stale_feed_timeout_secs: 30,
        }
    }

    fn make_candle(symbol: &str, close: f64, ts_secs: i64) -> Candle {
        let close_dec = Decimal::from_f64_retain(close).unwrap();
        Candle {
            symbol: symbol.to_string(),
            timeframe: Timeframe::M5,
            open: close_dec,
            high: close_dec,
            low: close_dec,
            close: close_dec,
            volume: Decimal::from(100),
            tick_count: 10,
            open_time: Utc.timestamp_opt(ts_secs, 0).unwrap(),
            close_time: Utc.timestamp_opt(ts_secs + 300, 0).unwrap(),
        }
    }

    #[test]
    fn test_ma_crossover_generates_signal_on_crossover() {
        let mut strat = MACrossoverStrategy::new(&make_config());

        // Feed enough candles with rising prices to initialize EMAs (fast=3, slow=5)
        // then switch direction to cause a crossover
        let prices = vec![
            100.0, 101.0, 102.0, 103.0, 104.0, // rising — fast > slow
            103.0, 102.0, 100.0, 98.0, // falling — fast < slow (crossover)
        ];

        let mut signals = Vec::new();
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            let s = strat.on_event(StrategyEvent::CandleComplete(candle));
            signals.extend(s);
        }

        // Should have at least one crossover signal
        assert!(
            !signals.is_empty(),
            "expected at least one crossover signal"
        );
        // The crossover from rising to falling should be a Sell
        let last_signal = signals.last().unwrap();
        assert_eq!(last_signal.side, Side::Sell);
    }

    #[test]
    fn test_ma_no_signal_before_initialization() {
        let mut strat = MACrossoverStrategy::new(&make_config());
        // Only 2 candles — not enough for slow_period=5
        let c1 = make_candle("BTC-USD", 100.0, 0);
        let c2 = make_candle("BTC-USD", 110.0, 300);

        let s1 = strat.on_event(StrategyEvent::CandleComplete(c1));
        let s2 = strat.on_event(StrategyEvent::CandleComplete(c2));

        assert!(s1.is_empty());
        assert!(s2.is_empty());
    }
}
