use crate::config::Config;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::candle::{Candle, Timeframe};
use crate::types::events::{SignalMetadata, TradeSignal, TradeTarget};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use tracing::info;

struct BandState {
    window: VecDeque<f64>,
    prev_close: Option<f64>,
}

pub struct BollingerBandsStrategy {
    period: usize,
    num_std: f64,
    timeframe: Timeframe,
    size_usd: Decimal,
    states: HashMap<String, BandState>,
}

impl BollingerBandsStrategy {
    pub fn new(config: &Config) -> Self {
        let timeframe = Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5);

        Self {
            period: config.bb_period,
            num_std: config.bb_num_std,
            timeframe,
            size_usd: config.bb_size_usd,
            states: HashMap::new(),
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

        let state = self
            .states
            .entry(candle.symbol.clone())
            .or_insert(BandState {
                window: VecDeque::with_capacity(self.period),
                prev_close: None,
            });

        // Maintain rolling window
        if state.window.len() == self.period {
            state.window.pop_front();
        }
        state.window.push_back(close);

        // Need full window before generating signals
        if state.window.len() < self.period {
            state.prev_close = Some(close);
            return Vec::new();
        }

        // Compute SMA
        let sum: f64 = state.window.iter().sum();
        let sma = sum / self.period as f64;

        // Compute standard deviation
        let variance: f64 =
            state.window.iter().map(|x| (x - sma).powi(2)).sum::<f64>() / self.period as f64;
        let std_dev = variance.sqrt();

        let upper = sma + self.num_std * std_dev;
        let lower = sma - self.num_std * std_dev;

        let band_width = upper - lower;

        // Detect crossings using previous close
        let signal = if let Some(prev) = state.prev_close {
            if close <= lower && prev > lower {
                // Crossed below lower band → oversold bounce → Buy
                let confidence = if band_width > 0.0 {
                    ((lower - close) / band_width).min(1.0)
                } else {
                    0.0
                };

                info!(
                    symbol = %candle.symbol,
                    close,
                    lower,
                    sma,
                    "BB buy signal: close crossed below lower band"
                );

                Some((Side::Buy, confidence))
            } else if close >= upper && prev < upper {
                // Crossed above upper band → overbought reversal → Sell
                let confidence = if band_width > 0.0 {
                    ((close - upper) / band_width).min(1.0)
                } else {
                    0.0
                };

                info!(
                    symbol = %candle.symbol,
                    close,
                    upper,
                    sma,
                    "BB sell signal: close crossed above upper band"
                );

                Some((Side::Sell, confidence))
            } else {
                None
            }
        } else {
            None
        };

        state.prev_close = Some(close);

        signal
            .into_iter()
            .map(|(side, confidence)| TradeSignal {
                target: TradeTarget::Spot {
                    symbol: candle.symbol.clone(),
                    exchange: None,
                },
                side,
                size_usd: self.size_usd,
                confidence,
                price: candle.close,
                metadata: SignalMetadata::BollingerBands {
                    sma,
                    upper,
                    lower,
                    close,
                    timeframe: self.timeframe.to_string(),
                },
                timestamp: Utc::now(),
            })
            .collect()
    }
}

impl Strategy for BollingerBandsStrategy {
    fn name(&self) -> &str {
        "bollinger-bands"
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
            strategy: crate::config::StrategyName::BollingerBands,
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
            ma_fast_period: 9,
            ma_slow_period: 21,
            ma_timeframe: "5m".into(),
            ma_size_usd: Decimal::from(500),
            bb_period: 5,
            bb_num_std: 2.0,
            bb_timeframe: "5m".into(),
            bb_size_usd: Decimal::from(500),
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
    fn test_no_signal_before_window_full() {
        let mut strat = BollingerBandsStrategy::new(&make_config());
        // bb_period=5, so 4 candles should produce no signals
        for i in 0..4 {
            let candle = make_candle("BTC-USD", 100.0 + i as f64, (i as i64) * 300);
            let signals = strat.on_event(StrategyEvent::CandleComplete(candle));
            assert!(signals.is_empty(), "no signal before window is full");
        }
    }

    #[test]
    fn test_buy_signal_on_lower_band_touch() {
        let mut strat = BollingerBandsStrategy::new(&make_config());
        // Feed 5 stable candles to fill window, then drop price sharply
        let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 80.0];
        let mut signals = Vec::new();
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            signals.extend(strat.on_event(StrategyEvent::CandleComplete(candle)));
        }
        assert!(
            !signals.is_empty(),
            "expected buy signal on lower band touch"
        );
        assert_eq!(signals.last().unwrap().side, Side::Buy);
    }

    #[test]
    fn test_sell_signal_on_upper_band_touch() {
        let mut strat = BollingerBandsStrategy::new(&make_config());
        // Feed 5 stable candles to fill window, then spike price sharply
        let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 120.0];
        let mut signals = Vec::new();
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            signals.extend(strat.on_event(StrategyEvent::CandleComplete(candle)));
        }
        assert!(
            !signals.is_empty(),
            "expected sell signal on upper band touch"
        );
        assert_eq!(signals.last().unwrap().side, Side::Sell);
    }

    #[test]
    fn test_no_signal_within_bands() {
        let mut strat = BollingerBandsStrategy::new(&make_config());
        // All prices nearly identical — close stays within bands
        let prices = vec![100.0, 100.1, 99.9, 100.0, 100.05, 100.1, 99.95];
        let mut signals = Vec::new();
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            signals.extend(strat.on_event(StrategyEvent::CandleComplete(candle)));
        }
        assert!(
            signals.is_empty(),
            "no signal expected when price stays within bands"
        );
    }
}
