use crate::types::candle::{Candle, Timeframe};
use crate::types::market::MarketTick;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

type CandleKey = (String, Timeframe);

struct PendingCandle {
    symbol: String,
    timeframe: Timeframe,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    tick_count: u64,
    open_time: DateTime<Utc>,
    period_start: i64, // truncated timestamp in seconds
}

pub struct CandleBuilder {
    timeframes: Vec<Timeframe>,
    pending: HashMap<CandleKey, PendingCandle>,
}

impl CandleBuilder {
    pub fn new(timeframes: Vec<Timeframe>) -> Self {
        Self {
            timeframes,
            pending: HashMap::new(),
        }
    }

    /// Process a tick and return any completed candles.
    pub fn on_tick(&mut self, tick: &MarketTick) -> Vec<Candle> {
        let mut completed = Vec::new();
        let mid = tick.mid();

        for &tf in &self.timeframes {
            let period_secs = tf.duration_secs();
            let tick_secs = tick.timestamp.timestamp();
            let current_period = tick_secs - (tick_secs % period_secs);

            let key = (tick.symbol.clone(), tf);

            if let Some(pending) = self.pending.get_mut(&key) {
                if current_period != pending.period_start {
                    // New period — finalize the pending candle
                    let candle = Candle {
                        symbol: pending.symbol.clone(),
                        timeframe: pending.timeframe,
                        open: pending.open,
                        high: pending.high,
                        low: pending.low,
                        close: pending.close,
                        volume: pending.volume,
                        tick_count: pending.tick_count,
                        open_time: pending.open_time,
                        close_time: tick.timestamp,
                    };
                    completed.push(candle);

                    // Start new candle
                    *pending = PendingCandle {
                        symbol: tick.symbol.clone(),
                        timeframe: tf,
                        open: mid,
                        high: mid,
                        low: mid,
                        close: mid,
                        volume: tick.volume_24h,
                        tick_count: 1,
                        open_time: tick.timestamp,
                        period_start: current_period,
                    };
                } else {
                    // Same period — update candle
                    if mid > pending.high {
                        pending.high = mid;
                    }
                    if mid < pending.low {
                        pending.low = mid;
                    }
                    pending.close = mid;
                    pending.volume += tick.volume_24h;
                    pending.tick_count += 1;
                }
            } else {
                // First tick for this key
                self.pending.insert(
                    key,
                    PendingCandle {
                        symbol: tick.symbol.clone(),
                        timeframe: tf,
                        open: mid,
                        high: mid,
                        low: mid,
                        close: mid,
                        volume: tick.volume_24h,
                        tick_count: 1,
                        open_time: tick.timestamp,
                        period_start: current_period,
                    },
                );
            }
        }

        completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::market::Exchange;
    use chrono::TimeZone;

    fn make_tick(symbol: &str, bid: f64, ask: f64, ts_secs: i64) -> MarketTick {
        MarketTick {
            exchange: Exchange::Kraken,
            symbol: symbol.to_string(),
            bid: Decimal::from_f64_retain(bid).unwrap(),
            ask: Decimal::from_f64_retain(ask).unwrap(),
            last: Decimal::from_f64_retain((bid + ask) / 2.0).unwrap(),
            volume_24h: Decimal::from(100),
            timestamp: Utc.timestamp_opt(ts_secs, 0).unwrap(),
        }
    }

    #[test]
    fn test_candle_builder_completes_candle_on_new_period() {
        let mut builder = CandleBuilder::new(vec![Timeframe::M1]);

        // Tick at t=0 (period 0)
        let candles = builder.on_tick(&make_tick("BTC-USD", 100.0, 102.0, 0));
        assert!(
            candles.is_empty(),
            "first tick should not complete a candle"
        );

        // Tick at t=30 (still period 0)
        let candles = builder.on_tick(&make_tick("BTC-USD", 105.0, 107.0, 30));
        assert!(candles.is_empty(), "same period should not complete");

        // Tick at t=60 (new period 1) — should finalize period 0 candle
        let candles = builder.on_tick(&make_tick("BTC-USD", 110.0, 112.0, 60));
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        assert_eq!(c.symbol, "BTC-USD");
        assert_eq!(c.tick_count, 2);
        // open was mid of first tick: (100+102)/2 = 101
        assert_eq!(c.open, Decimal::from(101));
        // close was mid of second tick: (105+107)/2 = 106
        assert_eq!(c.close, Decimal::from(106));
    }

    #[test]
    fn test_candle_builder_multiple_timeframes() {
        let mut builder = CandleBuilder::new(vec![Timeframe::M1, Timeframe::M5]);

        // Tick at t=0
        builder.on_tick(&make_tick("BTC-USD", 100.0, 100.0, 0));
        // Tick at t=60 — completes M1 but not M5
        let candles = builder.on_tick(&make_tick("BTC-USD", 100.0, 100.0, 60));
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].timeframe, Timeframe::M1);

        // Tick at t=300 — completes both M1 and M5
        let candles = builder.on_tick(&make_tick("BTC-USD", 100.0, 100.0, 300));
        assert_eq!(candles.len(), 2);
    }
}
