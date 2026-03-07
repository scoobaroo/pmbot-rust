use crate::config::Config;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::candle::{Candle, Timeframe};
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketDirection, PolymarketMarket};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use tracing::info;

/// Tracks position state per symbol to prevent duplicate signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolPosition {
    Flat,
    Long,
    Short,
}

struct BandState {
    window: VecDeque<f64>,
    prev_close: Option<f64>,
    position: SymbolPosition,
    cooldown_remaining: usize,
}

pub struct BollingerBandsStrategy {
    period: usize,
    num_std: f64,
    cooldown_candles: usize,
    timeframe: Timeframe,
    size_usd: Decimal,
    states: HashMap<String, BandState>,
    markets: HashMap<String, PolymarketMarket>,
    latest_prices: HashMap<String, AggregatedPrice>,
}

impl BollingerBandsStrategy {
    pub fn new(config: &Config) -> Self {
        let timeframe = Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5);

        Self {
            period: config.bb_period,
            num_std: config.bb_num_std,
            cooldown_candles: config.bb_cooldown_candles,
            timeframe,
            size_usd: config.bb_size_usd,
            states: HashMap::new(),
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
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
                position: SymbolPosition::Flat,
                cooldown_remaining: 0,
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

        // Decrement cooldown
        if state.cooldown_remaining > 0 {
            state.cooldown_remaining -= 1;
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

        // Detect crossings using previous close
        let signal = if let Some(prev) = state.prev_close {
            if close <= lower && prev > lower && state.position != SymbolPosition::Long {
                // Crossed below lower band → oversold bounce → Buy
                let confidence = ((close - sma).abs() / std_dev / self.num_std).clamp(0.1, 1.0);

                info!(
                    symbol = %candle.symbol,
                    close,
                    lower,
                    sma,
                    confidence,
                    "BB buy signal: close crossed below lower band"
                );

                Some((Side::Buy, confidence))
            } else if close >= upper && prev < upper && state.position != SymbolPosition::Short {
                // Crossed above upper band → overbought reversal → Sell
                let confidence = ((close - sma).abs() / std_dev / self.num_std).clamp(0.1, 1.0);

                info!(
                    symbol = %candle.symbol,
                    close,
                    upper,
                    sma,
                    confidence,
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

        let Some((side, confidence)) = signal else {
            return Vec::new();
        };

        // Update position and set cooldown
        state.position = match side {
            Side::Buy => SymbolPosition::Long,
            Side::Sell => SymbolPosition::Short,
        };
        state.cooldown_remaining = self.cooldown_candles;

        let mut signals = Vec::new();

        // 1. Spot signal (always)
        signals.push(TradeSignal {
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
            is_exit: false,
        });

        // 2. Polymarket signals for matching markets
        for market in self.markets.values() {
            if market.underlying_symbol != candle.symbol {
                continue;
            }

            let pm_side = match (side, market.direction) {
                // BB Buy (expect rise) + Bullish market → Buy YES
                (Side::Buy, MarketDirection::Bullish) => Side::Buy,
                // BB Buy (expect rise) + Bearish market → Sell (buy NO)
                (Side::Buy, MarketDirection::Bearish) => Side::Sell,
                // BB Sell (expect fall) + Bullish market → Sell (buy NO)
                (Side::Sell, MarketDirection::Bullish) => Side::Sell,
                // BB Sell (expect fall) + Bearish market → Buy YES
                (Side::Sell, MarketDirection::Bearish) => Side::Buy,
            };

            info!(
                symbol = %candle.symbol,
                market = %market.question,
                bb_side = %side,
                pm_side = %pm_side,
                "BB → Polymarket signal"
            );

            signals.push(TradeSignal {
                target: TradeTarget::Polymarket(market.clone()),
                side: pm_side,
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
                is_exit: false,
            });
        }

        signals
    }
}

impl Strategy for BollingerBandsStrategy {
    fn name(&self) -> &str {
        "bollinger-bands"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: true,
            candles: true,
            execution_feedback: false,
            polymarket_updates: true,
            ml_predictions: false,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::CandleComplete(candle) => self.process_candle(&candle),
            StrategyEvent::PriceUpdate(price) => {
                self.latest_prices.insert(price.symbol.clone(), price);
                Vec::new()
            }
            StrategyEvent::PolymarketUpdate(update) => {
                match update {
                    PolymarketUpdate::MarketsDiscovered(new_markets) => {
                        info!(count = new_markets.len(), "BB: markets discovered");
                        for market in new_markets {
                            self.markets.insert(market.condition_id.clone(), market);
                        }
                    }
                    PolymarketUpdate::PriceUpdate {
                        condition_id,
                        yes_price,
                        no_price,
                    } => {
                        if let Some(market) = self.markets.get_mut(&condition_id) {
                            market.implied_prob_yes = yes_price;
                            market.implied_prob_no = no_price;
                        }
                    }
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::candle::Candle;
    use crate::types::market::MarketType;
    use chrono::TimeZone;

    fn make_config() -> Config {
        crate::config::Config {
            mode: crate::config::RunMode::Paper,
            strategy: crate::config::StrategyName::BollingerBands,
            backtest_file: String::new(),
            coinbase_api_key: String::new(),
            coinbase_api_secret: String::new(),
            polymarket_private_key: String::new(),
            polygon_rpc_url: String::new(),
            ethereum_rpc_url: String::new(),
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
            bb_cooldown_candles: 3,
            fee_rate_bps: 156,
            maker_mode: true,
            heartbeat_interval_secs: 10,
            ws_book_max_stale_secs: 30,
            unified_bb_weight: 0.4,
            unified_ma_weight: 0.4,
            unified_book_weight: 0.3,
            arb_threshold: Decimal::new(97, 2),
            min_arb_profit: Decimal::new(3, 2),
            arb_size_usd: Decimal::from(5),
            updown_enabled: true,
            updown_only: false,
            log_level: "info".into(),
            ml_enabled: false,
            ml_server_url: String::new(),
            ml_timeout_ms: 50,
            ml_signal_weight: 0.5,
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

    #[test]
    fn test_no_duplicate_buy_when_long() {
        let mut strat = BollingerBandsStrategy::new(&make_config());
        // Set cooldown to 0 to isolate position tracking
        strat.cooldown_candles = 0;

        // Fill window with stable prices, then trigger buy
        let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 80.0];
        let mut buy_count = 0;
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            for s in strat.on_event(StrategyEvent::CandleComplete(candle)) {
                if s.side == Side::Buy {
                    buy_count += 1;
                }
            }
        }
        assert_eq!(buy_count, 1, "first buy signal emitted");

        // Now feed another drop — should NOT produce a second buy (already Long)
        // Reset window with prices that would cross lower band again
        let more_prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 78.0];
        for (i, &price) in more_prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, ((i + 10) as i64) * 300);
            for s in strat.on_event(StrategyEvent::CandleComplete(candle)) {
                if s.side == Side::Buy {
                    buy_count += 1;
                }
            }
        }
        assert_eq!(buy_count, 1, "no duplicate buy when already long");
    }

    #[test]
    fn test_cooldown_prevents_rapid_signals() {
        let mut strat = BollingerBandsStrategy::new(&make_config());
        // cooldown_candles = 3 by default

        // Fill window, trigger a buy signal
        let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 80.0];
        let mut signal_count = 0;
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            signal_count += strat.on_event(StrategyEvent::CandleComplete(candle)).len();
        }
        assert!(signal_count > 0, "initial signal emitted");

        // Feed 3 more candles during cooldown — even a sell cross should be suppressed
        let cooldown_prices = vec![120.0, 120.0, 120.0];
        let mut during_cooldown = 0;
        for (i, &price) in cooldown_prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, ((i + 10) as i64) * 300);
            during_cooldown += strat.on_event(StrategyEvent::CandleComplete(candle)).len();
        }
        assert_eq!(during_cooldown, 0, "no signals during cooldown period");
    }

    #[test]
    fn test_polymarket_signal_generated() {
        let mut strat = BollingerBandsStrategy::new(&make_config());

        // Inject a bullish Polymarket market
        let market = PolymarketMarket {
            condition_id: "cond-1".into(),
            token_id_yes: "yes-1".into(),
            token_id_no: "no-1".into(),
            question: "Will BTC reach $110k?".into(),
            underlying_symbol: "BTC-USD".into(),
            strike: Decimal::from(110_000),
            expiry: Utc::now() + chrono::Duration::days(7),
            implied_prob_yes: Decimal::new(50, 2),
            implied_prob_no: Decimal::new(50, 2),
            direction: MarketDirection::Bullish,
            market_type: MarketType::StrikeAbove,
        };
        strat.markets.insert(market.condition_id.clone(), market);

        // Fill window then trigger buy (oversold bounce)
        let prices = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 80.0];
        let mut signals = Vec::new();
        for (i, &price) in prices.iter().enumerate() {
            let candle = make_candle("BTC-USD", price, (i as i64) * 300);
            signals.extend(strat.on_event(StrategyEvent::CandleComplete(candle)));
        }

        // Should have both a Spot and a Polymarket signal
        let spot_signals: Vec<_> = signals
            .iter()
            .filter(|s| matches!(s.target, TradeTarget::Spot { .. }))
            .collect();
        let pm_signals: Vec<_> = signals
            .iter()
            .filter(|s| matches!(s.target, TradeTarget::Polymarket(_)))
            .collect();

        assert_eq!(spot_signals.len(), 1, "one spot signal");
        assert_eq!(pm_signals.len(), 1, "one polymarket signal");

        // BB Buy + Bullish market → PM Buy YES
        assert_eq!(spot_signals[0].side, Side::Buy);
        assert_eq!(pm_signals[0].side, Side::Buy);
    }
}
