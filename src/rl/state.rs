use crate::types::candle::Candle;
use crate::types::events::{ExecutionEvent, MlDirection, MlPrediction};
use crate::types::market::{AggregatedPrice, MarketType, PolymarketMarket};
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::{HashMap, VecDeque};

/// Number of features in the state vector.
pub const STATE_DIM: usize = 47;

/// Rolling price history length for return calculations.
const PRICE_HISTORY_LEN: usize = 60; // 60 ticks ≈ 1 minute at ~1 tick/sec

/// Builds the 47-feature state vector from accumulated event data.
pub struct StateBuilder {
    // Market state
    latest_prices: HashMap<String, AggregatedPrice>,
    price_history: HashMap<String, VecDeque<f64>>, // rolling vwap history

    // Polymarket book state (from BookCache reads done by bridge)
    pub poly_best_bid: f64,
    pub poly_best_ask: f64,
    pub poly_depth_bid: f64,
    pub poly_depth_ask: f64,

    // Active markets
    pub active_market: Option<PolymarketMarket>,

    // Candle state for BB/MA features
    bb_window: VecDeque<f64>, // close prices for BB
    ema_fast: f64,
    ema_slow: f64,
    ema_fast_count: usize,
    ema_slow_count: usize,

    // ML state
    latest_ml: Option<MlPrediction>,

    // Portfolio state
    pub position_size: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub peak_equity: f64,
    pub capital: f64,
    pub num_positions: usize,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub total_trade_pnl: f64,
    pub last_trade_pnl: f64,
    pub last_trade_time: Option<DateTime<Utc>>,

    // Config
    bb_period: usize,
    bb_num_std: f64,
    ma_fast_period: usize,
    ma_slow_period: usize,
}

impl StateBuilder {
    pub fn new(capital: f64, bb_period: usize, ma_fast_period: usize, ma_slow_period: usize) -> Self {
        Self {
            latest_prices: HashMap::new(),
            price_history: HashMap::new(),
            poly_best_bid: 0.0,
            poly_best_ask: 0.0,
            poly_depth_bid: 0.0,
            poly_depth_ask: 0.0,
            active_market: None,
            bb_window: VecDeque::with_capacity(bb_period + 1),
            ema_fast: 0.0,
            ema_slow: 0.0,
            ema_fast_count: 0,
            ema_slow_count: 0,
            latest_ml: None,
            position_size: 0.0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            peak_equity: capital,
            capital,
            num_positions: 0,
            total_trades: 0,
            winning_trades: 0,
            total_trade_pnl: 0.0,
            last_trade_pnl: 0.0,
            last_trade_time: None,
            bb_period,
            bb_num_std: 2.0,
            ma_fast_period,
            ma_slow_period,
        }
    }

    pub fn on_price_update(&mut self, price: &AggregatedPrice) {
        let vwap = dec_to_f64(price.vwap);
        let history = self
            .price_history
            .entry(price.symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(PRICE_HISTORY_LEN + 1));
        history.push_back(vwap);
        if history.len() > PRICE_HISTORY_LEN {
            history.pop_front();
        }
        self.latest_prices.insert(price.symbol.clone(), price.clone());
    }

    pub fn on_candle(&mut self, candle: &Candle) {
        let close = dec_to_f64(candle.close);

        // BB window
        self.bb_window.push_back(close);
        if self.bb_window.len() > self.bb_period {
            self.bb_window.pop_front();
        }

        // EMA fast
        self.ema_fast_count += 1;
        if self.ema_fast_count == 1 {
            self.ema_fast = close;
        } else {
            let k = 2.0 / (self.ma_fast_period as f64 + 1.0);
            self.ema_fast = close * k + self.ema_fast * (1.0 - k);
        }

        // EMA slow
        self.ema_slow_count += 1;
        if self.ema_slow_count == 1 {
            self.ema_slow = close;
        } else {
            let k = 2.0 / (self.ma_slow_period as f64 + 1.0);
            self.ema_slow = close * k + self.ema_slow * (1.0 - k);
        }
    }

    pub fn on_ml_prediction(&mut self, pred: &MlPrediction) {
        self.latest_ml = Some(pred.clone());
    }

    pub fn on_execution_event(&mut self, event: &ExecutionEvent) {
        match event {
            ExecutionEvent::OrderFilled(fill) => {
                let pnl = dec_to_f64(fill.size) * dec_to_f64(fill.price);
                self.total_trades += 1;
                self.last_trade_pnl = pnl;
                self.last_trade_time = Some(fill.timestamp);
            }
            ExecutionEvent::PositionUpdate(pos) => {
                self.position_size = dec_to_f64(pos.size);
                self.unrealized_pnl = dec_to_f64(pos.unrealized_pnl);
                self.realized_pnl = dec_to_f64(pos.realized_pnl);
            }
            _ => {}
        }
    }

    /// Build the 47-feature state vector. Returns None if insufficient data.
    pub fn build(&self, symbol: &str) -> Option<Vec<f64>> {
        let price = self.latest_prices.get(symbol)?;
        let now = price.timestamp;

        let vwap = dec_to_f64(price.vwap);
        if vwap == 0.0 {
            return None;
        }

        let mut state = Vec::with_capacity(STATE_DIM);

        // === Market features (18) ===
        state.push(vwap);                                    // 0: vwap
        state.push(dec_to_f64(price.best_bid));              // 1: best_bid
        state.push(dec_to_f64(price.best_ask));              // 2: best_ask
        state.push(dec_to_f64(price.spread));                // 3: spread
        state.push(price.volatility);                        // 4: volatility
        state.push(price.oracle_price.map(dec_to_f64).unwrap_or(vwap)); // 5: oracle_price
        state.push(self.implied_prob());                     // 6: implied_prob
        state.push(self.poly_best_bid);                      // 7: poly_best_bid
        state.push(self.poly_best_ask);                      // 8: poly_best_ask
        state.push(self.poly_depth_bid);                     // 9: poly_depth_bid
        state.push(self.poly_depth_ask);                     // 10: poly_depth_ask
        state.push(self.poly_spread());                      // 11: poly_spread
        state.push(self.ret(symbol, 1));                     // 12: return_1tick
        state.push(self.ret(symbol, 5));                     // 13: return_5tick
        state.push(self.ret(symbol, 15));                    // 14: return_15tick
        state.push(self.ret(symbol, 60));                    // 15: return_60tick
        state.push(dec_to_f64(price.spread) / vwap);         // 16: relative_spread
        state.push(price.num_feeds as f64);                  // 17: num_feeds

        // === Portfolio features (10) ===
        let equity = self.capital + self.realized_pnl + self.unrealized_pnl;
        let drawdown = if self.peak_equity > 0.0 {
            (self.peak_equity - equity) / self.peak_equity
        } else {
            0.0
        };
        let win_rate = if self.total_trades > 0 {
            self.winning_trades as f64 / self.total_trades as f64
        } else {
            0.5
        };
        let avg_trade_pnl = if self.total_trades > 0 {
            self.total_trade_pnl / self.total_trades as f64
        } else {
            0.0
        };
        let exposure_pct = if equity > 0.0 {
            self.position_size / equity
        } else {
            0.0
        };

        state.push(self.position_size);                      // 18: position_size
        state.push(self.unrealized_pnl);                     // 19: unrealized_pnl
        state.push(self.realized_pnl);                       // 20: realized_pnl
        state.push(drawdown);                                // 21: drawdown
        state.push(if equity > 0.0 { (equity - self.position_size) / equity } else { 1.0 }); // 22: cash_pct
        state.push(self.num_positions as f64);               // 23: num_positions
        state.push(win_rate);                                // 24: win_rate
        state.push(avg_trade_pnl);                           // 25: avg_trade_pnl
        state.push(self.last_trade_pnl);                     // 26: last_trade_pnl
        state.push(exposure_pct);                            // 27: exposure_pct

        // === Model features (12) ===
        let (ml_dir, ml_conf) = match &self.latest_ml {
            Some(pred) => {
                let dir = match pred.direction {
                    MlDirection::Up => 1.0,
                    MlDirection::Down => -1.0,
                };
                (dir, pred.confidence)
            }
            None => (0.0, 0.0),
        };
        let (bb_upper, bb_lower, bb_sma, bb_zscore) = self.bb_features(vwap);
        let book_imbalance = self.book_imbalance();
        let vol_regime = if price.volatility > 0.02 { 1.0 } else if price.volatility > 0.01 { 0.5 } else { 0.0 };
        let ma_cross = if self.ema_fast_count >= self.ma_fast_period && self.ema_slow_count >= self.ma_slow_period {
            (self.ema_fast - self.ema_slow) / self.ema_slow
        } else {
            0.0
        };
        let trend = self.ret(symbol, 60); // use 60-tick return as trend proxy

        state.push(ml_dir);                                  // 28: ml_direction
        state.push(ml_conf);                                 // 29: ml_confidence
        state.push(bb_upper);                                // 30: bb_upper
        state.push(bb_lower);                                // 31: bb_lower
        state.push(bb_sma);                                  // 32: bb_sma
        state.push(bb_zscore);                               // 33: bb_zscore
        state.push(self.ema_fast);                           // 34: ema_fast
        state.push(self.ema_slow);                           // 35: ema_slow
        state.push(ma_cross);                                // 36: ma_cross_signal
        state.push(book_imbalance);                          // 37: book_imbalance
        state.push(vol_regime);                              // 38: volatility_regime
        state.push(trend);                                   // 39: trend_strength

        // === Temporal features (7) ===
        let hour = now.hour() as f64;
        let dow = now.weekday().num_days_from_monday() as f64;
        let minutes_to_end = self.minutes_to_window_end(now);
        let is_weekend = if dow >= 5.0 { 1.0 } else { 0.0 };
        let secs_since_last = self
            .last_trade_time
            .map(|t| (now - t).num_seconds().max(0) as f64)
            .unwrap_or(3600.0); // default 1 hour

        state.push((hour * std::f64::consts::PI / 12.0).sin()); // 40: hour_sin
        state.push((hour * std::f64::consts::PI / 12.0).cos()); // 41: hour_cos
        state.push((dow * std::f64::consts::PI / 3.5).sin());   // 42: dow_sin
        state.push((dow * std::f64::consts::PI / 3.5).cos());   // 43: dow_cos
        state.push(minutes_to_end);                              // 44: minutes_to_window_end
        state.push(is_weekend);                                  // 45: is_weekend
        state.push(secs_since_last);                             // 46: secs_since_last_trade

        debug_assert_eq!(state.len(), STATE_DIM);
        Some(state)
    }

    // --- Helper methods ---

    fn implied_prob(&self) -> f64 {
        // Prefer live book midpoint over stale discovery-time implied prob
        if self.poly_best_bid > 0.0 && self.poly_best_ask > 0.0 {
            (self.poly_best_bid + self.poly_best_ask) / 2.0
        } else {
            self.active_market
                .as_ref()
                .map(|m| dec_to_f64(m.implied_prob_yes))
                .unwrap_or(0.5)
        }
    }

    fn poly_spread(&self) -> f64 {
        if self.poly_best_ask > 0.0 && self.poly_best_bid > 0.0 {
            self.poly_best_ask - self.poly_best_bid
        } else {
            0.0
        }
    }

    fn ret(&self, symbol: &str, lookback: usize) -> f64 {
        let history = match self.price_history.get(symbol) {
            Some(h) => h,
            None => return 0.0,
        };
        if history.len() < lookback + 1 {
            return 0.0;
        }
        let current = history[history.len() - 1];
        let past = history[history.len() - 1 - lookback];
        if past == 0.0 {
            return 0.0;
        }
        (current - past) / past
    }

    fn bb_features(&self, current_price: f64) -> (f64, f64, f64, f64) {
        if self.bb_window.len() < self.bb_period {
            return (0.0, 0.0, 0.0, 0.0);
        }
        let sum: f64 = self.bb_window.iter().sum();
        let sma = sum / self.bb_window.len() as f64;
        let variance: f64 = self.bb_window.iter().map(|x| (x - sma).powi(2)).sum::<f64>()
            / self.bb_window.len() as f64;
        let std_dev = variance.sqrt();
        let upper = sma + self.bb_num_std * std_dev;
        let lower = sma - self.bb_num_std * std_dev;
        let zscore = if std_dev > 0.0 {
            (current_price - sma) / std_dev
        } else {
            0.0
        };
        (upper, lower, sma, zscore)
    }

    fn book_imbalance(&self) -> f64 {
        let total = self.poly_depth_bid + self.poly_depth_ask;
        if total > 0.0 {
            (self.poly_depth_bid - self.poly_depth_ask) / total
        } else {
            0.0
        }
    }

    fn minutes_to_window_end(&self, now: DateTime<Utc>) -> f64 {
        match &self.active_market {
            Some(m) => match m.market_type {
                MarketType::UpDown { window_start_ts, window_secs } => {
                    let end_ts = window_start_ts + window_secs as i64;
                    let remaining = end_ts - now.timestamp();
                    (remaining as f64 / 60.0).max(0.0)
                }
                MarketType::General { end_date_ts } => {
                    let remaining = end_date_ts - now.timestamp();
                    (remaining as f64 / 60.0).max(0.0)
                }
                MarketType::StrikeAbove => {
                    let remaining = (m.expiry - now).num_seconds();
                    (remaining as f64 / 60.0).max(0.0)
                }
            },
            None => 0.0,
        }
    }
}

fn dec_to_f64(d: rust_decimal::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}
