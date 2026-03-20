use crate::config::Config;
use crate::polymarket::fees::FeeCalculator;
use crate::polymarket::ws_orderbook::BookCache;
use crate::strategy::traits::{Strategy, StrategyEvent, StrategySubscriptions};
use crate::types::events::{PolymarketUpdate, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{AggregatedPrice, MarketType, PolymarketMarket};
use crate::types::order::Side;
use chrono::Utc;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

/// Maximum seconds before window end to start looking for entries.
const ENTRY_WINDOW_SECS: i64 = 60;

/// Minimum seconds before window end — don't enter too late (order may not fill).
const MIN_TIME_REMAINING_SECS: i64 = 5;

/// Minimum price to buy (only buy near-certain outcomes).
const MIN_BUY_PRICE: Decimal = dec!(0.95);

/// Target buy price — we place limit orders at this price.
const TARGET_BUY_PRICE: Decimal = dec!(0.99);

/// Maximum price we'll pay — above this the edge is too thin after fees.
const MAX_BUY_PRICE: Decimal = dec!(0.995);

/// Minimum BTC price distance from strike to consider outcome "certain".
/// If BTC is $50+ above strike, "Up" is very likely to resolve true.
const MIN_PRICE_DISTANCE_PCT: f64 = 0.0005; // 0.05% of price (~$42 at $84k)

/// Resolution Sniper strategy for Polymarket UpDown markets.
///
/// Buys near-certain outcomes (95c-99c) in the final 60 seconds before
/// market resolution. When the price is clearly above/below the strike,
/// one side trades at ~99c. We buy at 99c via limit order, market resolves
/// at $1.00, profit is ~1c per share minus negligible fees.
///
/// At p=0.99, Polymarket fee ≈ 0.0002% — effectively zero.
/// Edge per trade: ~$1.24 on $124 position (1% return in <60 seconds).
///
/// Target frequency: ~1.25 trades per active minute across all UpDown markets.
pub struct ResolutionSniperStrategy {
    /// Active UpDown markets keyed by condition_id.
    markets: HashMap<String, PolymarketMarket>,
    /// Latest exchange prices per symbol.
    latest_prices: HashMap<String, AggregatedPrice>,
    /// Captured strike price at window start, per condition_id.
    strike_prices: HashMap<String, f64>,
    /// Markets we've already entered — one entry per market.
    entered_markets: HashSet<String>,
    /// Book cache for reading live Polymarket orderbook prices.
    book_cache: Option<BookCache>,
    ws_book_max_stale_secs: u64,
    /// Fee calculator.
    fee_calculator: FeeCalculator,
    /// Position size per trade (USD).
    size_usd: Decimal,
}

impl ResolutionSniperStrategy {
    pub fn new(config: &Config) -> Self {
        Self {
            markets: HashMap::new(),
            latest_prices: HashMap::new(),
            strike_prices: HashMap::new(),
            entered_markets: HashSet::new(),
            book_cache: None,
            ws_book_max_stale_secs: config.ws_book_max_stale_secs,
            fee_calculator: FeeCalculator::new(config.fee_rate_bps),
            size_usd: config.max_position_usd,
        }
    }

    pub fn with_book_cache(mut self, cache: BookCache) -> Self {
        self.book_cache = Some(cache);
        self
    }

    fn on_price_update(&mut self, price: &AggregatedPrice) -> Vec<TradeSignal> {
        self.latest_prices
            .insert(price.symbol.clone(), price.clone());

        let now = Utc::now();
        let mut signals = Vec::new();

        // Check all UpDown markets for this symbol
        let matching_markets: Vec<PolymarketMarket> = self
            .markets
            .values()
            .filter(|m| m.underlying_symbol == price.symbol)
            .cloned()
            .collect();

        for market in matching_markets {
            // Skip if already entered
            if self.entered_markets.contains(&market.condition_id) {
                continue;
            }

            let (window_start_ts, window_secs) = match market.market_type {
                MarketType::UpDown {
                    window_start_ts,
                    window_secs,
                } => (window_start_ts, window_secs),
                _ => continue,
            };

            let window_end_ts = window_start_ts + window_secs as i64;
            let time_remaining = window_end_ts - now.timestamp();

            // Only enter in the final window
            if time_remaining > ENTRY_WINDOW_SECS || time_remaining < MIN_TIME_REMAINING_SECS {
                continue;
            }

            let spot = price.vwap.to_f64().unwrap_or(0.0);

            // Get LIVE orderbook prices for both tokens to determine winning side.
            // The discovery-time implied_prob is stale (set 4+ min ago at ~50/50).
            // We need the current book prices to know which side is winning.
            let yes_bid = self.get_book_bid(&market.token_id_yes);
            let no_bid = self.get_book_bid(&market.token_id_no);

            let (winning_side, winning_prob) = match (yes_bid, no_bid) {
                (Some(yb), Some(nb)) => {
                    let yf = yb.to_f64().unwrap_or(0.5);
                    let nf = nb.to_f64().unwrap_or(0.5);
                    if yf > nf {
                        (Side::Buy, yf)
                    } else {
                        (Side::Sell, nf)
                    }
                }
                (Some(yb), None) => (Side::Buy, yb.to_f64().unwrap_or(0.5)),
                (None, Some(nb)) => (Side::Sell, nb.to_f64().unwrap_or(0.5)),
                (None, None) => {
                    // Fallback to discovery-time implied prob (less reliable)
                    let yf = market.implied_prob_yes.to_f64().unwrap_or(0.5);
                    let nf = market.implied_prob_no.to_f64().unwrap_or(0.5);
                    if yf > nf { (Side::Buy, yf) } else { (Side::Sell, nf) }
                }
            };

            // Only enter if one side is clearly winning (>85% implied from live book)
            if winning_prob < 0.85 {
                continue;
            }

            let strike = 0.0;
            let price_distance_pct = winning_prob - 0.5;

            // Aggressive pricing: bid at winning_prob + 2c to sweep the book.
            // If winning side bids at 0.90, we offer 0.92 to guarantee fill.
            // Capped at 0.95 — above that the edge is too thin.
            let entry_price = (Decimal::from_f64(winning_prob).unwrap_or(dec!(0.95))
                + dec!(0.02))
                .min(dec!(0.95));

            // Verify fee is negligible at this price
            let fee = self.fee_calculator.taker_fee(entry_price);
            let edge = Decimal::ONE - entry_price - fee;
            if edge <= Decimal::ZERO {
                debug!(
                    market = %market.question,
                    entry_price = %entry_price,
                    fee = %fee,
                    "sniper: negative edge after fees"
                );
                continue;
            }

            let edge_f64 = edge.to_f64().unwrap_or(0.0);

            info!(
                market = %market.question,
                side = if winning_side == Side::Buy { "UP (YES)" } else { "DOWN (NO)" },
                spot = format!("{:.2}", spot),
                strike = format!("{:.2}", strike),
                distance = format!("{:.2}%", price_distance_pct * 100.0),
                entry_price = %entry_price,
                fee = format!("{:.6}", fee),
                edge = format!("{:.4}", edge),
                time_remaining = format!("{}s", time_remaining),
                size_usd = %self.size_usd,
                "sniper: entering near-certain outcome"
            );

            self.entered_markets.insert(market.condition_id.clone());

            signals.push(TradeSignal {
                target: TradeTarget::Polymarket(market.clone()),
                side: winning_side,
                size_usd: self.size_usd,
                confidence: 0.95,
                price: entry_price.round_dp(2),
                metadata: SignalMetadata::ResolutionSniper {
                    spot,
                    strike,
                    distance_pct: price_distance_pct,
                    entry_price: entry_price.to_f64().unwrap_or(0.0),
                    edge: edge_f64,
                    time_remaining_secs: time_remaining as f64,
                },
                timestamp: now,
                is_exit: false,
            });
        }

        signals
    }

    fn on_markets_discovered(&mut self, markets: Vec<PolymarketMarket>) {
        for m in markets {
            if matches!(m.market_type, MarketType::UpDown { .. }) {
                self.markets.insert(m.condition_id.clone(), m);
            }
        }

        // Prune expired markets and their entered_markets entries
        let now = Utc::now().timestamp();
        let expired: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, m)| match m.market_type {
                MarketType::UpDown {
                    window_start_ts,
                    window_secs,
                } => window_start_ts + (window_secs as i64) < now,
                _ => false,
            })
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired {
            self.markets.remove(&id);
            self.entered_markets.remove(&id);
            self.strike_prices.remove(&id);
        }
    }

    fn get_book_ask(&self, token_id: &str) -> Option<Decimal> {
        let cache = self.book_cache.as_ref()?;
        // Try to read without blocking (best effort)
        let books = cache.try_read().ok()?;
        let snap = books.get(token_id)?;

        // Check staleness
        let age_secs = (Utc::now() - snap.timestamp).num_seconds();
        if age_secs > self.ws_book_max_stale_secs as i64 {
            return None;
        }

        if snap.best_ask > Decimal::ZERO {
            Some(snap.best_ask)
        } else {
            None
        }
    }

    fn get_book_bid(&self, token_id: &str) -> Option<Decimal> {
        let cache = self.book_cache.as_ref()?;
        let books = cache.try_read().ok()?;
        let snap = books.get(token_id)?;

        let age_secs = (Utc::now() - snap.timestamp).num_seconds();
        if age_secs > self.ws_book_max_stale_secs as i64 {
            return None;
        }

        if snap.best_bid > Decimal::ZERO {
            Some(snap.best_bid)
        } else {
            None
        }
    }
}

impl Strategy for ResolutionSniperStrategy {
    fn name(&self) -> &str {
        "resolution-sniper"
    }

    fn subscriptions(&self) -> StrategySubscriptions {
        StrategySubscriptions {
            price_updates: true,
            candles: false,
            execution_feedback: false,
            polymarket_updates: true,
            ml_predictions: false,
        }
    }

    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal> {
        match event {
            StrategyEvent::PriceUpdate(price) => self.on_price_update(&price),
            StrategyEvent::PolymarketUpdate(PolymarketUpdate::MarketsDiscovered(markets)) => {
                self.on_markets_discovered(markets);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::market::MarketDirection;
    use rust_decimal_macros::dec;

    fn make_market(condition_id: &str, strike: f64, window_start_ts: i64) -> PolymarketMarket {
        PolymarketMarket {
            condition_id: condition_id.to_string(),
            token_id_yes: "yes_token_1".to_string(),
            token_id_no: "no_token_1".to_string(),
            question: "BTC Up or Down - Test".to_string(),
            underlying_symbol: "BTC-USD".to_string(),
            strike: Decimal::from_f64(strike).unwrap(),
            expiry: Utc::now() + chrono::Duration::minutes(5),
            implied_prob_yes: dec!(0.99),
            implied_prob_no: dec!(0.01),
            direction: MarketDirection::Bullish,
            market_type: MarketType::UpDown {
                window_start_ts,
                window_secs: 300,
            },
        }
    }

    fn make_price(symbol: &str, vwap: f64) -> AggregatedPrice {
        use crate::types::market::Exchange;
        AggregatedPrice {
            symbol: symbol.to_string(),
            vwap: Decimal::from_f64(vwap).unwrap(),
            best_bid: Decimal::from_f64(vwap - 1.0).unwrap(),
            best_ask: Decimal::from_f64(vwap + 1.0).unwrap(),
            best_bid_exchange: Exchange::Binance,
            best_ask_exchange: Exchange::Binance,
            spread: dec!(2.0),
            volatility: 0.01,
            oracle_price: None,
            num_feeds: 3,
            timestamp: Utc::now(),
            trade_flow_imbalance: 0.0,
            recent_buy_volume: 0.0,
            recent_sell_volume: 0.0,
            funding_rate: None,
            mark_price: None,
            binance_mid: None,
        }
    }

    #[test]
    fn test_no_signal_too_early() {
        let config = test_config();
        let mut strat = ResolutionSniperStrategy::new(&config);

        // Market with 4 minutes remaining — too early
        let now = Utc::now().timestamp();
        let market = make_market("cond1", 84000.0, now - 60); // 60s elapsed, 240s remaining
        strat.on_markets_discovered(vec![market]);
        strat.strike_prices.insert("cond1".to_string(), 84000.0);

        let price = make_price("BTC-USD", 85000.0); // clearly above strike
        let signals = strat.on_price_update(&price);
        assert!(signals.is_empty(), "should not enter with >60s remaining");
    }

    #[test]
    fn test_signal_in_entry_window() {
        let config = test_config();
        let mut strat = ResolutionSniperStrategy::new(&config);

        // Market with 30s remaining — in the entry window
        let now = Utc::now().timestamp();
        let market = make_market("cond1", 84000.0, now - 270);
        strat.on_markets_discovered(vec![market]);
        strat.strike_prices.insert("cond1".to_string(), 84000.0);

        let price = make_price("BTC-USD", 85000.0); // clearly above strike
        let signals = strat.on_price_update(&price);
        assert_eq!(signals.len(), 1, "should enter when in window and price is clear");
        assert_eq!(signals[0].side, Side::Buy); // Up wins → buy YES
    }

    #[test]
    fn test_no_signal_when_uncertain() {
        let config = test_config();
        let mut strat = ResolutionSniperStrategy::new(&config);

        let now = Utc::now().timestamp();
        // Market with 50/50 implied — neither side is clearly winning
        let mut market = make_market("cond1", 84000.0, now - 270);
        market.implied_prob_yes = dec!(0.52);
        market.implied_prob_no = dec!(0.48);
        strat.on_markets_discovered(vec![market]);

        let price = make_price("BTC-USD", 84010.0);
        let signals = strat.on_price_update(&price);
        assert!(signals.is_empty(), "should not enter when implied prob < 80%");
    }

    #[test]
    fn test_one_entry_per_market() {
        let config = test_config();
        let mut strat = ResolutionSniperStrategy::new(&config);

        let now = Utc::now().timestamp();
        let market = make_market("cond1", 84000.0, now - 270);
        strat.on_markets_discovered(vec![market]);
        strat.strike_prices.insert("cond1".to_string(), 84000.0);

        let price = make_price("BTC-USD", 85000.0);
        let s1 = strat.on_price_update(&price);
        assert_eq!(s1.len(), 1);

        let s2 = strat.on_price_update(&price);
        assert!(s2.is_empty(), "should not enter same market twice");
    }

    #[test]
    fn test_down_side_entry() {
        let config = test_config();
        let mut strat = ResolutionSniperStrategy::new(&config);

        let now = Utc::now().timestamp();
        let mut market = make_market("cond1", 85000.0, now - 270);
        market.implied_prob_yes = dec!(0.01);
        market.implied_prob_no = dec!(0.99);
        strat.on_markets_discovered(vec![market]);
        strat.strike_prices.insert("cond1".to_string(), 85000.0);

        let price = make_price("BTC-USD", 84000.0); // below strike → Down wins
        let signals = strat.on_price_update(&price);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].side, Side::Sell); // Down wins → buy NO token
    }

    #[test]
    fn test_fee_negligible_at_99c() {
        let calc = FeeCalculator::new(0);
        let fee = calc.taker_fee(dec!(0.99));
        assert!(
            fee.to_f64().unwrap() < 0.001,
            "fee at p=0.99 should be negligible, got {}",
            fee
        );
    }

    fn test_config() -> Config {
        Config {
            mode: crate::config::RunMode::Paper,
            strategies: vec![],
            backtest_file: String::new(),
            coinbase_api_key: String::new(),
            coinbase_api_secret: String::new(),
            polymarket_private_key: String::new(),
            polygon_rpc_url: String::new(),
            ethereum_rpc_url: String::new(),
            symbols: vec!["BTC-USD".into()],
            min_edge_threshold: dec!(0.01),
            kelly_fraction_cap: dec!(0.5),
            volatility_window_hours: 24,
            max_position_usd: dec!(124),
            max_total_exposure_usd: dec!(5000),
            max_drawdown_pct: dec!(0.10),
            max_orders_per_minute: 60,
            ma_fast_period: 9,
            ma_slow_period: 21,
            ma_timeframe: "5m".into(),
            ma_size_usd: dec!(500),
            bb_period: 20,
            bb_num_std: 2.0,
            bb_timeframe: "5m".into(),
            bb_size_usd: dec!(500),
            bb_cooldown_candles: 3,
            fee_rate_bps: 0,
            maker_mode: true,
            heartbeat_interval_secs: 10,
            ws_book_max_stale_secs: 30,
            unified_bb_weight: 0.4,
            unified_ma_weight: 0.4,
            unified_book_weight: 0.3,
            arb_threshold: dec!(0.97),
            min_arb_profit: dec!(0.03),
            arb_size_usd: dec!(5),
            updown_enabled: true,
            updown_only: true,
            general_markets_enabled: false,
            general_poll_interval_secs: 30,
            general_min_liquidity: 5000,
            discount_rate: 0.1,
            ml_enabled: false,
            ml_server_url: String::new(),
            ml_timeout_ms: 50,
            ml_signal_weight: 0.5,
            binance_weight_multiplier: 5.0,
            chainlink_blend_weight: 0.1,
            log_level: "info".into(),
            stale_feed_timeout_secs: 30,
            rl_enabled: false,
            rl_server_url: String::new(),
            rl_timeout_ms: 100,
            rl_capital_usd: 1000.0,
            rl_log_transitions: false,
            rl_transition_log_path: String::new(),
            copy_trade_enabled: false,
            copy_trade_private_key: String::new(),
            copy_trade_funder_address: String::new(),
            copy_trade_targets: vec![],
            copy_trade_poll_interval_secs: 5,
            copy_trade_size_ratio: 0.1,
            copy_trade_max_position_usd: 50.0,
            copy_trade_max_latency_secs: 30,
            copy_trade_price_premium_pct: 0.05,
            copy_trade_max_slippage_pct: 0.15,
            web_enabled: false,
            web_port: 3000,
        }
    }
}
