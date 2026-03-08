use crate::types::events::PolymarketUpdate;
use crate::types::market::{MarketDirection, MarketTick, MarketType, PolymarketMarket};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// Generate synthetic Polymarket-style markets for backtesting.
///
/// For each symbol, creates markets at ±5%, ±10%, ±20% from the
/// initial price, with 7-day expiry and 50/50 implied odds.
pub fn generate_markets(
    symbols: &[String],
    initial_prices: &[(String, Decimal)],
) -> Vec<PolymarketMarket> {
    let mut markets = Vec::new();
    let offsets = [
        (0.95, MarketDirection::Bearish, "drop to"),
        (0.90, MarketDirection::Bearish, "drop to"),
        (1.05, MarketDirection::Bullish, "reach"),
        (1.10, MarketDirection::Bullish, "reach"),
        (1.20, MarketDirection::Bullish, "reach"),
    ];

    for (symbol, price) in initial_prices {
        if !symbols.contains(symbol) {
            continue;
        }

        for (i, (factor, direction, verb)) in offsets.iter().enumerate() {
            let strike = price * Decimal::from_f64_retain(*factor).unwrap_or(Decimal::ONE);
            let strike_display = strike.round_dp(0);

            markets.push(PolymarketMarket {
                condition_id: format!("bt-{}-{}", symbol, i),
                token_id_yes: format!("bt-yes-{}-{}", symbol, i),
                token_id_no: format!("bt-no-{}-{}", symbol, i),
                question: format!("Will {} {} ${}?", symbol, verb, strike_display),
                underlying_symbol: symbol.clone(),
                strike,
                expiry: Utc::now() + Duration::days(7),
                implied_prob_yes: dec!(0.50),
                implied_prob_no: dec!(0.50),
                direction: *direction,
                market_type: MarketType::StrikeAbove,
            });
        }
    }

    markets
}

/// Generate UpDown markets at every 5-minute boundary found in tick data.
///
/// Returns `Vec<(tick_index, Vec<PolymarketMarket>)>` — the tick index at which
/// each batch of markets should be injected during replay.
///
/// `window_secs`: window duration (default 300 = 5 minutes).
pub fn generate_updown_markets(
    ticks: &[MarketTick],
    window_secs: u64,
) -> Vec<(usize, Vec<PolymarketMarket>)> {
    if ticks.is_empty() {
        return Vec::new();
    }

    let mut result: Vec<(usize, Vec<PolymarketMarket>)> = Vec::new();

    // Track the next boundary timestamp per symbol
    let mut next_boundary: HashMap<String, i64> = HashMap::new();
    // Counter per symbol for unique condition IDs
    let mut counter: HashMap<String, u64> = HashMap::new();

    for (idx, tick) in ticks.iter().enumerate() {
        let ts = tick.timestamp.timestamp();
        let symbol = &tick.symbol;

        // Initialize: first tick sets the first boundary at the next aligned 5-min mark
        let boundary = next_boundary.entry(symbol.clone()).or_insert_with(|| {
            // Align to the next window_secs boundary
            let w = window_secs as i64;
            ((ts / w) + 1) * w
        });

        if ts >= *boundary {
            let window_start_ts = *boundary;
            let n = counter.entry(symbol.clone()).or_insert(0);

            let market = PolymarketMarket {
                condition_id: format!("bt-updown-{}-{}", symbol, n),
                token_id_yes: format!("bt-updown-yes-{}-{}", symbol, n),
                token_id_no: format!("bt-updown-no-{}-{}", symbol, n),
                question: format!(
                    "Will {} go up in the next {} minutes?",
                    symbol,
                    window_secs / 60
                ),
                underlying_symbol: symbol.clone(),
                strike: tick.mid(), // start price = mid at window open
                expiry: DateTime::from_timestamp(window_start_ts + window_secs as i64, 0)
                    .unwrap_or_else(Utc::now),
                implied_prob_yes: dec!(0.50),
                implied_prob_no: dec!(0.50),
                direction: MarketDirection::Bullish,
                market_type: MarketType::UpDown {
                    window_start_ts,
                    window_secs,
                },
            };

            *n += 1;
            // Advance to next boundary
            *boundary = window_start_ts + window_secs as i64;

            result.push((idx, vec![market]));
        }
    }

    result
}

/// Convert synthetic markets into a PolymarketUpdate for injection.
pub fn as_discovery_event(markets: Vec<PolymarketMarket>) -> PolymarketUpdate {
    PolymarketUpdate::MarketsDiscovered(markets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_markets_creates_correct_count() {
        let symbols = vec!["BTC-USD".into(), "ETH-USD".into()];
        let prices = vec![
            ("BTC-USD".into(), Decimal::from(100_000)),
            ("ETH-USD".into(), Decimal::from(3_500)),
        ];
        let markets = generate_markets(&symbols, &prices);
        // 5 offsets × 2 symbols = 10
        assert_eq!(markets.len(), 10);
    }

    #[test]
    fn test_generate_markets_directions() {
        let symbols = vec!["BTC-USD".into()];
        let prices = vec![("BTC-USD".into(), Decimal::from(100_000))];
        let markets = generate_markets(&symbols, &prices);

        let bearish: Vec<_> = markets
            .iter()
            .filter(|m| m.direction == MarketDirection::Bearish)
            .collect();
        let bullish: Vec<_> = markets
            .iter()
            .filter(|m| m.direction == MarketDirection::Bullish)
            .collect();
        assert_eq!(bearish.len(), 2);
        assert_eq!(bullish.len(), 3);
    }

    #[test]
    fn test_strike_prices_correct() {
        let symbols = vec!["BTC-USD".into()];
        let prices = vec![("BTC-USD".into(), Decimal::from(100_000))];
        let markets = generate_markets(&symbols, &prices);

        // Check that strikes are at expected offsets (round to handle f64 imprecision)
        let strikes: Vec<i64> = markets
            .iter()
            .map(|m| m.strike.round_dp(0).to_string().parse::<i64>().unwrap())
            .collect();
        assert!(strikes.contains(&95_000));
        assert!(strikes.contains(&90_000));
        assert!(strikes.contains(&105_000));
        assert!(strikes.contains(&110_000));
        assert!(strikes.contains(&120_000));
    }

    fn make_tick(symbol: &str, ts_secs: i64, price: f64) -> MarketTick {
        use crate::types::market::Exchange;
        let price_dec = Decimal::from_f64_retain(price).unwrap();
        MarketTick {
            exchange: Exchange::Coinbase,
            symbol: symbol.to_string(),
            bid: price_dec - dec!(0.50),
            ask: price_dec + dec!(0.50),
            last: price_dec,
            volume_24h: Decimal::from(1000),
            timestamp: DateTime::from_timestamp(ts_secs, 0).unwrap(),
        }
    }

    #[test]
    fn test_updown_markets_at_boundaries() {
        // Ticks every 60s from t=0 to t=900 (15 minutes)
        // Boundaries at 300, 600, 900 → 3 markets
        let ticks: Vec<MarketTick> = (0..=15)
            .map(|i| make_tick("BTC-USD", i * 60, 100_000.0))
            .collect();

        let markets = generate_updown_markets(&ticks, 300);
        assert_eq!(
            markets.len(),
            3,
            "expected 3 updown markets for 15 minutes of data"
        );

        // First market at boundary=300 (tick where ts >= 300)
        let (idx0, ref m0) = markets[0];
        assert!(ticks[idx0].timestamp.timestamp() >= 300);
        assert!(matches!(
            m0[0].market_type,
            MarketType::UpDown {
                window_start_ts: 300,
                window_secs: 300
            }
        ));

        // Second market at boundary=600
        let (idx1, ref m1) = markets[1];
        assert!(ticks[idx1].timestamp.timestamp() >= 600);
        assert!(matches!(
            m1[0].market_type,
            MarketType::UpDown {
                window_start_ts: 600,
                window_secs: 300
            }
        ));

        // Third market at boundary=900
        let (idx2, ref m2) = markets[2];
        assert!(ticks[idx2].timestamp.timestamp() >= 900);
        assert!(matches!(
            m2[0].market_type,
            MarketType::UpDown {
                window_start_ts: 900,
                window_secs: 300
            }
        ));
    }

    #[test]
    fn test_updown_empty_ticks() {
        let markets = generate_updown_markets(&[], 300);
        assert!(markets.is_empty());
    }

    #[test]
    fn test_updown_multi_symbol() {
        // Two symbols, ticks interleaved
        let ticks = vec![
            make_tick("BTC-USD", 0, 100_000.0),
            make_tick("ETH-USD", 0, 3_500.0),
            make_tick("BTC-USD", 300, 100_100.0),
            make_tick("ETH-USD", 300, 3_510.0),
        ];
        let markets = generate_updown_markets(&ticks, 300);
        assert_eq!(markets.len(), 2, "one market per symbol at boundary");

        let symbols: Vec<&str> = markets
            .iter()
            .map(|(_, m)| m[0].underlying_symbol.as_str())
            .collect();
        assert!(symbols.contains(&"BTC-USD"));
        assert!(symbols.contains(&"ETH-USD"));
    }
}
