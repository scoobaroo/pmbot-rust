use crate::types::events::PolymarketUpdate;
use crate::types::market::{MarketDirection, MarketType, PolymarketMarket};
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

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
}
