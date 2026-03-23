use rust_decimal::prelude::*;
use rust_decimal::Decimal;

/// Dynamic fee calculator for Polymarket's parabolic fee curve.
///
/// Crypto fee formula: `fee(p) = p * fee_rate * (p * (1 - p))^exponent`
/// With fee_rate=0.25 and exponent=2, peak effective rate is 1.56% at p=0.50.
#[derive(Debug, Clone)]
pub struct FeeCalculator {
    fee_rate: Decimal,
    exponent: u32,
}

impl FeeCalculator {
    pub fn new(_fee_rate_bps: u32) -> Self {
        // Crypto markets: fee_rate=0.25, exponent=2
        Self {
            fee_rate: Decimal::new(25, 2), // 0.25
            exponent: 2,
        }
    }

    /// Taker fee as an absolute amount per share at the given price.
    /// `price` is the order price (0..1 probability).
    pub fn taker_fee(&self, price: Decimal) -> Decimal {
        let one = Decimal::ONE;
        let pq = price * (one - price); // p * (1 - p)
        let mut pq_exp = Decimal::ONE;
        for _ in 0..self.exponent {
            pq_exp *= pq;
        }
        price * self.fee_rate * pq_exp
    }

    /// Total cost to take: taker fee + half-spread.
    pub fn total_cost(&self, price: Decimal, half_spread: Decimal) -> Decimal {
        self.taker_fee(price) + half_spread
    }

    /// Net expected value after fees and spread.
    /// Returns EV in probability units: estimated_prob - implied_price - total_cost.
    pub fn net_ev(&self, estimated_prob: f64, implied_price: Decimal, half_spread: Decimal) -> f64 {
        let cost = self.total_cost(implied_price, half_spread);
        let cost_f64 = cost.to_f64().unwrap_or(0.0);
        let implied_f64 = implied_price.to_f64().unwrap_or(0.5);
        estimated_prob - implied_f64 - cost_f64
    }
}

impl Default for FeeCalculator {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_fee_at_midpoint() {
        let calc = FeeCalculator::new(0);
        let fee = calc.taker_fee(dec!(0.50));
        // p=0.50: 0.50 * 0.25 * (0.50 * 0.50)^2 = 0.50 * 0.25 * 0.0625 = 0.0078125
        // Effective rate: 0.0078125 / 0.50 = 1.5625%
        let expected = dec!(0.0078125);
        assert_eq!(fee, expected, "fee at p=0.50 should be 0.0078125");
    }

    #[test]
    fn test_effective_rate_at_midpoint() {
        let calc = FeeCalculator::new(0);
        let fee = calc.taker_fee(dec!(0.50));
        let effective_pct = (fee / dec!(0.50) * dec!(100)).to_f64().unwrap();
        assert!(
            (effective_pct - 1.5625).abs() < 0.01,
            "effective rate at p=0.50 should be ~1.56%, got {}%",
            effective_pct
        );
    }

    #[test]
    fn test_fee_at_extremes() {
        let calc = FeeCalculator::new(0);
        assert_eq!(calc.taker_fee(Decimal::ZERO), Decimal::ZERO);
        assert_eq!(calc.taker_fee(Decimal::ONE), Decimal::ZERO);
    }

    #[test]
    fn test_fee_symmetry() {
        let calc = FeeCalculator::new(0);
        // fee(0.3) should equal fee(0.7) due to p*(1-p) symmetry...
        // Actually not symmetric because of the leading `p` factor.
        // fee(0.3) = 0.3 * 0.25 * (0.21)^2 = 0.003307...
        // fee(0.7) = 0.7 * 0.25 * (0.21)^2 = 0.007717...
        let fee_low = calc.taker_fee(dec!(0.30));
        let fee_high = calc.taker_fee(dec!(0.70));
        assert!(fee_high > fee_low, "fee at 0.70 should be higher than at 0.30");
    }

    #[test]
    fn test_total_cost() {
        let calc = FeeCalculator::new(0);
        let cost = calc.total_cost(dec!(0.50), dec!(0.01));
        // 0.0078125 + 0.01 = 0.0178125
        assert_eq!(cost, dec!(0.0178125));
    }

    #[test]
    fn test_fee_near_extremes_is_tiny() {
        let calc = FeeCalculator::new(0);
        // At p=0.05: 0.05 * 0.25 * (0.05 * 0.95)^2 = 0.05 * 0.25 * 0.002256 ≈ 0.0000282
        let fee = calc.taker_fee(dec!(0.05));
        assert!(
            fee.to_f64().unwrap() < 0.001,
            "fee near extremes should be tiny"
        );
    }
}
