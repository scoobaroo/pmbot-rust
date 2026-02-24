use rust_decimal::prelude::*;
use rust_decimal::Decimal;

/// Dynamic fee calculator for Polymarket's parabolic fee curve.
///
/// Fee formula: `fee(p) = p * (1 - p) * fee_rate_bps / 10_000`
/// Peak fee of fee_rate_bps/10_000 * 0.25 occurs at p = 0.50.
#[derive(Debug, Clone)]
pub struct FeeCalculator {
    fee_rate_bps: u32,
}

impl FeeCalculator {
    pub fn new(fee_rate_bps: u32) -> Self {
        Self { fee_rate_bps }
    }

    /// Taker fee as a fraction of notional at the given price.
    /// `price` is the order price (0..1 probability).
    pub fn taker_fee(&self, price: Decimal) -> Decimal {
        let one = Decimal::ONE;
        let bps = Decimal::from(self.fee_rate_bps);
        let ten_k = Decimal::from(10_000u32);
        price * (one - price) * bps / ten_k
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
        Self { fee_rate_bps: 156 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_fee_at_midpoint() {
        let calc = FeeCalculator::new(156);
        let fee = calc.taker_fee(dec!(0.50));
        // 0.50 * 0.50 * 156 / 10000 = 0.0039
        assert_eq!(fee, dec!(0.0039));
    }

    #[test]
    fn test_fee_at_extremes() {
        let calc = FeeCalculator::new(156);
        // At p=0 or p=1, fee should be 0
        assert_eq!(calc.taker_fee(Decimal::ZERO), Decimal::ZERO);
        assert_eq!(calc.taker_fee(Decimal::ONE), Decimal::ZERO);
    }

    #[test]
    fn test_fee_symmetry() {
        let calc = FeeCalculator::new(156);
        // fee(0.3) should equal fee(0.7) due to p*(1-p) symmetry
        assert_eq!(calc.taker_fee(dec!(0.30)), calc.taker_fee(dec!(0.70)));
    }

    #[test]
    fn test_total_cost() {
        let calc = FeeCalculator::new(156);
        let cost = calc.total_cost(dec!(0.50), dec!(0.01));
        // 0.0039 + 0.01 = 0.0139
        assert_eq!(cost, dec!(0.0139));
    }

    #[test]
    fn test_net_ev_positive() {
        let calc = FeeCalculator::new(156);
        // estimated_prob=0.60, implied=0.50, half_spread=0.01
        // cost = 0.0039 + 0.01 = 0.0139
        // net_ev = 0.60 - 0.50 - 0.0139 = 0.0861
        let ev = calc.net_ev(0.60, dec!(0.50), dec!(0.01));
        assert!((ev - 0.0861).abs() < 1e-6, "expected ~0.0861, got {}", ev);
    }

    #[test]
    fn test_net_ev_negative_after_costs() {
        let calc = FeeCalculator::new(156);
        // Small edge eaten by costs
        // estimated_prob=0.51, implied=0.50, half_spread=0.01
        // cost = 0.0039 + 0.01 = 0.0139
        // net_ev = 0.51 - 0.50 - 0.0139 = -0.0039
        let ev = calc.net_ev(0.51, dec!(0.50), dec!(0.01));
        assert!(
            ev < 0.0,
            "small edge should be negative after costs, got {}",
            ev
        );
    }

    #[test]
    fn test_custom_fee_rate() {
        let calc = FeeCalculator::new(200);
        let fee = calc.taker_fee(dec!(0.50));
        // 0.50 * 0.50 * 200 / 10000 = 0.005
        assert_eq!(fee, dec!(0.005));
    }
}
