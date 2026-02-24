use rust_decimal::Decimal;
use std::str::FromStr;

/// Kelly Criterion position sizing.
///
/// f* = (b*p - q) / b
///
/// where:
///   b = net fractional odds (payout per $1 risked)
///   p = probability of winning
///   q = probability of losing (1 - p)
///
/// Returns the fraction of bankroll to bet, capped at `max_fraction` (default: half-Kelly).
pub fn kelly_fraction(estimated_prob: f64, implied_prob: f64, max_fraction: f64) -> f64 {
    if estimated_prob <= 0.0 || estimated_prob >= 1.0 || implied_prob <= 0.0 || implied_prob >= 1.0
    {
        return 0.0;
    }

    // In a binary outcome market:
    // If we buy YES at price = implied_prob, payout is 1.0
    // Net odds b = (1.0 - implied_prob) / implied_prob
    let b = (1.0 - implied_prob) / implied_prob;
    let p = estimated_prob;
    let q = 1.0 - p;

    let f = (b * p - q) / b;

    if f <= 0.0 {
        return 0.0; // No edge, don't bet
    }

    // Cap at max_fraction (half-Kelly by default)
    f.min(max_fraction)
}

/// Compute position size in USD given kelly fraction and bankroll.
pub fn position_size_usd(
    kelly_frac: f64,
    bankroll_usd: Decimal,
    max_position_usd: Decimal,
) -> Decimal {
    if kelly_frac <= 0.0 {
        return Decimal::ZERO;
    }

    let frac = Decimal::from_str(&format!("{:.6}", kelly_frac)).unwrap_or(Decimal::ZERO);
    let raw_size = bankroll_usd * frac;

    raw_size.min(max_position_usd)
}

/// Calculate edge: estimated_prob - implied_prob (positive = we think YES is underpriced)
pub fn edge(estimated_prob: f64, implied_prob: f64) -> f64 {
    estimated_prob - implied_prob
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_edge() {
        // Our estimate equals market → no bet
        let f = kelly_fraction(0.5, 0.5, 0.5);
        assert!((f).abs() < 1e-10, "no edge should produce zero fraction");
    }

    #[test]
    fn test_positive_edge() {
        // We think 60% but market says 40%
        let f = kelly_fraction(0.6, 0.4, 1.0);
        assert!(f > 0.0, "positive edge should produce positive fraction");
        // Full Kelly: f = (1.5 * 0.6 - 0.4) / 1.5 = 0.333...
        assert!((f - 1.0 / 3.0).abs() < 1e-6, "expected ~0.333, got {}", f);
    }

    #[test]
    fn test_half_kelly_cap() {
        let f = kelly_fraction(0.6, 0.4, 0.5);
        assert!(f <= 0.5, "should be capped at half-Kelly max, got {}", f);
    }

    #[test]
    fn test_negative_edge() {
        // We think 30% but market says 60% → no bet
        let f = kelly_fraction(0.3, 0.6, 0.5);
        assert!((f).abs() < 1e-10, "negative edge should produce zero");
    }

    #[test]
    fn test_position_size() {
        let size = position_size_usd(0.1, Decimal::from(10000), Decimal::from(1000));
        assert_eq!(size, Decimal::from(1000)); // capped at max
    }
}
