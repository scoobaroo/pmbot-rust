use statrs::distribution::{ContinuousCDF, Normal};

/// Black-Scholes probability that price exceeds strike at expiry.
///
/// P(S_T > K) = N(d2)
///
/// where d2 = (ln(S/K) + (r - 0.5σ²)T) / (σ√T)
///
/// # Arguments
/// * `spot` - Current price (S)
/// * `strike` - Strike price (K)
/// * `time_to_expiry` - Time to expiry in years (T)
/// * `volatility` - Annualized volatility (σ)
/// * `risk_free_rate` - Risk-free rate (r), typically 0.0 for crypto
pub fn prob_above_strike(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    volatility: f64,
    risk_free_rate: f64,
) -> f64 {
    if spot <= 0.0 || strike <= 0.0 || time_to_expiry <= 0.0 || volatility <= 0.0 {
        // Edge cases: if already expired or invalid, return simple comparison
        return if spot > strike { 1.0 } else { 0.0 };
    }

    let d2 = ((spot / strike).ln() + (risk_free_rate - 0.5 * volatility.powi(2)) * time_to_expiry)
        / (volatility * time_to_expiry.sqrt());

    let normal = Normal::new(0.0, 1.0).unwrap();
    normal.cdf(d2)
}

/// Probability that price stays below strike at expiry.
pub fn prob_below_strike(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    volatility: f64,
    risk_free_rate: f64,
) -> f64 {
    1.0 - prob_above_strike(spot, strike, time_to_expiry, volatility, risk_free_rate)
}

/// Time to expiry in years from now to target datetime.
pub fn time_to_expiry_years(expiry: chrono::DateTime<chrono::Utc>) -> f64 {
    let now = chrono::Utc::now();
    let duration = expiry - now;
    let secs = duration.num_seconds().max(0) as f64;
    secs / (365.25 * 24.0 * 3600.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atm_option_near_50_percent() {
        // At-the-money, reasonable vol, should be near 50%
        let prob = prob_above_strike(100.0, 100.0, 1.0, 0.5, 0.0);
        assert!(
            (prob - 0.5).abs() < 0.1,
            "ATM prob should be near 0.5, got {}",
            prob
        );
    }

    #[test]
    fn test_deep_itm() {
        // Spot >> Strike → high probability
        let prob = prob_above_strike(100.0, 50.0, 0.1, 0.3, 0.0);
        assert!(
            prob > 0.95,
            "deep ITM should have prob > 0.95, got {}",
            prob
        );
    }

    #[test]
    fn test_deep_otm() {
        // Spot << Strike → low probability
        let prob = prob_above_strike(50.0, 100.0, 0.1, 0.3, 0.0);
        assert!(
            prob < 0.05,
            "deep OTM should have prob < 0.05, got {}",
            prob
        );
    }

    #[test]
    fn test_higher_vol_widens_distribution() {
        let prob_low_vol = prob_above_strike(100.0, 110.0, 1.0, 0.1, 0.0);
        let prob_high_vol = prob_above_strike(100.0, 110.0, 1.0, 0.8, 0.0);
        assert!(
            prob_high_vol > prob_low_vol,
            "higher vol should increase OTM prob: low={}, high={}",
            prob_low_vol,
            prob_high_vol
        );
    }

    #[test]
    fn test_expired_option() {
        let prob = prob_above_strike(100.0, 90.0, 0.0, 0.5, 0.0);
        assert!(
            (prob - 1.0).abs() < f64::EPSILON,
            "expired ITM should be 1.0"
        );
    }
}
