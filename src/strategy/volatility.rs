use chrono::{DateTime, Utc};
use std::collections::VecDeque;

/// Tracks rolling log-return volatility over a configurable window.
pub struct VolatilityTracker {
    /// (price, timestamp) history
    prices: VecDeque<(f64, DateTime<Utc>)>,
    window_hours: u64,
}

impl VolatilityTracker {
    pub fn new(window_hours: u64) -> Self {
        Self {
            prices: VecDeque::new(),
            window_hours,
        }
    }

    pub fn add_price(&mut self, price: f64, timestamp: DateTime<Utc>) {
        if price <= 0.0 {
            return;
        }
        self.prices.push_back((price, timestamp));
        self.trim_old(timestamp);
    }

    /// Annualized volatility: σ_daily * sqrt(365)
    /// σ_daily estimated from rolling log returns.
    pub fn annualized_volatility(&self) -> f64 {
        let returns = self.log_returns();
        if returns.len() < 2 {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;

        let std_dev = variance.sqrt();

        // Estimate periods per year based on average interval
        let periods_per_year = self.estimate_annual_periods();
        std_dev * periods_per_year.sqrt()
    }

    /// Raw log returns for the window.
    pub fn log_returns(&self) -> Vec<f64> {
        if self.prices.len() < 2 {
            return vec![];
        }
        self.prices
            .iter()
            .zip(self.prices.iter().skip(1))
            .map(|((p1, _), (p2, _))| (p2 / p1).ln())
            .collect()
    }

    fn trim_old(&mut self, now: DateTime<Utc>) {
        let cutoff = now - chrono::Duration::hours(self.window_hours as i64);
        while let Some((_, ts)) = self.prices.front() {
            if *ts < cutoff {
                self.prices.pop_front();
            } else {
                break;
            }
        }
    }

    fn estimate_annual_periods(&self) -> f64 {
        if self.prices.len() < 2 {
            return 365.0 * 24.0; // default: hourly
        }
        let first_ts = self.prices.front().unwrap().1;
        let last_ts = self.prices.back().unwrap().1;
        let duration_secs = (last_ts - first_ts).num_seconds().max(1) as f64;
        let n = (self.prices.len() - 1) as f64;
        let avg_interval_secs = duration_secs / n;

        // Periods in a year (365.25 days)
        let secs_per_year = 365.25 * 24.0 * 3600.0;
        secs_per_year / avg_interval_secs
    }

    pub fn sample_count(&self) -> usize {
        self.prices.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_volatility_with_constant_prices() {
        let mut tracker = VolatilityTracker::new(24);
        let base = Utc::now();
        for i in 0..100 {
            tracker.add_price(100.0, base + Duration::minutes(i));
        }
        let vol = tracker.annualized_volatility();
        assert!(
            vol < 0.001,
            "constant prices should have ~zero vol, got {}",
            vol
        );
    }

    #[test]
    fn test_volatility_with_varying_prices() {
        let mut tracker = VolatilityTracker::new(24);
        let base = Utc::now();
        let prices = [
            100.0, 102.0, 98.0, 105.0, 97.0, 103.0, 101.0, 99.0, 104.0, 96.0,
        ];
        for (i, p) in prices.iter().enumerate() {
            tracker.add_price(*p, base + Duration::minutes(i as i64));
        }
        let vol = tracker.annualized_volatility();
        assert!(vol > 0.0, "varying prices should have positive vol");
    }

    #[test]
    fn test_zero_price_ignored() {
        let mut tracker = VolatilityTracker::new(24);
        tracker.add_price(0.0, Utc::now());
        assert_eq!(tracker.sample_count(), 0);
    }
}
