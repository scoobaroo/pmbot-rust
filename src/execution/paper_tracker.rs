use crate::types::order::{Fill, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tracing::info;

/// Tracks paper trade positions and P&L.
///
/// Groups fills by condition_id, matches buys/sells into round-trips,
/// and reports cumulative P&L with periodic logging.
pub struct PaperTracker {
    /// Open position per condition_id: (side, avg_price, size, total_cost)
    positions: HashMap<String, PaperPosition>,
    /// Realized P&L from closed round-trips
    realized_pnl: Decimal,
    /// Total fees paid
    total_fees: Decimal,
    /// Total fills processed
    total_fills: u64,
    /// Last report time
    last_report: std::time::Instant,
}

struct PaperPosition {
    side: Side,
    avg_price: Decimal,
    size: Decimal,
}

impl Default for PaperTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PaperTracker {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
            realized_pnl: Decimal::ZERO,
            total_fees: Decimal::ZERO,
            total_fills: 0,
            last_report: std::time::Instant::now(),
        }
    }

    /// Process a fill and update P&L tracking.
    pub fn record_fill(&mut self, condition_id: &str, fill: &Fill) {
        self.total_fills += 1;
        self.total_fees += fill.fee;

        if let Some(pos) = self.positions.get_mut(condition_id) {
            if pos.side == fill.side {
                // Adding to position — average in
                let total_cost = pos.avg_price * pos.size + fill.price * fill.size;
                pos.size += fill.size;
                if pos.size > Decimal::ZERO {
                    pos.avg_price = total_cost / pos.size;
                }
            } else {
                // Closing position (partially or fully)
                let close_size = fill.size.min(pos.size);
                let pnl = match pos.side {
                    Side::Buy => (fill.price - pos.avg_price) * close_size,
                    Side::Sell => (pos.avg_price - fill.price) * close_size,
                };
                self.realized_pnl += pnl - fill.fee;

                pos.size -= close_size;
                if pos.size <= Decimal::ZERO {
                    self.positions.remove(condition_id);

                    // If fill size exceeds old position, open new position in opposite direction
                    let remainder = fill.size - close_size;
                    if remainder > Decimal::ZERO {
                        self.positions.insert(
                            condition_id.to_string(),
                            PaperPosition {
                                side: fill.side,
                                avg_price: fill.price,
                                size: remainder,
                            },
                        );
                    }
                }

                info!(
                    condition_id = condition_id,
                    pnl = %pnl,
                    realized_total = %self.realized_pnl,
                    "paper trade closed"
                );
            }
        } else {
            // New position
            self.positions.insert(
                condition_id.to_string(),
                PaperPosition {
                    side: fill.side,
                    avg_price: fill.price,
                    size: fill.size,
                },
            );
        }
    }

    /// Log a periodic summary if enough time has passed.
    pub fn maybe_report(&mut self) {
        if self.last_report.elapsed().as_secs() < 300 {
            return; // every 5 minutes
        }

        let open_count = self.positions.len();
        let open_notional: Decimal = self.positions.values().map(|p| p.avg_price * p.size).sum();

        info!(
            total_fills = self.total_fills,
            open_positions = open_count,
            open_notional = %open_notional,
            realized_pnl = %self.realized_pnl,
            total_fees = %self.total_fees,
            net_pnl = %(self.realized_pnl),
            "paper trading summary"
        );

        self.last_report = std::time::Instant::now();
    }

    pub fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    pub fn total_fees(&self) -> Decimal {
        self.total_fees
    }

    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn make_fill(side: Side, price: f64, size: f64) -> Fill {
        Fill {
            order_id: "test".into(),
            price: Decimal::from_f64_retain(price).unwrap(),
            size: Decimal::from_f64_retain(size).unwrap(),
            side,
            timestamp: Utc::now(),
            fee: dec!(0.01),
        }
    }

    #[test]
    fn test_round_trip_profit() {
        let mut tracker = PaperTracker::new();
        // Buy at 0.40, sell at 0.60 → profit
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.40, 100.0));
        assert_eq!(tracker.open_position_count(), 1);

        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.60, 100.0));
        assert_eq!(tracker.open_position_count(), 0);
        // PnL = (0.60 - 0.40) * 100 - 0.01 fee = 19.99
        assert!(
            tracker.realized_pnl() > Decimal::ZERO,
            "should be profitable"
        );
    }

    #[test]
    fn test_round_trip_loss() {
        let mut tracker = PaperTracker::new();
        // Buy at 0.60, sell at 0.40 → loss
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.60, 100.0));
        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.40, 100.0));
        assert!(tracker.realized_pnl() < Decimal::ZERO, "should be a loss");
    }

    #[test]
    fn test_partial_close() {
        let mut tracker = PaperTracker::new();
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.50, 100.0));
        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.60, 50.0));
        // Still have 50 units open
        assert_eq!(tracker.open_position_count(), 1);
        // Realized partial PnL = (0.60 - 0.50) * 50 - 0.01 = 4.99
        assert!(tracker.realized_pnl() > Decimal::ZERO);
    }

    #[test]
    fn test_multiple_positions() {
        let mut tracker = PaperTracker::new();
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.40, 100.0));
        tracker.record_fill("cond-2", &make_fill(Side::Sell, 0.70, 50.0));
        assert_eq!(tracker.open_position_count(), 2);
    }

    #[test]
    fn test_fee_tracking() {
        let mut tracker = PaperTracker::new();
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.50, 100.0));
        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.50, 100.0));
        // Even with zero price movement, fees should make us negative
        assert_eq!(tracker.total_fees(), dec!(0.02));
    }
}
