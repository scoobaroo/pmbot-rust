use crate::types::order::{Fill, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tracing::info;

/// Tracks paper trade positions and P&L.
///
/// Groups fills by condition_id, matches buys/sells into round-trips,
/// and reports cumulative P&L with periodic logging.
/// Tracks an arb pair (buy YES + buy NO) for resolution.
struct ArbPair {
    yes_cost: Decimal,
    no_cost: Decimal,
    size: Decimal,
}

pub struct PaperTracker {
    /// Starting capital for paper trading
    starting_capital: Decimal,
    /// Open position per condition_id: (side, avg_price, size, total_cost)
    positions: HashMap<String, PaperPosition>,
    /// Realized P&L from closed round-trips
    realized_pnl: Decimal,
    /// Total fees paid
    total_fees: Decimal,
    /// Total fills processed
    total_fills: u64,
    /// Total round-trips completed (wins + losses)
    total_round_trips: u64,
    /// Winning round-trips
    winning_trades: u64,
    /// Total capital deployed across all trades
    total_volume: Decimal,
    /// Last report time
    last_report: std::time::Instant,
    /// Open arb pairs keyed by condition_id
    arb_pairs: HashMap<String, ArbPair>,
    /// Partial arb legs waiting for the second fill: condition_id → (side, price, size)
    arb_pending: HashMap<String, (Side, Decimal, Decimal)>,
}

struct PaperPosition {
    side: Side,
    avg_price: Decimal,
    size: Decimal,
    /// For UpDown markets: (window_end_ts, underlying_symbol, start_price, signal_side)
    /// signal_side: Buy = Up token, Sell = Down token
    updown_meta: Option<(i64, String, f64, Side)>,
}

impl Default for PaperTracker {
    fn default() -> Self {
        Self::new(Decimal::from(100))
    }
}

impl PaperTracker {
    pub fn new(starting_capital: Decimal) -> Self {
        info!(starting_capital = %starting_capital, "paper tracker initialized");
        Self {
            starting_capital,
            positions: HashMap::new(),
            realized_pnl: Decimal::ZERO,
            total_fees: Decimal::ZERO,
            total_fills: 0,
            total_round_trips: 0,
            winning_trades: 0,
            total_volume: Decimal::ZERO,
            last_report: std::time::Instant::now(),
            arb_pairs: HashMap::new(),
            arb_pending: HashMap::new(),
        }
    }

    /// Current portfolio value = starting capital + realized P&L
    pub fn portfolio_value(&self) -> Decimal {
        self.starting_capital + self.realized_pnl
    }

    /// Return on starting capital as a percentage
    pub fn return_pct(&self) -> Decimal {
        if self.starting_capital > Decimal::ZERO {
            (self.realized_pnl / self.starting_capital) * Decimal::from(100)
        } else {
            Decimal::ZERO
        }
    }

    pub fn starting_capital(&self) -> Decimal {
        self.starting_capital
    }

    /// Process a fill and update P&L tracking.
    pub fn record_fill(&mut self, condition_id: &str, fill: &Fill) {
        self.total_fills += 1;
        self.total_fees += fill.fee;
        self.total_volume += fill.price * fill.size;

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
                let net_pnl = pnl - fill.fee;
                self.realized_pnl += net_pnl;
                self.total_round_trips += 1;
                if net_pnl > Decimal::ZERO {
                    self.winning_trades += 1;
                }

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
                                updown_meta: None,
                            },
                        );
                    }
                }

                info!(
                    condition_id = condition_id,
                    pnl = %pnl,
                    net_pnl = %net_pnl,
                    realized_total = %self.realized_pnl,
                    portfolio = %self.portfolio_value(),
                    return_pct = %format!("{:.2}%", self.return_pct()),
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
                    updown_meta: None,
                },
            );
        }
    }

    /// Record one leg of an arb fill. When both legs arrive, registers the pair.
    pub fn record_arb_fill(&mut self, condition_id: &str, side: Side, price: Decimal, size: Decimal) {
        self.total_fills += 1;
        self.total_volume += price * size;
        let fee = size * price * Decimal::new(2, 4); // 0.02% fee
        self.total_fees += fee;

        if let Some((first_side, first_price, first_size)) = self.arb_pending.remove(condition_id) {
            // Second leg arrived — register the complete pair
            let (yes_cost, no_cost) = if first_side == Side::Buy {
                (first_price, price) // first was YES, this is NO
            } else {
                (price, first_price) // first was NO, this is YES
            };
            let pair_size = first_size.min(size);

            info!(
                condition_id = condition_id,
                yes_cost = %yes_cost,
                no_cost = %no_cost,
                total_cost = %(yes_cost + no_cost),
                size = %pair_size,
                "arb pair registered"
            );

            self.arb_pairs.insert(
                condition_id.to_string(),
                ArbPair {
                    yes_cost,
                    no_cost,
                    size: pair_size,
                },
            );
        } else {
            // First leg — stash until second arrives
            self.arb_pending
                .insert(condition_id.to_string(), (side, price, size));
        }
    }

    /// Resolve arb pairs for expired markets.
    /// P&L = size × ($1 - yes_cost - no_cost) regardless of outcome.
    pub fn resolve_arb_pairs(&mut self, expired_condition_ids: &[String]) {
        for cid in expired_condition_ids {
            if let Some(pair) = self.arb_pairs.remove(cid) {
                let pnl = pair.size * (Decimal::ONE - pair.yes_cost - pair.no_cost);
                self.realized_pnl += pnl;
                self.total_round_trips += 1;
                if pnl > Decimal::ZERO {
                    self.winning_trades += 1;
                }

                info!(
                    condition_id = %cid,
                    yes_cost = %pair.yes_cost,
                    no_cost = %pair.no_cost,
                    size = %pair.size,
                    pnl = %pnl,
                    portfolio = %self.portfolio_value(),
                    "arb pair resolved"
                );
            }
            // Clean up any orphaned pending legs
            self.arb_pending.remove(cid);
        }
    }

    /// Log a periodic summary if enough time has passed.
    pub fn maybe_report(&mut self) {
        if self.last_report.elapsed().as_secs() < 300 {
            return; // every 5 minutes
        }

        let open_count = self.positions.len();
        let open_notional: Decimal = self.positions.values().map(|p| p.avg_price * p.size).sum();
        let win_rate = if self.total_round_trips > 0 {
            format!(
                "{:.1}%",
                (self.winning_trades as f64 / self.total_round_trips as f64) * 100.0
            )
        } else {
            "n/a".to_string()
        };

        info!(
            starting_capital = %self.starting_capital,
            portfolio_value = %self.portfolio_value(),
            return_pct = %format!("{:.2}%", self.return_pct()),
            realized_pnl = %self.realized_pnl,
            total_fees = %self.total_fees,
            total_fills = self.total_fills,
            round_trips = self.total_round_trips,
            win_rate = %win_rate,
            open_positions = open_count,
            open_notional = %open_notional,
            total_volume = %self.total_volume,
            "paper trading summary"
        );

        self.last_report = std::time::Instant::now();
    }

    /// Resolve an expired binary market position.
    ///
    /// For Up/Down markets: the token resolves to $1 if it won, $0 if it lost.
    /// `won` = true means the token we bought pays $1.
    pub fn resolve_market(&mut self, condition_id: &str, won: bool) {
        if let Some(pos) = self.positions.remove(condition_id) {
            let resolve_price = if won { Decimal::ONE } else { Decimal::ZERO };
            let pnl = match pos.side {
                Side::Buy => (resolve_price - pos.avg_price) * pos.size,
                Side::Sell => (pos.avg_price - resolve_price) * pos.size,
            };
            self.realized_pnl += pnl;
            self.total_round_trips += 1;
            if pnl > Decimal::ZERO {
                self.winning_trades += 1;
            }

            info!(
                condition_id = condition_id,
                side = %pos.side,
                entry_price = %pos.avg_price,
                resolve_price = %resolve_price,
                won = won,
                pnl = %pnl,
                portfolio = %self.portfolio_value(),
                return_pct = %format!("{:.2}%", self.return_pct()),
                "market resolved"
            );
        }
    }

    /// Attach UpDown resolution metadata to an open position.
    /// `signal_side`: Buy = we hold the Up token, Sell = we hold the Down token.
    pub fn set_updown_meta(
        &mut self,
        condition_id: &str,
        window_end_ts: i64,
        symbol: &str,
        start_price: f64,
        signal_side: Side,
    ) {
        if let Some(pos) = self.positions.get_mut(condition_id) {
            pos.updown_meta = Some((window_end_ts, symbol.to_string(), start_price, signal_side));
        }
    }

    /// Return condition_ids of expired UpDown positions (window_end_ts < now).
    pub fn expired_updown_condition_ids(&self) -> Vec<String> {
        let now = chrono::Utc::now().timestamp();
        self.positions
            .iter()
            .filter_map(|(cid, pos)| {
                let (window_end_ts, _, _, _) = pos.updown_meta.as_ref()?;
                if now > *window_end_ts {
                    Some(cid.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Resolve an UpDown position using the actual Polymarket outcome.
    /// `up_won`: true if the Up token resolved to $1.
    /// Uses the stored signal_side to determine if our token won:
    ///   signal_side == Buy (hold Up) → won = up_won
    ///   signal_side == Sell (hold Down) → won = !up_won
    pub fn resolve_by_outcome(&mut self, condition_id: &str, up_won: bool) {
        let pos = match self.positions.remove(condition_id) {
            Some(p) => p,
            None => return,
        };

        let signal_side = pos
            .updown_meta
            .as_ref()
            .map(|(_, _, _, side)| *side)
            .unwrap_or(pos.side);

        let won = match signal_side {
            Side::Buy => up_won,      // We hold Up token
            Side::Sell => !up_won,     // We hold Down token
        };

        let resolve_price = if won { Decimal::ONE } else { Decimal::ZERO };
        let pnl = (resolve_price - pos.avg_price) * pos.size;

        self.realized_pnl += pnl;
        self.total_round_trips += 1;
        if pnl > Decimal::ZERO {
            self.winning_trades += 1;
        }

        info!(
            condition_id = condition_id,
            side = %signal_side,
            entry_price = %pos.avg_price,
            won = won,
            up_won = up_won,
            pnl = %pnl,
            portfolio = %self.portfolio_value(),
            return_pct = %format!("{:.2}%", self.return_pct()),
            "updown market resolved via Polymarket oracle"
        );
    }

    /// Resolve ALL open UpDown positions using the latest known underlying prices.
    /// Used in backtest mode where the Gamma API is unavailable.
    /// Determines up_won by comparing start_price to latest_price for each symbol.
    pub fn resolve_all_by_last_price(&mut self, latest_prices: &HashMap<String, f64>) {
        let condition_ids: Vec<String> = self.positions.keys().cloned().collect();

        // Resolve arb pairs first
        self.resolve_arb_pairs(&condition_ids);

        // Collect UpDown positions to resolve
        let mut to_resolve: Vec<(String, bool)> = Vec::new();
        for cid in &condition_ids {
            if let Some(pos) = self.positions.get(cid) {
                if let Some((_, ref symbol, start_price, _)) = pos.updown_meta {
                    if let Some(&latest_price) = latest_prices.get(symbol) {
                        let up_won = latest_price > start_price;
                        to_resolve.push((cid.clone(), up_won));
                    }
                }
            }
        }

        for (cid, up_won) in to_resolve {
            self.resolve_by_outcome(&cid, up_won);
        }
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

    /// Return condition IDs of all open positions.
    pub fn open_condition_ids(&self) -> Vec<String> {
        self.positions.keys().cloned().collect()
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
        let mut tracker = PaperTracker::new(dec!(100));
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
        let mut tracker = PaperTracker::new(dec!(100));
        // Buy at 0.60, sell at 0.40 → loss
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.60, 100.0));
        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.40, 100.0));
        assert!(tracker.realized_pnl() < Decimal::ZERO, "should be a loss");
    }

    #[test]
    fn test_partial_close() {
        let mut tracker = PaperTracker::new(dec!(100));
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.50, 100.0));
        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.60, 50.0));
        // Still have 50 units open
        assert_eq!(tracker.open_position_count(), 1);
        // Realized partial PnL = (0.60 - 0.50) * 50 - 0.01 = 4.99
        assert!(tracker.realized_pnl() > Decimal::ZERO);
    }

    #[test]
    fn test_multiple_positions() {
        let mut tracker = PaperTracker::new(dec!(100));
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.40, 100.0));
        tracker.record_fill("cond-2", &make_fill(Side::Sell, 0.70, 50.0));
        assert_eq!(tracker.open_position_count(), 2);
    }

    #[test]
    fn test_fee_tracking() {
        let mut tracker = PaperTracker::new(dec!(100));
        tracker.record_fill("cond-1", &make_fill(Side::Buy, 0.50, 100.0));
        tracker.record_fill("cond-1", &make_fill(Side::Sell, 0.50, 100.0));
        // Even with zero price movement, fees should make us negative
        assert_eq!(tracker.total_fees(), dec!(0.02));
    }

    #[test]
    fn test_updown_resolve_win() {
        let mut tracker = PaperTracker::new(dec!(100));
        // Buy Up token at 0.45 (100 tokens)
        tracker.record_fill("updown-1", &make_fill(Side::Buy, 0.45, 100.0));
        assert_eq!(tracker.open_position_count(), 1);

        // Attach UpDown metadata: window already expired, BTC start = 67000
        tracker.set_updown_meta("updown-1", 0, "BTC-USD", 67000.0, Side::Buy);

        // Up won on Polymarket → our Up token resolves at $1
        tracker.resolve_by_outcome("updown-1", true);

        assert_eq!(tracker.open_position_count(), 0);
        // PnL = (1.0 - 0.45) * 100 = 55.0
        assert!(tracker.realized_pnl() > dec!(50));
    }

    #[test]
    fn test_updown_resolve_loss() {
        let mut tracker = PaperTracker::new(dec!(100));
        // Buy Up token at 0.55 (100 tokens)
        tracker.record_fill("updown-1", &make_fill(Side::Buy, 0.55, 100.0));

        // Window expired, BTC start = 67000
        tracker.set_updown_meta("updown-1", 0, "BTC-USD", 67000.0, Side::Buy);

        // Down won on Polymarket → our Up token resolves at $0
        tracker.resolve_by_outcome("updown-1", false);

        assert_eq!(tracker.open_position_count(), 0);
        // PnL = (0.0 - 0.55) * 100 = -55.0
        assert!(tracker.realized_pnl() < dec!(-50));
    }

    #[test]
    fn test_updown_not_expired_yet() {
        let mut tracker = PaperTracker::new(dec!(100));
        tracker.record_fill("updown-1", &make_fill(Side::Buy, 0.50, 100.0));

        // Window ends far in the future
        let future_ts = Utc::now().timestamp() + 3600;
        tracker.set_updown_meta("updown-1", future_ts, "BTC-USD", 67000.0, Side::Buy);

        // expired_updown_condition_ids should return empty
        let expired = tracker.expired_updown_condition_ids();
        assert!(expired.is_empty());

        // Position should still be open
        assert_eq!(tracker.open_position_count(), 1);
        assert_eq!(tracker.realized_pnl(), Decimal::ZERO);
    }

    #[test]
    fn test_updown_down_token_win() {
        let mut tracker = PaperTracker::new(dec!(100));
        // Side::Sell = bought the Down token at 0.40
        tracker.record_fill("updown-1", &make_fill(Side::Sell, 0.40, 100.0));

        // Window expired, ETH start = 3500
        tracker.set_updown_meta("updown-1", 0, "ETH-USD", 3500.0, Side::Sell);

        // Down won on Polymarket (up_won = false) → our Down token resolves at $1
        tracker.resolve_by_outcome("updown-1", false);

        assert_eq!(tracker.open_position_count(), 0);
        // PnL = (1.0 - 0.40) * 100 = 60.0
        assert!(tracker.realized_pnl() > dec!(50));
    }

    #[test]
    fn test_expired_updown_condition_ids() {
        let mut tracker = PaperTracker::new(dec!(100));
        tracker.record_fill("expired-1", &make_fill(Side::Buy, 0.50, 100.0));
        tracker.record_fill("not-expired", &make_fill(Side::Buy, 0.50, 100.0));
        tracker.record_fill("expired-2", &make_fill(Side::Sell, 0.40, 50.0));

        // expired-1: window already ended
        tracker.set_updown_meta("expired-1", 0, "BTC-USD", 67000.0, Side::Buy);
        // not-expired: window ends in the future
        let future_ts = Utc::now().timestamp() + 3600;
        tracker.set_updown_meta("not-expired", future_ts, "ETH-USD", 3500.0, Side::Buy);
        // expired-2: window already ended
        tracker.set_updown_meta("expired-2", 0, "SOL-USD", 150.0, Side::Sell);

        let mut expired = tracker.expired_updown_condition_ids();
        expired.sort();
        assert_eq!(expired, vec!["expired-1", "expired-2"]);
    }
}
