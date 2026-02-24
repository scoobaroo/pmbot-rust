use crate::types::order::Fill;
use rust_decimal::Decimal;

/// Backtest performance report.
#[derive(Debug)]
pub struct BacktestReport {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub total_pnl: Decimal,
    pub total_fees: Decimal,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub win_rate: f64,
    pub trade_log: Vec<TradeRecord>,
}

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub order_id: String,
    pub side: String,
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
    pub pnl: Decimal,
    pub timestamp: String,
}

impl BacktestReport {
    pub fn from_fills(
        fills: &[Fill],
        _final_prices: &std::collections::HashMap<String, Decimal>,
    ) -> Self {
        let total_trades = fills.len();
        let total_fees: Decimal = fills.iter().map(|f| f.fee).sum();

        // Simple PnL calculation: for completed round-trips
        let pnl_series: Vec<f64> = Vec::new();
        let cumulative_pnl = Decimal::ZERO;
        let max_drawdown = 0.0f64;
        let winning = 0usize;
        let losing = 0usize;

        let trade_log: Vec<TradeRecord> = fills
            .iter()
            .map(|f| {
                let pnl = Decimal::ZERO; // Simplified: full PnL requires matching buys/sells
                TradeRecord {
                    order_id: f.order_id.clone(),
                    side: format!("{}", f.side),
                    price: f.price,
                    size: f.size,
                    fee: f.fee,
                    pnl,
                    timestamp: f.timestamp.to_rfc3339(),
                }
            })
            .collect();

        // Sharpe ratio from PnL series
        let sharpe = if pnl_series.len() > 1 {
            let mean = pnl_series.iter().sum::<f64>() / pnl_series.len() as f64;
            let variance = pnl_series.iter().map(|r| (r - mean).powi(2)).sum::<f64>()
                / (pnl_series.len() - 1) as f64;
            let std_dev = variance.sqrt();
            if std_dev > 0.0 {
                mean / std_dev * (252.0_f64).sqrt() // annualized
            } else {
                0.0
            }
        } else {
            0.0
        };

        let win_rate = if total_trades > 0 {
            winning as f64 / total_trades as f64
        } else {
            0.0
        };

        Self {
            total_trades,
            winning_trades: winning,
            losing_trades: losing,
            total_pnl: cumulative_pnl - total_fees,
            total_fees,
            max_drawdown_pct: max_drawdown,
            sharpe_ratio: sharpe,
            win_rate,
            trade_log,
        }
    }

    pub fn print_summary(&self) {
        let separator = "=".repeat(60);
        println!("\n{}", separator);
        println!("         BACKTEST REPORT");
        println!("{}", separator);
        println!("Total Trades:      {}", self.total_trades);
        println!("Winning Trades:    {}", self.winning_trades);
        println!("Losing Trades:     {}", self.losing_trades);
        println!("Win Rate:          {:.2}%", self.win_rate * 100.0);
        println!("Total PnL:         ${}", self.total_pnl);
        println!("Total Fees:        ${}", self.total_fees);
        println!("Max Drawdown:      {:.2}%", self.max_drawdown_pct * 100.0);
        println!("Sharpe Ratio:      {:.3}", self.sharpe_ratio);
        println!("{}\n", separator);

        if !self.trade_log.is_empty() {
            println!("Recent Trades:");
            println!(
                "{:<36} {:>4} {:>10} {:>10} {:>10}",
                "Order ID", "Side", "Price", "Size", "Fee"
            );
            for trade in self.trade_log.iter().take(20) {
                println!(
                    "{:<36} {:>4} {:>10} {:>10} {:>10}",
                    &trade.order_id[..trade.order_id.len().min(36)],
                    trade.side,
                    trade.price,
                    trade.size,
                    trade.fee
                );
            }
        }
    }
}
