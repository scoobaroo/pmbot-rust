use crate::types::order::{Fill, Order, OrderStatus, Side};
use chrono::Utc;
use rust_decimal::Decimal;

/// Simulated exchange for backtesting.
pub struct SimExchange {
    slippage_bps: Decimal,
    fee_bps: Decimal,
}

impl SimExchange {
    pub fn new(slippage_bps: u32, fee_bps: u32) -> Self {
        Self {
            slippage_bps: Decimal::from(slippage_bps) / Decimal::from(10000),
            fee_bps: Decimal::from(fee_bps) / Decimal::from(10000),
        }
    }

    /// Simulate filling an order with slippage and fees.
    pub fn fill_order(&self, order: &mut Order) -> Fill {
        let slippage = order.price * self.slippage_bps;
        let fill_price = match order.side {
            Side::Buy => order.price + slippage,
            Side::Sell => order.price - slippage,
        };

        let fee = order.size * fill_price * self.fee_bps;

        order.status = OrderStatus::Filled;
        order.filled_size = order.size;
        order.updated_at = Utc::now();

        Fill {
            order_id: order.id.clone(),
            price: fill_price,
            size: order.size,
            side: order.side,
            timestamp: Utc::now(),
            fee,
        }
    }
}

impl Default for SimExchange {
    fn default() -> Self {
        Self::new(5, 20) // 5 bps slippage, 20 bps fee
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::order::OrderType;

    #[test]
    fn test_fill_with_slippage() {
        let sim = SimExchange::new(10, 0); // 10 bps slippage, no fee
        let mut order = Order::new(
            "cond1".to_string(),
            "tok1".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(50, 2), // 0.50
            Decimal::from(100),
        );

        let fill = sim.fill_order(&mut order);
        assert!(
            fill.price > Decimal::new(50, 2),
            "buy should have positive slippage"
        );
        assert_eq!(order.status, OrderStatus::Filled);
    }
}
