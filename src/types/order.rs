use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Decimal,
    pub size: Decimal,
    pub filled_size: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Order {
    pub fn new(
        condition_id: String,
        token_id: String,
        side: Side,
        order_type: OrderType,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            condition_id,
            token_id,
            side,
            order_type,
            price,
            size,
            filled_size: Decimal::ZERO,
            status: OrderStatus::Pending,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn remaining(&self) -> Decimal {
        self.size - self.filled_size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub order_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub side: Side,
    pub timestamp: DateTime<Utc>,
    pub fee: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    pub size: Decimal,
    pub avg_entry_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
}

impl Position {
    pub fn notional(&self) -> Decimal {
        self.size * self.avg_entry_price
    }
}
