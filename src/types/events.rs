use super::candle::Candle;
use super::market::{AggregatedPrice, Exchange, MarketTick, PolymarketMarket};
use super::order::{Fill, Order, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// Events produced by exchange WebSocket feeds.
#[derive(Debug, Clone)]
pub enum ExchangeEvent {
    Tick(MarketTick),
    Connected(Exchange),
    Disconnected(Exchange),
}

/// Events produced by the aggregator.
#[derive(Debug, Clone)]
pub enum AggregatorEvent {
    PriceUpdate(AggregatedPrice),
    CandleComplete(Candle),
    StaleFeed {
        exchange: Exchange,
        symbol: String,
        last_seen: DateTime<Utc>,
    },
}

/// What the signal targets for execution.
#[derive(Debug, Clone)]
pub enum TradeTarget {
    Polymarket(PolymarketMarket),
    Spot {
        symbol: String,
        exchange: Option<Exchange>,
    },
}

/// Strategy-specific metadata attached to a signal.
#[derive(Debug, Clone)]
pub enum SignalMetadata {
    BlackScholes {
        estimated_prob: f64,
        implied_prob: f64,
        edge: f64,
        kelly_fraction: f64,
    },
    MACrossover {
        fast_ema: f64,
        slow_ema: f64,
        timeframe: String,
    },
}

/// Trading signals from the strategy engine.
#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub target: TradeTarget,
    pub side: super::order::Side,
    pub size_usd: Decimal,
    pub confidence: f64,
    pub price: Decimal,
    pub metadata: SignalMetadata,
    pub timestamp: DateTime<Utc>,
}

/// Feedback from execution engine to strategy.
#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    OrderPlaced(Order),
    OrderFilled(Fill),
    OrderCancelled { order_id: String, reason: String },
    OrderFailed { order_id: String, error: String },
    PositionUpdate(Position),
}

/// Updates from Polymarket market scanner.
#[derive(Debug, Clone)]
pub enum PolymarketUpdate {
    MarketsDiscovered(Vec<PolymarketMarket>),
    PriceUpdate {
        condition_id: String,
        yes_price: Decimal,
        no_price: Decimal,
    },
}
