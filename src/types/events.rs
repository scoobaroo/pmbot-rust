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

/// ML prediction from the Python prediction server.
#[derive(Debug, Clone)]
pub struct MlPrediction {
    pub symbol: String,
    pub direction: MlDirection,
    pub confidence: f64,
    pub model: String,
    pub latency_ms: f64,
    pub timestamp: DateTime<Utc>,
}

/// ML predicted direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MlDirection {
    Up,
    Down,
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
    BollingerBands {
        sma: f64,
        upper: f64,
        lower: f64,
        close: f64,
        timeframe: String,
    },
    Unified {
        estimated_prob: f64,
        implied_prob: f64,
        raw_edge: f64,
        adjusted_edge: f64,
        confluence_multiplier: f64,
        bb_score: f64,
        ma_score: f64,
        book_score: f64,
        ml_score: f64,
        kelly_fraction: f64,
        spot: f64,
    },
    UpDown {
        estimated_prob_up: f64,
        implied_prob_up: f64,
        start_price: f64,
        spot: f64,
        time_remaining_secs: f64,
        raw_edge: f64,
        adjusted_edge: f64,
        confluence_multiplier: f64,
        bb_score: f64,
        ma_score: f64,
        book_score: f64,
        ml_score: f64,
        kelly_fraction: f64,
    },
    Arbitrage {
        yes_ask: Decimal,
        no_ask: Decimal,
        total_cost: Decimal,
        profit_pct: Decimal,
    },
    HedgedLp {
        hedge_profit: f64,
        net_hedge_profit: f64,
        reward_score: f64,
        expected_return: f64,
        kelly_fraction: f64,
        yes_ask: f64,
        no_ask: f64,
        is_hedge: bool,
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
    /// true = selling tokens we already own (exit), false = buying tokens (entry)
    pub is_exit: bool,
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
