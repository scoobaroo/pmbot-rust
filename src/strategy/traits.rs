use crate::types::candle::Candle;
use crate::types::events::{ExecutionEvent, MlPrediction, PolymarketUpdate, TradeSignal};
use crate::types::market::AggregatedPrice;

/// What events a strategy wants to receive.
pub struct StrategySubscriptions {
    pub price_updates: bool,
    pub candles: bool,
    pub execution_feedback: bool,
    pub polymarket_updates: bool,
    pub ml_predictions: bool,
}

/// Events dispatched to strategies.
pub enum StrategyEvent {
    PriceUpdate(AggregatedPrice),
    CandleComplete(Candle),
    ExecutionFeedback(ExecutionEvent),
    PolymarketUpdate(PolymarketUpdate),
    MlUpdate(MlPrediction),
}

/// Pluggable strategy trait. `on_event` is synchronous — pure computation, no I/O.
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn subscriptions(&self) -> StrategySubscriptions;
    fn on_event(&mut self, event: StrategyEvent) -> Vec<TradeSignal>;
}
