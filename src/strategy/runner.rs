use crate::strategy::traits::{Strategy, StrategyEvent};
use crate::types::events::{AggregatorEvent, ExecutionEvent, PolymarketUpdate, TradeSignal};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

pub struct StrategyRunner {
    strategy: Box<dyn Strategy>,
}

impl StrategyRunner {
    pub fn new(strategy: Box<dyn Strategy>) -> Self {
        Self { strategy }
    }

    pub async fn run(
        mut self,
        mut agg_rx: mpsc::Receiver<AggregatorEvent>,
        mut exec_rx: mpsc::Receiver<ExecutionEvent>,
        mut poly_rx: mpsc::Receiver<PolymarketUpdate>,
        signal_tx: mpsc::Sender<TradeSignal>,
        shutdown: CancellationToken,
    ) {
        let subs = self.strategy.subscriptions();
        info!(strategy = self.strategy.name(), "strategy runner started");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!(strategy = self.strategy.name(), "strategy runner shutting down");
                    return;
                }
                Some(event) = agg_rx.recv() => {
                    match event {
                        AggregatorEvent::PriceUpdate(price) if subs.price_updates => {
                            let signals = self.strategy.on_event(StrategyEvent::PriceUpdate(price));
                            self.send_signals(&signals, &signal_tx).await;
                        }
                        AggregatorEvent::CandleComplete(candle) if subs.candles => {
                            let signals = self.strategy.on_event(StrategyEvent::CandleComplete(candle));
                            self.send_signals(&signals, &signal_tx).await;
                        }
                        AggregatorEvent::StaleFeed { exchange, symbol, .. } => {
                            tracing::warn!(exchange = %exchange, symbol = %symbol, "stale feed — strategy pausing for symbol");
                        }
                        _ => {} // subscription not active for this event type
                    }
                }
                Some(event) = poly_rx.recv(), if subs.polymarket_updates => {
                    let signals = self.strategy.on_event(StrategyEvent::PolymarketUpdate(event));
                    self.send_signals(&signals, &signal_tx).await;
                }
                Some(event) = exec_rx.recv(), if subs.execution_feedback => {
                    let signals = self.strategy.on_event(StrategyEvent::ExecutionFeedback(event));
                    self.send_signals(&signals, &signal_tx).await;
                }
            }
        }
    }

    async fn send_signals(&self, signals: &[TradeSignal], signal_tx: &mpsc::Sender<TradeSignal>) {
        for signal in signals {
            debug!(strategy = self.strategy.name(), side = %signal.side, size = %signal.size_usd, "signal generated");
            let _ = signal_tx.send(signal.clone()).await;
        }
    }
}
