use crate::strategy::traits::{Strategy, StrategyEvent};
use crate::types::events::{AggregatorEvent, ExecutionEvent, MlPrediction, PolymarketUpdate, TradeSignal};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct StrategyRunner {
    strategy: Box<dyn Strategy>,
}

/// Counters for periodic stats logging.
struct RunnerStats {
    price_updates: u64,
    candles: u64,
    signals_emitted: u64,
    fills: u64,
    rejections: u64,
    ml_predictions: u64,
    last_report: std::time::Instant,
}

impl RunnerStats {
    fn new() -> Self {
        Self {
            price_updates: 0,
            candles: 0,
            signals_emitted: 0,
            fills: 0,
            rejections: 0,
            ml_predictions: 0,
            last_report: std::time::Instant::now(),
        }
    }

    fn maybe_report(&mut self, strategy_name: &str) {
        let elapsed = self.last_report.elapsed();
        if elapsed.as_secs() >= 60 {
            info!(
                strategy = strategy_name,
                price_updates = self.price_updates,
                candles = self.candles,
                signals = self.signals_emitted,
                fills = self.fills,
                rejections = self.rejections,
                ml_predictions = self.ml_predictions,
                elapsed_secs = elapsed.as_secs(),
                "strategy runner stats"
            );
            // Reset counters
            self.price_updates = 0;
            self.candles = 0;
            self.signals_emitted = 0;
            self.fills = 0;
            self.rejections = 0;
            self.ml_predictions = 0;
            self.last_report = std::time::Instant::now();
        }
    }
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
        mut ml_rx: Option<mpsc::Receiver<MlPrediction>>,
        signal_tx: mpsc::Sender<TradeSignal>,
        shutdown: CancellationToken,
    ) {
        let subs = self.strategy.subscriptions();
        let name = self.strategy.name().to_string();
        info!(strategy = %name, "strategy runner started");

        let mut stats = RunnerStats::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!(strategy = %name, "strategy runner shutting down");
                    return;
                }
                Some(event) = agg_rx.recv() => {
                    match event {
                        AggregatorEvent::PriceUpdate(price) if subs.price_updates => {
                            stats.price_updates += 1;
                            let signals = self.strategy.on_event(StrategyEvent::PriceUpdate(price));
                            stats.signals_emitted += signals.len() as u64;
                            self.send_signals(&signals, &signal_tx).await;
                        }
                        AggregatorEvent::CandleComplete(candle) if subs.candles => {
                            stats.candles += 1;
                            let signals = self.strategy.on_event(StrategyEvent::CandleComplete(candle));
                            stats.signals_emitted += signals.len() as u64;
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
                    stats.signals_emitted += signals.len() as u64;
                    self.send_signals(&signals, &signal_tx).await;
                }
                Some(event) = exec_rx.recv(), if subs.execution_feedback => {
                    match &event {
                        ExecutionEvent::OrderFilled(_) => stats.fills += 1,
                        ExecutionEvent::OrderFailed { .. } => stats.rejections += 1,
                        _ => {}
                    }
                    let signals = self.strategy.on_event(StrategyEvent::ExecutionFeedback(event));
                    stats.signals_emitted += signals.len() as u64;
                    self.send_signals(&signals, &signal_tx).await;
                }
                Some(prediction) = async {
                    match ml_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                }, if subs.ml_predictions => {
                    stats.ml_predictions += 1;
                    let signals = self.strategy.on_event(StrategyEvent::MlUpdate(prediction));
                    stats.signals_emitted += signals.len() as u64;
                    self.send_signals(&signals, &signal_tx).await;
                }
            }
            stats.maybe_report(&name);
        }
    }

    async fn send_signals(&self, signals: &[TradeSignal], signal_tx: &mpsc::Sender<TradeSignal>) {
        for signal in signals {
            info!(
                strategy = self.strategy.name(),
                side = %signal.side,
                size_usd = %signal.size_usd,
                confidence = format!("{:.2}%", signal.confidence * 100.0),
                price = %signal.price,
                "signal generated"
            );
            let _ = signal_tx.send(signal.clone()).await;
        }
    }
}
