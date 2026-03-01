use crate::config::{Config, RunMode};
use crate::execution::paper_tracker::PaperTracker;
use crate::execution::risk::RiskManager;
use crate::polymarket::client::{OrderParams, PolymarketClient};
use crate::types::events::{ExecutionEvent, TradeSignal, TradeTarget};
use crate::types::order::{Fill, Order, OrderStatus, OrderType, Side};
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct ExecutionEngine {
    config: Config,
    client: Option<Arc<PolymarketClient>>,
    risk: RiskManager,
    open_orders: HashMap<String, Order>,
    maker_mode: bool,
    paper_tracker: PaperTracker,
}

impl ExecutionEngine {
    pub fn new(config: Config, client: Option<PolymarketClient>) -> Self {
        let risk = RiskManager::new(&config);
        let maker_mode = config.maker_mode;
        Self {
            maker_mode,
            client: client.map(Arc::new),
            risk,
            open_orders: HashMap::new(),
            paper_tracker: PaperTracker::new(),
            config,
        }
    }

    pub async fn run(
        mut self,
        mut signal_rx: mpsc::Receiver<TradeSignal>,
        exec_tx: mpsc::Sender<ExecutionEvent>,
        shutdown: CancellationToken,
    ) {
        info!(
            mode = ?self.config.mode,
            maker_mode = self.maker_mode,
            "Execution engine started"
        );

        // Spawn heartbeat task if we have a live client
        if self.config.mode == RunMode::Live {
            if let Some(ref client) = self.client {
                let hb_client = Arc::clone(client);
                let hb_shutdown = shutdown.clone();
                let interval_secs = self.config.heartbeat_interval_secs;
                tokio::spawn(async move {
                    run_heartbeat(hb_client, interval_secs, hb_shutdown).await;
                });
            }
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!(
                        realized_pnl = %self.paper_tracker.realized_pnl(),
                        total_fees = %self.paper_tracker.total_fees(),
                        open_positions = self.paper_tracker.open_position_count(),
                        "Execution engine shutting down — final paper P&L"
                    );
                    self.cancel_all_open_orders(&exec_tx).await;
                    return;
                }
                Some(signal) = signal_rx.recv() => {
                    self.handle_signal(signal, &exec_tx).await;
                }
            }
        }
    }

    async fn handle_signal(&mut self, signal: TradeSignal, exec_tx: &mpsc::Sender<ExecutionEvent>) {
        // Risk check
        match self.risk.check(&signal) {
            Ok(()) => {}
            Err(rejection) => {
                warn!(rejection = %rejection, "signal rejected by risk manager");
                return;
            }
        }

        match &signal.target {
            TradeTarget::Polymarket(market) => {
                self.handle_polymarket_signal(&signal, market, exec_tx)
                    .await;
            }
            TradeTarget::Spot { symbol, .. } => {
                self.handle_spot_signal(&signal, symbol, exec_tx).await;
            }
        }

        self.risk.record_order();
    }

    async fn handle_polymarket_signal(
        &mut self,
        signal: &TradeSignal,
        market: &crate::types::market::PolymarketMarket,
        exec_tx: &mpsc::Sender<ExecutionEvent>,
    ) {
        let token_id = match signal.side {
            Side::Buy => &market.token_id_yes,
            Side::Sell => &market.token_id_no,
        };

        let price = signal.price;

        let token_size = if price > Decimal::ZERO {
            signal.size_usd / price
        } else {
            return;
        };

        let order = Order::new(
            market.condition_id.clone(),
            token_id.clone(),
            signal.side,
            OrderType::Limit,
            price,
            token_size,
        );

        info!(
            order_id = %order.id,
            side = %order.side,
            price = %order.price,
            size = %order.size,
            "executing polymarket order"
        );

        match self.config.mode {
            RunMode::Live => {
                self.execute_live(order, exec_tx).await;
            }
            RunMode::Paper | RunMode::Backtest => {
                self.execute_simulated(order, exec_tx).await;
            }
        }
    }

    async fn handle_spot_signal(
        &mut self,
        signal: &TradeSignal,
        symbol: &str,
        exec_tx: &mpsc::Sender<ExecutionEvent>,
    ) {
        let price = signal.price;

        let token_size = if price > Decimal::ZERO {
            signal.size_usd / price
        } else {
            return;
        };

        let order = Order::new(
            format!("spot:{symbol}"),
            symbol.to_string(),
            signal.side,
            OrderType::Market,
            price,
            token_size,
        );

        info!(
            order_id = %order.id,
            side = %order.side,
            symbol = symbol,
            price = %order.price,
            size = %order.size,
            "executing spot order (simulated)"
        );

        // Spot orders are always simulated — bot trades only on Polymarket
        self.execute_simulated(order, exec_tx).await;
    }

    async fn execute_live(&mut self, mut order: Order, exec_tx: &mpsc::Sender<ExecutionEvent>) {
        let client = match &self.client {
            Some(c) => c,
            None => {
                error!("no Polymarket client configured for live mode");
                return;
            }
        };

        let post_only = self.maker_mode;
        let fee_rate_bps = Some(self.config.fee_rate_bps);

        let params = OrderParams {
            token_id: &order.token_id,
            side: order.side,
            price: order.price,
            size: order.size,
            order_type: "GTC",
            post_only,
            fee_rate_bps,
        };

        match client.place_order(&params).await {
            Ok(order_id) => {
                order.id = order_id;
                order.status = OrderStatus::Open;
                let _ = exec_tx
                    .send(ExecutionEvent::OrderPlaced(order.clone()))
                    .await;
                self.open_orders.insert(order.id.clone(), order);
            }
            Err(e) => {
                let err_msg = e.to_string();

                // If post-only order was rejected (would cross spread), retry as taker
                if post_only && err_msg.contains("would cross") {
                    warn!(
                        order_id = %order.id,
                        "post-only order rejected (would cross spread), retrying as taker"
                    );
                    let taker_params = OrderParams {
                        token_id: &order.token_id,
                        side: order.side,
                        price: order.price,
                        size: order.size,
                        order_type: "GTC",
                        post_only: false,
                        fee_rate_bps,
                    };
                    match client.place_order(&taker_params).await {
                        Ok(order_id) => {
                            order.id = order_id;
                            order.status = OrderStatus::Open;
                            let _ = exec_tx
                                .send(ExecutionEvent::OrderPlaced(order.clone()))
                                .await;
                            self.open_orders.insert(order.id.clone(), order);
                            return;
                        }
                        Err(e2) => {
                            order.status = OrderStatus::Failed;
                            let _ = exec_tx
                                .send(ExecutionEvent::OrderFailed {
                                    order_id: order.id,
                                    error: e2.to_string(),
                                })
                                .await;
                            return;
                        }
                    }
                }

                order.status = OrderStatus::Failed;
                let _ = exec_tx
                    .send(ExecutionEvent::OrderFailed {
                        order_id: order.id,
                        error: err_msg,
                    })
                    .await;
            }
        }
    }

    async fn execute_simulated(
        &mut self,
        mut order: Order,
        exec_tx: &mpsc::Sender<ExecutionEvent>,
    ) {
        // Simulate immediate fill for paper/backtest
        order.status = OrderStatus::Filled;
        order.filled_size = order.size;
        order.updated_at = Utc::now();

        let fill = Fill {
            order_id: order.id.clone(),
            price: order.price,
            size: order.size,
            side: order.side,
            timestamp: Utc::now(),
            fee: order.size * order.price * Decimal::new(2, 4), // 0.02% fee
        };

        // Track paper P&L
        self.paper_tracker.record_fill(&order.condition_id, &fill);
        self.paper_tracker.maybe_report();

        let _ = exec_tx
            .send(ExecutionEvent::OrderPlaced(order.clone()))
            .await;
        let _ = exec_tx.send(ExecutionEvent::OrderFilled(fill)).await;
    }

    async fn cancel_all_open_orders(&mut self, exec_tx: &mpsc::Sender<ExecutionEvent>) {
        let order_ids: Vec<String> = self.open_orders.keys().cloned().collect();

        for order_id in order_ids {
            if let Some(client) = &self.client {
                if let Err(e) = client.cancel_order(&order_id).await {
                    error!(order_id = %order_id, error = %e, "failed to cancel order on shutdown");
                }
            }
            let _ = exec_tx
                .send(ExecutionEvent::OrderCancelled {
                    order_id: order_id.clone(),
                    reason: "shutdown".to_string(),
                })
                .await;
            self.open_orders.remove(&order_id);
        }
    }
}

/// Background heartbeat loop: sends a heartbeat every `interval_secs`.
async fn run_heartbeat(
    client: Arc<PolymarketClient>,
    interval_secs: u64,
    shutdown: CancellationToken,
) {
    info!(interval_secs, "heartbeat task started");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("heartbeat task shutting down");
                return;
            }
            _ = interval.tick() => {
                if let Err(e) = client.send_heartbeat().await {
                    warn!(error = %e, "heartbeat failed");
                }
            }
        }
    }
}
