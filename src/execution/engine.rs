use crate::config::{Config, RunMode};
use crate::execution::paper_tracker::PaperTracker;
use crate::execution::risk::RiskManager;
use crate::polymarket::client::{OrderParams, PolymarketClient};
use crate::polymarket::market_scanner::check_resolutions;
use crate::types::events::{ExecutionEvent, SignalMetadata, TradeSignal, TradeTarget};
use crate::types::market::{MarketDirection, MarketType};
use crate::types::order::{Fill, Order, OrderStatus, OrderType, Side};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Market metadata stored per condition_id for backtest resolution.
struct BacktestMarketMeta {
    underlying_symbol: String,
    strike: Decimal,
    direction: MarketDirection,
}

pub struct ExecutionEngine {
    config: Config,
    client: Option<Arc<PolymarketClient>>,
    risk: RiskManager,
    open_orders: HashMap<String, Order>,
    maker_mode: bool,
    paper_tracker: PaperTracker,
    /// HTTP client for querying Gamma API resolution status.
    http: reqwest::Client,
    /// Latest known underlying prices per symbol (for backtest resolution).
    last_underlying_prices: HashMap<String, f64>,
    /// Market metadata per condition_id (for backtest resolution).
    backtest_market_meta: HashMap<String, BacktestMarketMeta>,
    /// In backtest mode, tracks the latest signal timestamp for time-aware resolution.
    backtest_time: Option<DateTime<Utc>>,
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
            paper_tracker: PaperTracker::new(Decimal::from(100)),
            http: reqwest::Client::new(),
            last_underlying_prices: HashMap::new(),
            backtest_market_meta: HashMap::new(),
            backtest_time: None,
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

        // Resolve expired UpDown positions every 10 seconds
        let mut resolve_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    // Final resolution pass before reporting
                    if self.config.mode == RunMode::Backtest {
                        // Backtest: resolve all positions using last known prices
                        // (Gamma API unavailable for synthetic condition IDs)
                        self.resolve_backtest_positions();
                    } else {
                        self.resolve_expired_via_api().await;
                    }
                    info!(
                        starting_capital = %self.paper_tracker.starting_capital(),
                        portfolio_value = %self.paper_tracker.portfolio_value(),
                        return_pct = %format!("{:.2}%", self.paper_tracker.return_pct()),
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
                    // In backtest mode, resolve expired positions after every signal
                    if self.config.mode == RunMode::Backtest {
                        self.resolve_backtest_expired();
                    }
                }
                _ = resolve_interval.tick() => {
                    if self.config.mode != RunMode::Backtest {
                        self.resolve_expired_via_api().await;
                    }
                }
            }
        }
    }

    async fn handle_signal(&mut self, signal: TradeSignal, exec_tx: &mpsc::Sender<ExecutionEvent>) {
        // Track backtest time from signal timestamps
        if self.config.mode == RunMode::Backtest {
            self.backtest_time = Some(signal.timestamp);
        }

        // Track latest underlying price for backtest resolution
        match &signal.metadata {
            SignalMetadata::Unified { spot, .. } if *spot > 0.0 => {
                if let TradeTarget::Polymarket(market) = &signal.target {
                    self.last_underlying_prices
                        .insert(market.underlying_symbol.clone(), *spot);
                }
            }
            SignalMetadata::UpDown { spot, .. } if *spot > 0.0 => {
                if let TradeTarget::Polymarket(market) = &signal.target {
                    self.last_underlying_prices
                        .insert(market.underlying_symbol.clone(), *spot);
                }
            }
            _ => {}
        }

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
                // Store market metadata for backtest resolution
                if self.config.mode == RunMode::Backtest {
                    self.backtest_market_meta
                        .entry(market.condition_id.clone())
                        .or_insert_with(|| BacktestMarketMeta {
                            underlying_symbol: market.underlying_symbol.clone(),
                            strike: market.strike,
                            direction: market.direction,
                        });
                }
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
        let is_arb = matches!(signal.metadata, SignalMetadata::Arbitrage { .. });

        // Token selection: Buy → yes token, Sell → no token
        let token_id = match signal.side {
            Side::Buy => &market.token_id_yes,
            Side::Sell => &market.token_id_no,
        };

        // CLOB side: BUY for entries (acquiring tokens), SELL for exits (selling owned tokens)
        let clob_side = if signal.is_exit {
            Side::Sell
        } else {
            Side::Buy
        };

        // Round price to 2 decimal places (standard Polymarket tick size)
        let price = signal.price.round_dp(2);

        let token_size = if price > Decimal::ZERO {
            // Truncate token size to 2 decimal places to avoid CLOB precision errors
            (signal.size_usd / price).round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
        } else {
            return;
        };

        let order = Order::new(
            market.condition_id.clone(),
            token_id.clone(),
            clob_side,
            OrderType::Limit,
            price,
            token_size,
        );

        info!(
            order_id = %order.id,
            side = %order.side,
            price = %order.price,
            size = %order.size,
            is_arb = is_arb,
            "executing polymarket order"
        );

        // Extract UpDown metadata before execution (for paper tracking resolution)
        let updown_meta = match (&market.market_type, &signal.metadata) {
            (
                MarketType::UpDown {
                    window_start_ts,
                    window_secs,
                },
                SignalMetadata::UpDown { start_price, .. },
            ) => {
                let window_end_ts = window_start_ts + *window_secs as i64;
                // Track latest underlying price for backtest resolution
                self.last_underlying_prices
                    .insert(market.underlying_symbol.clone(), *start_price);
                Some((
                    market.condition_id.clone(),
                    window_end_ts,
                    market.underlying_symbol.clone(),
                    *start_price,
                    signal.side,
                ))
            }
            (
                MarketType::UpDown {
                    window_start_ts,
                    window_secs,
                },
                SignalMetadata::HedgedLp { .. },
            ) => {
                let window_end_ts = window_start_ts + *window_secs as i64;
                // Use strike (start price) from market — HedgedLP tracks this separately
                let start_price = market.strike.to_f64().unwrap_or(0.0);
                Some((
                    market.condition_id.clone(),
                    window_end_ts,
                    market.underlying_symbol.clone(),
                    start_price,
                    signal.side,
                ))
            }
            _ => None,
        };

        // Store arb metadata for paper tracking after execution
        let arb_meta = if is_arb {
            Some((market.condition_id.clone(), signal.side, price, token_size))
        } else {
            None
        };

        match self.config.mode {
            RunMode::Live => {
                self.execute_live(order, exec_tx).await;
            }
            RunMode::Paper | RunMode::Backtest => {
                if is_arb {
                    // For arb signals, use arb-specific paper tracking instead of regular record_fill
                    self.execute_simulated_arb(order, exec_tx).await;
                } else {
                    self.execute_simulated(order, exec_tx).await;
                }
            }
        }

        // Attach UpDown resolution metadata to the paper position
        if let Some((condition_id, window_end_ts, symbol, start_price, signal_side)) =
            updown_meta
        {
            info!(
                condition_id = %condition_id,
                window_end_ts = window_end_ts,
                symbol = %symbol,
                start_price = start_price,
                signal_side = ?signal_side,
                "setting updown meta on position"
            );
            self.paper_tracker.set_updown_meta(
                &condition_id,
                window_end_ts,
                &symbol,
                start_price,
                signal_side,
            );
        } else {
            info!("no updown_meta extracted for this signal");
        }

        // Record arb fill in paper tracker
        if let Some((condition_id, side, fill_price, fill_size)) = arb_meta {
            self.paper_tracker
                .record_arb_fill(&condition_id, side, fill_price, fill_size);
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
            neg_risk: None, // auto-detect via CLOB API
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
                        neg_risk: None,
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

    /// Simulated execution for arb signals — sends fill events but skips record_fill
    /// (arb P&L is tracked separately via record_arb_fill).
    async fn execute_simulated_arb(
        &mut self,
        mut order: Order,
        exec_tx: &mpsc::Sender<ExecutionEvent>,
    ) {
        order.status = OrderStatus::Filled;
        order.filled_size = order.size;
        order.updated_at = Utc::now();

        let fill = Fill {
            order_id: order.id.clone(),
            price: order.price,
            size: order.size,
            side: order.side,
            timestamp: Utc::now(),
            fee: order.size * order.price * Decimal::new(2, 4),
        };

        let _ = exec_tx
            .send(ExecutionEvent::OrderPlaced(order.clone()))
            .await;
        let _ = exec_tx.send(ExecutionEvent::OrderFilled(fill)).await;
    }

    /// Resolve all open positions at backtest shutdown using last known prices.
    /// For StrikeAbove: Bullish ("reach $X") wins if spot > strike; Bearish ("drop to $X") wins if spot < strike.
    /// For UpDown: up_won if spot > start_price.
    fn resolve_backtest_positions(&mut self) {
        // First try UpDown resolution via last_underlying_prices
        self.paper_tracker
            .resolve_all_by_last_price(&self.last_underlying_prices);

        // Then resolve StrikeAbove positions using market metadata
        let open_ids: Vec<String> = self.paper_tracker.open_condition_ids();
        for cid in open_ids {
            if let Some(meta) = self.backtest_market_meta.get(&cid) {
                if let Some(&spot) = self.last_underlying_prices.get(&meta.underlying_symbol) {
                    let spot_dec = Decimal::from_f64_retain(spot).unwrap_or(Decimal::ZERO);
                    let won = match meta.direction {
                        MarketDirection::Bullish => spot_dec > meta.strike, // "Will BTC reach $X?"
                        MarketDirection::Bearish => spot_dec < meta.strike, // "Will BTC drop to $X?"
                    };
                    self.paper_tracker.resolve_market(&cid, won);
                }
            }
        }
    }

    /// Resolve expired UpDown positions during backtest replay using last known prices.
    fn resolve_backtest_expired(&mut self) {
        let now_ts = match self.backtest_time {
            Some(t) => t.timestamp(),
            None => return,
        };

        let expired_ids = self.paper_tracker.expired_updown_condition_ids(now_ts);
        if expired_ids.is_empty() {
            return;
        }

        // Also resolve any arb pairs on expired markets
        self.paper_tracker.resolve_arb_pairs(&expired_ids);

        for cid in &expired_ids {
            if let Some((symbol, start_price)) = self.paper_tracker.get_updown_meta(cid) {
                if let Some(&latest_price) = self.last_underlying_prices.get(&symbol) {
                    let up_won = latest_price >= start_price;
                    self.paper_tracker.resolve_by_outcome(cid, up_won);
                }
            }
        }

        self.paper_tracker.maybe_report();
    }

    /// Poll Gamma API for expired UpDown market resolutions.
    async fn resolve_expired_via_api(&mut self) {
        let now_ts = Utc::now().timestamp();
        let open_count = self.paper_tracker.open_position_count();
        let expired_ids = self.paper_tracker.expired_updown_condition_ids(now_ts);
        if expired_ids.is_empty() {
            if open_count > 0 {
                info!(
                    open_positions = open_count,
                    now_ts = now_ts,
                    "resolve tick: no expired positions yet"
                );
            }
            return;
        }

        info!(
            count = expired_ids.len(),
            ids = ?expired_ids,
            "checking expired markets for resolution"
        );

        let resolved = check_resolutions(&self.http, &expired_ids).await;
        for (condition_id, up_won) in resolved {
            self.paper_tracker.resolve_by_outcome(&condition_id, up_won);
        }

        self.paper_tracker.maybe_report();
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
