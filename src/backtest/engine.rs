use crate::backtest::{data_loader, synthetic_markets};
use crate::config::Config;
use crate::types::events::{ExchangeEvent, PolymarketUpdate};
use std::path::Path;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Replay historical data through the same channel pipeline as live mode.
///
/// When `polymarket_tx` is provided, generates UpDown markets at 5-minute
/// boundaries and injects them at the correct tick index during replay.
pub async fn run_backtest(
    config: &Config,
    exchange_tx: mpsc::Sender<ExchangeEvent>,
    polymarket_tx: Option<mpsc::Sender<PolymarketUpdate>>,
    shutdown: CancellationToken,
) {
    let path = Path::new(&config.backtest_file);

    let ticks = match data_loader::load_ticks(path) {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "failed to load backtest data");
            return;
        }
    };

    if ticks.is_empty() {
        error!("no ticks loaded for backtest");
        return;
    }

    info!(
        ticks = ticks.len(),
        first = %ticks.first().unwrap().timestamp,
        last = %ticks.last().unwrap().timestamp,
        "replaying backtest data"
    );

    // Pre-compute UpDown market injection points
    let updown_schedule = if polymarket_tx.is_some() {
        let schedule = synthetic_markets::generate_updown_markets(&ticks, 300);
        info!(
            updown_markets = schedule.len(),
            "pre-computed UpDown markets for backtest"
        );
        schedule
    } else {
        Vec::new()
    };

    // Build a quick-lookup: tick_index → position in schedule
    let mut schedule_idx = 0;

    for (tick_idx, tick) in ticks.iter().enumerate() {
        if shutdown.is_cancelled() {
            info!("backtest interrupted by shutdown");
            return;
        }

        // Inject UpDown markets at the correct tick
        if let Some(ref poly_tx) = polymarket_tx {
            while schedule_idx < updown_schedule.len()
                && updown_schedule[schedule_idx].0 <= tick_idx
            {
                let markets = updown_schedule[schedule_idx].1.clone();
                let event = PolymarketUpdate::MarketsDiscovered(markets);
                let _ = poly_tx.send(event).await;
                schedule_idx += 1;
            }
        }

        let _ = exchange_tx.send(ExchangeEvent::Tick(tick.clone())).await;

        // Small yield to let the pipeline process
        tokio::task::yield_now().await;
    }

    info!("backtest replay complete");
}
