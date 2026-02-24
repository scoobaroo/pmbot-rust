use crate::backtest::data_loader;
use crate::config::Config;
use crate::types::events::ExchangeEvent;
use std::path::Path;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Replay historical data through the same channel pipeline as live mode.
pub async fn run_backtest(
    config: &Config,
    exchange_tx: mpsc::Sender<ExchangeEvent>,
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

    for tick in &ticks {
        if shutdown.is_cancelled() {
            info!("backtest interrupted by shutdown");
            return;
        }

        let _ = exchange_tx.send(ExchangeEvent::Tick(tick.clone())).await;

        // Small yield to let the pipeline process
        tokio::task::yield_now().await;
    }

    info!("backtest replay complete");
}
