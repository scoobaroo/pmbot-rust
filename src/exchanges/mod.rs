pub mod binance;
pub mod chainlink;
pub mod coinbase;
pub mod common;
pub mod okx;

use crate::types::events::ExchangeEvent;
use crate::types::market::Exchange;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Trait for all exchange WebSocket feeds.
#[allow(async_fn_in_trait)]
pub trait ExchangeFeed {
    fn exchange(&self) -> Exchange;

    async fn run(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<ExchangeEvent>,
        shutdown: CancellationToken,
    );
}

/// Exponential backoff for reconnection: 1s, 2s, 4s, ..., max 60s
pub fn backoff_delay(attempt: u32) -> std::time::Duration {
    let secs = (1u64 << attempt.min(6)).min(60);
    std::time::Duration::from_secs(secs)
}
