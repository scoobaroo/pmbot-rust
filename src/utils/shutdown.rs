use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn create_shutdown_token() -> CancellationToken {
    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");
        info!("shutdown signal received, cancelling all tasks...");
        shutdown_token.cancel();
    });

    token
}
