use clap::Parser;
use pmbot_rust::config::{Cli, Config, RunMode, StrategyName};
use pmbot_rust::exchanges::binance::BinanceFeed;
use pmbot_rust::exchanges::bitfinex::BitfinexFeed;
use pmbot_rust::exchanges::coinbase::CoinbaseFeed;
use pmbot_rust::exchanges::kraken::KrakenFeed;
use pmbot_rust::exchanges::okx::OkxFeed;
use pmbot_rust::exchanges::ExchangeFeed;
use pmbot_rust::polymarket::ws_orderbook;
use pmbot_rust::types::candle::Timeframe;
use pmbot_rust::types::events::{
    AggregatorEvent, ExchangeEvent, ExecutionEvent, PolymarketUpdate, TradeSignal,
};
use tokio::sync::mpsc;
use tracing::info;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = Config::load(&cli);

    pmbot_rust::utils::logging::init(&config.log_level);
    let shutdown = pmbot_rust::utils::shutdown::create_shutdown_token();

    info!(mode = ?config.mode, strategy = ?config.strategy, symbols = ?config.symbols, "pmbot starting");

    // Create channels
    let (exchange_tx, exchange_rx) = mpsc::channel::<ExchangeEvent>(1024);
    let (aggregator_tx, aggregator_rx) = mpsc::channel::<AggregatorEvent>(256);
    let (signal_tx, signal_rx) = mpsc::channel::<TradeSignal>(64);
    let (execution_tx, execution_rx) = mpsc::channel::<ExecutionEvent>(64);
    let (polymarket_tx, polymarket_rx) = mpsc::channel::<PolymarketUpdate>(128);

    let symbols = config.symbols.clone();

    // Determine which candle timeframes the strategy needs
    let candle_timeframes = match config.strategy {
        StrategyName::MaCrossover => {
            let tf = Timeframe::from_str_loose(&config.ma_timeframe).unwrap_or(Timeframe::M5);
            vec![tf]
        }
        StrategyName::BollingerBands => {
            let tf = Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5);
            vec![tf]
        }
        StrategyName::BlackScholes => vec![], // no candles needed
    };

    // Create shared BookCache for Polymarket orderbook data
    let book_cache = ws_orderbook::new_book_cache();

    // Create strategy via factory, injecting book cache for Black-Scholes
    let strategy: Box<dyn pmbot_rust::strategy::traits::Strategy> = match config.strategy {
        StrategyName::BlackScholes => {
            let bs = pmbot_rust::strategy::black_scholes::BlackScholesStrategy::new(&config)
                .with_book_cache(book_cache.clone());
            Box::new(bs)
        }
        StrategyName::MaCrossover => {
            Box::new(pmbot_rust::strategy::ma_crossover::MACrossoverStrategy::new(&config))
        }
        StrategyName::BollingerBands => {
            Box::new(pmbot_rust::strategy::bollinger::BollingerBandsStrategy::new(&config))
        }
    };
    let needs_polymarket = strategy.subscriptions().polymarket_updates;

    match config.mode {
        RunMode::Backtest => {
            // Backtest mode: replay CSV data through the pipeline
            let bt_shutdown = shutdown.clone();
            let bt_exchange_tx = exchange_tx.clone();
            let bt_config = config.clone();
            tokio::spawn(async move {
                pmbot_rust::backtest::engine::run_backtest(&bt_config, bt_exchange_tx, bt_shutdown)
                    .await;
            });
        }
        _ => {
            // Live/Paper: spawn all 4 exchange feeds
            macro_rules! spawn_feed {
                ($feed:expr) => {{
                    let tx = exchange_tx.clone();
                    let syms = symbols.clone();
                    let sd = shutdown.clone();
                    tokio::spawn(async move {
                        $feed.run(syms, tx, sd).await;
                    });
                }};
            }
            spawn_feed!(KrakenFeed::new());
            spawn_feed!(CoinbaseFeed::new());
            spawn_feed!(BitfinexFeed);
            spawn_feed!(BinanceFeed);
            spawn_feed!(OkxFeed);

            // Spawn Polymarket market scanner only if strategy needs it
            if needs_polymarket {
                // Channel for scanner → orderbook stream token ID discovery
                let (token_tx, token_rx) = mpsc::channel::<Vec<String>>(16);

                let scanner = pmbot_rust::polymarket::market_scanner::MarketScanner::new(
                    symbols.clone(),
                    token_tx,
                );
                let poly_sd = shutdown.clone();
                tokio::spawn(async move {
                    scanner.run(polymarket_tx, poly_sd).await;
                });

                // Spawn Polymarket WebSocket orderbook stream
                // Waits for token IDs from the scanner before connecting
                let ws_book_cache = book_cache.clone();
                let ws_sd = shutdown.clone();
                tokio::spawn(async move {
                    let stream = ws_orderbook::OrderbookStream::new(token_rx, ws_book_cache);
                    stream.run(ws_sd).await;
                });
            }
        }
    }

    // Drop the original sender so channels close when all producers finish
    drop(exchange_tx);

    // Spawn aggregator
    let aggregator = pmbot_rust::aggregator::Aggregator::new(
        config.stale_feed_timeout_secs,
        config.volatility_window_hours,
        candle_timeframes,
    );
    let agg_sd = shutdown.clone();
    let vol_window = config.volatility_window_hours;
    tokio::spawn(async move {
        aggregator
            .run(exchange_rx, aggregator_tx, agg_sd, vol_window)
            .await;
    });

    // Spawn strategy runner
    let runner = pmbot_rust::strategy::runner::StrategyRunner::new(strategy);
    let strat_sd = shutdown.clone();
    tokio::spawn(async move {
        runner
            .run(
                aggregator_rx,
                execution_rx,
                polymarket_rx,
                signal_tx,
                strat_sd,
            )
            .await;
    });

    // Spawn execution engine
    let poly_client = if config.mode == RunMode::Live {
        // In live mode, derive API credentials and create client
        match pmbot_rust::polymarket::auth::derive_api_credentials(&config.polymarket_private_key)
            .await
        {
            Ok(creds) => Some(pmbot_rust::polymarket::client::PolymarketClient::new(creds)),
            Err(e) => {
                tracing::error!(error = %e, "failed to derive Polymarket credentials");
                None
            }
        }
    } else {
        None
    };

    // Create Kraken spot client for live spot execution
    let spot_client = if config.mode == RunMode::Live
        && !config.kraken_api_key.is_empty()
        && !config.kraken_api_secret.is_empty()
    {
        match pmbot_rust::exchanges::kraken_trading::KrakenSpotClient::new(
            config.kraken_api_key.clone(),
            config.kraken_api_secret.clone(),
        ) {
            Ok(client) => {
                info!("Kraken spot client initialized for live trading");
                Some(client)
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to initialize Kraken spot client");
                None
            }
        }
    } else {
        None
    };

    let exec_engine = pmbot_rust::execution::engine::ExecutionEngine::new(
        config.clone(),
        poly_client,
        spot_client,
    );
    let exec_sd = shutdown.clone();
    tokio::spawn(async move {
        exec_engine.run(signal_rx, execution_tx, exec_sd).await;
    });

    // Wait for shutdown
    shutdown.cancelled().await;
    info!("pmbot shutdown complete");
}
