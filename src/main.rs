use clap::Parser;
use pmbot_rust::config::{Cli, Config, RunMode, StrategyName};
use pmbot_rust::exchanges::binance::BinanceFeed;
use pmbot_rust::exchanges::coinbase::CoinbaseFeed;
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

    if config.mode == RunMode::Live {
        tracing::warn!(
            max_position_usd = %config.max_position_usd,
            max_total_exposure_usd = %config.max_total_exposure_usd,
            "LIVE MODE — real money at risk. Position sizes capped by --live-max-position-usd"
        );
    }

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
        StrategyName::Unified => {
            let bb_tf = Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5);
            let ma_tf = Timeframe::from_str_loose(&config.ma_timeframe).unwrap_or(Timeframe::M5);
            if bb_tf == ma_tf {
                vec![bb_tf]
            } else {
                vec![bb_tf, ma_tf]
            }
        }
    };

    // Create shared BookCache for Polymarket orderbook data
    let book_cache = ws_orderbook::new_book_cache();

    // Create strategy via factory, injecting book cache for Black-Scholes / Unified
    let strategy: Box<dyn pmbot_rust::strategy::traits::Strategy> = match config.strategy {
        StrategyName::BlackScholes => {
            let bs = pmbot_rust::strategy::black_scholes::BlackScholesStrategy::new(&config)
                .with_book_cache(book_cache.clone());
            Box::new(bs)
        }
        StrategyName::Unified => {
            let unified = pmbot_rust::strategy::unified::UnifiedStrategy::new(&config)
                .with_book_cache(book_cache.clone());
            Box::new(unified)
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

            // Inject synthetic Polymarket markets for strategies that need them
            if needs_polymarket {
                use pmbot_rust::backtest::synthetic_markets;
                use rust_decimal::Decimal;

                // Generate markets at standard offsets from assumed initial prices
                let initial_prices: Vec<(String, Decimal)> = config
                    .symbols
                    .iter()
                    .map(|s| {
                        let price = match s.as_str() {
                            "BTC-USD" => Decimal::from(100_000),
                            "ETH-USD" => Decimal::from(3_500),
                            _ => Decimal::from(1_000),
                        };
                        (s.clone(), price)
                    })
                    .collect();

                let markets = synthetic_markets::generate_markets(&config.symbols, &initial_prices);
                let event = synthetic_markets::as_discovery_event(markets);
                info!(mode = "backtest", "injecting synthetic Polymarket markets");
                let _ = polymarket_tx.send(event).await;
            }
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
            spawn_feed!(CoinbaseFeed::new());
            spawn_feed!(BinanceFeed);
            spawn_feed!(OkxFeed);

            // Spawn Chainlink oracle feed (polling, not WebSocket)
            {
                let chainlink_feed = pmbot_rust::exchanges::chainlink::ChainlinkFeed::new(
                    &config.polygon_rpc_url,
                    &config.ethereum_rpc_url,
                );
                let chainlink_tx = exchange_tx.clone();
                let chainlink_syms = symbols.clone();
                let chainlink_sd = shutdown.clone();
                tokio::spawn(async move {
                    chainlink_feed
                        .run(chainlink_syms, chainlink_tx, chainlink_sd)
                        .await;
                });
            }

            // Spawn Polymarket market scanner only if strategy needs it
            if needs_polymarket {
                // Channel for scanner → orderbook stream token ID discovery
                let (token_tx, token_rx) = mpsc::channel::<Vec<String>>(16);

                let scanner = pmbot_rust::polymarket::market_scanner::MarketScanner::new(
                    symbols.clone(),
                    token_tx,
                    config.updown_enabled,
                    config.updown_only,
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
            Ok((creds, signer)) => Some(pmbot_rust::polymarket::client::PolymarketClient::new(
                creds, signer,
            )),
            Err(e) => {
                tracing::error!(error = %e, "failed to derive Polymarket credentials");
                None
            }
        }
    } else {
        None
    };

    let exec_engine =
        pmbot_rust::execution::engine::ExecutionEngine::new(config.clone(), poly_client);
    let exec_sd = shutdown.clone();
    tokio::spawn(async move {
        exec_engine.run(signal_rx, execution_tx, exec_sd).await;
    });

    // Wait for shutdown
    shutdown.cancelled().await;
    info!("pmbot shutdown complete");
}
