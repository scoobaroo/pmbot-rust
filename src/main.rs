use clap::Parser;
use pmbot_rust::config::{Cli, Config, RunMode, StrategyName};
use pmbot_rust::exchanges::binance::BinanceFeed;
use pmbot_rust::exchanges::coinbase::CoinbaseFeed;
use pmbot_rust::exchanges::okx::OkxFeed;
use pmbot_rust::exchanges::ExchangeFeed;
use pmbot_rust::ml::bridge::{MlBridge, MlBridgeConfig};
use pmbot_rust::polymarket::ws_orderbook;
use pmbot_rust::strategy::factory::create_strategy;
use pmbot_rust::types::candle::Timeframe;
use pmbot_rust::types::events::{
    AggregatorEvent, ExchangeEvent, ExecutionEvent, MlPrediction, PolymarketUpdate, TradeSignal,
};
use std::collections::HashSet;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = Config::load(&cli);

    pmbot_rust::utils::logging::init(&config.log_level);
    let shutdown = pmbot_rust::utils::shutdown::create_shutdown_token();

    info!(mode = ?config.mode, strategies = ?config.strategies, symbols = ?config.symbols, "pmbot starting");

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

    // Create shared BookCache for Polymarket orderbook data
    let book_cache = ws_orderbook::new_book_cache();

    // Create Polymarket trade activity cache (shared with RL bridge + trade poller)
    let trade_activity_cache = pmbot_rust::polymarket::trade_poller::new_trade_activity_cache();

    // Create web state early so strategies can push events to the dashboard
    let web_state = if config.web_enabled {
        Some(pmbot_rust::web::state::new_web_state())
    } else {
        None
    };

    // Build all strategy instances and compute union of needs
    let mut strategies: Vec<Box<dyn pmbot_rust::strategy::traits::Strategy>> = Vec::new();
    let mut all_timeframes: HashSet<Timeframe> = HashSet::new();
    let mut needs_polymarket = false;

    for strategy_name in &config.strategies {
        let strategy = create_strategy(strategy_name, &config, Some(book_cache.clone()), web_state.clone());
        let subs = strategy.subscriptions();
        needs_polymarket |= subs.polymarket_updates;

        // Collect candle timeframes per strategy
        let tfs = candle_timeframes_for(strategy_name, &config);
        all_timeframes.extend(tfs);

        strategies.push(strategy);
    }

    // RL bridge needs Polymarket markets and M5 candles for state features
    if config.rl_enabled {
        needs_polymarket = true;
        all_timeframes.insert(Timeframe::M5);
    }

    // Copy-trade bridge needs Polymarket market discovery
    if config.copy_trade_enabled {
        needs_polymarket = true;
    }

    let candle_timeframes: Vec<Timeframe> = all_timeframes.into_iter().collect();

    // Create broadcast channels for fan-out to multiple strategy runners
    let (agg_broadcast_tx, _) = broadcast::channel::<AggregatorEvent>(256);
    let (exec_broadcast_tx, _) = broadcast::channel::<ExecutionEvent>(64);
    let (poly_broadcast_tx, _) = broadcast::channel::<PolymarketUpdate>(128);
    let (ml_broadcast_tx, _) = broadcast::channel::<MlPrediction>(64);

    // Spawn strategy runners — each gets its own broadcast receiver
    for strategy in strategies {
        let runner = pmbot_rust::strategy::runner::StrategyRunner::new(strategy);
        let agg_rx = agg_broadcast_tx.subscribe();
        let exec_rx = exec_broadcast_tx.subscribe();
        let poly_rx = poly_broadcast_tx.subscribe();
        let ml_rx = if config.ml_enabled {
            Some(ml_broadcast_tx.subscribe())
        } else {
            None
        };
        let sig_tx = signal_tx.clone();
        let strat_sd = shutdown.clone();
        tokio::spawn(async move {
            runner
                .run(agg_rx, exec_rx, poly_rx, ml_rx, sig_tx, strat_sd)
                .await;
        });
    }

    match config.mode {
        RunMode::Backtest => {
            // Backtest mode: replay CSV data through the pipeline
            let bt_shutdown = shutdown.clone();
            let bt_exchange_tx = exchange_tx.clone();
            let bt_config = config.clone();
            let bt_poly_tx = if needs_polymarket {
                Some(polymarket_tx.clone())
            } else {
                None
            };
            let bt_done_shutdown = shutdown.clone();
            tokio::spawn(async move {
                pmbot_rust::backtest::engine::run_backtest(
                    &bt_config,
                    bt_exchange_tx,
                    bt_poly_tx,
                    bt_shutdown,
                )
                .await;
                // Let the pipeline drain (aggregator → strategy → execution)
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                info!("backtest pipeline drained, triggering shutdown");
                bt_done_shutdown.cancel();
            });
        }
        _ => {
            // Live/Paper: spawn all exchange feeds
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

            // Spawn Binance Futures feed for real-time funding rate
            {
                let futures_feed = pmbot_rust::exchanges::binance_futures::BinanceFuturesFeed;
                let futures_tx = exchange_tx.clone();
                let futures_syms = symbols.clone();
                let futures_sd = shutdown.clone();
                tokio::spawn(async move {
                    futures_feed.run(futures_syms, futures_tx, futures_sd).await;
                });
            }

            // Spawn Chainlink oracle feed
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

            // Spawn Polymarket market scanner only if any strategy needs it
            if needs_polymarket {
                let (token_tx, token_rx) = mpsc::channel::<Vec<String>>(16);

                let scanner = pmbot_rust::polymarket::market_scanner::MarketScanner::new(
                    symbols.clone(),
                    token_tx,
                    config.updown_enabled,
                    config.updown_only,
                    config.general_markets_enabled,
                    config.general_poll_interval_secs,
                    config.general_min_liquidity,
                );
                let poly_sd = shutdown.clone();
                tokio::spawn(async move {
                    scanner.run(polymarket_tx, poly_sd).await;
                });

                // Spawn Polymarket WebSocket orderbook stream
                let ws_book_cache = book_cache.clone();
                let ws_sd = shutdown.clone();
                tokio::spawn(async move {
                    let stream = ws_orderbook::OrderbookStream::new(token_rx, ws_book_cache);
                    stream.run(ws_sd).await;
                });

                // Spawn Polymarket trade activity poller (for RL state features)
                if config.rl_enabled {
                    let (trade_token_tx, trade_token_rx) = mpsc::channel::<Vec<String>>(16);
                    let poller = pmbot_rust::polymarket::trade_poller::TradePoller::new(
                        trade_activity_cache.clone(),
                    );
                    let poller_sd = shutdown.clone();
                    tokio::spawn(async move {
                        poller.run(trade_token_rx, poller_sd).await;
                    });

                    // Forward discovered token IDs to trade poller via polymarket broadcast
                    let poly_bcast_for_tokens = poly_broadcast_tx.subscribe();
                    let _token_fwd_sd = shutdown.clone();
                    tokio::spawn(async move {
                        let mut rx = poly_bcast_for_tokens;
                        while let Ok(event) = rx.recv().await {
                            if let pmbot_rust::types::events::PolymarketUpdate::MarketsDiscovered(markets) = event {
                                let tokens: Vec<String> = markets.iter().map(|m| m.token_id_yes.clone()).collect();
                                if !tokens.is_empty() {
                                    let _ = trade_token_tx.send(tokens).await;
                                }
                            }
                        }
                    });
                }
            }
        }
    }

    // Drop the original sender so channels close when all producers finish
    drop(exchange_tx);

    // Conditionally spawn copy-trade bridge (uses separate Polymarket account)
    let _copy_trader_cache = if config.copy_trade_enabled {
        let cache = pmbot_rust::copy_trade::types::new_copy_trader_cache();

        // Derive credentials for the copy-trade account (separate wallet)
        let ct_poly_client = if !config.copy_trade_private_key.is_empty()
            && config.mode == RunMode::Live
        {
            match pmbot_rust::polymarket::auth::derive_api_credentials(
                &config.copy_trade_private_key,
            )
            .await
            {
                Ok((creds, signer)) => {
                    info!(
                        wallet = %creds.wallet_address,
                        "copy-trade: Polymarket credentials derived for separate account"
                    );
                    let funder = if config.copy_trade_funder_address.is_empty() {
                        None
                    } else {
                        Some(config.copy_trade_funder_address.clone())
                    };
                    Some(pmbot_rust::polymarket::client::PolymarketClient::new_with_funder(
                        creds, signer, funder,
                    ))
                }
                Err(e) => {
                    tracing::error!(error = %e, "copy-trade: failed to derive Polymarket credentials");
                    None
                }
            }
        } else {
            if config.copy_trade_private_key.is_empty() {
                info!("copy-trade: no POLYMARKET_COPYTRADE_PRIVATE_KEY set, running in paper mode");
            }
            None
        };

        let bridge = pmbot_rust::copy_trade::bridge::CopyTradeBridge::new(
            pmbot_rust::copy_trade::types::CopyTradeConfig {
                enabled: config.copy_trade_enabled,
                targets: config.copy_trade_targets.clone(),
                poll_interval_secs: config.copy_trade_poll_interval_secs,
                size_ratio: config.copy_trade_size_ratio,
                max_position_usd: config.copy_trade_max_position_usd,
                max_latency_secs: config.copy_trade_max_latency_secs,
                price_premium_pct: config.copy_trade_price_premium_pct,
                max_slippage_pct: config.copy_trade_max_slippage_pct,
            },
            ct_poly_client,
            cache.clone(),
            None, // RL state sharing wired below if RL enabled
        );
        let ct_poly_rx = poly_broadcast_tx.subscribe();
        let ct_sd = shutdown.clone();
        tokio::spawn(async move {
            bridge.run(ct_poly_rx, ct_sd).await;
        });
        info!(targets = ?config.copy_trade_targets, "copy-trade bridge spawned");
        Some(cache)
    } else {
        None
    };

    // Conditionally spawn web dashboard (reuse web_state created earlier for strategies)
    if let Some(ref web_state) = web_state {
        // Pre-populate with copy-trade targets
        {
            let mut wallets = web_state.wallets.write().await;
            for addr in &config.copy_trade_targets {
                wallets.insert(
                    addr.clone(),
                    pmbot_rust::web::state::WalletData {
                        address: addr.clone(),
                        ..Default::default()
                    },
                );
            }
        }

        // Initial data fetch
        {
            let mut wallets = web_state.wallets.write().await;
            for data in wallets.values_mut() {
                pmbot_rust::web::handlers::refresh_wallet(&web_state.client, data).await;
            }
        }

        let ws = web_state.clone();
        let web_port = config.web_port;
        let web_sd = shutdown.clone();
        tokio::spawn(async move {
            pmbot_rust::web::server::run(ws, web_port, web_sd).await;
        });

        let ws2 = web_state.clone();
        let refresh_sd = shutdown.clone();
        tokio::spawn(async move {
            pmbot_rust::web::server::background_refresh(ws2, refresh_sd).await;
        });

        // Poll Polymarket API for trade resolutions (WON/LOST)
        let ws3 = web_state.clone();
        let resolve_sd = shutdown.clone();
        let resolve_wallet = config.copy_trade_funder_address.clone();
        tokio::spawn(async move {
            pmbot_rust::web::server::poll_resolutions(ws3, resolve_wallet, resolve_sd).await;
        });

        info!(port = config.web_port, "web dashboard spawned");
    }

    // Conditionally spawn RL bridge (standalone task, not a Strategy)
    if config.rl_enabled {
        use pmbot_rust::rl::bridge::{RlBridge, RlBridgeConfig};
        let rl_bridge = RlBridge::new(
            RlBridgeConfig {
                server_url: config.rl_server_url.clone(),
                timeout_ms: config.rl_timeout_ms,
                capital_usd: config.rl_capital_usd,
                log_transitions: config.rl_log_transitions,
                transition_log_path: config.rl_transition_log_path.clone(),
            },
            if needs_polymarket {
                Some(book_cache.clone())
            } else {
                None
            },
            if needs_polymarket {
                Some(trade_activity_cache.clone())
            } else {
                None
            },
            _copy_trader_cache.clone(),
        );
        let rl_sig_tx = signal_tx.clone();
        let rl_agg_rx = agg_broadcast_tx.subscribe();
        let rl_exec_rx = exec_broadcast_tx.subscribe();
        let rl_poly_rx = poly_broadcast_tx.subscribe();
        let rl_ml_rx = ml_broadcast_tx.subscribe();
        let rl_sd = shutdown.clone();
        tokio::spawn(async move {
            rl_bridge
                .run(rl_agg_rx, rl_exec_rx, rl_poly_rx, rl_ml_rx, rl_sig_tx, rl_sd)
                .await;
        });
        info!(server_url = %config.rl_server_url, "RL bridge spawned");
    }

    // Conditionally create ML channels and bridge
    let ml_candle_tx = if config.ml_enabled {
        let (candle_tx, candle_rx) = mpsc::channel::<AggregatorEvent>(256);
        let (prediction_tx, prediction_rx) = mpsc::channel::<MlPrediction>(64);

        let bridge = MlBridge::new(
            MlBridgeConfig {
                server_url: config.ml_server_url.clone(),
                timeout_ms: config.ml_timeout_ms,
                active_yes_token_id: std::env::var("ML_POLY_YES_TOKEN").ok(),
            },
            if needs_polymarket {
                Some(book_cache.clone())
            } else {
                None
            },
        );
        let ml_sd = shutdown.clone();
        tokio::spawn(async move {
            bridge.run(candle_rx, prediction_tx, ml_sd).await;
        });
        info!(server_url = %config.ml_server_url, "ML bridge spawned");

        // Forward ML predictions to broadcast
        let ml_bcast = ml_broadcast_tx.clone();
        tokio::spawn(async move {
            let mut rx = prediction_rx;
            while let Some(pred) = rx.recv().await {
                let _ = ml_bcast.send(pred);
            }
        });

        Some(candle_tx)
    } else {
        None
    };

    // Spawn aggregator
    let aggregator = pmbot_rust::aggregator::Aggregator::new(
        config.stale_feed_timeout_secs,
        config.volatility_window_hours,
        candle_timeframes,
        config.binance_weight_multiplier,
        config.chainlink_blend_weight,
    );
    let agg_sd = shutdown.clone();
    let vol_window = config.volatility_window_hours;
    tokio::spawn(async move {
        aggregator
            .run(exchange_rx, aggregator_tx, ml_candle_tx, agg_sd, vol_window)
            .await;
    });

    // Forward aggregator events to broadcast channel for strategy fan-out
    {
        let agg_bcast = agg_broadcast_tx.clone();
        tokio::spawn(async move {
            let mut rx = aggregator_rx;
            while let Some(event) = rx.recv().await {
                let _ = agg_bcast.send(event);
            }
        });
    }

    // Forward execution events to broadcast channel
    {
        let exec_bcast = exec_broadcast_tx.clone();
        tokio::spawn(async move {
            let mut rx = execution_rx;
            while let Some(event) = rx.recv().await {
                let _ = exec_bcast.send(event);
            }
        });
    }

    // Forward polymarket updates to broadcast channel
    {
        let poly_bcast = poly_broadcast_tx.clone();
        tokio::spawn(async move {
            let mut rx = polymarket_rx;
            while let Some(event) = rx.recv().await {
                let _ = poly_bcast.send(event);
            }
        });
    }

    // Spawn execution engine
    // Use copytrade key if configured (routes all strategies through the funded account)
    let exec_private_key = if !config.copy_trade_private_key.is_empty() {
        info!("execution engine using copytrade account");
        &config.copy_trade_private_key
    } else {
        &config.polymarket_private_key
    };
    let exec_funder = if !config.copy_trade_funder_address.is_empty() {
        Some(config.copy_trade_funder_address.clone())
    } else {
        None
    };
    let poly_client = if config.mode == RunMode::Live {
        match pmbot_rust::polymarket::auth::derive_api_credentials(exec_private_key)
            .await
        {
            Ok((creds, signer)) => Some(
                pmbot_rust::polymarket::client::PolymarketClient::new_with_funder(
                    creds, signer, exec_funder,
                ),
            ),
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

/// Determine which candle timeframes a given strategy needs.
fn candle_timeframes_for(name: &StrategyName, config: &Config) -> Vec<Timeframe> {
    match name {
        StrategyName::MaCrossover => {
            let tf = Timeframe::from_str_loose(&config.ma_timeframe).unwrap_or(Timeframe::M5);
            vec![tf]
        }
        StrategyName::BollingerBands => {
            let tf = Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5);
            vec![tf]
        }
        StrategyName::BlackScholes | StrategyName::HedgedLp | StrategyName::Discount | StrategyName::Orb | StrategyName::Sniper => vec![],
        StrategyName::Unified => {
            let bb_tf = Timeframe::from_str_loose(&config.bb_timeframe).unwrap_or(Timeframe::M5);
            let ma_tf = Timeframe::from_str_loose(&config.ma_timeframe).unwrap_or(Timeframe::M5);
            if bb_tf == ma_tf {
                vec![bb_tf]
            } else {
                vec![bb_tf, ma_tf]
            }
        }
    }
}
