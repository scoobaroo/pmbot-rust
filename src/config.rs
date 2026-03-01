use clap::{Parser, ValueEnum};
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunMode {
    Live,
    Paper,
    Backtest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StrategyName {
    BlackScholes,
    MaCrossover,
    BollingerBands,
    Unified,
}

#[derive(Parser, Debug)]
#[command(name = "pmbot", about = "Polymarket trading bot")]
pub struct Cli {
    #[arg(long, value_enum, default_value = "paper")]
    pub mode: RunMode,

    #[arg(long, value_enum, default_value = "unified")]
    pub strategy: StrategyName,

    #[arg(long, default_value = "data/backtest.csv")]
    pub backtest_file: String,

    #[arg(long, default_value = "true")]
    pub maker_mode: bool,

    /// Maximum position size in USD for live mode (safety cap).
    /// Defaults to $50 to prevent large losses during initial live testing.
    #[arg(long, default_value = "50")]
    pub live_max_position_usd: u64,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub mode: RunMode,
    pub strategy: StrategyName,
    pub backtest_file: String,

    // API keys
    pub coinbase_api_key: String,
    pub coinbase_api_secret: String,
    pub polymarket_private_key: String,
    pub polygon_rpc_url: String,
    pub ethereum_rpc_url: String,

    // Trading parameters
    pub symbols: Vec<String>,
    pub min_edge_threshold: Decimal,
    pub kelly_fraction_cap: Decimal,
    pub volatility_window_hours: u64,
    pub max_position_usd: Decimal,
    pub max_total_exposure_usd: Decimal,
    pub max_drawdown_pct: Decimal,
    pub max_orders_per_minute: u32,

    // MA crossover strategy parameters
    pub ma_fast_period: usize,
    pub ma_slow_period: usize,
    pub ma_timeframe: String,
    pub ma_size_usd: Decimal,

    // Bollinger Bands strategy parameters
    pub bb_period: usize,
    pub bb_num_std: f64,
    pub bb_timeframe: String,
    pub bb_size_usd: Decimal,
    pub bb_cooldown_candles: usize,

    // Polymarket fee/execution settings
    pub fee_rate_bps: u32,
    pub maker_mode: bool,
    pub heartbeat_interval_secs: u64,
    pub ws_book_max_stale_secs: u64,

    // Unified strategy weights
    pub unified_bb_weight: f64,
    pub unified_ma_weight: f64,

    // Up/Down 5-minute markets
    pub updown_enabled: bool,
    pub updown_only: bool,

    // System
    pub log_level: String,
    pub stale_feed_timeout_secs: u64,
}

impl Config {
    pub fn load(cli: &Cli) -> Self {
        let _ = dotenvy::dotenv();

        let mut max_position_usd = dec_or("MAX_POSITION_USD", "1000");
        let mut max_total_exposure_usd = dec_or("MAX_TOTAL_EXPOSURE_USD", "5000");

        // In live mode, enforce safety caps
        if cli.mode == RunMode::Live {
            let live_cap = Decimal::from(cli.live_max_position_usd);
            if max_position_usd > live_cap {
                max_position_usd = live_cap;
            }
            // Total exposure capped at 5× position cap
            let exposure_cap = live_cap * Decimal::from(5);
            if max_total_exposure_usd > exposure_cap {
                max_total_exposure_usd = exposure_cap;
            }
        }

        Self {
            mode: cli.mode,
            strategy: cli.strategy,
            backtest_file: cli.backtest_file.clone(),
            coinbase_api_key: env_or("COINBASE_API_KEY", ""),
            coinbase_api_secret: env_or("COINBASE_API_SECRET", ""),
            polymarket_private_key: env_or("POLYMARKET_PRIVATE_KEY", ""),
            polygon_rpc_url: env_or("POLYGON_RPC_URL", "https://polygon-rpc.com"),
            ethereum_rpc_url: env_or("ETHEREUM_RPC_URL", "https://eth.llamarpc.com"),
            symbols: env_or("SYMBOLS", "BTC-USD,ETH-USD")
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            min_edge_threshold: dec_or("MIN_EDGE_THRESHOLD", "0.03"),
            kelly_fraction_cap: dec_or("KELLY_FRACTION_CAP", "0.5"),
            volatility_window_hours: u64_or("VOLATILITY_WINDOW_HOURS", 24),
            max_position_usd,
            max_total_exposure_usd,
            max_drawdown_pct: dec_or("MAX_DRAWDOWN_PCT", "0.10"),
            max_orders_per_minute: u32_or("MAX_ORDERS_PER_MINUTE", 10),
            ma_fast_period: usize_or("MA_FAST_PERIOD", 9),
            ma_slow_period: usize_or("MA_SLOW_PERIOD", 21),
            ma_timeframe: env_or("MA_TIMEFRAME", "5m"),
            ma_size_usd: dec_or("MA_SIZE_USD", "500"),
            bb_period: usize_or("BB_PERIOD", 20),
            bb_num_std: f64_or("BB_NUM_STD", 2.0),
            bb_timeframe: env_or("BB_TIMEFRAME", "5m"),
            bb_size_usd: dec_or("BB_SIZE_USD", "500"),
            bb_cooldown_candles: usize_or("BB_COOLDOWN_CANDLES", 3),
            fee_rate_bps: u32_or("FEE_RATE_BPS", 156),
            maker_mode: cli.maker_mode,
            heartbeat_interval_secs: u64_or("HEARTBEAT_INTERVAL_SECS", 10),
            ws_book_max_stale_secs: u64_or("WS_BOOK_MAX_STALE_SECS", 30),
            unified_bb_weight: f64_or("UNIFIED_BB_WEIGHT", 0.4),
            unified_ma_weight: f64_or("UNIFIED_MA_WEIGHT", 0.4),
            updown_enabled: bool_or("UPDOWN_ENABLED", true),
            updown_only: bool_or("UPDOWN_ONLY", false),
            log_level: env_or("LOG_LEVEL", "info"),
            stale_feed_timeout_secs: u64_or("STALE_FEED_TIMEOUT_SECS", 30),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn dec_or(key: &str, default: &str) -> Decimal {
    let val = env_or(key, default);
    Decimal::from_str(&val).unwrap_or_else(|_| Decimal::from_str(default).unwrap())
}

fn u64_or(key: &str, default: u64) -> u64 {
    env_or(key, &default.to_string()).parse().unwrap_or(default)
}

fn u32_or(key: &str, default: u32) -> u32 {
    env_or(key, &default.to_string()).parse().unwrap_or(default)
}

fn usize_or(key: &str, default: usize) -> usize {
    env_or(key, &default.to_string()).parse().unwrap_or(default)
}

fn f64_or(key: &str, default: f64) -> f64 {
    env_or(key, &default.to_string()).parse().unwrap_or(default)
}

fn bool_or(key: &str, default: bool) -> bool {
    env_or(key, &default.to_string()).parse().unwrap_or(default)
}
