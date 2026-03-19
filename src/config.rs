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
    Discount,
    MaCrossover,
    BollingerBands,
    Unified,
    HedgedLp,
    Orb,
}

#[derive(Parser, Debug)]
#[command(name = "pmbot", about = "Polymarket trading bot")]
pub struct Cli {
    #[arg(long, value_enum, default_value = "paper")]
    pub mode: RunMode,

    #[arg(long, value_enum, default_value = "unified", num_args = 1..)]
    pub strategy: Vec<StrategyName>,

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
    pub strategies: Vec<StrategyName>,
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
    pub unified_book_weight: f64,

    // Arb fallback parameters
    pub arb_threshold: Decimal,
    pub min_arb_profit: Decimal,
    pub arb_size_usd: Decimal,

    // Up/Down 5-minute markets
    pub updown_enabled: bool,
    pub updown_only: bool,

    // General market scanner
    pub general_markets_enabled: bool,
    pub general_poll_interval_secs: u64,
    pub general_min_liquidity: u64,

    // Discount strategy
    pub discount_rate: f64,

    // ML prediction service
    pub ml_enabled: bool,
    pub ml_server_url: String,
    pub ml_timeout_ms: u64,
    pub ml_signal_weight: f64,

    // RL (PPO) agent
    pub rl_enabled: bool,
    pub rl_server_url: String,
    pub rl_timeout_ms: u64,
    pub rl_capital_usd: f64,
    pub rl_log_transitions: bool,
    pub rl_transition_log_path: String,

    // Copy-trade bridge (uses separate Polymarket account)
    pub copy_trade_enabled: bool,
    pub copy_trade_private_key: String,
    pub copy_trade_funder_address: String,
    pub copy_trade_targets: Vec<String>,
    pub copy_trade_poll_interval_secs: u64,
    pub copy_trade_size_ratio: f64,
    pub copy_trade_max_position_usd: f64,

    // Web dashboard
    pub web_enabled: bool,
    pub web_port: u16,

    // Aggregator price weighting
    /// Multiplier on Binance volume in VWAP calc (e.g. 5.0 = 5× Binance weight).
    /// Higher values make the bot track Binance price moves faster.
    pub binance_weight_multiplier: f64,
    /// Weight of Chainlink oracle in final VWAP blend (0.0 = ignore, 0.5 = equal weight).
    pub chainlink_blend_weight: f64,

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
            strategies: cli.strategy.clone(),
            backtest_file: cli.backtest_file.clone(),
            coinbase_api_key: env_or("COINBASE_API_KEY", ""),
            coinbase_api_secret: env_or("COINBASE_API_SECRET", ""),
            polymarket_private_key: env_or("POLYMARKET_PRIVATE_KEY", ""),
            polygon_rpc_url: env_or("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com"),
            ethereum_rpc_url: env_or("ETHEREUM_RPC_URL", "https://eth.llamarpc.com"),
            symbols: env_or("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD,BNB-USD,HYPE-USD")
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            min_edge_threshold: dec_or("MIN_EDGE_THRESHOLD", "0.04"),
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
            fee_rate_bps: u32_or("FEE_RATE_BPS", 1000),
            maker_mode: cli.maker_mode,
            heartbeat_interval_secs: u64_or("HEARTBEAT_INTERVAL_SECS", 10),
            ws_book_max_stale_secs: u64_or("WS_BOOK_MAX_STALE_SECS", 30),
            unified_bb_weight: f64_or("UNIFIED_BB_WEIGHT", 0.4),
            unified_ma_weight: f64_or("UNIFIED_MA_WEIGHT", 0.4),
            unified_book_weight: f64_or("UNIFIED_BOOK_WEIGHT", 0.3),
            arb_threshold: dec_or("ARB_THRESHOLD", "0.97"),
            min_arb_profit: dec_or("MIN_ARB_PROFIT", "0.03"),
            arb_size_usd: dec_or("ARB_SIZE_USD", "5"),
            updown_enabled: bool_or("UPDOWN_ENABLED", true),
            updown_only: bool_or("UPDOWN_ONLY", true),
            general_markets_enabled: bool_or("GENERAL_MARKETS_ENABLED", true),
            general_poll_interval_secs: u64_or("GENERAL_POLL_INTERVAL_SECS", 30),
            general_min_liquidity: u64_or("GENERAL_MIN_LIQUIDITY", 5000),
            discount_rate: f64_or("DISCOUNT_RATE", 0.1),
            ml_enabled: bool_or("ML_ENABLED", false),
            ml_server_url: env_or("ML_SERVER_URL", "http://localhost:8089"),
            ml_timeout_ms: u64_or("ML_TIMEOUT_MS", 50),
            ml_signal_weight: f64_or("ML_SIGNAL_WEIGHT", 0.5),
            copy_trade_enabled: bool_or("COPY_TRADE_ENABLED", false),
            copy_trade_private_key: env_or("POLYMARKET_COPYTRADE_PRIVATE_KEY", ""),
            copy_trade_funder_address: env_or("COPYTRADE_FUNDER_ADDRESS", ""),
            copy_trade_targets: env_or("COPY_TRADE_TARGETS", "")
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_lowercase())
                .collect(),
            copy_trade_poll_interval_secs: u64_or("COPY_TRADE_POLL_INTERVAL_SECS", 5),
            copy_trade_size_ratio: f64_or("COPY_TRADE_SIZE_RATIO", 0.1),
            copy_trade_max_position_usd: f64_or("COPY_TRADE_MAX_POSITION_USD", 50.0),
            web_enabled: bool_or("WEB_ENABLED", false),
            web_port: u64_or("WEB_PORT", 3000) as u16,
            rl_enabled: bool_or("RL_ENABLED", false),
            rl_server_url: env_or("RL_SERVER_URL", "http://localhost:8090"),
            rl_timeout_ms: u64_or("RL_TIMEOUT_MS", 100),
            rl_capital_usd: f64_or("RL_CAPITAL_USD", 1000.0),
            rl_log_transitions: bool_or("RL_LOG_TRANSITIONS", true),
            rl_transition_log_path: env_or("RL_TRANSITION_LOG", "data/rl_transitions.jsonl"),
            binance_weight_multiplier: f64_or("BINANCE_WEIGHT_MULTIPLIER", 5.0),
            chainlink_blend_weight: f64_or("CHAINLINK_BLEND_WEIGHT", 0.1),
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
