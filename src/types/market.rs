use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Coinbase,
    Binance,
    Okx,
    Chainlink,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::Coinbase => write!(f, "Coinbase"),
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Okx => write!(f, "OKX"),
            Exchange::Chainlink => write!(f, "Chainlink"),
        }
    }
}

/// A single price update from an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTick {
    pub exchange: Exchange,
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub last: Decimal,
    pub volume_24h: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl MarketTick {
    pub fn mid(&self) -> Decimal {
        (self.bid + self.ask) / Decimal::TWO
    }

    pub fn spread(&self) -> Decimal {
        self.ask - self.bid
    }
}

/// Aggregated view across exchanges for a single symbol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedPrice {
    pub symbol: String,
    pub vwap: Decimal,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub best_bid_exchange: Exchange,
    pub best_ask_exchange: Exchange,
    pub spread: Decimal,
    pub volatility: f64,
    pub num_feeds: usize,
    pub timestamp: DateTime<Utc>,
    /// Chainlink oracle price, if available. Used for UpDown market resolution alignment.
    pub oracle_price: Option<Decimal>,
    /// Trade flow imbalance from Binance @aggTrade: (buy_vol - sell_vol) / total, range [-1, 1].
    pub trade_flow_imbalance: f64,
    /// Rolling 5-min buy-side volume from Binance aggregate trades.
    pub recent_buy_volume: f64,
    /// Rolling 5-min sell-side volume from Binance aggregate trades.
    pub recent_sell_volume: f64,
    /// Current funding rate from Binance Futures @markPrice (updated every 1s).
    pub funding_rate: Option<f64>,
    /// Binance Futures mark price.
    pub mark_price: Option<Decimal>,
    /// Binance spot mid-price, if available.
    pub binance_mid: Option<Decimal>,
}

/// Whether a market is bullish ("reach $X") or bearish ("dip to $X").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketDirection {
    Bullish, // "Will X reach $Y" — P(S > K)
    Bearish, // "Will X dip to $Y" — P(S < K)
}

/// What kind of Polymarket crypto market this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketType {
    /// Traditional strike-based: "Will BTC reach $100k?"
    StrikeAbove,
    /// Rolling 5-minute up/down: resolves Up if end_price >= start_price.
    UpDown {
        window_start_ts: i64,
        window_secs: u64,
    },
    /// General binary market (sports, politics, weather, etc.) — penny-picking at 95%+.
    General {
        end_date_ts: i64,
    },
}

/// Polymarket market representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketMarket {
    pub condition_id: String,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub question: String,
    pub underlying_symbol: String,
    pub strike: Decimal,
    pub expiry: DateTime<Utc>,
    pub implied_prob_yes: Decimal,
    pub implied_prob_no: Decimal,
    pub direction: MarketDirection,
    pub market_type: MarketType,
}

/// A price level in an order book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Snapshot of an order book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: DateTime<Utc>,
}
