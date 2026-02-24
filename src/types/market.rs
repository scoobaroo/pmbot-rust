use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Kraken,
    Coinbase,
    Bitfinex,
    Ndax,
    Binance,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::Kraken => write!(f, "Kraken"),
            Exchange::Coinbase => write!(f, "Coinbase"),
            Exchange::Bitfinex => write!(f, "Bitfinex"),
            Exchange::Ndax => write!(f, "NDAX"),
            Exchange::Binance => write!(f, "Binance"),
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
