use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Timeframe {
    M1,
    M5,
    M15,
    H1,
}

impl Timeframe {
    pub fn duration(&self) -> chrono::Duration {
        match self {
            Timeframe::M1 => chrono::Duration::minutes(1),
            Timeframe::M5 => chrono::Duration::minutes(5),
            Timeframe::M15 => chrono::Duration::minutes(15),
            Timeframe::H1 => chrono::Duration::hours(1),
        }
    }

    pub fn duration_secs(&self) -> i64 {
        match self {
            Timeframe::M1 => 60,
            Timeframe::M5 => 300,
            Timeframe::M15 => 900,
            Timeframe::H1 => 3600,
        }
    }

    /// Parse from string like "1m", "5m", "15m", "1h".
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "1m" => Some(Timeframe::M1),
            "5m" => Some(Timeframe::M5),
            "15m" => Some(Timeframe::M15),
            "1h" => Some(Timeframe::H1),
            _ => None,
        }
    }
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Timeframe::M1 => write!(f, "1m"),
            Timeframe::M5 => write!(f, "5m"),
            Timeframe::M15 => write!(f, "15m"),
            Timeframe::H1 => write!(f, "1h"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Candle {
    pub symbol: String,
    pub timeframe: Timeframe,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub tick_count: u64,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
}
