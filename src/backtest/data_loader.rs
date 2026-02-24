use crate::types::market::{Exchange, MarketTick};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum DataLoadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    #[error("parse error at row {row}: {msg}")]
    Parse { row: usize, msg: String },
}

/// CSV format:
/// timestamp,exchange,symbol,bid,ask,last,volume_24h
///
/// Example:
/// 2024-01-15T10:30:00Z,Kraken,BTC-USD,42500.50,42501.00,42500.75,1234.56
pub fn load_ticks(path: &Path) -> Result<Vec<MarketTick>, DataLoadError> {
    info!(path = %path.display(), "loading backtest data");

    let mut reader = csv::Reader::from_path(path)?;
    let mut ticks = Vec::new();

    for (i, result) in reader.records().enumerate() {
        let record = result?;
        let row = i + 2; // 1-indexed, plus header

        let timestamp: DateTime<Utc> = record
            .get(0)
            .ok_or_else(|| DataLoadError::Parse {
                row,
                msg: "missing timestamp".to_string(),
            })?
            .parse()
            .map_err(|e| DataLoadError::Parse {
                row,
                msg: format!("bad timestamp: {}", e),
            })?;

        let exchange = match record.get(1).unwrap_or("Kraken") {
            "Kraken" => Exchange::Kraken,
            "Coinbase" => Exchange::Coinbase,
            "Bitfinex" => Exchange::Bitfinex,
            "NDAX" => Exchange::Ndax,
            "Binance" => Exchange::Binance,
            "OKX" => Exchange::Okx,
            other => {
                return Err(DataLoadError::Parse {
                    row,
                    msg: format!("unknown exchange: {}", other),
                })
            }
        };

        let symbol = record
            .get(2)
            .ok_or_else(|| DataLoadError::Parse {
                row,
                msg: "missing symbol".to_string(),
            })?
            .to_string();

        let bid = parse_decimal(&record, 3, row, "bid")?;
        let ask = parse_decimal(&record, 4, row, "ask")?;
        let last = parse_decimal(&record, 5, row, "last")?;
        let volume = parse_decimal(&record, 6, row, "volume").unwrap_or(Decimal::ZERO);

        ticks.push(MarketTick {
            exchange,
            symbol,
            bid,
            ask,
            last,
            volume_24h: volume,
            timestamp,
        });
    }

    info!(count = ticks.len(), "loaded backtest ticks");
    Ok(ticks)
}

fn parse_decimal(
    record: &csv::StringRecord,
    index: usize,
    row: usize,
    field: &str,
) -> Result<Decimal, DataLoadError> {
    let s = record.get(index).ok_or_else(|| DataLoadError::Parse {
        row,
        msg: format!("missing {}", field),
    })?;
    Decimal::from_str(s).map_err(|e| DataLoadError::Parse {
        row,
        msg: format!("bad {}: {}", field, e),
    })
}
