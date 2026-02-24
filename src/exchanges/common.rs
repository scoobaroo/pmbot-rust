use crate::types::market::Exchange;

/// Normalize exchange-specific symbols to canonical format (e.g., "BTC-USD").
pub fn normalize_symbol(exchange: Exchange, raw: &str) -> String {
    match exchange {
        Exchange::Kraken => normalize_kraken(raw),
        Exchange::Coinbase => normalize_coinbase(raw),
        Exchange::Bitfinex => normalize_bitfinex(raw),
        Exchange::Ndax => normalize_ndax(raw),
        Exchange::Binance => normalize_binance(raw),
        Exchange::Okx => normalize_okx(raw),
    }
}

/// Convert canonical symbol to exchange-specific format.
pub fn to_exchange_symbol(exchange: Exchange, canonical: &str) -> String {
    let parts: Vec<&str> = canonical.split('-').collect();
    if parts.len() != 2 {
        return canonical.to_string();
    }
    let (base, quote) = (parts[0], parts[1]);

    match exchange {
        Exchange::Kraken => {
            let base = match base {
                "BTC" => "XBT",
                other => other,
            };
            format!("{}/{}", base, quote)
        }
        Exchange::Coinbase => format!("{}-{}", base, quote),
        Exchange::Bitfinex => {
            let base = match base {
                "USDT" => "UST",
                other => other,
            };
            format!("t{}{}", base, quote)
        }
        Exchange::Ndax => {
            // NDAX uses InstrumentId, but for subscription we use the symbol
            format!("{}{}", base, quote)
        }
        Exchange::Binance => {
            // Binance WS subscriptions require lowercase: "btcusdt"
            let quote = match quote {
                "USD" => "USDT",
                other => other,
            };
            format!("{}{}", base, quote).to_lowercase()
        }
        Exchange::Okx => {
            // OKX uses "BTC-USDT" format
            let quote = match quote {
                "USD" => "USDT",
                other => other,
            };
            format!("{}-{}", base, quote)
        }
    }
}

fn normalize_kraken(raw: &str) -> String {
    let raw = raw.replace('/', "-");
    raw.replace("XBT", "BTC")
}

fn normalize_coinbase(raw: &str) -> String {
    // Coinbase already uses "BTC-USD"
    raw.to_string()
}

fn normalize_bitfinex(raw: &str) -> String {
    // "tBTCUSD" → "BTC-USD"
    let s = raw.strip_prefix('t').unwrap_or(raw);
    if s.len() == 6 {
        let (base, quote) = s.split_at(3);
        let base = match base {
            "UST" => "USDT",
            other => other,
        };
        format!("{}-{}", base, quote)
    } else if s.contains(':') {
        let parts: Vec<&str> = s.split(':').collect();
        format!("{}-{}", parts[0], parts.get(1).unwrap_or(&"USD"))
    } else {
        s.to_string()
    }
}

fn normalize_ndax(raw: &str) -> String {
    // "BTCUSD" → "BTC-USD"
    if raw.len() == 6 {
        let (base, quote) = raw.split_at(3);
        format!("{}-{}", base, quote)
    } else {
        raw.to_string()
    }
}

fn normalize_binance(raw: &str) -> String {
    // "BTCUSDT" or "btcusdt" → "BTC-USD"
    let upper = raw.to_uppercase();
    for suffix in &["USDT", "BUSD", "USD"] {
        if let Some(base) = upper.strip_suffix(suffix) {
            return format!("{}-USD", base);
        }
    }
    upper
}

fn normalize_okx(raw: &str) -> String {
    // "BTC-USDT" → "BTC-USD", "ETH-USDT" → "ETH-USD"
    if let Some((base, quote)) = raw.split_once('-') {
        let quote = match quote {
            "USDT" | "BUSD" => "USD",
            other => other,
        };
        format!("{}-{}", base, quote)
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_normalization() {
        assert_eq!(normalize_symbol(Exchange::Kraken, "XBT/USD"), "BTC-USD");
        assert_eq!(normalize_symbol(Exchange::Kraken, "ETH/USD"), "ETH-USD");
    }

    #[test]
    fn test_coinbase_normalization() {
        assert_eq!(normalize_symbol(Exchange::Coinbase, "BTC-USD"), "BTC-USD");
    }

    #[test]
    fn test_bitfinex_normalization() {
        assert_eq!(normalize_symbol(Exchange::Bitfinex, "tBTCUSD"), "BTC-USD");
        assert_eq!(normalize_symbol(Exchange::Bitfinex, "tETHUSD"), "ETH-USD");
    }

    #[test]
    fn test_ndax_normalization() {
        assert_eq!(normalize_symbol(Exchange::Ndax, "BTCUSD"), "BTC-USD");
    }

    #[test]
    fn test_binance_normalization() {
        assert_eq!(normalize_symbol(Exchange::Binance, "BTCUSDT"), "BTC-USD");
        assert_eq!(normalize_symbol(Exchange::Binance, "ETHUSDT"), "ETH-USD");
        assert_eq!(normalize_symbol(Exchange::Binance, "btcusdt"), "BTC-USD");
        assert_eq!(normalize_symbol(Exchange::Binance, "BTCBUSD"), "BTC-USD");
        assert_eq!(normalize_symbol(Exchange::Binance, "BTCUSD"), "BTC-USD");
    }

    #[test]
    fn test_binance_to_exchange_symbol() {
        assert_eq!(to_exchange_symbol(Exchange::Binance, "BTC-USD"), "btcusdt");
        assert_eq!(to_exchange_symbol(Exchange::Binance, "ETH-USD"), "ethusdt");
    }

    #[test]
    fn test_okx_normalization() {
        assert_eq!(normalize_symbol(Exchange::Okx, "BTC-USDT"), "BTC-USD");
        assert_eq!(normalize_symbol(Exchange::Okx, "ETH-USDT"), "ETH-USD");
        assert_eq!(normalize_symbol(Exchange::Okx, "BTC-USD"), "BTC-USD");
    }

    #[test]
    fn test_okx_to_exchange_symbol() {
        assert_eq!(to_exchange_symbol(Exchange::Okx, "BTC-USD"), "BTC-USDT");
        assert_eq!(to_exchange_symbol(Exchange::Okx, "ETH-USD"), "ETH-USDT");
    }

    #[test]
    fn test_to_exchange_symbol() {
        assert_eq!(to_exchange_symbol(Exchange::Kraken, "BTC-USD"), "XBT/USD");
        assert_eq!(to_exchange_symbol(Exchange::Coinbase, "BTC-USD"), "BTC-USD");
        assert_eq!(to_exchange_symbol(Exchange::Bitfinex, "BTC-USD"), "tBTCUSD");
        assert_eq!(to_exchange_symbol(Exchange::Ndax, "BTC-USD"), "BTCUSD");
        assert_eq!(to_exchange_symbol(Exchange::Binance, "BTC-USD"), "btcusdt");
        assert_eq!(to_exchange_symbol(Exchange::Okx, "BTC-USD"), "BTC-USDT");
    }
}
