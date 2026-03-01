use crate::types::market::Exchange;

/// Normalize exchange-specific symbols to canonical format (e.g., "BTC-USD").
pub fn normalize_symbol(exchange: Exchange, raw: &str) -> String {
    match exchange {
        Exchange::Coinbase => normalize_coinbase(raw),
        Exchange::Binance => normalize_binance(raw),
        Exchange::Okx => normalize_okx(raw),
        Exchange::Chainlink => raw.to_string(), // already canonical BTC-USD
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
        Exchange::Coinbase => format!("{}-{}", base, quote),
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
        Exchange::Chainlink => canonical.to_string(), // identity
    }
}

fn normalize_coinbase(raw: &str) -> String {
    // Coinbase already uses "BTC-USD"
    raw.to_string()
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
    fn test_coinbase_normalization() {
        assert_eq!(normalize_symbol(Exchange::Coinbase, "BTC-USD"), "BTC-USD");
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
        assert_eq!(to_exchange_symbol(Exchange::Coinbase, "BTC-USD"), "BTC-USD");
        assert_eq!(to_exchange_symbol(Exchange::Binance, "BTC-USD"), "btcusdt");
        assert_eq!(to_exchange_symbol(Exchange::Okx, "BTC-USD"), "BTC-USDT");
    }
}
