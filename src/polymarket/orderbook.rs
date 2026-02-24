use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

/// Extract implied probability from a Polymarket orderbook response.
pub fn implied_probability_from_book(book: &Value) -> Option<(Decimal, Decimal)> {
    // Polymarket book format: {"bids": [...], "asks": [...]}
    let bids = book.get("bids")?.as_array()?;
    let asks = book.get("asks")?.as_array()?;

    let best_bid = bids
        .first()
        .and_then(|b| b.get("price"))
        .and_then(|p| p.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    let best_ask = asks
        .first()
        .and_then(|a| a.get("price"))
        .and_then(|p| p.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ONE);

    // Implied prob is midpoint of best bid/ask
    let implied_yes = (best_bid + best_ask) / Decimal::TWO;
    let implied_no = Decimal::ONE - implied_yes;

    Some((implied_yes, implied_no))
}

/// Calculate market depth at a given price level.
pub fn depth_at_price(book: &Value, side: &str, price: Decimal) -> Decimal {
    let levels = match book.get(side).and_then(|s| s.as_array()) {
        Some(levels) => levels,
        None => return Decimal::ZERO,
    };

    levels
        .iter()
        .filter_map(|level| {
            let p = level
                .get("price")
                .and_then(|p| p.as_str())
                .and_then(|s| Decimal::from_str(s).ok())?;
            let size = level
                .get("size")
                .and_then(|s| s.as_str())
                .and_then(|s| Decimal::from_str(s).ok())?;

            if (side == "bids" && p >= price) || (side == "asks" && p <= price) {
                Some(size)
            } else {
                None
            }
        })
        .sum()
}
