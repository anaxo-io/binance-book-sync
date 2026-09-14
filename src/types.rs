//! Wire types and the events this crate emits.
//!
//! Prices and quantities are carried as `i64` scaled by 10⁹ — the `_e9` suffix on each
//! field is a reminder. Binance quotes them as decimal strings; keeping them as scaled
//! integers avoids floating-point error without forcing a particular decimal type on the
//! caller. To feed them into a book that uses its own fixed-point type, wrap the integer:
//!
//! ```
//! # use binance_book_sync::PriceLevel;
//! let level = PriceLevel { price_e9: 50_000_000_000_000, qty_e9: 1_500_000_000 };
//! assert_eq!(level.price_e9 as f64 / 1e9, 50_000.0);
//! ```

use serde::Deserialize;

use crate::error::{Error, Result};

/// Scale applied to prices and quantities: 10⁹.
pub const SCALE_E9: i64 = 1_000_000_000;

/// A price level from a snapshot or a diff update.
///
/// A quantity of zero means the level has been removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLevel {
    /// Price, scaled by 10⁹.
    pub price_e9: i64,
    /// Quantity, scaled by 10⁹. Zero removes the level.
    pub qty_e9: i64,
}

impl PriceLevel {
    /// Build a level from Binance's decimal strings.
    pub fn from_strs(price: &str, qty: &str, context: &'static str) -> Result<Self> {
        Ok(Self {
            price_e9: parse_e9(price, context)?,
            qty_e9: parse_e9(qty, context)?,
        })
    }

    /// Whether this level removes the price rather than setting a quantity.
    pub fn is_removal(&self) -> bool {
        self.qty_e9 == 0
    }
}

/// Parse a decimal string into an integer scaled by 10⁹.
///
/// Parsing is done on the string rather than via `f64` so that the nine decimal places
/// Binance can send are preserved exactly.
///
/// ```
/// # use binance_book_sync::parse_e9;
/// assert_eq!(parse_e9("50000.00", "price").unwrap(), 50_000_000_000_000);
/// assert_eq!(parse_e9("0.000000001", "price").unwrap(), 1);
/// assert!(parse_e9("nonsense", "price").is_err());
/// ```
pub fn parse_e9(value: &str, context: &'static str) -> Result<i64> {
    let invalid = || Error::Decimal {
        context,
        value: value.to_string(),
    };

    let text = value.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };

    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };

    if int_part.is_empty() && frac_part.is_empty() {
        return Err(invalid());
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    if frac_part.len() > 9 {
        // More precision than the scale can hold; Binance does not send this, and
        // silently rounding a price is worse than refusing it.
        return Err(invalid());
    }

    let integer: i64 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().map_err(|_| invalid())?
    };

    let mut fraction: i64 = if frac_part.is_empty() {
        0
    } else {
        frac_part.parse().map_err(|_| invalid())?
    };
    for _ in frac_part.len()..9 {
        fraction *= 10;
    }

    let scaled = integer
        .checked_mul(SCALE_E9)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(invalid)?;

    Ok(if negative { -scaled } else { scaled })
}

/// A depth snapshot fetched from the REST endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// `lastUpdateId` of the snapshot.
    pub last_update_id: u64,
    /// Bid levels, as sent by Binance.
    pub bids: Vec<PriceLevel>,
    /// Ask levels, as sent by Binance.
    pub asks: Vec<PriceLevel>,
}

/// A diff depth update from the WebSocket stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthUpdate {
    /// `U`: first update id covered by this event.
    pub first_update_id: u64,
    /// `u`: final update id covered by this event.
    pub last_update_id: u64,
    /// Bid changes.
    pub bids: Vec<PriceLevel>,
    /// Ask changes.
    pub asks: Vec<PriceLevel>,
}

/// What the synchroniser produces for the caller to apply to their book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepthEvent {
    /// Replace the whole book with these levels.
    ///
    /// Emitted once the snapshot has been reconciled with the buffered updates, and again
    /// after every resynchronisation.
    Snapshot {
        /// Symbol as Binance names it, for example `BTCUSDT`.
        symbol: String,
        /// `lastUpdateId` of the snapshot.
        last_update_id: u64,
        /// Bid levels.
        bids: Vec<PriceLevel>,
        /// Ask levels.
        asks: Vec<PriceLevel>,
    },

    /// Apply these changes to the book.
    Delta {
        /// Symbol as Binance names it.
        symbol: String,
        /// `u` of the event, the book's new sequence number.
        last_update_id: u64,
        /// Bid changes.
        bids: Vec<PriceLevel>,
        /// Ask changes.
        asks: Vec<PriceLevel>,
    },

    /// The stream skipped updates, so the book can no longer be trusted.
    ///
    /// A [`DepthEvent::Resync`] follows while a fresh snapshot is fetched. Stop applying
    /// deltas until the next [`DepthEvent::Snapshot`] arrives.
    Gap {
        /// Symbol as Binance names it.
        symbol: String,
        /// The update id the book expected next.
        expected: u64,
        /// The first update id actually received.
        received: u64,
    },

    /// Resynchronisation has begun; the previous book contents are stale.
    Resync {
        /// Symbol as Binance names it.
        symbol: String,
    },
}

/// Raw depth update as it appears on the wire.
#[derive(Debug, Deserialize)]
pub(crate) struct WireDepthUpdate {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub last_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    pub asks: Vec<(String, String)>,
}

/// Raw snapshot as it appears on the wire.
#[derive(Debug, Deserialize)]
pub(crate) struct WireSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

pub(crate) fn levels_from_wire(
    raw: &[(String, String)],
    context: &'static str,
) -> Result<Vec<PriceLevel>> {
    raw.iter()
        .map(|(price, qty)| PriceLevel::from_strs(price, qty, context))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_decimals() {
        assert_eq!(parse_e9("50000.00", "t").unwrap(), 50_000_000_000_000);
        assert_eq!(parse_e9("1", "t").unwrap(), SCALE_E9);
        assert_eq!(parse_e9("0", "t").unwrap(), 0);
        assert_eq!(parse_e9("0.5", "t").unwrap(), 500_000_000);
    }

    #[test]
    fn preserves_full_precision() {
        // f64 cannot hold this exactly; string parsing can.
        assert_eq!(parse_e9("0.000000001", "t").unwrap(), 1);
        assert_eq!(
            parse_e9("123456.123456789", "t").unwrap(),
            123_456_123_456_789
        );
    }

    #[test]
    fn handles_signs_and_whitespace() {
        assert_eq!(parse_e9(" 1.5 ", "t").unwrap(), 1_500_000_000);
        assert_eq!(parse_e9("+1.5", "t").unwrap(), 1_500_000_000);
        assert_eq!(parse_e9("-1.5", "t").unwrap(), -1_500_000_000);
    }

    #[test]
    fn rejects_rubbish_rather_than_defaulting_to_zero() {
        // The original implementation turned unparseable prices into 0.0, which silently
        // becomes a level removal.
        assert!(parse_e9("nonsense", "t").is_err());
        assert!(parse_e9("", "t").is_err());
        assert!(parse_e9("1.2.3", "t").is_err());
        assert!(parse_e9("1e5", "t").is_err());
        assert!(parse_e9("0.0000000001", "t").is_err()); // ten decimals, beyond the scale
    }

    #[test]
    fn level_detects_removal() {
        assert!(PriceLevel::from_strs("50000", "0", "t")
            .unwrap()
            .is_removal());
        assert!(!PriceLevel::from_strs("50000", "0.1", "t")
            .unwrap()
            .is_removal());
    }
}
