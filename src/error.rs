//! Error type for synchronising a Binance order book.

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Something went wrong fetching, parsing, or streaming Binance depth data.
///
/// The two transport errors are boxed: `reqwest::Error` and `tungstenite::Error` are large
/// enough that embedding them would make every `Result` in the crate expensive to move.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The REST snapshot request failed at the transport level.
    #[error("snapshot request failed: {0}")]
    Http(#[source] Box<reqwest::Error>),

    /// Binance answered the snapshot request with a non-success status.
    #[error("snapshot request returned {status}")]
    SnapshotStatus {
        /// HTTP status returned by Binance.
        status: u16,
        /// Response body, truncated for logging.
        body: String,
    },

    /// Binance rate-limited the snapshot request and the retries were exhausted.
    #[error("rate limited by Binance after {attempts} attempts")]
    RateLimited {
        /// Number of attempts made.
        attempts: u32,
    },

    /// A JSON payload did not match the expected shape.
    #[error("could not parse {context}: {source}")]
    Json {
        /// What was being parsed, for example `depth snapshot`.
        context: &'static str,
        /// The underlying serde error.
        #[source]
        source: serde_json::Error,
    },

    /// A price or quantity string was not a number.
    #[error("invalid decimal {value:?} in {context}")]
    Decimal {
        /// What was being parsed, for example `bid price`.
        context: &'static str,
        /// The offending text.
        value: String,
    },

    /// The WebSocket connection failed or dropped.
    #[error("websocket error: {0}")]
    WebSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),

    /// Binance rejected the subscription request.
    #[error("binance rejected the subscription: {0}")]
    Subscription(String),
}

impl From<reqwest::Error> for Error {
    fn from(source: reqwest::Error) -> Self {
        Self::Http(Box::new(source))
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(source: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_stays_small_enough_to_pass_around() {
        // The point of boxing the transport errors: keep Result cheap to move.
        assert!(
            std::mem::size_of::<Error>() <= 48,
            "Error grew to {} bytes",
            std::mem::size_of::<Error>()
        );
    }

    #[test]
    fn messages_name_what_failed() {
        let err = Error::SnapshotStatus {
            status: 500,
            body: "boom".to_string(),
        };
        assert_eq!(err.to_string(), "snapshot request returned 500");

        let err = Error::Decimal {
            context: "bid price",
            value: "nonsense".to_string(),
        };
        assert_eq!(err.to_string(), "invalid decimal \"nonsense\" in bid price");

        let err = Error::RateLimited { attempts: 3 };
        assert_eq!(err.to_string(), "rate limited by Binance after 3 attempts");
    }
}
