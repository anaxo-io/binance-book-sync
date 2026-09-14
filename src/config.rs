//! Configuration for the synchroniser.

use std::time::Duration;

/// How often Binance pushes diff updates for a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpdateSpeed {
    /// `@depth@100ms` — the usual choice.
    #[default]
    Fast,
    /// `@depth@1000ms`.
    Slow,
}

impl UpdateSpeed {
    pub(crate) fn suffix(self) -> &'static str {
        match self {
            UpdateSpeed::Fast => "100ms",
            UpdateSpeed::Slow => "1000ms",
        }
    }
}

/// Settings for connecting to Binance and keeping a book in sync.
///
/// [`Default`] targets Binance spot with sensible retry behaviour.
///
/// ```
/// use binance_book_sync::SyncConfig;
///
/// let config = SyncConfig {
///     snapshot_depth: 1000,
///     ..SyncConfig::default()
/// };
/// assert_eq!(config.rest_url, "https://api.binance.com");
/// ```
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Base REST URL, without a trailing slash.
    pub rest_url: String,
    /// WebSocket endpoint for raw streams.
    pub ws_url: String,
    /// Levels requested per snapshot. Binance accepts 5, 10, 20, 50, 100, 500, 1000, 5000;
    /// the larger values cost more request weight.
    pub snapshot_depth: u16,
    /// How often diff updates arrive.
    pub update_speed: UpdateSpeed,
    /// Attempts made before a snapshot fetch is reported as failed.
    pub snapshot_retries: u32,
    /// Delay before the first snapshot retry; subsequent retries back off linearly.
    pub snapshot_retry_delay: Duration,
    /// Delay before the first reconnection attempt.
    pub reconnect_initial_backoff: Duration,
    /// Ceiling for reconnection backoff.
    pub reconnect_max_backoff: Duration,
    /// Updates buffered while waiting for a snapshot before the oldest are dropped.
    ///
    /// A buffer that keeps growing means snapshots are failing; dropping is preferable to
    /// unbounded memory, and the resulting gap is reported.
    pub max_buffered_updates: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            rest_url: "https://api.binance.com".to_string(),
            ws_url: "wss://stream.binance.com/ws".to_string(),
            snapshot_depth: 1000,
            update_speed: UpdateSpeed::default(),
            snapshot_retries: 3,
            snapshot_retry_delay: Duration::from_secs(1),
            reconnect_initial_backoff: Duration::from_secs(1),
            reconnect_max_backoff: Duration::from_secs(60),
            max_buffered_updates: 1000,
        }
    }
}
