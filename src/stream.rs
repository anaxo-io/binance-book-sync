//! Driving the state machine from a live Binance connection.

use std::collections::HashMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::config::SyncConfig;
use crate::error::{Error, Result};
use crate::rest::SnapshotFetcher;
use crate::sync::BookSync;
use crate::types::{levels_from_wire, DepthEvent, DepthUpdate, WireDepthUpdate};

/// Connects to Binance, keeps one book per symbol in sync, and emits [`DepthEvent`]s.
///
/// Reconnects with exponential backoff. After a reconnect every book is rebuilt from a
/// fresh snapshot, because updates missed while disconnected cannot be recovered; a
/// [`DepthEvent::Resync`] is emitted for each symbol so the caller stops trusting stale
/// state.
pub struct DepthStream {
    symbols: Vec<String>,
    config: SyncConfig,
    fetcher: SnapshotFetcher,
}

impl DepthStream {
    /// Build a stream for `symbols`, named exactly as Binance names them, for example
    /// `BTCUSDT`. Symbols are passed through unchanged; only the stream name is
    /// lowercased, as Binance requires.
    pub fn new(symbols: Vec<String>, config: SyncConfig) -> Result<Self> {
        let fetcher = SnapshotFetcher::new(&config)?;
        Ok(Self {
            symbols,
            config,
            fetcher,
        })
    }

    /// Run the stream, sending events to the returned receiver.
    ///
    /// The task ends when the receiver is dropped. `buffer` bounds the channel: if the
    /// consumer falls behind, sends block rather than growing without limit.
    pub fn spawn(self, buffer: usize) -> mpsc::Receiver<DepthEvent> {
        let (tx, rx) = mpsc::channel(buffer);
        tokio::spawn(async move {
            if let Err(error) = self.run(tx).await {
                tracing::error!(%error, "depth stream stopped");
            }
        });
        rx
    }

    async fn run(self, tx: mpsc::Sender<DepthEvent>) -> Result<()> {
        let mut backoff = self.config.reconnect_initial_backoff;

        loop {
            let mut connected = false;
            match self.session(&tx, &mut connected).await {
                Ok(()) => {
                    tracing::info!("stream closed");
                }
                Err(error) => {
                    tracing::warn!(%error, "stream failed");
                }
            }

            if tx.is_closed() {
                return Ok(());
            }

            // A connection that was actually established starts the backoff over, so a
            // long-lived stream that drops once does not reconnect at the ceiling. Only
            // repeated failures to connect at all are backed off.
            backoff = if connected {
                self.config.reconnect_initial_backoff
            } else {
                next_backoff(backoff, self.config.reconnect_max_backoff)
            };

            tracing::info!(backoff_ms = backoff.as_millis() as u64, "reconnecting");
            tokio::time::sleep(backoff).await;
        }
    }

    /// One connection's lifetime. Returns when the socket closes.
    async fn session(&self, tx: &mpsc::Sender<DepthEvent>, connected: &mut bool) -> Result<()> {
        let (stream, _) = connect_async(&self.config.ws_url).await?;
        *connected = true;

        let (mut write, mut read) = stream.split();

        write
            .send(Message::Text(self.subscribe_message().into()))
            .await?;

        let mut books: HashMap<String, BookSync> = self
            .symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    BookSync::with_buffer_limit(symbol.clone(), self.config.max_buffered_updates),
                )
            })
            .collect();

        for symbol in &self.symbols {
            if tx
                .send(DepthEvent::Resync {
                    symbol: symbol.clone(),
                })
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        while let Some(message) = read.next().await {
            let text = match message? {
                Message::Text(text) => text,
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await?;
                    continue;
                }
                Message::Close(_) => return Ok(()),
                _ => continue,
            };

            let events = self.handle_text(text.as_str(), &mut books).await?;
            for event in events {
                if tx.send(event).await.is_err() {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Parse one message and advance the relevant book.
    async fn handle_text(
        &self,
        text: &str,
        books: &mut HashMap<String, BookSync>,
    ) -> Result<Vec<DepthEvent>> {
        let json: serde_json::Value = match serde_json::from_str(text) {
            Ok(json) => json,
            Err(source) => {
                return Err(Error::Json {
                    context: "stream message",
                    source,
                })
            }
        };

        // Subscription acknowledgements and errors carry an `id` rather than an event type.
        if json.get("id").is_some() {
            if let Some(message) = json.get("msg").and_then(|m| m.as_str()) {
                return Err(Error::Subscription(message.to_string()));
            }
            return Ok(Vec::new());
        }

        if json.get("e").and_then(|e| e.as_str()) != Some("depthUpdate") {
            return Ok(Vec::new());
        }

        let wire: WireDepthUpdate = serde_json::from_value(json).map_err(|source| Error::Json {
            context: "depth update",
            source,
        })?;

        let Some(book) = books.get_mut(&wire.symbol) else {
            return Ok(Vec::new());
        };

        let update = DepthUpdate {
            first_update_id: wire.first_update_id,
            last_update_id: wire.last_update_id,
            bids: levels_from_wire(&wire.bids, "update bid")?,
            asks: levels_from_wire(&wire.asks, "update ask")?,
        };

        let mut events = book.apply_update(update);

        if book.needs_snapshot() {
            // Fetching inline pauses reading; Binance buffers for us, and the alternative
            // (fetching concurrently) would let updates race ahead of the snapshot they
            // are meant to be reconciled against.
            match self.fetcher.fetch(book.symbol()).await {
                Ok(snapshot) => events.extend(book.apply_snapshot(snapshot)),
                Err(error) => {
                    tracing::warn!(symbol = book.symbol(), %error, "snapshot fetch failed");
                    book.snapshot_failed();
                }
            }
        }

        Ok(events)
    }

    fn subscribe_message(&self) -> String {
        let params: Vec<String> = self
            .symbols
            .iter()
            .map(|symbol| {
                format!(
                    "{}@depth@{}",
                    symbol.to_lowercase(),
                    self.config.update_speed.suffix()
                )
            })
            .collect();

        serde_json::json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1,
        })
        .to_string()
    }
}

/// Backoff sequence used between reconnection attempts, exposed for testing.
pub(crate) fn next_backoff(current: Duration, max: Duration) -> Duration {
    (current * 2).min(max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UpdateSpeed;

    fn stream(symbols: &[&str], speed: UpdateSpeed) -> DepthStream {
        DepthStream::new(
            symbols.iter().map(|s| s.to_string()).collect(),
            SyncConfig {
                update_speed: speed,
                ..SyncConfig::default()
            },
        )
        .expect("building a client with default config cannot fail")
    }

    #[test]
    fn subscribe_message_lowercases_only_the_stream_name() {
        let message = stream(&["BTCUSDT", "ETHUSDT"], UpdateSpeed::Fast).subscribe_message();
        assert!(message.contains("btcusdt@depth@100ms"));
        assert!(message.contains("ethusdt@depth@100ms"));
        assert!(message.contains("\"method\":\"SUBSCRIBE\""));
    }

    #[test]
    fn subscribe_message_honours_update_speed() {
        let message = stream(&["BTCUSDT"], UpdateSpeed::Slow).subscribe_message();
        assert!(message.contains("btcusdt@depth@1000ms"));
    }

    #[test]
    fn symbols_are_not_rewritten() {
        // The original implementation rewrote BTCUSD to btcusdt and stripped USD suffixes,
        // silently subscribing to a different instrument than the caller asked for.
        let message = stream(&["BTCUSD"], UpdateSpeed::Fast).subscribe_message();
        assert!(message.contains("btcusd@depth@100ms"));
        assert!(!message.contains("btcusdt"));
    }

    #[test]
    fn backoff_doubles_up_to_the_ceiling() {
        let max = Duration::from_secs(60);
        assert_eq!(
            next_backoff(Duration::from_secs(1), max),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(16), max),
            Duration::from_secs(32)
        );
        assert_eq!(next_backoff(Duration::from_secs(40), max), max);
        assert_eq!(next_backoff(max, max), max);
    }
}
