//! The synchronisation state machine.
//!
//! This module contains no I/O. It is fed updates and snapshots and returns the events a
//! caller should apply, which makes the awkward part — deciding what to keep, what to
//! discard, and when the book is no longer trustworthy — directly testable.
//!
//! The procedure is Binance's [How to manage a local order book correctly][docs]:
//!
//! 1. Buffer the diff updates arriving on the stream.
//! 2. Fetch a depth snapshot.
//! 3. Drop buffered updates whose `u` is at or below the snapshot's `lastUpdateId`.
//! 4. The first update applied must straddle the snapshot: `U <= lastUpdateId + 1 <= u`.
//! 5. From then on each update must continue the sequence: `U == previous u + 1`.
//!
//! Any violation of 4 or 5 means updates were missed, so the book is abandoned and rebuilt
//! from a fresh snapshot.
//!
//! [docs]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly

use crate::types::{DepthEvent, DepthUpdate, Snapshot};

/// Where a book is in the synchronisation procedure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Updates are being buffered until a usable snapshot arrives.
    AwaitingSnapshot,
    /// The book is in sync; updates apply directly.
    Live,
}

/// Tracks one symbol's synchronisation state.
///
/// ```
/// use binance_book_sync::{BookSync, DepthEvent, DepthUpdate, PriceLevel, Snapshot, SyncState};
///
/// let mut sync = BookSync::new("BTCUSDT");
/// assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
///
/// // Updates arriving before the snapshot are buffered, not emitted.
/// let events = sync.apply_update(DepthUpdate {
///     first_update_id: 100,
///     last_update_id: 105,
///     bids: vec![PriceLevel { price_e9: 50_000_000_000_000, qty_e9: 1_000_000_000 }],
///     asks: vec![],
/// });
/// assert!(events.is_empty());
/// assert!(sync.needs_snapshot());
///
/// // A snapshot that straddles the buffered update puts the book live.
/// let events = sync.apply_snapshot(Snapshot {
///     last_update_id: 99,
///     bids: vec![],
///     asks: vec![],
/// });
/// assert!(matches!(events[0], DepthEvent::Snapshot { .. }));
/// assert!(matches!(events[1], DepthEvent::Delta { .. }));
/// assert_eq!(sync.state(), SyncState::Live);
/// ```
#[derive(Debug)]
pub struct BookSync {
    symbol: String,
    state: SyncState,
    last_update_id: Option<u64>,
    buffered: Vec<DepthUpdate>,
    max_buffered: usize,
}

impl BookSync {
    /// Track `symbol`, buffering up to 1000 updates while awaiting a snapshot.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self::with_buffer_limit(symbol, 1000)
    }

    /// Track `symbol` with an explicit buffer limit.
    pub fn with_buffer_limit(symbol: impl Into<String>, max_buffered: usize) -> Self {
        Self {
            symbol: symbol.into(),
            state: SyncState::AwaitingSnapshot,
            last_update_id: None,
            buffered: Vec::new(),
            max_buffered: max_buffered.max(1),
        }
    }

    /// The symbol being tracked, as Binance names it.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Current state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// The book's sequence number, once live.
    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    /// Number of updates currently buffered.
    pub fn buffered_len(&self) -> usize {
        self.buffered.len()
    }

    /// Whether the caller should fetch a snapshot and hand it to
    /// [`BookSync::apply_snapshot`].
    ///
    /// True once at least one update has been buffered, because a snapshot fetched before
    /// any update has arrived cannot be reconciled against anything.
    pub fn needs_snapshot(&self) -> bool {
        self.state == SyncState::AwaitingSnapshot && !self.buffered.is_empty()
    }

    /// Feed a diff update from the stream.
    ///
    /// While awaiting a snapshot the update is buffered and nothing is emitted. Once live,
    /// the update is either applied, ignored as stale, or found to skip the sequence — in
    /// which case the book is abandoned and rebuilt.
    pub fn apply_update(&mut self, update: DepthUpdate) -> Vec<DepthEvent> {
        match self.state {
            SyncState::AwaitingSnapshot => {
                self.buffer(update);
                Vec::new()
            }
            SyncState::Live => self.apply_live(update),
        }
    }

    /// Feed a snapshot fetched while [`BookSync::needs_snapshot`] was true.
    ///
    /// Returns a [`DepthEvent::Snapshot`] followed by a [`DepthEvent::Delta`] for each
    /// buffered update that belongs after it. If the snapshot is too old to join up with
    /// the buffered updates it is rejected: the book stays in [`SyncState::AwaitingSnapshot`]
    /// and the caller should fetch another.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Vec<DepthEvent> {
        let snapshot_id = snapshot.last_update_id;

        // Updates fully covered by the snapshot carry no new information.
        let pending: Vec<DepthUpdate> = self
            .buffered
            .drain(..)
            .filter(|u| u.last_update_id > snapshot_id)
            .collect();

        // The first surviving update must straddle the snapshot, otherwise the changes
        // between them were never seen and the book would be silently wrong.
        if let Some(first) = pending.first() {
            if first.first_update_id > snapshot_id + 1 {
                tracing::warn!(
                    symbol = %self.symbol,
                    snapshot_id,
                    first_update_id = first.first_update_id,
                    "snapshot predates the buffered updates; fetching another"
                );
                self.buffered = pending;
                return Vec::new();
            }
        }

        let mut events = Vec::with_capacity(pending.len() + 1);
        events.push(DepthEvent::Snapshot {
            symbol: self.symbol.clone(),
            last_update_id: snapshot_id,
            bids: snapshot.bids,
            asks: snapshot.asks,
        });

        self.state = SyncState::Live;
        self.last_update_id = Some(snapshot_id);

        for update in pending {
            // Replay through the live path so buffered updates get the same sequence
            // checks as streamed ones.
            events.extend(self.apply_live(update));
        }

        events
    }

    /// Report that fetching a snapshot failed.
    ///
    /// Buffered updates are kept so a later snapshot can still be reconciled, subject to
    /// the buffer limit.
    pub fn snapshot_failed(&mut self) {
        tracing::warn!(symbol = %self.symbol, buffered = self.buffered.len(), "snapshot fetch failed");
    }

    /// Abandon the book and start over, as after a disconnect.
    ///
    /// Emits [`DepthEvent::Resync`] so the caller knows to stop trusting what it holds.
    pub fn reset(&mut self) -> Vec<DepthEvent> {
        self.state = SyncState::AwaitingSnapshot;
        self.last_update_id = None;
        self.buffered.clear();
        vec![DepthEvent::Resync {
            symbol: self.symbol.clone(),
        }]
    }

    fn buffer(&mut self, update: DepthUpdate) {
        if self.buffered.len() >= self.max_buffered {
            // Dropping the oldest keeps the newest updates, which are the ones a future
            // snapshot can still join onto.
            self.buffered.remove(0);
            tracing::warn!(
                symbol = %self.symbol,
                max_buffered = self.max_buffered,
                "buffer full while awaiting snapshot; dropped oldest update"
            );
        }
        self.buffered.push(update);
    }

    fn apply_live(&mut self, update: DepthUpdate) -> Vec<DepthEvent> {
        let last = match self.last_update_id {
            Some(id) => id,
            None => {
                self.buffer(update);
                return Vec::new();
            }
        };

        // Already covered by the snapshot or an earlier update.
        if update.last_update_id <= last {
            return Vec::new();
        }

        // A jump means updates were missed; the book cannot be repaired by applying this.
        if update.first_update_id > last + 1 {
            tracing::warn!(
                symbol = %self.symbol,
                expected = last + 1,
                received = update.first_update_id,
                "sequence gap; rebuilding from a fresh snapshot"
            );

            self.state = SyncState::AwaitingSnapshot;
            self.last_update_id = None;
            self.buffered.clear();
            self.buffered.push(update.clone());

            return vec![
                DepthEvent::Gap {
                    symbol: self.symbol.clone(),
                    expected: last + 1,
                    received: update.first_update_id,
                },
                DepthEvent::Resync {
                    symbol: self.symbol.clone(),
                },
            ];
        }

        self.last_update_id = Some(update.last_update_id);

        vec![DepthEvent::Delta {
            symbol: self.symbol.clone(),
            last_update_id: update.last_update_id,
            bids: update.bids,
            asks: update.asks,
        }]
    }
}
