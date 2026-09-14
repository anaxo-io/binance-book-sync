//! Keep a local Binance order book in sync.
//!
//! Binance publishes order books as a REST snapshot plus a stream of diff updates, and
//! joining the two correctly is fiddly: updates arrive before the snapshot is fetched,
//! some of them are already covered by it, and a single missed message leaves a book that
//! looks healthy and is quietly wrong. This crate implements Binance's documented
//! [procedure][docs] for doing it properly, and tells you when it has failed.
//!
//! It does not store a book. It emits [`DepthEvent`]s — a snapshot to replace your book,
//! deltas to apply, and a gap warning when the sequence breaks — and leaves the data
//! structure to you.
//!
//! # Example
//!
//! ```no_run
//! use binance_book_sync::{DepthEvent, DepthStream, SyncConfig};
//!
//! # async fn run() -> Result<(), binance_book_sync::Error> {
//! let stream = DepthStream::new(vec!["BTCUSDT".to_string()], SyncConfig::default())?;
//! let mut events = stream.spawn(1024);
//!
//! while let Some(event) = events.recv().await {
//!     match event {
//!         DepthEvent::Snapshot { symbol, last_update_id, bids, asks } => {
//!             println!("{symbol}: book rebuilt at {last_update_id}, {} bids", bids.len());
//!             let _ = asks;
//!         }
//!         DepthEvent::Delta { symbol, last_update_id, bids, asks } => {
//!             println!("{symbol}: update {last_update_id}");
//!             let _ = (bids, asks);
//!         }
//!         DepthEvent::Gap { symbol, expected, received } => {
//!             eprintln!("{symbol}: missed updates, expected {expected} got {received}");
//!         }
//!         DepthEvent::Resync { symbol } => {
//!             eprintln!("{symbol}: discard the book, a snapshot is coming");
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Prices
//!
//! Prices and quantities are `i64` values scaled by 10⁹, named with an `_e9` suffix. They
//! are parsed from Binance's decimal strings directly, never through `f64`, so the nine
//! decimal places Binance can send survive intact. The crate deliberately does not impose
//! a decimal type on you; wrap the integer in whatever your book uses.
//!
//! # Testing the awkward part
//!
//! [`BookSync`] is the state machine on its own, with no I/O. It can be driven through
//! snapshots, stale updates, sequence gaps and recovery synchronously, which is how this
//! crate's own tests cover the procedure.
//!
//! ```
//! use binance_book_sync::{BookSync, DepthUpdate, Snapshot, SyncState};
//!
//! let mut sync = BookSync::new("BTCUSDT");
//! sync.apply_update(DepthUpdate {
//!     first_update_id: 100,
//!     last_update_id: 105,
//!     bids: vec![],
//!     asks: vec![],
//! });
//!
//! // A snapshot older than the buffered updates leaves a hole, so it is refused.
//! let events = sync.apply_snapshot(Snapshot { last_update_id: 50, bids: vec![], asks: vec![] });
//! assert!(events.is_empty());
//! assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
//! ```
//!
//! [docs]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod config;
pub mod error;
pub mod rest;
pub mod stream;
pub mod sync;
pub mod types;

pub use config::{SyncConfig, UpdateSpeed};
pub use error::{Error, Result};
pub use rest::SnapshotFetcher;
pub use stream::DepthStream;
pub use sync::{BookSync, SyncState};
pub use types::{parse_e9, DepthEvent, DepthUpdate, PriceLevel, Snapshot, SCALE_E9};
