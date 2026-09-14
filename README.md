# binance-book-sync

[![CI](https://github.com/anaxo-io/binance-book-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/anaxo-io/binance-book-sync/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://blog.rust-lang.org/)

Keeps a local Binance order book in sync, and tells you when it has failed.

## Why

Binance publishes order books as a REST snapshot plus a stream of diff updates. Joining
the two is documented but fiddly: updates arrive before you have the snapshot, some are
already covered by it, the snapshot can be too old to use, and one missed message leaves
a book that looks perfectly healthy and is quietly wrong.

Most implementations get the happy path right and the recovery path wrong, because the
recovery path is hard to test when the sequence logic is tangled up with HTTP and
WebSocket calls. Here the procedure is a plain state machine with no I/O, so every branch
— stale updates, an outdated snapshot, a gap mid-stream — is tested directly.

This crate does not store a book. It emits events and leaves the data structure to you;
pair it with [`orderbook`](https://github.com/anaxo-io/orderbook-rs) or your own.

## Quick start

```toml
[dependencies]
binance-book-sync = { git = "https://github.com/anaxo-io/binance-book-sync", tag = "v0.1.0" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use binance_book_sync::{DepthEvent, DepthStream, SyncConfig};

#[tokio::main]
async fn main() -> Result<(), binance_book_sync::Error> {
    let stream = DepthStream::new(vec!["BTCUSDT".to_string()], SyncConfig::default())?;
    let mut events = stream.spawn(1024);

    while let Some(event) = events.recv().await {
        match event {
            DepthEvent::Snapshot { symbol, last_update_id, bids, asks } => {
                println!("{symbol}: rebuilt at {last_update_id} ({} bids, {} asks)",
                    bids.len(), asks.len());
            }
            DepthEvent::Delta { symbol, last_update_id, bids, asks } => {
                println!("{symbol}: update {last_update_id} ({} bids, {} asks)",
                    bids.len(), asks.len());
            }
            DepthEvent::Gap { symbol, expected, received } => {
                eprintln!("{symbol}: missed updates, expected {expected} got {received}");
            }
            DepthEvent::Resync { symbol } => {
                eprintln!("{symbol}: discard the book, a snapshot is coming");
            }
        }
    }
    Ok(())
}
```

Run it against the live exchange:

```bash
cargo run --example stream_btcusdt
```

## Features

- **The documented procedure, implemented properly.** Buffer updates, fetch a snapshot,
  discard what the snapshot already covers, require the first update to straddle it, then
  require each subsequent update to continue the sequence. See
  [`tests/sync_state_machine.rs`](tests/sync_state_machine.rs).
- **Gaps are reported, not hidden.** A missed update produces `Gap` with the expected and
  received ids, followed by `Resync`, and the offending update is kept so the rebuild can
  use it. The book is never advanced across a hole.
- **An outdated snapshot is rejected.** If a snapshot predates the buffered updates, the
  changes between them were never observed, so it is refused and another is fetched.
- **Exact prices.** Decimal strings are parsed straight to integers scaled by 10⁹, never
  through `f64`. An unparseable price is an error rather than a silent zero — which, for a
  quantity, would mean deleting a level.
- **Symbols are passed through unchanged.** What you subscribe to is what you get.
- **Reconnection that settles down.** Backoff doubles up to a ceiling and resets once a
  connection is established, so one drop after a week of uptime does not reconnect at the
  maximum delay.
- **Testable without a network.** [`BookSync`] is the whole procedure with no I/O.

## Non-goals

- **No order book.** This crate emits changes; it does not apply them.
- **Binance spot only.** Futures use different endpoints and carry an extra `pu` field.
- **No trading.** Market data only — no orders, no authentication, no signing.
- **One connection per stream.** Binance limits streams per connection, so a large symbol
  list needs several `DepthStream`s.
- **Snapshots are fetched inline**, briefly pausing reads on that connection. Fetching
  concurrently would let updates race ahead of the snapshot they are reconciled against.

## Documentation

```bash
cargo doc --open
```

Every public item is documented and the examples in the docs run as tests.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bug reports with a failing test are especially
welcome — `BookSync` is synchronous and needs no network, so a reproduction is usually a
few lines.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed
as above, without any additional terms or conditions.
