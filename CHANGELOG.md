# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-09-14

Initial release.

### Added

- `BookSync`: Binance's "manage a local order book correctly" procedure as a state machine
  with no I/O — buffers diff updates, reconciles them against a REST snapshot, discards
  updates the snapshot already covers, rejects a snapshot too old to join onto the buffer,
  and reports sequence gaps instead of applying past them.
- `DepthStream`: drives the state machine from a live connection, with reconnection backoff
  and per-symbol resynchronisation, emitting events over a bounded `tokio::sync::mpsc`
  channel.
- `SnapshotFetcher`: REST depth snapshots with retries on transport failures and throttling.
- `DepthEvent`: `Snapshot`, `Delta`, `Gap` and `Resync`.
- `parse_e9`: exact decimal-string parsing into integers scaled by 10⁹, never via `f64`.
- `SyncConfig`: endpoints, snapshot depth, update speed, retry and backoff settings, and
  the bound on buffered updates.

### Notes on the extraction

This crate began as the Binance client in a private trading workspace. The behaviour was
reworked rather than copied:

- The state machine was separated from the network code. Previously the procedure lived
  inside a 400-line message handler alongside `reqwest` and `tungstenite` calls, so it
  could only be exercised through mocked HTTP and most of its branches were never asserted.
- Prices are parsed from strings to scaled integers. The original parsed to `f64` with
  `unwrap_or(0.0)`, which turned an unparseable price into a level deletion.
- Symbols are passed through unchanged. The original rewrote `BTCUSD` to `btcusdt` and
  stripped `USD` suffixes, subscribing to a different instrument than the caller asked for.
- Reconnection backoff resets after a connection is established, rather than growing
  monotonically for the lifetime of the process.
- Lock poisoning can no longer take down a feed: the shared `Mutex<HashMap<..>>` and its
  `unwrap()` calls are gone, since each book is owned by the task that drives it.

### Known limitations

- Binance spot only. Futures use different endpoints and an extra `pu` field.
- A snapshot is fetched inline, pausing reads on that connection until it returns. Fetching
  concurrently would let updates race ahead of the snapshot they are reconciled against.
- One connection per `DepthStream`. Binance caps streams per connection, so a large symbol
  list needs several.

[Unreleased]: https://github.com/anaxo-io/binance-book-sync/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/anaxo-io/binance-book-sync/releases/tag/v0.1.0
