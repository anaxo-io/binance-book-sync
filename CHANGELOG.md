# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-14

### Changed

- Update `tokio-tungstenite` from 0.24 to 0.30. `Message::Text` carries `Utf8Bytes`
  rather than `String` from 0.26 onward
  ([#4](https://github.com/anaxo-io/binance-book-sync/pull/4)).

- Update `reqwest` from 0.12 to 0.13. The bundled `rustls-tls` feature is replaced by a
  TLS implementation plus a root store, so `rustls` + `webpki-roots` reproduces the
  previous behaviour, and the query-string builder moved behind an opt-in `query` feature
  ([#5](https://github.com/anaxo-io/binance-book-sync/pull/5)).

  Both are **breaking for this crate**, not merely internal: `reqwest::Error` and
  `tungstenite::Error` are reachable through the public `Error::Http` and
  `Error::WebSocket` variants, so code matching on them must move to the new versions.
  That is why this is 0.2.0 rather than a patch release.

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

[Unreleased]: https://github.com/anaxo-io/binance-book-sync/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/anaxo-io/binance-book-sync/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/anaxo-io/binance-book-sync/releases/tag/v0.1.0
