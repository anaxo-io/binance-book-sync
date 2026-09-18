# CLAUDE.md

Guidance for Claude Code working in this repository.

## What this is

`binance-book-sync` implements Binance's documented "how to manage a local order book"
procedure: buffer diff updates, fetch a REST snapshot, discard what the snapshot already
covers, then require every subsequent update to continue the sequence. It emits
`DepthEvent`s — `Snapshot`, `Delta`, `Gap`, `Resync` — and stores no book of its own.

Extracted from a private trading monorepo as a standalone crate. Nothing about that
application belongs in here, and the crate deliberately has **no dependency on
`depthbook`**: it reports events, and whoever wants book state keeps it themselves.

## Quality gates

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
```

CI runs those plus an MSRV check against Rust 1.88 and `cargo deny check`. No test touches
the network: the REST paths use `mockito` and the state machine is driven directly.

## What this crate is careful about

`CONTRIBUTING.md` states the review rules. The ones that matter most:

- **The sync procedure is the product.** `BookSync` is a pure state machine with no I/O,
  which is what makes `tests/sync_state_machine.rs` able to cover the awkward cases:
  a snapshot older than the buffered updates, a first update that fails to straddle the
  snapshot, a gap mid-stream. A change to the ordering rules needs a test in that file
  first.
- **A gap is reported, never papered over.** A missed update produces `Gap` with the
  expected and received identifiers, followed by `Resync`, and the offending update is
  kept so the caller can rebuild from it. The book must never be advanced across a hole.
- **Prices never pass through `f64`.** Binance sends decimal strings; they are parsed
  straight to `i64` scaled by 10⁹ (`parse_e9`, `SCALE_E9`). An unparseable price is an
  error, not a zero — a zero quantity would silently delete a level.
- **Symbols are passed through verbatim.** No case surgery, no suffix rewriting. What the
  caller subscribes to is what the caller gets back.
- **Reconnect backoff resets after a successful connection.** Otherwise a connection that
  drops once a week eventually reconnects at the ceiling delay. The reset lives in
  `stream.rs` and is **not covered by a test**, so it is exactly the sort of line a
  refactor drops silently. Verify it by hand, or add the test.

## Repository conventions

- Dual licensed MIT OR Apache-2.0. Fresh history; never reference the monorepo it came
  from, by name or by path.
- `#![forbid(unsafe_code)]`, `#![warn(missing_docs)]`. Errors are a `thiserror` enum; no
  `anyhow` in library code. Logging is `tracing`, never `println!`.
- `CHANGELOG.md` follows Keep a Changelog. **Dependency bumps get an entry too**, saying
  what the upgrade needed. Note that `reqwest` and `tokio-tungstenite` types are reachable
  through public `Error` variants, so bumping either is a breaking change for this crate,
  not an internal detail.
- Conventional-commit subjects. Do not commit unless Hicham asks.
- Run `gitleaks protect --staged` before every push.
- Dependabot must not bump `dtolnay/rust-toolchain`: that tag names a Rust release, not an
  action version, so a bump asks CI to install a toolchain that does not exist.
- `gh pr checks` returns nothing for these repos. Read status from
  `gh run list --json conclusion` and `gh run view <id> --json jobs` instead.
