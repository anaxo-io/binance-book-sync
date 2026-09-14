# Contributing

Thanks for taking the time to contribute.

## Development setup

```bash
git clone https://github.com/anaxo-io/binance-book-sync
cd binance-book-sync
cargo test --all-features
```

The toolchain is pinned in `rust-toolchain.toml`; `rustup` picks it up automatically. No
tests reach the network: the state machine is synchronous, and the REST tests run against
a local mock server.

## Before opening a pull request

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
```

All four must pass. CI runs the same commands plus an MSRV check against Rust 1.85 and
`cargo deny check`.

## Guidelines

- **The state machine is the contract.** `tests/sync_state_machine.rs` encodes Binance's
  documented procedure. If a change alters which events are emitted, or in what order, say
  why in the pull request.
- **Keep I/O out of `sync.rs`.** The reason the awkward cases are testable at all is that
  the procedure has no network in it. New sequencing logic belongs there, with a test that
  drives it synchronously.
- **Never turn a parse failure into a value.** A price that will not parse is an error. The
  implementation this crate was extracted from used `unwrap_or(0.0)`, and a zero quantity
  means "delete this level".
- **No `unsafe`.** The crate is `#![forbid(unsafe_code)]`.
- **Public items need docs.** The crate builds with `#![warn(missing_docs)]` and CI treats
  rustdoc warnings as errors.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`,
`perf:`, `refactor:`, `test:`, `chore:`.

## Licence

Contributions are dual-licensed under MIT and Apache-2.0, matching the crate.
