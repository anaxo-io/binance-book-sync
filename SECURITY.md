# Security Policy

## Supported versions

The latest released version receives security fixes. This crate has not yet reached 1.0;
the API may change between minor versions.

## Reporting a vulnerability

Report security issues privately to **security@anaxo.io**. Please do not open a public
issue.

Include:

- a description of the issue and its impact,
- the version or commit affected,
- steps to reproduce, ideally a failing test.

You will receive an acknowledgement within 72 hours. Fixes are disclosed publicly once
released, or after 90 days, whichever comes first.

## Scope

This crate reads public market data. It holds no credentials, signs no requests, and places
no orders, so there is no authentication surface to attack.

What is in scope:

- Parsing untrusted input from the network — malformed JSON, oversized payloads, or decimal
  strings crafted to cause a panic or an incorrect value.
- Any path where a sequence gap could go unreported, since a caller trusting a silently
  wrong order book is the most damaging failure this crate can have.
- Resource exhaustion, such as unbounded buffering while a snapshot is unavailable.

The crate is `#![forbid(unsafe_code)]`, so memory-safety issues would have to come from a
dependency; please report those upstream as well.
