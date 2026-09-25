# Agent instructions

Write all code comments and documentation in English.

## Product boundary

- P2P transport facade: addresses, streams, fallback and guarantees over the cvln and ctrn networks.
- Offers one transport interface to `cmsg` and chooses among backends that satisfy the required guarantees.
- Never falls back silently below a required guarantee (for example anonymity).
- Not sync: CRDTs and device consistency belong elsewhere.
- This crate is FSL-1.1-ALv2 (own substantive logic). Commodity wrappers around established libraries belong in LGPL crates in corbet-foss, not here.

## Quality boundary

- `cargo fmt --check`, `cargo clippy --all-targets` (no warnings),
  `cargo test` — all green before every commit. No local workstation
  builds; use GHA, Crow as fallback.
