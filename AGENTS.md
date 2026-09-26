# Agent instructions

Write all code comments and documentation in English.

## Product boundary

- P2P transport facade: addresses, streams, fallback and guarantees over the
  cvln (Veilid) and ctrn (Tor) networks.
- Offers one transport interface to `cmsg` (and the vault's device port and
  `cdht`'s DHT capability) and chooses among backends that satisfy the
  required guarantees.
- Never falls back silently, never below a required guarantee (for example
  anonymity). Every switch of the active backend must stay observable.
- Multiplexing (`yamux`) and framing (`tokio-util` length-delimited codec)
  happen once, here. Do not invent framing; do not add a second multiplexer.
- Payload-agnostic. Not sync: record semantics belong to `cdht`.
- Two licenses in this repository:
  - `cmsh` (root crate, `src/`, `browser/`): FSL-1.1-ALv2 (own decisions:
    policy, selection).
  - `cmsh-api/`: LGPL-3.0-only WITH LGPL-3.0-linking-exception. The backend
    contract that LGPL leaves implement. It must never depend on the FSL
    crate, and must contain no policy or selection logic.
- Commodity wrappers around established libraries belong in LGPL crates in
  corbet-foss, not here.

## Quality boundary

- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo test --workspace --all-features` and
  the wasm32 builds — all green before every commit. No local workstation
  builds; use GHA, Crow as fallback (see README, CI).
- Behaviour changes to selection need a test in `src/tests.rs` against the
  mock backends.
