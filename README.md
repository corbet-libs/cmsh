# cmsh

**P2P transport facade.**

`cmsh` is the member side's one transport interface. Its consumers are `cmsg`
packages (live messaging, inbox, board), the vault's "send to my device" port
and `cdht` (through the DHT capability, so `cdht` never skips to a network
leaf). Its providers are network leaves implementing the backend contract:
`cvln` (Veilid) and `ctrn` (Tor). Fallback between them is executed by `cfbk`.

Status: implemented natively (0.0.0, not published), tested against in-memory
mock backends. Browser bindings: frame codec only (see TODO).

## What it does

- **Addresses:** abstract `Address` = `Scheme` (`tor`, `veilid`, …) + opaque
  bytes. Only the leaf serving a scheme interprets its bytes.
- **Policy:** the consumer's `Policy` (default **anonymous only**) plus the
  operator's minimum standard (`MeshBuilder::minimum`). A backend below either
  is never used, whatever fails.
- **Selection:** the configured order, executed by `cfbk`. A connection uses
  exactly one network. `dial(peer_addresses)` tries usable backends in order,
  one at a time; a network-level error (`ErrorKind::Network`) marks that backend
  failed and moves on, an unreachable peer moves on without touching health.
  Every change of the active backend is recorded (`take_switches`): no silent
  fallback. Recovery is explicit (`report(scheme, Health::Healthy)`).
- **Listening:** `listen()` listens on every acceptable backend at once and
  returns the addresses to publish (the future signed address list) plus a
  handle yielding inbound connections or `ListenerClosed` (for example a dead
  Veilid route: the address must be republished).
- **Streams:** each backend byte stream is multiplexed once, here, with
  [`yamux`](https://crates.io/crates/yamux) (`Connection::open/accept` →
  `Substream`, at most 64 per connection). Message framing uses `tokio-util`'s
  `LengthDelimitedCodec` (4-byte big-endian length, ≤ 1 MiB): `frame::framed`
  for streams, `frame::FrameCodec` for push-based use. No own framing code.
- **Capabilities surfaced:** `status()` per backend (capabilities, health,
  acceptable, missing properties) and `offered()` (what the mesh can do right
  now, for example "offline delivery available").
- **Optional hooks:** `datagrams()` (only backends declaring datagrams) and
  `dht()` (only backends declaring a DHT; this is `cdht`'s Veilid filling path).
  Registration rejects a backend whose declared capabilities disagree with the
  hooks it provides.
- **Runtime-agnostic:** background futures go to the embedding runtime through
  `Spawn` (Tokio natively, `spawn_local` in the browser later).
- **Tests:** feature `mock` exposes in-memory backends (`mock::MockNetwork`,
  `mock::MockBackend`) for consumers' tests.

## Layout and licenses

```
cmsh/           FSL-1.1-ALv2   the facade (this crate)
cmsh/cmsh-api/  LGPL-3.0-only WITH LGPL-3.0-linking-exception   the backend contract
browser/        FSL-1.1-ALv2   browser glue (framed-stream.mjs)
```

`ctrn` and `cvln` are LGPL leaves and must not depend on FSL code, so the
contract they implement (`Backend`, `Listener`, `Datagrams`, `Dht`, `Address`,
`Capabilities`, `Error`) lives in the separate LGPL crate `cmsh-api` ("LGPL
executes, FSL decides": the contract and the leaves execute, the facade's
policy and selection decide). It shares this repository because it changes
together with the facade; see [cmsh-api/README.md](cmsh-api/README.md).

Dependencies: `cmsh` → `cmsh-api`, `cfbk` (LGPL, `corbet-foss/cfbk`), `yamux`,
`tokio-util`, `futures`.

## CI

`.github/workflows/check.yml` runs fmt, clippy (`-D warnings`), tests and the
wasm32 builds. `cfbk` is a private git dependency in another repository;
GitHub Actions cannot fetch it without a stored credential, and the CI policy
keeps secrets out of external CI. Until Julian decides (read-only deploy key,
public repositories, or Crow), the workflow stops at a preflight step, and
validation runs on exact source snapshots in the private staging repository
`julian-corbet/transport-ci`.

## TODO

- Browser: bind `Mesh` to JS (backends as JS objects, `spawn_local`), package
  with the leaves' Wasm builds; tab leader election belongs to `cvln`.
- Optional padding to fixed size classes (after compression, before encryption
  is the messaging side's job; the transport pads frames).
- Canonical encoding for published address lists (belongs with the signed
  statements in the board library, `cbrd`).
- Idle/keepalive policy per connection, and automatic re-listen + republish on
  `ListenerClosed`.
- Health probing is the consumer's job today (`report`); decide whether the
  facade should probe failed backends itself.

## Boundaries

What it does:

- Offers one transport interface to `cmsg` and chooses among backends that
  satisfy the required guarantees.

What it never does:

- Never falls back silently, and never below a required guarantee (for
  example anonymity).
- Never interprets payloads.
- Not sync: record semantics belong to `cdht`, device consistency to `cvlt`.

## License

Copyright 2026 Julian Y. Richard Corbet. Licensed under the
[Functional Source License, Version 1.1, ALv2 Future License](LICENSE.md)
(FSL-1.1-ALv2), except the `cmsh-api/` directory, which is licensed under
LGPL-3.0-only WITH LGPL-3.0-linking-exception (see
[cmsh-api/LICENSE.md](cmsh-api/LICENSE.md)).
