# cmsh-api

**Backend contract for the `cmsh` transport facade.**

The traits and types that network leaves implement and the facade consumes:

- `Scheme` and `Address`: a network name plus opaque, backend-defined bytes
  (an onion address for Tor, a private route blob for Veilid). `Debug` never
  prints address bytes.
- `Capabilities`: anonymous, datagrams, offline delivery, DHT, latency class.
- `ByteStream` / `BoxStream`: reliable ordered byte streams (`futures-io`
  `AsyncRead + AsyncWrite`).
- `Backend` (listen, dial, declare capabilities), `Listener`, the optional
  `Datagrams` hook and the optional `Dht` hook (Veilid-shaped records for
  `cdht`; keys handed in are purpose keys only).
- `Error` / `ErrorKind`: coarse kinds with static descriptions; never peer,
  key or payload data. `Network` (the network is down) is distinct from
  `Unreachable` (this peer is), which is what the facade's fallback needs.

Native and `wasm32` builds: `MaybeSend`/`MaybeSync` are `Send`/`Sync` on native
targets and empty on `wasm32`, and the async traits drop the `Send` bound there.

## Why this crate exists (license layout)

`ctrn` and `cvln` are LGPL leaves in `corbet-foss`; `cmsh` is the FSL facade in
`corbet-libs`. An LGPL leaf must not depend on FSL code, so the contract the
leaves implement cannot live in the FSL crate. It lives here, in a separate
**LGPL** crate inside the `cmsh` repository:

```
cmsh (FSL) ──► cmsh-api (LGPL) ◄── ctrn (LGPL), cvln (LGPL)
     └──────► cfbk (LGPL)
```

It sits in the `cmsh` repository because the contract changes together with the
facade (workspace rule: packages that change together share a repository). It
has its own license files and no dependency on the FSL crate. It can move to its
own `corbet-foss` repository by transfer later; that needs a name under the
naming rule, which is Julian's call.

## Boundaries

What it does:

- Defines the contract between the facade and network leaves.

What it never does:

- No policy, selection or fallback (that is `cmsh` and `cfbk`).
- No I/O and no network code.

## License

Copyright 2026 Julian Y. Richard Corbet. This directory is licensed under
[LGPL-3.0-only](LICENSES/LGPL-3.0-only.txt)
[WITH LGPL-3.0-linking-exception](LICENSES/LGPL-3.0-linking-exception.txt).
See [LICENSE.md](LICENSE.md). The rest of the `cmsh` repository is FSL-1.1-ALv2.
