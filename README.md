# cmsh

**P2P transport facade.**

`cmsh` will be the member-side transport facade: listen and dial by abstract address, reliable streams, backend capabilities and guarantees, and switching between network leaves (`cvln` Veilid primary, `ctrn` Tor fallback).

Status: name reserved, no implementation yet.

## Boundaries

What it does:

- Offers one transport interface to `cmsg` and chooses among backends that satisfy the required guarantees.

What it never does:

- Never falls back silently below a required guarantee (for example anonymity).
- Not sync: CRDTs and device consistency belong elsewhere.

## License

Copyright 2026 Julian Y. Richard Corbet. Licensed under the
[Functional Source License, Version 1.1, ALv2 Future License](LICENSE.md)
(FSL-1.1-ALv2).
