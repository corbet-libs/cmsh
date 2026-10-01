# Mesh and streams contract

Mesh is a transport-neutral facade over the existing LGPL `cmsh-api` byte-stream
port and `cfbk` selection. Backend addresses remain opaque scheme/bytes pairs;
only their owning adapter parses them. No Tor type enters the Mesh API.
The launch configuration registers only Tor. Veilid is not implemented here.
The facade owns no profile, message, acknowledgement, key or record state.

`Mesh::builder`, `listen`, `dial`, `status`, `offered`, `report`, `take_switches`
retain the existing API. Readiness is derived from configured capabilities and
reported health. Registering a backend is a trusted composition operation: start
it first, and report subsequent loss before selecting it again. The minimum and
caller policy are passed to `cfbk`; failure never relaxes those requirements.
Capabilities are implementation claims under each backend's documented threat
model, not a statement that anonymity systems are equivalent. Listening does
not try failed backends. Each listener emits its terminal event once. Dropping
or closing `Listening` cancels idle accepts and drops every owned listener.

`Session` supplies bounded yamux multiplexing (64 streams; 16 queued accepts).
Streams is a module in this repository pending its final library name.
`streams::FramedStream::open(io, limits, clock)`, `send_frame`, `next_frame`,
`finish`, `cancel` operate on `futures-io::AsyncRead + AsyncWrite`. The injected
clock supplies monotonic milliseconds and asynchronous deadline wakeups.
No storage, RNG, application retry or background task belongs to framing.

The frame is a u32 big-endian length followed by opaque bytes. Empty frames are
valid; payload limits are positive and at most 1 MiB. The format is inherited
from Mesh's maintained tokio-util codec. cmsg's former nonempty application
constraint belongs to its message decoder. Length is checked before allocation
of an oversized receive payload or copying an oversized send. Push input is
bounded to one maximum frame plus its prefix, and partial receive state is
bounded. The operation wrapper has one send/read operation and no user queue;
backpressure covers the entire frame and flush. Callers bound their own queues.

A synchronous open reaches Open or returns an error. Operations temporarily
own the I/O, with stored state Failed until success; cancellation therefore
drops the stream. Open can reach Closed on clean EOF/finish, or Failed on
malformed input, I/O failure, cancellation, timeout or clock regression.
Finish drains/flushed output and closes the write half before releasing I/O.
Partial frames never escape; a prefix alone at EOF is malformed. A successful
send only means local transport completion, never durable peer acceptance.

Native and wasm execute identical split/coalesced/empty/maximal frame vectors,
partial writes, oversized lengths, every truncated prefix/payload, cancellation,
slow peers, exact deadlines, clock regression and closure. Selection tests use
real in-memory byte streams with controlled faults and real cfbk logic. Real
Tor composition tests live with the ctrn adapter and use this same Mesh API.
