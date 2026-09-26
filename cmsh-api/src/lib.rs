//! Backend contract for the `cmsh` transport facade.
//!
//! Network leaves (`ctrn` for Tor, `cvln` for Veilid) implement [`Backend`];
//! the FSL facade `cmsh` consumes it. This crate holds only the contract:
//! addresses, capabilities, the byte-stream and backend traits, the optional
//! datagram and DHT hooks, and the error vocabulary. It contains no policy and
//! no fallback logic ("LGPL executes, FSL decides").
//!
//! Wasm: every trait is object-safe and uses [`MaybeSend`]/[`MaybeSync`], which
//! mean `Send`/`Sync` on native targets and nothing on `wasm32`, where browser
//! futures are not `Send`.
#![forbid(unsafe_code)]

mod address;
mod capabilities;
mod dht;
mod error;

pub use address::{Address, MAX_ADDRESS_BYTES, Scheme};
pub use async_trait::async_trait;
pub use capabilities::{Capabilities, LatencyClass};
pub use dht::{Dht, DhtKeyPair, DhtSchema, DhtSchemaMember, DhtValue, RecordKey};
pub use error::{Error, ErrorKind};
pub use zeroize;

use futures_io::{AsyncRead, AsyncWrite};

/// `Send` on native targets; no bound on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}
/// `Send` on native targets; no bound on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native targets; no bound on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}
/// `Sync` on native targets; no bound on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}

/// A reliable, ordered, bidirectional byte stream with flow control.
///
/// Closing the write half (`poll_close`) signals end of stream to the peer.
/// Payload bytes are opaque: backends never interpret them.
pub trait ByteStream: AsyncRead + AsyncWrite + Unpin + MaybeSend {}
impl<T: AsyncRead + AsyncWrite + Unpin + MaybeSend + ?Sized> ByteStream for T {}

/// An owned byte stream from any backend.
pub type BoxStream = Box<dyn ByteStream>;

/// One network leaf: listen and dial by [`Address`], declare [`Capabilities`].
///
/// A backend serves exactly one [`Scheme`]. It never decides fallback, never
/// interprets payloads, and reports errors with an [`ErrorKind`] that lets the
/// facade distinguish "this network is down" ([`ErrorKind::Network`]) from
/// "this peer is unreachable" ([`ErrorKind::Unreachable`]).
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Backend: MaybeSend + MaybeSync {
    /// The address scheme this backend serves.
    fn scheme(&self) -> Scheme;

    /// What this backend guarantees. Must be stable for the backend's lifetime.
    fn capabilities(&self) -> Capabilities;

    /// Start accepting inbound streams. The listener's address is what the
    /// member publishes (for example in a signed address list).
    async fn listen(&self) -> Result<Box<dyn Listener>, Error>;

    /// Open a stream to `address`, which must use this backend's scheme.
    async fn dial(&self, address: &Address) -> Result<BoxStream, Error>;

    /// Unreliable datagrams, only where the network supports them. Must be
    /// `Some` exactly when [`Capabilities::datagrams`] is set.
    fn datagrams(&self) -> Option<&dyn Datagrams> {
        None
    }

    /// The DHT hook (for `cdht`'s network filling), only where the network
    /// provides a DHT. Must be `Some` exactly when [`Capabilities::dht`] is set.
    fn dht(&self) -> Option<&dyn Dht> {
        None
    }
}

/// Accepts inbound streams at one published address.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Listener: MaybeSend {
    /// The address peers dial to reach this listener.
    fn address(&self) -> &Address;

    /// Wait for the next inbound stream. An error of kind
    /// [`ErrorKind::Closed`] means the listener (for example its route) is
    /// gone and a new one must be created and republished.
    async fn accept(&mut self) -> Result<BoxStream, Error>;
}

/// Unreliable, unordered, size-bounded datagrams.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Datagrams: MaybeSend + MaybeSync {
    /// Largest payload `send` accepts.
    fn max_payload(&self) -> usize;

    /// Send one datagram. Success means handed to the network, not delivered.
    async fn send(&self, to: &Address, payload: &[u8]) -> Result<(), Error>;

    /// Receive the next datagram addressed to any of this backend's listeners.
    /// The sender is anonymous: any reply address must be inside the payload.
    async fn receive(&self) -> Result<Vec<u8>, Error>;
}
