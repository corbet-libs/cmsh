//! The DHT hook: signed, replicated records with owner and writer slots.
//!
//! Shaped like Veilid DHT records (owner, member writers with their own
//! subkeys, per-subkey sequence numbers) so that `cdht` can fill its records
//! from any network that provides a DHT without depending on Veilid types.
//! Keys handed in here are **purpose keys** (one record, one writer slot), never
//! a member's root or device keys.
use crate::{Error, MaybeSend, MaybeSync};
use std::fmt;

/// Opaque, backend-defined record key (for Veilid: the typed record key
/// including its encryption secret, in its canonical text form).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RecordKey(pub Vec<u8>);

impl fmt::Debug for RecordKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordKey({} bytes)", self.0.len())
    }
}

/// A writer member of a record schema: a public key and its number of subkeys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtSchemaMember {
    /// Raw Ed25519 public key (32 bytes) of the member's purpose key.
    pub public_key: Vec<u8>,
    /// Subkeys this member may write.
    pub subkeys: u16,
}

/// Record layout: subkeys for the owner, followed by each member's subkeys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtSchema {
    /// Subkeys only the owner may write (first in the record).
    pub owner_subkeys: u16,
    /// Additional writers, each with their own subkey range.
    pub members: Vec<DhtSchemaMember>,
}

/// An Ed25519 purpose key pair for owning or writing one record.
/// The secret half is wiped on drop and never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct DhtKeyPair {
    /// Raw Ed25519 public key (32 bytes).
    pub public_key: Vec<u8>,
    /// Raw Ed25519 secret key (32 bytes).
    pub secret_key: zeroize::Zeroizing<Vec<u8>>,
}

impl fmt::Debug for DhtKeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DhtKeyPair(..)")
    }
}

/// One subkey value as stored in the DHT.
#[derive(Clone, PartialEq, Eq)]
pub struct DhtValue {
    /// Sequence number; a write must exceed the stored one.
    pub seq: u32,
    /// Raw Ed25519 public key of the writer that signed the value.
    pub writer: Vec<u8>,
    /// Payload (opaque; `cdht` encrypts and signs its own records).
    pub data: Vec<u8>,
}

impl fmt::Debug for DhtValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DhtValue(seq {}, {} bytes)", self.seq, self.data.len())
    }
}

/// DHT operations offered by a backend (Veilid today). Payload-agnostic.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Dht: MaybeSend + MaybeSync {
    /// Largest value `set` accepts.
    fn max_value_bytes(&self) -> usize;

    /// Create a record with `schema`, owned by `owner` (a fresh owner key is
    /// generated when `None`), and open it for writing by the owner.
    async fn create(
        &self,
        schema: &DhtSchema,
        owner: Option<&DhtKeyPair>,
    ) -> Result<RecordKey, Error>;

    /// Open an existing record, optionally as a writer.
    async fn open(&self, key: &RecordKey, writer: Option<&DhtKeyPair>) -> Result<(), Error>;

    /// Read one subkey. `refresh` asks the network instead of the local copy.
    async fn get(
        &self,
        key: &RecordKey,
        subkey: u32,
        refresh: bool,
    ) -> Result<Option<DhtValue>, Error>;

    /// Write one subkey as `writer` (default: the key the record was opened
    /// with). Returns `Some(newer)` when the network holds a newer value, in
    /// which case nothing was written.
    async fn set(
        &self,
        key: &RecordKey,
        subkey: u32,
        data: &[u8],
        writer: Option<&DhtKeyPair>,
    ) -> Result<Option<DhtValue>, Error>;

    /// Close a record opened by `create` or `open`.
    async fn close(&self, key: &RecordKey) -> Result<(), Error>;
}
