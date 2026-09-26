//! In-memory backends for tests (feature `mock`). Not a network: streams are
//! in-process pipes, and "anonymous" is only what the test declares.
use cmsh_api::{
    Address, Backend, BoxStream, Capabilities, Datagrams, Dht, DhtKeyPair, DhtSchema, DhtValue,
    Error, ErrorKind, Listener, RecordKey, Scheme, async_trait,
};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::lock::Mutex as AsyncMutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::compat::TokioAsyncReadCompatExt;

const PIPE_BYTES: usize = 64 * 1024;
const DATAGRAM_BYTES: usize = 1024;

type Inbox = mpsc::UnboundedSender<Vec<u8>>;

#[derive(Default)]
struct State {
    listeners: HashMap<Address, mpsc::Sender<BoxStream>>,
    datagram_inboxes: HashMap<Address, Inbox>,
    next: u64,
}

/// A shared in-memory "internet" connecting mock backends.
#[derive(Clone, Default)]
pub struct MockNetwork {
    state: Arc<Mutex<State>>,
}

impl MockNetwork {
    /// A new empty network.
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A controllable in-memory backend.
pub struct MockBackend {
    scheme: Scheme,
    capabilities: Capabilities,
    network: MockNetwork,
    down: AtomicBool,
    dials: AtomicUsize,
    datagrams: Option<MockDatagrams>,
    dht: Option<MockDht>,
}

impl MockBackend {
    /// A backend serving `scheme` with the declared capabilities. The datagram
    /// and DHT hooks exist exactly when the capabilities declare them.
    pub fn new(network: &MockNetwork, scheme: &str, capabilities: Capabilities) -> Arc<Self> {
        let (sender, receiver) = mpsc::unbounded();
        Arc::new(Self {
            scheme: Scheme::new(scheme).expect("valid mock scheme"),
            capabilities,
            network: network.clone(),
            down: AtomicBool::new(false),
            dials: AtomicUsize::new(0),
            datagrams: capabilities.datagrams.then(|| MockDatagrams {
                scheme: Scheme::new(scheme).expect("valid mock scheme"),
                network: network.clone(),
                inbox: sender,
                receiver: AsyncMutex::new(receiver),
            }),
            dht: capabilities.dht.then(MockDht::default),
        })
    }

    /// Simulate the whole network failing (`true`) or recovering.
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    /// Number of dial attempts that reached this backend.
    pub fn dial_count(&self) -> usize {
        self.dials.load(Ordering::SeqCst)
    }

    fn check_up(&self) -> Result<(), Error> {
        if self.down.load(Ordering::SeqCst) {
            Err(Error::new(ErrorKind::Network, "mock network down"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Backend for MockBackend {
    fn scheme(&self) -> Scheme {
        self.scheme.clone()
    }

    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    async fn listen(&self) -> Result<Box<dyn Listener>, Error> {
        self.check_up()?;
        let (sender, receiver) = mpsc::channel(8);
        let mut state = self.network.state();
        state.next += 1;
        let address = Address::new(
            self.scheme.clone(),
            format!("mock-{}", state.next).into_bytes(),
        )?;
        state.listeners.insert(address.clone(), sender);
        if let Some(datagrams) = &self.datagrams {
            state
                .datagram_inboxes
                .insert(address.clone(), datagrams.inbox.clone());
        }
        Ok(Box::new(MockListener { address, receiver }))
    }

    async fn dial(&self, address: &Address) -> Result<BoxStream, Error> {
        self.dials.fetch_add(1, Ordering::SeqCst);
        self.check_up()?;
        address.expect_scheme(&self.scheme)?;
        let (local, remote) = tokio::io::duplex(PIPE_BYTES);
        let mut listener = self
            .network
            .state()
            .listeners
            .get(address)
            .cloned()
            .ok_or(Error::new(ErrorKind::Unreachable, "no mock listener"))?;
        let remote: BoxStream = Box::new(remote.compat());
        futures::SinkExt::send(&mut listener, remote)
            .await
            .map_err(|_| Error::new(ErrorKind::Unreachable, "mock listener gone"))?;
        Ok(Box::new(local.compat()))
    }

    fn datagrams(&self) -> Option<&dyn Datagrams> {
        self.datagrams.as_ref().map(|d| d as &dyn Datagrams)
    }

    fn dht(&self) -> Option<&dyn Dht> {
        self.dht.as_ref().map(|d| d as &dyn Dht)
    }
}

struct MockListener {
    address: Address,
    receiver: mpsc::Receiver<BoxStream>,
}

#[async_trait]
impl Listener for MockListener {
    fn address(&self) -> &Address {
        &self.address
    }

    async fn accept(&mut self) -> Result<BoxStream, Error> {
        self.receiver
            .next()
            .await
            .ok_or(Error::new(ErrorKind::Closed, "mock listener closed"))
    }
}

struct MockDatagrams {
    scheme: Scheme,
    network: MockNetwork,
    inbox: Inbox,
    receiver: AsyncMutex<mpsc::UnboundedReceiver<Vec<u8>>>,
}

#[async_trait]
impl Datagrams for MockDatagrams {
    fn max_payload(&self) -> usize {
        DATAGRAM_BYTES
    }

    async fn send(&self, to: &Address, payload: &[u8]) -> Result<(), Error> {
        to.expect_scheme(&self.scheme)?;
        if payload.len() > DATAGRAM_BYTES {
            return Err(Error::new(ErrorKind::Limit, "datagram too large"));
        }
        // Unreliable by contract: an unknown destination silently drops.
        if let Some(inbox) = self.network.state().datagram_inboxes.get(to) {
            let _ = inbox.unbounded_send(payload.to_vec());
        }
        Ok(())
    }

    async fn receive(&self) -> Result<Vec<u8>, Error> {
        self.receiver
            .lock()
            .await
            .next()
            .await
            .ok_or(Error::new(ErrorKind::Closed, "mock datagrams closed"))
    }
}

struct Record {
    /// Writer public key per subkey.
    writers: Vec<Vec<u8>>,
    values: Vec<Option<DhtValue>>,
    opened_as: Option<DhtKeyPair>,
}

/// Minimal in-memory DHT honoring schema slots and sequence numbers.
#[derive(Default)]
struct MockDht {
    records: Mutex<HashMap<Vec<u8>, Record>>,
    next: AtomicUsize,
}

impl MockDht {
    fn records(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Record>> {
        self.records.lock().unwrap_or_else(|e| e.into_inner())
    }
}

const NOT_FOUND: Error = Error::new(ErrorKind::Unreachable, "mock record not found");

#[async_trait]
impl Dht for MockDht {
    fn max_value_bytes(&self) -> usize {
        32 * 1024
    }

    async fn create(
        &self,
        schema: &DhtSchema,
        owner: Option<&DhtKeyPair>,
    ) -> Result<RecordKey, Error> {
        let number = self.next.fetch_add(1, Ordering::SeqCst);
        let owner = owner.cloned().unwrap_or_else(|| DhtKeyPair {
            public_key: format!("owner-{number}").into_bytes(),
            secret_key: cmsh_api::zeroize::Zeroizing::new(vec![0; 32]),
        });
        let mut writers = vec![owner.public_key.clone(); schema.owner_subkeys as usize];
        for member in &schema.members {
            writers.extend(std::iter::repeat_n(
                member.public_key.clone(),
                member.subkeys as usize,
            ));
        }
        let key = format!("record-{number}").into_bytes();
        self.records().insert(
            key.clone(),
            Record {
                values: vec![None; writers.len()],
                writers,
                opened_as: Some(owner),
            },
        );
        Ok(RecordKey(key))
    }

    async fn open(&self, key: &RecordKey, writer: Option<&DhtKeyPair>) -> Result<(), Error> {
        let mut records = self.records();
        let record = records.get_mut(&key.0).ok_or(NOT_FOUND)?;
        record.opened_as = writer.cloned();
        Ok(())
    }

    async fn get(
        &self,
        key: &RecordKey,
        subkey: u32,
        _refresh: bool,
    ) -> Result<Option<DhtValue>, Error> {
        let records = self.records();
        let record = records.get(&key.0).ok_or(NOT_FOUND)?;
        record
            .values
            .get(subkey as usize)
            .cloned()
            .ok_or(Error::new(ErrorKind::InvalidAddress, "subkey out of range"))
    }

    async fn set(
        &self,
        key: &RecordKey,
        subkey: u32,
        data: &[u8],
        writer: Option<&DhtKeyPair>,
    ) -> Result<Option<DhtValue>, Error> {
        let mut records = self.records();
        let record = records.get_mut(&key.0).ok_or(NOT_FOUND)?;
        let writer = writer
            .or(record.opened_as.as_ref())
            .ok_or(Error::new(ErrorKind::Unsupported, "record not writable"))?
            .public_key
            .clone();
        let index = subkey as usize;
        let allowed = record
            .writers
            .get(index)
            .ok_or(Error::new(ErrorKind::InvalidAddress, "subkey out of range"))?;
        if *allowed != writer {
            return Err(Error::new(ErrorKind::Unsupported, "writer not allowed"));
        }
        let seq = match &record.values[index] {
            Some(current) => current.seq + 1,
            None => 0,
        };
        record.values[index] = Some(DhtValue {
            seq,
            writer,
            data: data.to_vec(),
        });
        Ok(None)
    }

    async fn close(&self, key: &RecordKey) -> Result<(), Error> {
        let mut records = self.records();
        let record = records.get_mut(&key.0).ok_or(NOT_FOUND)?;
        record.opened_as = None;
        Ok(())
    }
}
