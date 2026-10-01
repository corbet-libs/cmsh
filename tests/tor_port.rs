//! Mesh's downward Tor adapter over real Node/Ferry with external byte I/O.
//! This is adapter evidence; actual onion/private-Tor evidence belongs to ctrn CI.
#![cfg(feature = "tor")]
use cmsh::{Backend, Incoming, Mesh};
use futures::{SinkExt, StreamExt, channel::mpsc, lock::Mutex};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::compat::TokioAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;

struct Time;
impl ctrn::Clock for Time {
    fn now(&self) -> u64 {
        0
    }
    fn sleep_until(&self, _: u64) -> ctrn::BoxFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
}
fn spawn(f: cmsh::BoxFuture<'static, ()>) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::spawn(f);
    }
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(f);
}
struct ByteNetwork {
    online: AtomicBool,
    here: ctrn::Address,
    there: ctrn::Address,
    incoming: Arc<Mutex<mpsc::Receiver<ctrn::BoxStream>>>,
    outgoing: mpsc::Sender<ctrn::BoxStream>,
}
struct ByteListener {
    address: ctrn::Address,
    incoming: Arc<Mutex<mpsc::Receiver<ctrn::BoxStream>>>,
}
#[cfg_attr(not(target_arch = "wasm32"), ctrn::async_trait)]
#[cfg_attr(target_arch = "wasm32", ctrn::async_trait(?Send))]
impl ctrn::Listener for ByteListener {
    fn address(&self) -> &ctrn::Address {
        &self.address
    }
    async fn accept(&mut self) -> Result<ctrn::BoxStream, ctrn::Error> {
        self.incoming
            .lock()
            .await
            .next()
            .await
            .ok_or_else(|| ctrn::Error::new(ctrn::ErrorKind::Closed, "byte fixture closed"))
    }
}
#[cfg_attr(not(target_arch = "wasm32"), ctrn::async_trait)]
#[cfg_attr(target_arch = "wasm32", ctrn::async_trait(?Send))]
impl ctrn::Network for ByteNetwork {
    async fn start(&self, _: &ctrn::Scope) -> Result<(), ctrn::Error> {
        self.online.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn ready(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }
    fn stop(&self) {
        self.online.store(false, Ordering::SeqCst);
    }
    async fn listen(&self) -> Result<Box<dyn ctrn::Listener>, ctrn::Error> {
        Ok(Box::new(ByteListener {
            address: self.here.clone(),
            incoming: self.incoming.clone(),
        }))
    }
    async fn dial(&self, to: &ctrn::OnionEndpoint) -> Result<ctrn::BoxStream, ctrn::Error> {
        if to.address() != self.there {
            return Err(ctrn::Error::new(
                ctrn::ErrorKind::Unreachable,
                "unknown byte peer",
            ));
        }
        let (a, b) = tokio::io::duplex(64);
        self.outgoing
            .clone()
            .send(Box::new(b.compat()))
            .await
            .map_err(|_| ctrn::Error::new(ctrn::ErrorKind::Closed, "byte peer closed"))?;
        Ok(Box::new(a.compat()))
    }
}
async fn adapter(network: Arc<ByteNetwork>, scope: u8) -> Arc<cmsh::Tor> {
    let clock: Arc<dyn ctrn::Clock> = Arc::new(Time);
    let node = Arc::new(
        ctrn::Node::new(
            network,
            clock.clone(),
            ctrn::Scope([scope; 32]),
            ctrn::Limits {
                streams: 4,
                bootstrap_ms: 1000,
                operation_ms: 1000,
            },
        )
        .unwrap(),
    );
    node.start().await.unwrap();
    Arc::new(cmsh::Tor(
        ctrn::messages::Messages::new(
            node,
            Arc::new(spawn),
            clock,
            ctrn::MessageLimits {
                max_payload: 1024,
                timeout_ms: 1000,
            },
        )
        .unwrap(),
    ))
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn mesh_tor_port_uses_bounded_messages_and_original_one_use_reply() {
    let a = ctrn::OnionEndpoint::parse(
        "a4dqobyha4dqobyha4dqobyha4dqobyha4dqobyha4dqobyha4dwc6ad.onion",
        443,
    )
    .unwrap()
    .address();
    let b = ctrn::OnionEndpoint::parse(
        "baeaqcaibaeaqcaibaeaqcaibaeaqcaibaeaqcaibaeaqcaibaecymad.onion",
        443,
    )
    .unwrap()
    .address();
    let (to_a, from_b) = mpsc::channel(2);
    let (to_b, from_a) = mpsc::channel(2);
    let a_address = a.clone();
    let a = adapter(
        Arc::new(ByteNetwork {
            online: AtomicBool::new(false),
            here: a.clone(),
            there: b.clone(),
            incoming: Arc::new(Mutex::new(from_b)),
            outgoing: to_b,
        }),
        1,
    )
    .await;
    let b = adapter(
        Arc::new(ByteNetwork {
            online: AtomicBool::new(false),
            here: b,
            there: a_address,
            incoming: Arc::new(Mutex::new(from_a)),
            outgoing: to_a,
        }),
        2,
    )
    .await;
    assert_eq!(a.max_payload(), 1024);
    assert!(!a.capabilities().dht);
    let ma = Mesh::builder(Arc::new(spawn))
        .backend(a.clone())
        .build()
        .unwrap();
    let mb = Mesh::builder(Arc::new(spawn)).backend(b).build().unwrap();
    let listening = mb.listen().await.unwrap();
    let receive = async {
        let Incoming::Message { payload, .. } = listening.next().await.unwrap() else {
            panic!("message")
        };
        assert_eq!(payload, b"one-way");
        let Incoming::Call { payload, reply, .. } = listening.next().await.unwrap() else {
            panic!("call")
        };
        assert_eq!(payload, vec![9; 1024]);
        reply.send(b"reply").await.unwrap();
    };
    let send = async {
        ma.app_message(listening.addresses(), b"one-way")
            .await
            .unwrap();
        assert_eq!(
            ma.app_call(listening.addresses(), &[9; 1024])
                .await
                .unwrap(),
            b"reply"
        );
    };
    futures::join!(send, receive);
    let other = cmsh::Address::new(cmsh::Scheme::new("other").unwrap(), vec![1]).unwrap();
    assert_eq!(
        a.app_call(&other, b"x").await.unwrap_err().kind(),
        cmsh::ErrorKind::Unsupported
    );
    let malformed = cmsh::Address::new(a.scheme(), vec![1]).unwrap();
    assert_eq!(
        a.app_message(&malformed, b"x").await.unwrap_err().kind(),
        cmsh::ErrorKind::InvalidAddress
    );
}
