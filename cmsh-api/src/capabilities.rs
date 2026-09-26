//! What a backend guarantees.

/// Expected latency class of a backend, best first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LatencyClass {
    /// Suitable for live voice (tens of milliseconds one way).
    Realtime,
    /// Suitable for live text (hundreds of milliseconds).
    Interactive,
    /// Seconds or more; store-and-forward.
    Bulk,
}

impl LatencyClass {
    /// All classes, best first.
    pub const ALL: [LatencyClass; 3] = [Self::Realtime, Self::Interactive, Self::Bulk];
}

/// Capabilities a backend declares. The facade trusts these declarations only
/// as far as it trusts the backend code; it checks the hooks for consistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Capabilities {
    /// Neither side learns the other's network location (IP address), and the
    /// network operators cannot link the two ends.
    pub anonymous: bool,
    /// Unreliable datagrams via [`crate::Backend::datagrams`].
    pub datagrams: bool,
    /// Delivery while the recipient is offline (store-and-forward).
    pub offline_delivery: bool,
    /// A DHT via [`crate::Backend::dht`].
    pub dht: bool,
    /// Expected latency.
    pub latency: LatencyClass,
}
