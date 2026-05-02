//! Outbound network transport.
//!
//! A single [`Network`] instance is held by the Core; all I/O flows
//! through it. Responses received from peers are routed back into the
//! Core's mailbox rather than returned synchronously from a send call,
//! consistent with the single-mailbox runtime design (see `DESIGN.md`
//! §15.1.3).

/// Network transport.
///
/// Concrete surface will be filled in once the request/response types
/// are settled.
pub trait Network: Send + Sync + 'static {}

/// Default in-process [`Network`] implementation, built on channels.
/// Intended for tests and single-process benchmarks.
#[derive(Default)]
pub struct InProcessNetwork;

impl InProcessNetwork {
    /// Create a new in-process network.
    pub fn new() -> Self {
        Self
    }
}

impl Network for InProcessNetwork {}
