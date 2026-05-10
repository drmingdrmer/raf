use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::NodeId;

#[derive(Debug)]
/// Leader-side replication progress for one target node.
pub struct ReplicationState {
    /// Target node this state describes.
    pub target: NodeId,

    /// Greatest log index known to match the target.
    pub matched: u64,

    /// Upper bound used by the bisection probe.
    pub end: u64,

    /// Per-target permit that allows at most one in-flight append.
    pub inflight: Arc<Semaphore>,
}

impl ReplicationState {
    /// Create replication state for `target` with an initial upper bound.
    pub fn new(target: NodeId, end: u64) -> Self {
        Self {
            target,
            matched: 0,
            end,
            inflight: Arc::new(Semaphore::new(1)),
        }
    }
}
