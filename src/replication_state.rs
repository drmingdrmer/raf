use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::NodeId;

#[derive(Debug)]
pub struct ReplicationState {
    pub target: NodeId,

    pub matched: u64,
    pub end: u64,

    pub inflight: Arc<Semaphore>,
}

impl ReplicationState {
    pub fn new(target: NodeId, end: u64) -> Self {
        Self {
            target,
            matched: 0,
            end,
            inflight: Arc::new(Semaphore::new(1)),
        }
    }
}
