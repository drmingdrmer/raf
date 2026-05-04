use crate::NodeId;

pub struct ReplicationState {
    pub target: NodeId,

    pub start: u64,
    pub end: u64,

    pub inflight: bool,
}

impl ReplicationState {
    pub fn new(target: NodeId, end: u64) -> Self {
        Self {
            target,
            start: 0,
            end,
            inflight: false,
        }
    }
}
