use crate::LogIndex;
use crate::NodeId;
use crate::ReplicationState;

/// Public replication progress for one target node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationMetrics {
    /// Target node this replication state describes.
    pub target: NodeId,

    /// Greatest log index known to match the target.
    pub matched: LogIndex,

    /// Upper bound used by the leader's bisection probe.
    pub end: LogIndex,

    /// Whether there is an in-flight append request for this target.
    pub inflight: bool,
}

impl ReplicationMetrics {
    /// Build metrics from the internal replication state.
    pub(crate) fn from_replication_state(replication: &ReplicationState) -> Self {
        Self {
            target: replication.target,
            matched: replication.matched,
            end: replication.end,
            inflight: replication.inflight.available_permits() == 0,
        }
    }
}
