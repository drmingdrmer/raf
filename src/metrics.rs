//! Observable node metrics.

use std::collections::BTreeMap;

use crate::LogIndex;
use crate::NodeId;
use crate::Term;
use crate::node_role::NodeRole;
use crate::replication_metrics::ReplicationMetrics;

/// Observable metrics snapshot for a `raf` node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metrics {
    /// Local node id.
    pub id: NodeId,

    /// Current static voting membership.
    pub membership: Vec<NodeId>,

    /// Current protocol role.
    pub role: NodeRole,

    /// Active candidacy or leadership term, if this node has one.
    pub active_term: Option<Term>,

    /// Greatest log index known to be committed by this node.
    pub committed: LogIndex,

    /// Next writable term slot.
    pub next_term_slot: LogIndex,

    /// Next writable log slot.
    pub next_log_slot: LogIndex,

    /// Votes granted to this node in its active term.
    pub granted_votes: Vec<NodeId>,

    /// Replication progress keyed by target node id.
    pub replications: BTreeMap<NodeId, ReplicationMetrics>,
}

impl Metrics {
    /// Build the initial metrics value used before the Core publishes
    /// its first storage-backed snapshot.
    pub(crate) fn initial(id: NodeId, membership: Vec<NodeId>) -> Self {
        Self {
            id,
            membership,
            role: NodeRole::Follower,
            active_term: None,
            committed: 0,
            next_term_slot: 0,
            next_log_slot: 0,
            granted_votes: Vec::new(),
            replications: BTreeMap::new(),
        }
    }
}
