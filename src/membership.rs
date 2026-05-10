use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Static set of nodes participating in the current cluster.
pub struct Membership {
    /// Node ids that are voting members of the cluster.
    node_ids: Vec<NodeId>,
}

impl Membership {
    /// Create a membership from a fixed list of node ids.
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        Self { node_ids }
    }

    /// Return the voting node ids.
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// Return true when `granted_by` contains enough votes for a quorum.
    pub fn is_quorum(&self, granted_by: &[NodeId]) -> bool {
        granted_by.len() > self.node_ids.len() / 2
    }
}
