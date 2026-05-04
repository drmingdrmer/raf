use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    node_ids: Vec<NodeId>,
}

impl Membership {
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        Self { node_ids }
    }

    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    pub fn is_quorum(&self, granted_by: &[NodeId]) -> bool {
        granted_by.len() > self.node_ids.len() / 2
    }
}
