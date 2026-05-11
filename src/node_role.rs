/// Current protocol role of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// The node is not currently a candidate or leader.
    Follower,

    /// The node has started an election and is waiting for quorum.
    Candidate,

    /// The node has reached election quorum and may serve writes.
    Leader,
}
