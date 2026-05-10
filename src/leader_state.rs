//! [`LeaderState`] — Core-side state held when this node is a
//! candidate or an established leader.
//!
//! Followers carry no leader state (`leader: None` on Core).
//! Candidates and established leaders carry a `LeaderState`
//! (`leader: Some(_)`); the `established` flag distinguishes the
//! two.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io;

use tokio::sync::oneshot;

use crate::LogIndex;
use crate::NodeId;
use crate::ReplicationState;
use crate::WriteReply;

/// Election / leadership state.
///
/// Present on a node that has issued a `RequestVote` for its own
/// candidacy and not yet stepped down. While present, the running
/// tally of granted votes accumulates as replies arrive; once the
/// tally reaches quorum, the leader is **established** and may
/// serve application writes (see `DESIGN.md` §8.4 and §9).
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct LeaderState {
    /// The candidate's chosen leader term.
    pub term: u64,

    /// Node IDs that have granted votes for this term,
    /// including this node itself. Granting is monotone: a peer
    /// is added on the first granted reply and never removed.
    pub granted_votes: HashSet<NodeId>,

    /// `true` once the granted tally has reached a quorum of the
    /// cluster. Flips false → true once and never reverses within
    /// the same `LeaderState` instance. Only an established leader
    /// may serve application writes.
    pub established: bool,

    /// Replication state for every node, including the leader itself.
    pub replications: BTreeMap<NodeId, ReplicationState>,

    pub committed: u64,

    pub pending_writes: VecDeque<(LogIndex, oneshot::Sender<Result<WriteReply, io::Error>>)>,
}
