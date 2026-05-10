//! [`RequestVote`] — the leader-election RPC.
//!
//! A candidate sends this to the other nodes to claim a
//! term slot as its leader identity. The request also carries the
//! candidate's last log id so voters can compare freshness against
//! their own state. See `DESIGN.md` §7.1 and §8.

use crate::log_id::LogId;
use crate::term::Term;

/// Leader-election request, modeled on Raft's `RequestVote` RPC.
///
/// Sent from a candidate to another node to claim a term slot as
/// the candidate's leader identity.
#[derive(Debug, Clone)]
pub struct RequestVote {
    /// The candidate's chosen leader identity.
    pub term: Term,

    pub last_log_id: LogId,
}
