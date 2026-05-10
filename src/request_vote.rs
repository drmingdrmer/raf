//! [`RequestVote`] — the leader-election RPC.
//!
//! A candidate sends this to the other nodes to claim a
//! `leader_index` (the next position past the end of its local log)
//! as its leader identity. The request also carries the candidate's
//! last log id so voters can compare history freshness against their
//! own state. See `DESIGN.md` §7.1 and §8.

use crate::log_id::LogId;
use crate::term::Term;

/// Leader-election request, modeled on Raft's `RequestVote` RPC.
///
/// Sent from a candidate to another node to claim `leader_index`
/// as the candidate's leader identity. The candidate's identity is
/// the next position past the end of its local log — the index at
/// which it would write its first entry as leader. Calling the
/// identity by the position it claims removes the need for a
/// separate term.
#[derive(Debug, Clone)]
pub struct RequestVote {
    /// The candidate's chosen leader identity — the next index past
    /// the end of its local log.
    pub term: Term,

    pub last_log_id: LogId,
}
