//! [`RequestVote`] — the leader-election RPC.
//!
//! A candidate sends this to the other nodes to claim a
//! `leader_index` (the next position past the end of its local log)
//! as its leader identity. The [`AcceptedContent`] field carries
//! the candidate's last known accepted content, so voters can
//! compare freshness against their own state. See `DESIGN.md`
//! §7.1 and §8.

use crate::accepted_content::AcceptedContent;

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
    pub leader_index: u64,

    /// The candidate's last known accepted content. Voters use this
    /// as the freshness comparator: a vote is granted only if the
    /// candidate's `accepted` is at least as up-to-date as the
    /// voter's own. See `DESIGN.md` §6.3 and §8.
    pub accepted: AcceptedContent,
}
