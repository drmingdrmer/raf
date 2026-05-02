//! [`RequestVote`] — the leader-election RPC.
//!
//! A candidate sends this to the other nodes to claim a
//! `leader_index` (the next position past the end of its local log)
//! as its leader identity. The pair
//! (`accepted_index`, `accepted_leader_index`) carries the
//! candidate's last known accepted content, so voters can compare
//! freshness against their own state. See `DESIGN.md` §7.1 and §8.

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

    /// The candidate's local accepted index — the length of the
    /// decided prefix on this candidate's storage. See `DESIGN.md`
    /// §6.3.
    pub accepted_index: u64,

    /// The leader_index recorded at log position
    /// `accepted_index - 1` — the identity of the leader that
    /// produced the candidate's last accepted entry. Together with
    /// [`accepted_index`](Self::accepted_index) this pair forms the
    /// candidate's *last known accepted content*, which voters
    /// compare against their own to decide whether the candidate's
    /// log is at least as up-to-date as theirs.
    pub accepted_leader_index: u64,
}
