//! [`RequestVoteReply`] — response to [`crate::RequestVote`].

use crate::Clock;
use crate::hisotory_id::HistoryId;

/// Response to a [`crate::RequestVote`] request.
///
/// Carries the responder's local state regardless of whether the
/// vote was granted. When `granted == false`, the candidate
/// compares its own values against the reply to determine which
/// condition caused the rejection — an occupied `leader_index`, or
/// a stale `accepted` cursor. See `DESIGN.md` §7.1 and §8.3.
#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    /// Whether the responder granted the vote.
    pub granted: bool,

    /// The responder's most-recently-seen leader_index — the
    /// identity of the most recent leader the responder is aware
    /// of. If `granted` is false because the candidate's
    /// `leader_index` was already occupied or was not higher than
    /// this value, the candidate uses this to skip ahead.
    pub clock_len: Clock,

    /// The responder's local [`AcceptedContent`]. Used by the
    /// candidate as the freshness comparator if the vote was
    /// rejected on freshness grounds.
    pub last_history: HistoryId,
}
