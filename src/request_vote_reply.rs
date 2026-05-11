//! [`RequestVoteReply`] — response to [`crate::RequestVote`].

use crate::LogIndex;
use crate::log_id::LogId;

/// Response to a [`crate::RequestVote`] request.
///
/// Carries the responder's local state regardless of whether the
/// vote was granted. When `granted == false`, the candidate
/// compares its own values against the reply to determine which
/// condition caused the rejection: a stale term or a stale log id.
/// See `DESIGN.md` §7.1 and §8.3.
#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    /// Whether the responder granted the vote.
    pub granted: bool,

    /// The responder's next writable term slot. If the candidate is
    /// behind, it can use this to skip to a newer slot.
    pub next_term_slot: LogIndex,

    /// The responder's local last log id. Used by the candidate as
    /// the freshness comparator if the vote was rejected on freshness
    /// grounds.
    pub last_log_id: LogId,
}

impl RequestVoteReply {
    /// Return this reply with its grant flag set to `granted`.
    pub fn with_granted(mut self, granted: bool) -> Self {
        self.granted = granted;
        self
    }
}
