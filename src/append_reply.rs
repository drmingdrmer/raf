use crate::LogIndex;
use crate::Term;
use crate::log_id::LogId;

/// Reply returned by a follower for an [`AppendRequest`](crate::AppendRequest).
pub struct AppendReply {
    /// The latest term known by the responder.
    pub term: Term,

    /// The greatest log id matched by the responder in the append window.
    pub matched: Option<LogId>,

    /// The first index in the append window that conflicted, if no prefix matched.
    pub conflict: Option<LogIndex>,
}
