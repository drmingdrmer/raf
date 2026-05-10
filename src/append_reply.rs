use crate::LogIndex;
use crate::Term;
use crate::log_id::LogId;

pub struct AppendReply {
    // TODO: also return the last empty slot index?
    pub term: Term,
    pub matched: Option<LogId>,
    pub conflict: Option<LogIndex>,
}
