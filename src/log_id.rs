use crate::LogIndex;
use crate::Term;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
/// Identity of a log entry.
pub struct LogId {
    /// Leader term recorded at this log index.
    pub term: Term,

    /// Log index of the entry.
    pub index: LogIndex,
}

impl LogId {
    /// Build a log id from a term and index.
    pub fn new(term: Term, index: LogIndex) -> Self {
        Self { term, index }
    }
}
