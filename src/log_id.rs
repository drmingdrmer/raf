use crate::LogIndex;
use crate::Term;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogId {
    pub term: Term,
    pub index: LogIndex,
}

impl LogId {
    pub fn new(clock: Term, index: LogIndex) -> Self {
        Self { term: clock, index }
    }
}
