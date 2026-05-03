use crate::Clock;
use crate::Index;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HistoryId {
    pub clock: Clock,
    pub index: Index,
}

impl HistoryId {
    pub fn new(clock: Clock, index: Index) -> Self {
        Self { clock, index }
    }
}
