use crate::Clock;
use crate::Index;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogId {
    pub clock: Clock,
    pub index: Index,
}

impl LogId {
    pub fn new(clock: Clock, index: Index) -> Self {
        Self { clock, index }
    }
}
