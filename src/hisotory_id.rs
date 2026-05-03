use crate::Clock;
use crate::Index;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HistoryId {
    pub clock: Clock,
    pub index: Index,
}
