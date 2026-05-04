use crate::Clock;
use crate::Index;
use crate::hisotory_id::HistoryId;

pub struct AppendReply {
    pub clock: Clock,
    pub matched: Option<HistoryId>,
    pub conflict: Option<Index>,
}
