use crate::NodeId;

pub struct ReplicationState {
    pub target: NodeId,

    pub start: u64,
    pub end: u64,

    pub history_id: HistoryId,
    pub membership: Membership,
    pub clock_array: ClockArray,
}
