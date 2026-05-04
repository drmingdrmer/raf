use crate::Clock;
use crate::Cmd;
use crate::Index;

pub struct AppendRequest {
    pub clock: Clock,
    pub since: Index,
    pub payloads: Vec<(Clock, Cmd)>,
}
