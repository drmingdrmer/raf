use crate::Clock;
use crate::Cmd;
use crate::Index;

pub struct AppendRequest {
    pub payloads: Vec<(Index, Clock, Cmd)>,
}
