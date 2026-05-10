use crate::Cmd;
use crate::LogIndex;
use crate::Term;

pub struct AppendRequest {
    pub term: Term,

    pub assume_matched_at: LogIndex,

    pub terms: Vec<Term>,
    pub cmds: Vec<Cmd>,
}
