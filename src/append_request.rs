use crate::Cmd;
use crate::LogIndex;
use crate::Term;

/// Append RPC sent by a leader to probe and replicate a log window.
pub struct AppendRequest {
    /// Leader term carried by this request.
    pub term: Term,

    /// Greatest log index the leader knows to be committed.
    pub commit_index: LogIndex,

    /// First log index included in `terms` and `cmds`.
    pub assume_matched_at: LogIndex,

    /// Term window starting at `assume_matched_at`.
    pub terms: Vec<Term>,

    /// Command window corresponding to `terms`.
    pub cmds: Vec<Cmd>,
}
