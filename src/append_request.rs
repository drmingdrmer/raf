use crate::Cmd;
use crate::LogIndex;
use crate::Term;
use crate::log_id::LogId;

/// Append RPC sent by a leader to probe and replicate a log window.
pub struct AppendRequest {
    /// Leader term carried by this request.
    pub term: Term,

    /// Greatest log index the leader knows to be committed.
    pub commit_index: LogIndex,

    /// Log id immediately before the entries carried by this request.
    pub prev_log_id: LogId,

    /// Term entries after `prev_log_id`.
    pub terms: Vec<Term>,

    /// Command entries corresponding to `terms`.
    pub cmds: Vec<Cmd>,
}
