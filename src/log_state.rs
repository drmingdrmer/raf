//! [`HistoryState`] — the snapshot of persistent log state returned by
//! [`crate::HistoryStorage::read`].

use crate::ArrayChunk;
use crate::Cmd;

pub type CmdChunk = ArrayChunk<Cmd>;
