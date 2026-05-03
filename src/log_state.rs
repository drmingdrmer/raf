//! [`HistoryState`] — the snapshot of persistent log state returned by
//! [`crate::HistoryStorage::read`].

use crate::ArrayChunk;
use crate::Cmd;
use crate::accepted_content::AcceptedContent;

pub type CmdChunk = ArrayChunk<Cmd>;
