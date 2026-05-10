//! [`CmdChunk`] — the snapshot of persistent command state returned by
//! [`crate::history_storage::CmdArray::read`].

use crate::ArrayChunk;
use crate::Cmd;

pub type CmdChunk = ArrayChunk<Cmd>;
