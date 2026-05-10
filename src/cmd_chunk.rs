//! [`CmdChunk`] — the snapshot of command entries returned by
//! [`crate::cmd_array::CmdArray::read_cmds`].

use crate::ArrayChunk;
use crate::Cmd;

/// Command entries returned together with the source command-array length.
pub type CmdChunk = ArrayChunk<Cmd>;
