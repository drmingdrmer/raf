//! [`CmdChunk`] — the snapshot of command entries returned by
//! [`crate::cmd_array::CmdArray::read_cmds`].

use crate::ArrayChunk;
use crate::Cmd;

pub type CmdChunk = ArrayChunk<Cmd>;
