//! In-memory command sequence for a `raf` node.

use std::ops::Range;

use crate::Cmd;
use crate::cmd_chunk::CmdChunk;

pub struct CmdArray {
    cmds: Vec<Cmd>,
}

impl CmdArray {
    pub fn new(mut cmds: Vec<Cmd>) -> Self {
        if cmds.is_empty() {
            cmds.push(Cmd::empty());
        }

        Self { cmds }
    }

    pub fn cmds_len(&self) -> u64 {
        self.cmds.len() as u64
    }

    pub fn append_cmds(&mut self, cmds: Vec<Cmd>) {
        self.cmds.extend(cmds);
    }

    pub fn truncate_cmds(&mut self, after: u64) {
        self.cmds.truncate(after as usize);
    }

    pub fn read_cmds(&self, range: Range<u64>) -> CmdChunk {
        let len = self.cmds_len();
        let cmds = if range.start < len {
            let end = range.end.min(len);
            self.cmds[range.start as usize..end as usize].to_vec()
        } else {
            Vec::new()
        };

        CmdChunk::new(len, cmds)
    }
}
