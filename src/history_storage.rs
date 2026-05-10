//! Persistent state interface for a `raf` node.
//!
//! `raf` does not store log payloads — only a sequence of opaque 64-bit
//! identifiers (the *log identity*). The application is responsible
//! for mapping each identifier to its payload and persisting that
//! mapping in its own storage. See `DESIGN.md` §6.

use std::ops::Range;

use crate::Cmd;
use crate::log_state::CmdChunk;

pub struct CmdArray {
    cmds: Vec<Cmd>,
}

impl CmdArray {
    pub fn new(cmds: Vec<Cmd>) -> Self {
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
