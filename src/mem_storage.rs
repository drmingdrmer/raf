use std::ops::Range;

use crate::ArrayChunk;
use crate::Cmd;
use crate::Storage;
use crate::Term;
use crate::cmd_array::CmdArray;
use crate::term_array::TermArray;

/// In-memory storage implementation backed by term and command arrays.
pub struct MemStorage {
    /// In-memory term array.
    terms: TermArray,

    /// In-memory command array.
    cmds: CmdArray,
}

impl MemStorage {
    /// Create an empty in-memory storage with initialized index zero.
    pub fn new() -> Self {
        Self {
            terms: TermArray::new(vec![0]),
            cmds: CmdArray::new(vec![Cmd::empty()]),
        }
    }

    /// Create an in-memory storage from existing arrays.
    pub fn from_arrays(terms: TermArray, cmds: CmdArray) -> Self {
        Self { terms, cmds }
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemStorage {
    async fn update_terms(&mut self, since: u64, terms: &[Term]) {
        self.terms.update_terms(since, terms);
    }

    async fn read_terms(&self, range: Range<u64>) -> ArrayChunk<Term> {
        self.terms.read_terms(range)
    }

    async fn append_cmds(&mut self, cmds: Vec<Cmd>) {
        self.cmds.append_cmds(cmds);
    }

    async fn truncate_cmds(&mut self, after: u64) {
        self.cmds.truncate_cmds(after);
    }

    async fn read_cmds(&self, range: Range<u64>) -> ArrayChunk<Cmd> {
        self.cmds.read_cmds(range)
    }
}
