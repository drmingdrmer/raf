use std::ops::Range;

use crate::ArrayChunk;
use crate::Cmd;
use crate::Term;
use crate::cmd_array::CmdArray;
use crate::term_array::TermArray;

pub trait Storage {
    fn update_terms(&mut self, since: u64, terms: &[Term]) -> impl Future<Output = ()> + Send;
    fn read_terms(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Term>> + Send;

    fn append_cmds(&mut self, cmds: Vec<Cmd>) -> impl Future<Output = ()> + Send;
    fn truncate_cmds(&mut self, after: u64) -> impl Future<Output = ()> + Send;
    fn read_cmds(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Cmd>> + Send;
}

pub struct MemStorage {
    terms: TermArray,
    cmds: CmdArray,
}

impl MemStorage {
    pub fn new() -> Self {
        Self {
            terms: TermArray::new(vec![0]),
            cmds: CmdArray::new(vec![Cmd::empty()]),
        }
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
