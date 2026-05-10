use std::future::Future;
use std::ops::Range;

use crate::ArrayChunk;
use crate::Cmd;
use crate::Term;
use crate::cmd_array::CmdArray;
use crate::term_array::TermArray;

pub trait Storage: Send + Sync {
    fn update_terms(&mut self, since: u64, terms: &[Term]) -> impl Future<Output = ()> + Send;
    fn read_terms(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Term>> + Send;

    fn append_cmds(&mut self, cmds: Vec<Cmd>) -> impl Future<Output = ()> + Send;
    fn truncate_cmds(&mut self, after: u64) -> impl Future<Output = ()> + Send;
    fn read_cmds(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Cmd>> + Send;
}

pub trait StorageExt: Storage {
    fn terms_len(&self) -> impl Future<Output = u64> + Send {
        let range = 0..0;
        async move { self.read_terms(range).await.len }
    }

    fn last_term(&self) -> impl Future<Output = Term> + Send {
        async move {
            let len = self.terms_len().await;
            self.read_terms(len - 1..len).await.entries.last().copied().unwrap_or(0)
        }
    }

    fn read_one_term(&self, index: u64) -> impl Future<Output = Term> + Send {
        async move { self.read_terms(index..index + 1).await.entries[0] }
    }

    fn fill_terms_gap(&mut self, since: u64) -> impl Future<Output = ()> + Send {
        async move {
            let len = self.terms_len().await;
            let start = since.min(len);
            for index in start..len {
                self.update_terms(index, &[index as Term]).await;
            }
        }
    }

    fn cmds_len(&self) -> impl Future<Output = u64> + Send {
        let range = 0..0;
        async move { self.read_cmds(range).await.len }
    }
}

impl<S> StorageExt for S where S: Storage {}

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
