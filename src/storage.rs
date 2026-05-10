use std::future::Future;
use std::ops::Range;

use crate::ArrayChunk;
use crate::Cmd;
use crate::Term;
pub trait Storage: Send + Sync + 'static {
    fn update_terms(&mut self, since: u64, terms: &[Term]) -> impl Future<Output = ()> + Send;
    fn read_terms(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Term>> + Send;

    fn append_cmds(&mut self, cmds: Vec<Cmd>) -> impl Future<Output = ()> + Send;
    fn truncate_cmds(&mut self, after: u64) -> impl Future<Output = ()> + Send;
    fn read_cmds(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Cmd>> + Send;
}
