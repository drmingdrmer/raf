use std::future::Future;
use std::ops::Range;

use crate::ArrayChunk;
use crate::Cmd;
use crate::Term;

/// Persistent storage interface used by the protocol core.
pub trait Storage: Send + Sync + 'static {
    /// Overwrite a contiguous range in the term array.
    fn update_terms(&mut self, since: u64, terms: &[Term]) -> impl Future<Output = ()> + Send;

    /// Read a contiguous range from the term array.
    fn read_terms(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Term>> + Send;

    /// Append commands at the end of the command array.
    fn append_cmds(&mut self, cmds: Vec<Cmd>) -> impl Future<Output = ()> + Send;

    /// Truncate the command array to `after` entries.
    fn truncate_cmds(&mut self, after: u64) -> impl Future<Output = ()> + Send;

    /// Read a contiguous range from the command array.
    fn read_cmds(&self, range: Range<u64>) -> impl Future<Output = ArrayChunk<Cmd>> + Send;
}
