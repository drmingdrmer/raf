use crate::Storage;
use crate::Term;

/// Convenience methods derived from the base [`Storage`] operations.
pub trait StorageExt: Storage {
    /// Return the current term-array length.
    fn terms_len(&self) -> impl Future<Output = u64> + Send {
        let range = 0..0;
        async move { self.read_terms(range).await.len }
    }

    /// Return the last stored term, or zero if storage is empty.
    fn last_term(&self) -> impl Future<Output = Term> + Send {
        async move {
            let len = self.terms_len().await;
            self.read_terms(len - 1..len).await.entries.last().copied().unwrap_or(0)
        }
    }

    /// Read the term stored at one index.
    fn read_one_term(&self, index: u64) -> impl Future<Output = Term> + Send {
        async move { self.read_terms(index..index + 1).await.entries[0] }
    }

    /// Fill existing term slots in `[since, len)` with their own index.
    fn fill_terms_gap(&mut self, since: u64) -> impl Future<Output = ()> + Send {
        async move {
            let len = self.terms_len().await;
            let start = since.min(len);
            for index in start..len {
                self.update_terms(index, &[index as Term]).await;
            }
        }
    }

    /// Return the current command-array length.
    fn cmds_len(&self) -> impl Future<Output = u64> + Send {
        let range = 0..0;
        async move { self.read_cmds(range).await.len }
    }
}

impl<S> StorageExt for S where S: Storage {}
