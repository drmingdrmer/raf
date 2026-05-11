use std::io;

use crate::Storage;
use crate::Term;

/// Convenience methods derived from the base [`Storage`] operations.
pub trait StorageExt: Storage {
    /// Return the current term-array length.
    fn terms_len(&self) -> impl Future<Output = io::Result<u64>> + Send {
        let range = 0..0;
        async move { Ok(self.read_terms(range).await?.len) }
    }

    /// Return the last stored term.
    fn last_term(&self) -> impl Future<Output = io::Result<Term>> + Send {
        async move {
            let len = self.terms_len().await?;
            let index = len.checked_sub(1).unwrap();
            Ok(self.read_terms(index..index + 1).await?.entries.into_iter().next().unwrap())
        }
    }

    /// Read the term stored at one index.
    fn read_one_term(&self, index: u64) -> impl Future<Output = io::Result<Term>> + Send {
        async move { Ok(self.read_terms(index..index + 1).await?.entries.into_iter().next().unwrap()) }
    }

    /// Fill existing term slots in `[since, len)` with their own index.
    fn fill_terms_gap(&mut self, since: u64) -> impl Future<Output = io::Result<()>> + Send {
        async move {
            let len = self.terms_len().await?;
            let start = since.min(len);
            for index in start..len {
                self.update_terms(index, &[index as Term]).await?;
            }

            Ok(())
        }
    }

    /// Return the current command-array length.
    fn cmds_len(&self) -> impl Future<Output = io::Result<u64>> + Send {
        let range = 0..0;
        async move { Ok(self.read_cmds(range).await?.len) }
    }
}

impl<S> StorageExt for S where S: Storage {}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;
    use crate::ArrayChunk;
    use crate::Cmd;

    struct FailingStorage;

    impl Storage for FailingStorage {
        async fn update_terms(&mut self, _since: u64, _terms: &[Term]) -> io::Result<()> {
            Err(io::Error::other("term update failed"))
        }

        async fn read_terms(&self, _range: Range<u64>) -> io::Result<ArrayChunk<Term>> {
            Err(io::Error::other("term read failed"))
        }

        async fn append_cmds(&mut self, _cmds: Vec<Cmd>) -> io::Result<()> {
            Err(io::Error::other("command append failed"))
        }

        async fn truncate_cmds(&mut self, _after: u64) -> io::Result<()> {
            Err(io::Error::other("command truncate failed"))
        }

        async fn read_cmds(&self, _range: Range<u64>) -> io::Result<ArrayChunk<Cmd>> {
            Err(io::Error::other("command read failed"))
        }
    }

    struct MissingTermStorage;

    impl Storage for MissingTermStorage {
        async fn update_terms(&mut self, _since: u64, _terms: &[Term]) -> io::Result<()> {
            Ok(())
        }

        async fn read_terms(&self, _range: Range<u64>) -> io::Result<ArrayChunk<Term>> {
            Ok(ArrayChunk {
                len: 0,
                entries: Vec::new(),
            })
        }

        async fn append_cmds(&mut self, _cmds: Vec<Cmd>) -> io::Result<()> {
            Ok(())
        }

        async fn truncate_cmds(&mut self, _after: u64) -> io::Result<()> {
            Ok(())
        }

        async fn read_cmds(&self, _range: Range<u64>) -> io::Result<ArrayChunk<Cmd>> {
            Ok(ArrayChunk {
                len: 0,
                entries: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn terms_len_propagates_storage_error() {
        let storage = FailingStorage;

        let err = storage.terms_len().await.unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(err.to_string(), "term read failed");
    }

    #[tokio::test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    async fn read_one_term_panics_for_missing_index() {
        let storage = MissingTermStorage;

        let _ = storage.read_one_term(0).await;
    }
}
