//! Persistent state interface for a `raf` node.
//!
//! `raf` does not store log payloads — only a sequence of opaque 64-bit
//! identifiers (the *log identity*). The application is responsible
//! for mapping each identifier to its payload and persisting that
//! mapping in its own storage. See `DESIGN.md` §6.

use std::future::Future;
use std::io;
use std::ops::Range;

/// Storage interface for the `raf` log.
///
/// The log is a sequence of `u64` identifiers; this trait exposes the
/// minimum surface needed to make that sequence durable and replayable.
///
/// All methods return [`io::Result`] — storage failures are I/O
/// failures by nature, and a single concrete error type keeps the
/// trait simple. Methods are declared as `fn ... -> impl Future + Send`
/// rather than `async fn` so the returned futures are guaranteed
/// `Send` — the Core runs as a `tokio::spawn`'d task and must be able
/// to `.await` storage calls across task boundaries.
pub trait Storage: Send + Sync + 'static {
    /// Append a sequence of log-entry identifiers to the end of the log.
    fn append(&mut self, ids: &[u64]) -> impl Future<Output = io::Result<()>> + Send;

    /// Remove all entries at position `from` and beyond.
    ///
    /// `from` is a 0-based position into the log array.
    fn truncate(&mut self, from: u64) -> impl Future<Output = io::Result<()>> + Send;

    /// Read the identifiers at positions in `range`, together with
    /// the total length of the stored log.
    ///
    /// Used by the Core at startup to rebuild its in-memory log from
    /// persistent storage; not on the steady-state hot path.
    /// Positions are 0-based into the log array; `range` is half-open
    /// (`start..end`), with `end` exclusive.
    ///
    /// Returns `(entries, len)` where `entries` are the identifiers
    /// at the requested positions and `len` is the total number of
    /// entries currently in the log — independent of `range`. `len`
    /// lets the Core size its in-memory log without a separate call.
    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<(Vec<u64>, u64)>> + Send;
}
