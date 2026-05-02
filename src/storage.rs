//! Persistent state interface for a `raf` node.
//!
//! `raf` does not store log payloads — only a sequence of opaque 64-bit
//! identifiers (the *log identity*). The application is responsible
//! for mapping each identifier to its payload and persisting that
//! mapping in its own storage. See `DESIGN.md` §6.

use std::future::Future;
use std::io;
use std::ops::Range;

use crate::log_state::LogState;

/// Storage interface for the `raf` log.
///
/// The log is a sequence of `u64` identifiers, plus a single
/// persistent `u64` cursor — the *accepted index* (see `DESIGN.md`
/// §6.3). This trait exposes the minimum surface needed to make
/// that state durable and replayable.
///
/// All methods return [`io::Result`] — storage failures are I/O
/// failures by nature, and a single concrete error type keeps the
/// trait simple. Methods are declared as `fn ... -> impl Future + Send`
/// rather than `async fn` so the returned futures are guaranteed
/// `Send` — the Core runs as a `tokio::spawn`'d task and must be able
/// to `.await` storage calls across task boundaries.
pub trait Storage: Send + Sync + 'static {
    /// Write `ids` at positions `[from..from + ids.len())`,
    /// overwriting any prior values at those positions. Positions
    /// outside that range are unchanged.
    ///
    /// `from` may be greater than the highest previously-written
    /// position; the intervening positions become **holes** that a
    /// later `append` may fill. There is no truncate operation —
    /// stale trailing entries persist in storage until they are
    /// themselves overwritten by a future `append`. See
    /// `DESIGN.md` §6.2.
    fn append(&mut self, from: u64, ids: &[u64]) -> impl Future<Output = io::Result<()>> + Send;

    /// Persist the accepted index — the boundary above which entries
    /// have not yet been confirmed identical to the leader's log.
    ///
    /// The accepted index is monotone non-decreasing: each successful
    /// `accept(idx)` advances the cursor to `idx` (or leaves it
    /// unchanged if `idx` is less than the current value).
    fn accept(&mut self, idx: u64) -> impl Future<Output = io::Result<()>> + Send;

    /// Read a snapshot of persistent log state.
    ///
    /// Used by the Core at startup to rebuild its in-memory state
    /// from persistent storage; not on the steady-state hot path.
    /// Positions are 0-based into the log array; `range` is half-open
    /// (`start..end`), with `end` exclusive.
    ///
    /// The returned [`LogState`] carries the entries at positions in
    /// `range` (each as `Option<u64>` — `None` is a hole), the
    /// persisted accepted index, and the total length, so the Core
    /// can rebuild full state in a single call.
    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<LogState>> + Send;
}
