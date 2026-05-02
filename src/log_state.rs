//! [`LogState`] — the snapshot of persistent log state returned by
//! [`crate::Storage::read`].

use crate::accepted_content::AcceptedContent;

/// Snapshot of the persistent log state.
///
/// Bundles the requested entries with the persistent values the
/// Core needs to rebuild its in-memory state on startup: the
/// accepted-content cursor and the total log length.
#[derive(Debug)]
pub struct LogState {
    /// Entries at the positions requested in the `read` call.
    ///
    /// `Some(id)` is a written entry; `None` is a *hole* — a
    /// position that has not yet been written to. Holes can arise
    /// because `Storage::append` accepts any starting position,
    /// so entries may be written out of order. See `DESIGN.md`
    /// §6.2.
    pub entries: Vec<Option<u64>>,

    /// Persistent accepted-content cursor splitting the log into
    /// a decided prefix (positions `< accepted.index`, confirmed
    /// identical to the leader's log) and a speculative suffix
    /// (positions `>= accepted.index`, still subject to
    /// overwrite). See `DESIGN.md` §6.3.
    pub accepted: AcceptedContent,

    /// Total number of entries currently in the log, independent of
    /// the requested range.
    pub len: u64,
}

impl LogState {
    /// Whether the persisted [`accepted`](Self::accepted) cursor
    /// is *valid* — i.e., the entire stored log is accepted
    /// (`accepted.index >= len`).
    ///
    /// When `false`, storage holds a speculative tail of entries
    /// past `accepted.index` that have not yet been confirmed by
    /// the current leader. The Core uses this distinction at
    /// startup: a valid cursor means the entire stored log is
    /// safe to use as-is; an invalid one means the trailing
    /// entries must be re-validated against the next leader
    /// before they can be relied on. See `DESIGN.md` §6.3.
    pub fn is_accepted_valid(&self) -> bool {
        self.accepted.index >= self.len
    }
}
