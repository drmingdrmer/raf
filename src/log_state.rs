//! [`HistoryState`] — the snapshot of persistent log state returned by
//! [`crate::HistoryStorage::read`].

use crate::Cmd;
use crate::accepted_content::AcceptedContent;

/// Snapshot of the persistent log state.
///
/// Bundles the requested entries with the persistent values the
/// Core needs to rebuild its in-memory state on startup: the
/// accepted-content cursor and the total log length.
#[derive(Debug)]
pub struct HistoryState {
    /// Total number of entries currently in the log, independent of
    /// the requested range.
    pub len: u64,

    /// Entries at the positions requested in the `read` call.
    ///
    /// `Some(id)` is a written entry; `None` is a *hole* — a
    /// position that has not yet been written to. Holes can arise
    /// because `Storage::append` accepts any starting position,
    /// so entries may be written out of order. See `DESIGN.md`
    /// §6.2.
    pub entries: Vec<Cmd>,
}
