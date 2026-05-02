//! In-memory mirror of the persistent log state, owned by the Core.
//!
//! `raf` serves replication out of memory; persistent storage
//! (`Storage`) is consulted only at startup. This struct is the
//! Core-owned in-memory mirror, kept in lockstep with `Storage` —
//! every successful `Storage::append` / `Storage::accept` has a
//! matching update here. See `DESIGN.md` §15.1.2.

use crate::accepted_content::AcceptedContent;

/// In-memory mirror of the persistent log state.
///
/// Mirrors the values the Core consumes from `Storage::read` at
/// startup, then keeps them current as the Core processes events.
#[derive(Debug)]
pub(crate) struct State {
    /// In-memory copy of the log. `None` is a *hole* — a position
    /// that has not been written to yet (see `DESIGN.md` §6.2). The
    /// vector length equals the log's `len` (highest written
    /// position + 1, or 0 if empty).
    pub log: Vec<Option<u64>>,

    /// Accepted-content cursor — the freshness comparator (§6.3).
    pub accepted: AcceptedContent,

    /// Value of the last non-hole entry in `log` — the highest
    /// `leader_index` this node has ever stored (§6.4). `None` if
    /// the log has no written entries.
    pub last_leader_index: Option<u64>,
}

impl State {
    /// State for a fresh node: empty log, default accepted cursor,
    /// no last leader.
    pub fn fresh() -> Self {
        Self {
            log: Vec::new(),
            accepted: AcceptedContent::default(),
            last_leader_index: None,
        }
    }

    /// Number of entries in the log, including holes.
    ///
    /// Equals "highest written position + 1", or 0 if the log is
    /// empty. Used as the candidate's chosen identity in §8.1.
    pub fn len(&self) -> u64 {
        self.log.len() as u64
    }
}
