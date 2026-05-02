//! [`AcceptedContent`] — the `(leader_index, index)` cursor that
//! identifies the last known accepted entry.

/// The last known accepted content — a pair `(leader_index, index)`
/// marking the boundary between the decided prefix and the
/// speculative suffix of a node's log.
///
/// `index` is the count of decided entries (positions `< index`
/// are decided). `leader_index` is the leader_index recorded at
/// log position `index - 1` — the identity of the leader that
/// produced the last accepted entry.
///
/// The pair is compared **lexicographically**, `leader_index`
/// first then `index` — the same shape Raft uses for
/// `(lastTerm, lastLogIndex)`. See `DESIGN.md` §6.3 and §7.1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct AcceptedContent {
    /// The leader_index at log position `index - 1`. Undefined
    /// when `index == 0` (no accepted entries yet); the
    /// convention is to treat this as `0` in that case.
    pub leader_index: u64,

    /// The accepted prefix length: positions `[0..index)` are
    /// decided.
    pub index: u64,
}
