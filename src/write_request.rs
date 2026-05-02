//! [`WriteRequest`] — application request asking the leader to
//! occupy one log entry position on its behalf.
//!
//! The request itself is opaque from `raf`'s perspective; the
//! leader assigns the position. The application correlates the
//! reply (`WriteReply`, with the committed index) back to its own
//! data. See `DESIGN.md` §9.

/// Application write request — "claim the next log entry position".
///
/// `id` is an application-supplied identifier, used by the
/// application to correlate the [`WriteReply`](crate::WriteReply)
/// back to its own data. Opaque to `raf`.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Opaque application identifier for this write.
    pub id: u64,
}
