//! [`WriteReply`] — reply to [`crate::WriteRequest`].
//!
//! Returned by the leader once the write has been committed by a
//! quorum (see `DESIGN.md` §9). On non-leader nodes,
//! [`crate::Raf::write`] returns an `io::Error` instead of a
//! reply; the application interprets this as a leader-redirect
//! signal.

/// Reply to a successful application write.
#[derive(Debug, Clone)]
pub struct WriteReply {
    /// Log position at which the write was committed.
    pub index: u64,
}
