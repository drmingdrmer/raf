//! Outbound network transport.
//!
//! `raf` exposes outbound consensus RPCs through [`Network`].
//!
//! The Core calls these methods directly and awaits the replies;
//! outbound responses do not loop back through the Core's mailbox.
//! Inbound RPCs *from* peers do arrive via the mailbox.

use std::future::Future;
use std::io;

use crate::AppendRequest;
use crate::append_reply::AppendReply;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;

/// Network transport.
///
/// Methods return `impl Future + Send` rather than `async fn` so
/// the futures stay `Send` across the Core's spawned task.
pub trait Network: Send + Sync + 'static {
    /// Forward a [`RequestVote`] to the node identified by `target`,
    /// await the reply, and return it.
    ///
    /// Implementations own the send-and-await round-trip — the
    /// Core does not see the wire-level send / receive split. The
    /// `target` is an opaque node identifier (`u64`).
    fn request_vote(
        &self,
        target: u64,
        req: RequestVote,
    ) -> impl Future<Output = Result<RequestVoteReply, io::Error>> + Send;

    /// Forward an [`AppendRequest`] to `target` and return its reply.
    fn append(&self, target: u64, req: AppendRequest) -> impl Future<Output = Result<AppendReply, io::Error>> + Send;
}
