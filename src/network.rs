//! Outbound network transport.
//!
//! `raf` exposes a single outbound RPC for now —
//! [`Network::request_vote`] — used during leader election.
//! The Core calls it directly and awaits the reply; outbound
//! responses do not loop back through the Core's mailbox. Inbound
//! RPCs *from* peers do arrive via the mailbox (see `DESIGN.md`
//! §15.1.3).

use std::future::Future;
use std::io;

use crate::AppendRequest;
use crate::append_reply::AppendReply;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;

/// Network transport.
///
/// One method so far; more RPCs will be added as the protocol fills
/// in. Methods return `impl Future + Send` rather than `async fn`
/// so the futures stay `Send` across the Core's spawned task.
pub trait Network: Send + Sync + 'static {
    /// Forward a [`RequestVote`] to the node identified by `target`,
    /// await the reply, and return it.
    ///
    /// Implementations own the send-and-await round-trip — the
    /// Core does not see the wire-level send / receive split. The
    /// `target` is an opaque node identifier (`u64`).
    fn request_vote(&self, target: u64, req: RequestVote) -> impl Future<Output = io::Result<RequestVoteReply>> + Send;

    fn append(
        &self,
        target: u64,
        req: crate::append_request::AppendRequest,
    ) -> impl Future<Output = io::Result<AppendReply>> + Send;
}

/// Default in-process [`Network`] implementation, intended for
/// tests and single-process benchmarks.
///
/// Currently a stub — `send_request_vote` returns an `Err`. The
/// channel-based routing will be filled in once the multi-node
/// setup is wired.
#[derive(Default)]
pub struct InProcessNetwork;

impl InProcessNetwork {
    /// Create a new in-process network.
    pub fn new() -> Self {
        Self
    }
}

impl Network for InProcessNetwork {
    async fn request_vote(&self, _target: u64, _req: RequestVote) -> io::Result<RequestVoteReply> {
        Err(io::Error::other(
            "InProcessNetwork::send_request_vote not yet implemented",
        ))
    }

    async fn append(&self, target: u64, req: AppendRequest) -> io::Result<AppendReply> {
        todo!()
    }
}
