//! Application-side control handle for a running [`crate::Raf`] node.

use std::io;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::AppendReply;
use crate::AppendRequest;
use crate::core::Event;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::write_reply::WriteReply;
use crate::write_request::WriteRequest;

/// Cheap-clone control handle to a running `raf` node.
///
/// Internally just an `mpsc` sender into the Core's mailbox, so cloning
/// is the cost of an `Arc` bump.
#[derive(Clone)]
pub struct Handle {
    mailbox_tx: UnboundedSender<Event>,
}

impl Handle {
    pub(crate) fn new(mailbox_tx: UnboundedSender<Event>) -> Self {
        Self { mailbox_tx }
    }

    /// `true` once the Core task has terminated and is no longer
    /// accepting events.
    pub fn is_closed(&self) -> bool {
        self.mailbox_tx.is_closed()
    }

    /// Trigger a local election attempt.
    ///
    /// This only enqueues the election event. The Core owns the
    /// actual election state transition and outbound RequestVote RPCs.
    pub fn elect(&self) -> Result<(), io::Error> {
        self.mailbox_tx.send(Event::Elect {}).map_err(|_| io::Error::other("Core mailbox closed"))
    }

    /// Submit an inbound `RequestVote` RPC to the Core and await the
    /// reply.
    ///
    /// Used by the inbound transport: when a peer's `RequestVote`
    /// arrives on the wire, the transport calls this method, the Core
    /// decides the vote (§8.3), and the reply is shipped back through
    /// a oneshot channel so the transport can forward it on the wire.
    ///
    /// Errors when the Core has already shut down — either the
    /// mailbox is closed, or the Core dropped the reply channel
    /// without sending.
    pub async fn request_vote(&self, req: RequestVote) -> Result<RequestVoteReply, io::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox_tx
            .send(Event::RequestVote { req, reply_tx })
            .map_err(|_| io::Error::other("Core mailbox closed"))?;
        reply_rx.await.map_err(|_| io::Error::other("Core dropped RequestVote reply channel"))
    }

    pub async fn append(&self, req: AppendRequest) -> Result<AppendReply, io::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox_tx
            .send(Event::Append { req, reply_tx })
            .map_err(|_| io::Error::other("Core mailbox closed"))?;
        reply_rx.await.map_err(|_| io::Error::other("Core dropped Append reply channel"))
    }

    /// Submit an application write to the leader and await commit.
    ///
    /// Only an established leader produces an `Ok` reply (with the
    /// committed log position). On any other node — follower or
    /// still-electing candidate — the inner reply is an `io::Error`,
    /// which the application interprets as "this node is not the
    /// leader; talk to someone else." See `DESIGN.md` §9.
    ///
    /// The outer `io::Error` covers Core-shutdown cases (mailbox
    /// closed, reply channel dropped).
    pub async fn write(&self, req: WriteRequest) -> Result<WriteReply, io::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox_tx
            .send(Event::Write { req, reply_tx })
            .map_err(|_| io::Error::other("Core mailbox closed"))?;
        reply_rx.await.map_err(|_| io::Error::other("Core dropped Write reply channel"))?
    }
}
