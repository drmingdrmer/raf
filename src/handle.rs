//! Application-side control handle for a running [`crate::Raf`] node.

use std::io;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::core::Event;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;

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
    pub async fn request_vote(&self, req: RequestVote) -> io::Result<RequestVoteReply> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox_tx
            .send(Event::RequestVote { req, reply_tx })
            .map_err(|_| io::Error::other("Core mailbox closed"))?;
        reply_rx.await.map_err(|_| io::Error::other("Core dropped RequestVote reply channel"))
    }
}
