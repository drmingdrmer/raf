//! Top-level entry point: [`Raf`].

use std::io;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::AppendReply;
use crate::AppendRequest;
use crate::Membership;
use crate::NodeId;
use crate::Storage;
use crate::WriteReply;
use crate::WriteRequest;
use crate::core::Core;
use crate::event::Event;
use crate::network::Network;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;

/// A running `raf` node.
///
/// Internally a thin wrapper around the Core's mailbox sender, so
/// cloning is cheap (an `Arc` bump). The application clones it
/// freely; both the application and the inbound transport drive
/// the Core through this type.
#[derive(Clone)]
pub struct Raf {
    /// Sender used by public APIs to enqueue work in the Core mailbox.
    mailbox_tx: UnboundedSender<Event>,
}

impl Raf {
    /// Start a new `raf` node.
    ///
    /// Spawns the Core task on the current Tokio runtime. Must be
    /// called from within a Tokio runtime context. The `network`
    /// is wrapped in [`Arc`] internally so the Core can clone it
    /// cheaply when spawning outbound RPCs as parallel tasks
    /// (see `DESIGN.md` §15.1.3).
    pub fn new<S, N>(id: NodeId, membership: Membership, storage: S, network: N) -> Self
    where
        S: Storage + 'static,
        N: Network,
    {
        let mailbox_tx = Core::spawn(id, membership, storage, Arc::new(network));
        Self { mailbox_tx }
    }

    /// `true` once the Core task has terminated and is no longer
    /// accepting events.
    pub fn is_closed(&self) -> bool {
        self.mailbox_tx.is_closed()
    }

    /// Trigger a local election attempt.
    ///
    /// Enqueues the election event; the Core owns the actual
    /// election state transition and outbound `RequestVote` RPCs.
    pub fn elect(&self) -> Result<(), io::Error> {
        self.mailbox_tx.send(Event::Elect {}).map_err(|_| io::Error::other("Core mailbox closed"))
    }

    /// Submit an inbound `RequestVote` RPC to the Core and await
    /// the reply.
    ///
    /// Used by the inbound transport: when a peer's `RequestVote`
    /// arrives on the wire, the transport calls this method, the
    /// Core decides the vote (§8.3), and the reply is shipped
    /// back through a oneshot channel so the transport can
    /// forward it on the wire.
    ///
    /// Errors when the Core has already shut down.
    pub async fn request_vote(&self, req: RequestVote) -> Result<RequestVoteReply, io::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox_tx
            .send(Event::RequestVote { req, reply_tx })
            .map_err(|_| io::Error::other("Core mailbox closed"))?;
        reply_rx.await.map_err(|_| io::Error::other("Core dropped RequestVote reply channel"))
    }

    /// Submit an inbound `Append` RPC to the Core and await the
    /// reply. See `DESIGN.md` §9.2.
    pub async fn append(&self, req: AppendRequest) -> Result<AppendReply, io::Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mailbox_tx
            .send(Event::Append { req, reply_tx })
            .map_err(|_| io::Error::other("Core mailbox closed"))?;
        reply_rx.await.map_err(|_| io::Error::other("Core dropped Append reply channel"))
    }

    /// Submit an application write to the leader and await commit.
    ///
    /// Only an established leader produces an `Ok` reply (with
    /// the committed log position). On any other node — follower
    /// or still-electing candidate — the inner reply is an
    /// `io::Error`, which the application interprets as "this
    /// node is not the leader; talk to someone else." See
    /// `DESIGN.md` §9.
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
