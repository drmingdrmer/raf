//! Singleton event-loop core for a `raf` node.

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

use crate::network::Network;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::storage::Storage;

/// Internal mailbox event.
///
/// Each variant is one inbound thing the Core has to react to. For
/// inbound RPCs the variant carries a `oneshot::Sender` — the
/// transport that received the wire-level RPC owns the receiver and
/// forwards the reply back to the calling peer.
pub(crate) enum Event {
    /// Inbound `RequestVote` from a peer; the Core decides the vote
    /// per `DESIGN.md` §8.3 and ships the reply back through
    /// `reply_tx`.
    RequestVote {
        req: RequestVote,
        reply_tx: oneshot::Sender<RequestVoteReply>,
    },
}

pub(crate) struct Core<S, N>
where
    S: Storage,
    N: Network,
{
    storage: S,
    #[allow(dead_code)]
    network: N,
    mailbox: UnboundedReceiver<Event>,
}

impl<S, N> Core<S, N>
where
    S: Storage,
    N: Network,
{
    /// Spawn the Core onto the current Tokio runtime; return a sender
    /// to its mailbox.
    pub(crate) fn spawn(storage: S, network: N) -> UnboundedSender<Event> {
        let (tx, rx) = unbounded_channel();
        let core = Self {
            storage,
            network,
            mailbox: rx,
        };
        tokio::spawn(core.run());
        tx
    }

    /// Single-mailbox event loop. All inbound traffic — application
    /// commands, network requests, network responses — arrives here as
    /// an [`Event`] and is dispatched inline.
    async fn run(mut self) {
        while let Some(event) = self.mailbox.recv().await {
            match event {
                Event::RequestVote { req, reply_tx } => {
                    self.handle_request_vote(req, reply_tx).await;
                }
            }
        }
    }

    /// Decide an inbound `RequestVote` per `DESIGN.md` §8.3.
    ///
    /// Reads current state from `Storage` on every call. The Core
    /// keeps no in-memory mirror — `Storage` is the single source of
    /// truth (see `DESIGN.md` §15.1.4 and §15.1.1).
    ///
    /// Grant iff both:
    /// 1. `req.leader_index >= log.len` — the candidate's claimed identity is past every position
    ///    we have ever written, so it is automatically higher than any leader_index we have granted
    ///    before (the protocol invariant `value <= position` makes the explicit "higher than
    ///    last_leader_index" check redundant — see §6.4).
    /// 2. `req.accepted >= log.accepted` — lex-compared on `(leader_index, index)`; the candidate
    ///    is at least as up-to-date as we are (§6.3).
    async fn handle_request_vote(&mut self, req: RequestVote, reply_tx: oneshot::Sender<RequestVoteReply>) {
        // Empty range: only the metadata (len, accepted,
        // last_leader_index) is needed for the decision.
        let log = self.storage.read(0..0).await.expect("storage read failed during RequestVote");

        let position_unclaimed = req.leader_index >= log.len;
        let fresh_enough = req.accepted >= log.accepted;
        let granted = position_unclaimed && fresh_enough;

        // The reply's `last_leader_index` reports the post-decision
        // state: when granted, the candidate's own identity is now
        // the highest leader_index this node has stored; when
        // rejected, the prior value is unchanged.
        let last_leader_index = if granted {
            // Storage failure here is unrecoverable for the Core —
            // the durability story is broken, so bail loudly rather
            // than silently degrade the grant into a reject.
            self.storage
                .append(req.leader_index, &[req.leader_index])
                .await
                .expect("storage append failed during RequestVote grant");
            req.leader_index
        } else {
            log.last_leader_index.unwrap_or(0)
        };

        let _ = reply_tx.send(RequestVoteReply {
            granted,
            last_leader_index,
            accepted: log.accepted,
        });
    }
}
