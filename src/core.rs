//! Singleton event-loop core for a `raf` node.

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

use crate::network::Network;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::state::State;
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
    state: State,
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
            state: State::fresh(),
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
    /// Grant iff both:
    /// 1. `req.leader_index >= state.len()` — the candidate's claimed identity is past every
    ///    position we have ever written, so it is automatically higher than any leader_index we
    ///    have granted before (the protocol invariant `value <= position` makes the explicit
    ///    "higher than last_leader_index" check redundant — see §6.4).
    /// 2. `req.accepted >= state.accepted` — lex-compared on `(leader_index, index)`; the candidate
    ///    is at least as up-to-date as we are (§6.3).
    ///
    /// On grant, persist the position-as-identity entry
    /// (`storage.append(leader_index, &[leader_index])`) before
    /// updating in-memory state. The reply always carries the
    /// responder's local `last_leader_index` and `accepted`, so a
    /// rejected candidate can tell which condition fired and skip
    /// ahead accordingly.
    async fn handle_request_vote(&mut self, req: RequestVote, reply_tx: oneshot::Sender<RequestVoteReply>) {
        let position_unclaimed = req.leader_index >= self.state.len();
        let fresh_enough = req.accepted >= self.state.accepted;
        let granted = position_unclaimed && fresh_enough;

        if granted {
            // Storage failure here is unrecoverable for the Core —
            // the durability story is broken, so bail loudly rather
            // than silently degrade the grant into a reject.
            self.storage
                .append(req.leader_index, &[req.leader_index])
                .await
                .expect("storage append failed during RequestVote grant");

            let pos = req.leader_index as usize;
            if pos >= self.state.log.len() {
                self.state.log.resize(pos + 1, None);
            }
            self.state.log[pos] = Some(req.leader_index);
            self.state.last_leader_index = Some(req.leader_index);
        }

        let _ = reply_tx.send(RequestVoteReply {
            granted,
            last_leader_index: self.state.last_leader_index.unwrap_or(0),
            accepted: self.state.accepted,
        });
    }
}
