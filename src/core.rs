//! Singleton event-loop core for a `raf` node.

use std::io;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

use crate::Membership;
use crate::NodeId;
use crate::clock_storage::ClockArray;
use crate::hisotory_id::HistoryId;
use crate::history_storage::CmdArray;
use crate::leader_state::LeaderState;
use crate::network::Network;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::write_reply::WriteReply;
use crate::write_request::WriteRequest;

/// Internal mailbox event.
///
/// Each variant is one inbound thing the Core has to react to. For
/// inbound RPCs and application requests the variant carries a
/// `oneshot::Sender` — the caller (transport or application) owns
/// the receiver and awaits the reply.
pub(crate) enum Event {
    Elect {},
    /// Inbound `RequestVote` from a peer; the Core decides the vote
    /// per `DESIGN.md` §8.3 and ships the reply back through
    /// `reply_tx`.
    RequestVote {
        req: RequestVote,
        reply_tx: oneshot::Sender<RequestVoteReply>,
    },
    /// Application write request submitted via
    /// [`crate::Handle::write`] — handled per `DESIGN.md` §9.
    /// Only an established leader produces an `Ok` reply; everyone
    /// else returns an `io::Error`.
    Write {
        req: WriteRequest,
        reply_tx: oneshot::Sender<io::Result<WriteReply>>,
    },
}

pub(crate) struct Core<N>
where N: Network
{
    clock_storage: ClockArray,

    history: CmdArray,

    /// Held in `Arc` so outbound RPCs can be cloned into spawned
    /// tasks (see `DESIGN.md` §15.1.3).
    #[allow(dead_code)]
    network: Arc<N>,

    id: NodeId,

    membership: Membership,

    /// Election / leadership state. `None` on followers; `Some`
    /// while a candidate or established leader. See
    /// [`LeaderState`] and `DESIGN.md` §8.4.
    leader: Option<LeaderState>,

    mailbox: UnboundedReceiver<Event>,
}

impl<N> Core<N>
where N: Network
{
    /// Spawn the Core onto the current Tokio runtime; return a sender
    /// to its mailbox.
    pub(crate) fn spawn(clock_storage: ClockArray, storage: CmdArray, network: Arc<N>) -> UnboundedSender<Event> {
        let (tx, rx) = unbounded_channel();
        let core = Self {
            clock_storage,
            history: storage,
            network,
            leader: None,
            mailbox: rx,
        };
        tokio::spawn(core.run());
        tx
    }

    /// Single-mailbox event loop. All inbound traffic — application
    /// commands, network requests, network responses — arrives here as
    /// an [`Event`] and is dispatched inline.
    async fn run(mut self) -> Result<(), io::Error> {
        while let Some(event) = self.mailbox.recv().await {
            match event {
                Event::Elect {} => {
                    self.elect().await?;
                }
                Event::RequestVote { req, reply_tx } => {
                    self.handle_request_vote(req, reply_tx).await;
                }
                Event::Write { req, reply_tx } => {
                    self.handle_write(req, reply_tx).await;
                }
            }
        }

        Ok(())
    }

    async fn elect(&mut self) -> Result<(), io::Error> {
        self.do_elect().await
    }

    async fn do_elect(&mut self) -> Result<(), io::Error> {
        let clock = self.clock_storage.len();

        self.leader = Some(LeaderState {
            leader_index: self.clock_storage.len(),
            granted_votes: std::iter::once(0).collect(), // grant self vote
            established: false,
        });

        self.clock_storage.update(clock, &[clock]);

        self.spawn_request_vote_rpcs(clock).await?;

        Ok(())
    }

    async fn spawn_request_vote_rpcs(&mut self, clock: u64) -> Result<(), io::Error> {
        for peer in self.membership.node_ids() {
            if peer == &self.id {
                continue;
            }

            let req = RequestVote {
                clock,
                last_history: HistoryId::new(clock, self.history.len()),
            };
            let network = Arc::clone(&self.network);
            let reply_tx = self.mailbox.clone();
            tokio::spawn(async move {
                match network.request_vote(peer, req).await {
                    Ok(reply) => {
                        let _ = reply_tx.send(Event::RequestVote {
                            req,
                            reply_tx: oneshot::channel().0,
                        });
                    }
                    Err(e) => {
                        eprintln!("failed to send RequestVote to peer {}: {}", peer, e);
                    }
                }
            });
        }

        Ok(())
    }

    /// Decide an inbound `RequestVote` per `DESIGN.md` §8.3.
    ///
    /// Reads current state from `Storage` on every call. The Core
    /// keeps no in-memory mirror of log state — `Storage` is the
    /// single source of truth (see `DESIGN.md` §15.1.4 and
    /// §15.1.1).
    ///
    /// Grant iff both:
    /// 1. `req.leader_index >= log.len` — the candidate's claimed identity is past every position
    ///    we have ever written, so it is automatically higher than any leader_index we have granted
    ///    before (the protocol invariant `value <= position` makes the explicit "higher than
    ///    last_leader_index" check redundant — see §6.4).
    /// 2. `req.accepted >= log.accepted` — lex-compared on `(leader_index, index)`; the candidate
    ///    is at least as up-to-date as we are (§6.3).
    async fn handle_request_vote(&mut self, req: RequestVote, reply_tx: oneshot::Sender<RequestVoteReply>) {
        let local_clock_len = self.clock_storage.len();
        let local_history_len = self.history.len();
        let local_last_history_clock = self.clock_storage.read_one(local_clock_len - 1).unwrap();
        let local_last_history_id = HistoryId::new(local_last_history_clock, local_history_len - 1);

        if req.clock < local_clock_len {
            let _ = reply_tx.send(RequestVoteReply {
                granted: false,
                clock_len: local_clock_len,
                last_history: local_last_history_id,
            });
            return;
        }

        if req.last_history <= local_last_history_id {
            let _ = reply_tx.send(RequestVoteReply {
                granted: false,
                clock_len: local_clock_len,
                last_history: local_last_history_id,
            });
            return;
        }

        // reset all leader or candidate
        self.leader = None;

        let _len = self.clock_storage.update(local_clock_len, &[req.clock]);

        let _ = reply_tx.send(RequestVoteReply {
            granted: true,
            clock_len: local_clock_len,
            last_history: local_last_history_id,
        });
    }

    /// Handle an application write request per `DESIGN.md` §9.
    ///
    /// Only an *established* leader serves writes. A node is an
    /// established leader iff it holds a [`LeaderState`] **and**
    /// that state's `established` flag is set. Anything else
    /// (follower, or candidate still gathering votes) returns an
    /// `io::Error` — the application interprets that as "talk to a
    /// different node".
    async fn handle_write(&mut self, req: WriteRequest, reply_tx: oneshot::Sender<io::Result<WriteReply>>) {
        let is_established_leader = self.leader.as_ref().is_some_and(|state| state.established);

        if !is_established_leader {
            let _ = reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests")));
            return;
        }

        self.dispatch_leader_write(req, reply_tx).await;
    }

    /// Leader-side write handling — placeholder.
    ///
    /// TODO: implement leader-side write replication.
    /// Will (1) append the request locally at `log.len`, (2)
    /// replicate the new entry to peers, (3) advance the accepted
    /// index once a quorum has acked, and (4) reply with the
    /// committed index.
    async fn dispatch_leader_write(&mut self, _req: WriteRequest, reply_tx: oneshot::Sender<io::Result<WriteReply>>) {
        let _ = reply_tx.send(Err(io::Error::other(
            "leader-side write replication not yet implemented",
        )));
    }
}
