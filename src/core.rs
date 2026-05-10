//! Singleton event-loop core for a `raf` node.

use std::io;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

use crate::AppendReply;
use crate::Cmd;
use crate::Membership;
use crate::NodeId;
use crate::ReplicationState;
use crate::Term;
use crate::append_request::AppendRequest;
use crate::clock_storage::TermArray;
use crate::history_storage::CmdArray;
use crate::leader_state::LeaderState;
use crate::log_id::LogId;
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
    RequestVoteReply {
        sending_term: Term,
        target: NodeId,
        reply: RequestVoteReply,
    },
    Append {
        req: AppendRequest,
        reply_tx: oneshot::Sender<AppendReply>,
    },
    AppendReply {
        sending_term: Term,
        target: NodeId,
        reply: AppendReply,
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
    term_storage: TermArray,

    cmds: CmdArray,

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

    mailbox_tx: UnboundedSender<Event>,
    mailbox: UnboundedReceiver<Event>,
}

impl<N> Core<N>
where N: Network
{
    /// Spawn the Core onto the current Tokio runtime; return a sender
    /// to its mailbox.
    pub(crate) fn spawn(
        id: NodeId,
        membership: Membership,
        term_storage: TermArray,
        storage: CmdArray,
        network: Arc<N>,
    ) -> UnboundedSender<Event> {
        let (tx, rx) = unbounded_channel();
        let core = Self {
            term_storage,
            cmds: storage,
            network,
            id,
            membership,
            leader: None,
            mailbox_tx: tx.clone(),
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
                    let reply = self.handle_request_vote(req).await?;
                    reply_tx.send(reply).ok();
                }
                Event::RequestVoteReply {
                    sending_term,
                    target,
                    reply,
                } => {
                    self.handle_request_vote_reply(sending_term, target, reply).await;
                }
                Event::Append { req, reply_tx } => {
                    let reply = self.handle_append(req).await?;
                    reply_tx.send(reply).ok();
                }
                Event::AppendReply {
                    sending_term,
                    target,
                    reply,
                } => {
                    self.handle_append_reply(sending_term, target, reply).await;
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
        let term = self.term_storage.len();

        self.leader = Some(LeaderState {
            term: self.term_storage.len(),
            granted_votes: std::iter::once(0).collect(), // grant self vote
            established: false,
            replications: Default::default(),
            committed: 0,
        });

        self.term_storage.update(term, &[term]);

        self.spawn_request_vote_rpcs(term).await?;

        Ok(())
    }

    async fn spawn_request_vote_rpcs(&mut self, clock: u64) -> Result<(), io::Error> {
        for peer in self.membership.node_ids() {
            if peer == &self.id {
                continue;
            }

            let last_history_id = self.last_log_id().await;

            let req = RequestVote {
                term: clock,
                last_log_id: last_history_id,
            };

            let network = Arc::clone(&self.network);
            let reply_tx = self.mailbox_tx.clone();

            let sending_term = clock;
            let target = *peer;

            tokio::spawn(async move {
                match network.request_vote(target, req).await {
                    Ok(reply) => {
                        reply_tx
                            .send(Event::RequestVoteReply {
                                sending_term,
                                target,
                                reply,
                            })
                            .ok();
                    }
                    Err(e) => {
                        eprintln!("failed to send RequestVote to peer {}: {}", target, e);
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
    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteReply, io::Error> {
        let local_last = self.term_storage.last();

        let local_cmds_len = self.cmds.len();
        let local_last_cmd_term = self.term_storage.read_one(local_cmds_len - 1);
        let local_last_log_id = LogId::new(local_last_cmd_term, local_cmds_len - 1);

        if req.term < local_last.term {
            return Ok(RequestVoteReply {
                granted: false,
                term_len: local_last.index + 1,
                last_history: local_last_log_id,
            });
        }

        if req.last_log_id <= local_last_log_id {
            return Ok(RequestVoteReply {
                granted: false,
                term_len: local_last.index + 1,
                last_history: local_last_log_id,
            });
        }

        // reset all leader or candidate
        self.leader = None;

        let _len = self.term_storage.update(local_last.index + 1, &[req.term]);

        Ok(RequestVoteReply {
            granted: true,
            term_len: local_last.index + 1,
            last_history: local_last_log_id,
        })
    }

    async fn handle_request_vote_reply(
        &mut self,
        sending_term: Term,
        target: NodeId,
        reply: RequestVoteReply,
    ) -> Option<()> {
        let leader = self.leader.as_mut()?;

        if leader.term != sending_term {
            return None;
        }

        if reply.granted {
            leader.granted_votes.insert(target);
            let granted_votes = leader.granted_votes.iter().cloned().collect::<Vec<_>>();
            if self.membership.is_quorum(&granted_votes) {
                self.establish_leader().await;
            }
        } else {
            self.leader = None;
            // TODO: save max-term-len
        }
        None
    }

    async fn establish_leader(&mut self) {
        let leader = self.leader.as_mut().unwrap();
        leader.established = true;

        let cmds_len = self.cmds.len();

        for target in self.membership.node_ids().iter().cloned() {
            if target == self.id {
                continue;
            }

            let replication = ReplicationState::new(target, cmds_len);

            leader.replications.insert(target, replication);
        }

        let cmds_len = self.cmds.len();
        let n = self.term_storage.len() - cmds_len;

        self.term_storage.fill_gap(cmds_len);

        // Occupy all entries with no-op Cmd.
        let cmds = vec![Cmd::empty(); n as usize];
        self.cmds.append(cmds);

        self.try_initialize_replication().await;
    }

    async fn try_initialize_replication(&mut self) {
        let leader = self.leader.as_mut().unwrap();
        let sending_term = leader.term;

        for replication in leader.replications.values_mut() {
            let permit = replication.inflight.clone().try_acquire_owned();

            let permit = match permit {
                Ok(permit) => permit,
                Err(_) => continue, // already inflight; skip
            };

            let start = (replication.matched + replication.end) / 2;
            let len = 64;
            let net = self.network.clone();
            let tx = self.mailbox_tx.clone();

            // TODO: len should not exceed cmds.len()

            // includes the last matched, will be used in the role of `prev`
            let terms = self.term_storage.read(start..start + len).entries;
            let cmds = self.cmds.read(start..start + len).entries;

            let append_request = AppendRequest {
                term: leader.term,
                assume_matched_at: start,
                terms,
                cmds,
            };
            let target = replication.target;

            tokio::spawn(async move {
                let _permit = permit;

                let reply = net.append(target, append_request).await;
                let Ok(reply) = reply else {
                    eprintln!("failed to send Append to peer {}: {}", target, reply.err().unwrap());
                    return;
                };

                tx.send(Event::AppendReply {
                    sending_term,
                    target,
                    reply,
                })
                .ok();
            });
        }
    }

    async fn handle_append(&mut self, append: AppendRequest) -> Result<AppendReply, io::Error> {
        //
        let last = self.term_storage.last();
        if append.term > last.term {
            // TODO: save last-seen, instead of update storage. updating storage means accept a
            // request-vote. self.term_storage.update(append.term, &[append.term]);
        } else if append.term < last.term {
            return Ok(AppendReply {
                term: last.term,
                matched: None,
                conflict: None,
            });
        } else {
            // equal
        }

        let start = append.assume_matched_at;
        let end = append.assume_matched_at + append.terms.len() as u64;
        let end = end.min(self.cmds.len());

        // find the matches

        let local_terms = self.term_storage.read(start..end).entries;
        let mut last_matched = None;

        for i in start..end {
            if local_terms[(i - start) as usize] == append.terms[(i - start) as usize] {
                last_matched = Some(i);
            } else {
                break;
            }
        }

        let Some(last_matched) = last_matched else {
            return Ok(AppendReply {
                term: last.term,
                matched: None,
                conflict: Some(start),
            });
        };

        if last_matched < end - 1 {
            self.cmds.truncate(last_matched + 1);
        }

        self.term_storage.update(append.assume_matched_at, &append.terms);
        self.cmds.append(append.cmds);

        Ok(AppendReply {
            term: last.term,
            matched: Some(LogId::new(
                *append.terms.last().unwrap(),
                append.assume_matched_at + append.terms.len() as u64 - 1,
            )),
            conflict: None,
        })
    }

    async fn handle_append_reply(&mut self, sending_term: Term, target: NodeId, reply: AppendReply) {
        let Some(leader) = self.leader.as_mut() else {
            return;
        };

        if leader.term != sending_term {
            return;
        }

        if reply.term > leader.term {
            // TODO: save last seen
            self.leader = None;
            return;
        }

        let Some(replication) = leader.replications.get_mut(&target) else {
            // target is removed.
            return;
        };

        if let Some(conflict) = reply.conflict {
            replication.end = conflict;
            return;
        }

        if let Some(matched) = reply.matched {
            replication.matched = matched.index;

            self.try_update_committed().await;
        }
    }

    async fn try_update_committed(&mut self) {
        let Some(leader) = self.leader.as_mut() else {
            return;
        };

        let mut match_indices = leader
            .replications
            .values()
            .filter(|r| r.matched >= leader.term)
            .map(|r| (r.matched, r.target))
            .collect::<Vec<_>>();
        match_indices.sort_unstable();

        let mut committed = 0;
        while !match_indices.is_empty() {
            let node_ids = match_indices.iter().map(|(_, id)| *id).collect::<Vec<_>>();

            if self.membership.is_quorum(&node_ids) {
                committed = match_indices[0].0;
            }
        }

        if committed > leader.committed {
            leader.committed = committed;
        }
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

    async fn last_log_id(&self) -> LogId {
        let cmds_len = self.cmds.len();

        let index = cmds_len - 1;

        let last_term = self.term_storage.read_one(index);
        LogId::new(last_term, index)
    }
}
