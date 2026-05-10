//! Singleton event-loop core for a `raf` node.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

use crate::Cmd;
use crate::Membership;
use crate::NodeId;
use crate::ReplicationState;
use crate::Term;
use crate::append_reply::AppendReply;
use crate::append_request::AppendRequest;
use crate::cmd_array::CmdArray;
use crate::event::Event;
use crate::leader_state::LeaderState;
use crate::log_id::LogId;
use crate::network::Network;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::term_array::TermArray;
use crate::write_reply::WriteReply;
use crate::write_request::WriteRequest;

pub(crate) struct Core<N>
where N: Network
{
    terms: TermArray,

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
        terms: TermArray,
        cmds: CmdArray,
        network: Arc<N>,
    ) -> UnboundedSender<Event> {
        let (tx, rx) = unbounded_channel();
        let core = Self {
            terms,
            cmds,
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
            self.handle_event(event).await?;
            self.try_initialize_replication().await;
        }

        Ok(())
    }

    async fn handle_event(&mut self, event: Event) -> Result<(), io::Error> {
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

        Ok(())
    }

    async fn elect(&mut self) -> Result<(), io::Error> {
        self.do_elect().await
    }

    async fn do_elect(&mut self) -> Result<(), io::Error> {
        let term = self.terms.terms_len();
        let mut replications = BTreeMap::new();
        replications.insert(self.id, ReplicationState::new(self.id, self.cmds.cmds_len()));

        self.leader = Some(LeaderState {
            term: self.terms.terms_len(),
            granted_votes: std::iter::once(self.id).collect(), // grant self vote
            established: false,
            replications,
            committed: 0,
            pending_writes: Default::default(),
        });

        self.terms.update_terms(term, &[term]);

        self.spawn_request_vote_rpcs(term).await?;

        Ok(())
    }

    async fn spawn_request_vote_rpcs(&mut self, term: u64) -> Result<(), io::Error> {
        for peer in self.membership.node_ids() {
            if peer == &self.id {
                continue;
            }

            let last_log_id = self.last_log_id().await;

            let req = RequestVote { term, last_log_id };

            let network = Arc::clone(&self.network);
            let reply_tx = self.mailbox_tx.clone();

            let sending_term = term;
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
    /// Reads current state from the term and command arrays on every call.
    ///
    /// Grant iff both:
    /// 1. `req.term >= local.last_term` — the candidate is not behind the latest term we have
    ///    stored or reserved.
    /// 2. `req.last_log_id > local.last_log_id` — the candidate's history is fresher than ours.
    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteReply, io::Error> {
        let local_term_len = self.terms.terms_len();
        let local_last_term = self.terms.last_term();

        let local_cmds_len = self.cmds.cmds_len();
        let local_last_cmd_term = self.terms.read_one_term(local_cmds_len - 1);
        let local_last_log_id = LogId::new(local_last_cmd_term, local_cmds_len - 1);

        if req.term < local_last_term {
            return Ok(RequestVoteReply {
                granted: false,
                term_len: local_term_len,
                last_log_id: local_last_log_id,
            });
        }

        if req.last_log_id <= local_last_log_id {
            return Ok(RequestVoteReply {
                granted: false,
                term_len: local_term_len,
                last_log_id: local_last_log_id,
            });
        }

        // reset all leader or candidate
        self.leader = None;

        let _len = self.terms.update_terms(local_term_len, &[req.term]);

        Ok(RequestVoteReply {
            granted: true,
            term_len: local_term_len,
            last_log_id: local_last_log_id,
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

        let cmds_len = self.cmds.cmds_len();

        for target in self.membership.node_ids().iter().cloned() {
            if target == self.id {
                continue;
            }

            let replication = ReplicationState::new(target, cmds_len);

            leader.replications.insert(target, replication);
        }

        let cmds_len = self.cmds.cmds_len();
        let n = self.terms.terms_len() - cmds_len;

        self.terms.fill_terms_gap(cmds_len);

        // Occupy all entries with no-op Cmd.
        let cmds = vec![Cmd::empty(); n as usize];
        self.cmds.append_cmds(cmds);

        if let Some(replication) = leader.replications.get_mut(&self.id) {
            replication.matched = self.cmds.cmds_len() - 1;
            replication.end = self.cmds.cmds_len();
        }
    }

    async fn try_initialize_replication(&mut self) {
        let Some(leader) = self.leader.as_mut() else {
            return;
        };

        if !leader.established {
            return;
        }

        let sending_term = leader.term;

        for replication in leader.replications.values_mut() {
            if replication.target == self.id {
                continue;
            }

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
            let terms = self.terms.read_terms(start..start + len).entries;
            let cmds = self.cmds.read_cmds(start..start + len).entries;

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
        let last_term = self.terms.last_term();
        if append.term > last_term {
            // TODO: save last-seen, instead of updating terms. Updating terms means accepting a
            // RequestVote.
        } else if append.term < last_term {
            return Ok(AppendReply {
                term: last_term,
                matched: None,
                conflict: None,
            });
        } else {
            // equal
        }

        let start = append.assume_matched_at;
        let end = append.assume_matched_at + append.terms.len() as u64;
        let end = end.min(self.cmds.cmds_len());

        // find the matches

        let local_terms = self.terms.read_terms(start..end).entries;
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
                term: last_term,
                matched: None,
                conflict: Some(start),
            });
        };

        if last_matched < end - 1 {
            self.cmds.truncate_cmds(last_matched + 1);
        }

        self.terms.update_terms(append.assume_matched_at, &append.terms);

        let append_from = self.cmds.cmds_len().saturating_sub(append.assume_matched_at) as usize;
        if append_from < append.cmds.len() {
            self.cmds.append_cmds(append.cmds[append_from..].to_vec());
        }

        Ok(AppendReply {
            term: last_term,
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

            match_indices.remove(0);
        }

        if committed > leader.committed {
            leader.committed = committed;

            self.respond_write_replies(committed).await;
        }
    }

    async fn respond_write_replies(&mut self, committed: u64) {
        let Some(leader) = self.leader.as_mut() else {
            return;
        };

        while let Some((index, _)) = leader.pending_writes.front() {
            let index = *index;
            if index <= committed {
                let (_, reply_tx) = leader.pending_writes.pop_front().unwrap();
                reply_tx.send(Ok(WriteReply { index })).ok();
            } else {
                break;
            }
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
    async fn handle_write(&mut self, req: WriteRequest, reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>) {
        let Some(leader) = self.leader.as_mut() else {
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return;
        };

        if !leader.established {
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return;
        }

        self.dispatch_leader_write(req, reply_tx).await;
    }

    /// Leader-side write handling — placeholder.
    ///
    /// TODO: implement leader-side write replication.
    /// Will (1) append the request locally at `log.len`, (2)
    /// replicate the new entry to peers, (3) advance the committed
    /// index once a quorum has acked, and (4) reply with that index.
    async fn dispatch_leader_write(
        &mut self,
        _req: WriteRequest,
        reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>,
    ) {
        let Some(leader) = self.leader.as_mut() else {
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return;
        };

        self.terms.update_terms(self.cmds.cmds_len(), &[leader.term]);
        self.cmds.append_cmds(vec![Cmd::empty()]);

        let index = self.cmds.cmds_len() - 1;
        if let Some(replication) = leader.replications.get_mut(&self.id) {
            replication.matched = index;
            replication.end = self.cmds.cmds_len();
        }
        leader.pending_writes.push_back((index, reply_tx));
    }

    async fn last_log_id(&self) -> LogId {
        let cmds_len = self.cmds.cmds_len();

        let index = cmds_len - 1;

        let last_term = self.terms.read_one_term(index);
        LogId::new(last_term, index)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::append_reply::AppendReply;
    use crate::append_request::AppendRequest;
    use crate::cmd_array::CmdArray;
    use crate::term_array::TermArray;

    struct NoopNetwork;

    impl Network for NoopNetwork {
        async fn request_vote(&self, _target: u64, _req: RequestVote) -> Result<RequestVoteReply, io::Error> {
            unreachable!("test network should not send RequestVote")
        }

        async fn append(&self, _target: u64, _req: AppendRequest) -> Result<AppendReply, io::Error> {
            unreachable!("test network should not send Append")
        }
    }

    fn new_core(terms: Vec<Term>, cmds_len: usize, membership: Membership) -> Core<NoopNetwork> {
        let (mailbox_tx, mailbox) = unbounded_channel();

        Core {
            terms: TermArray::new(terms),
            cmds: CmdArray::new(vec![Cmd::empty(); cmds_len]),
            network: Arc::new(NoopNetwork),
            id: 1,
            membership,
            leader: None,
            mailbox_tx,
            mailbox,
        }
    }

    #[tokio::test]
    async fn handle_append_appends_only_missing_commands() {
        let mut core = new_core(vec![1, 1], 2, Membership::new(vec![1, 2, 3]));

        let append = AppendRequest {
            term: 1,
            assume_matched_at: 1,
            terms: vec![1, 1, 1],
            cmds: vec![Cmd::empty(); 3],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(core.terms.terms_len(), 4);
        assert_eq!(core.cmds.cmds_len(), 4);
        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));

        let append = AppendRequest {
            term: 1,
            assume_matched_at: 1,
            terms: vec![1, 1, 1],
            cmds: vec![Cmd::empty(); 3],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(core.terms.terms_len(), 4);
        assert_eq!(core.cmds.cmds_len(), 4);
        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));
    }

    #[tokio::test]
    async fn try_update_committed_advances_candidate_set() {
        let mut core = new_core(vec![1; 13], 13, Membership::new(vec![1, 2, 3, 4, 5]));

        core.leader = Some(LeaderState {
            term: 10,
            granted_votes: [1, 2, 3].into(),
            established: true,
            replications: Default::default(),
            committed: 0,
            pending_writes: Default::default(),
        });

        let leader = core.leader.as_mut().unwrap();
        leader.replications.insert(1, ReplicationState {
            target: 1,
            matched: 12,
            end: 13,
            inflight: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        leader.replications.insert(2, ReplicationState {
            target: 2,
            matched: 10,
            end: 13,
            inflight: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        leader.replications.insert(3, ReplicationState {
            target: 3,
            matched: 12,
            end: 13,
            inflight: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        leader.replications.insert(4, ReplicationState {
            target: 4,
            matched: 12,
            end: 13,
            inflight: Arc::new(tokio::sync::Semaphore::new(1)),
        });

        core.try_update_committed().await;

        assert_eq!(core.leader.as_ref().unwrap().committed, 12);
    }
}
