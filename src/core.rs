//! Singleton event-loop core for a `raf` node.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::Cmd;
use crate::LogIndex;
use crate::Membership;
use crate::Metrics;
use crate::NodeId;
use crate::ReplicationState;
use crate::Storage;
use crate::Term;
use crate::append_reply::AppendReply;
use crate::append_request::AppendRequest;
use crate::event::Event;
use crate::leader_state::LeaderState;
use crate::log_id::LogId;
use crate::network::Network;
use crate::node_role::NodeRole;
use crate::raf::Raf;
use crate::replication_metrics::ReplicationMetrics;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::storage_ext::StorageExt;
use crate::write_reply::WriteReply;
use crate::write_request::WriteRequest;

/// Single-task protocol core for one `raf` node.
pub(crate) struct Core<S, N>
where
    S: Storage,
    N: Network,
{
    /// Persistent protocol storage.
    storage: S,

    /// Held in `Arc` so outbound RPCs can be cloned into spawned
    /// tasks (see `DESIGN.md` §15.1.3).
    #[allow(dead_code)]
    network: Arc<N>,

    /// Local node id.
    id: NodeId,

    /// Static cluster membership.
    membership: Membership,

    /// Greatest log index this node knows to be committed.
    committed: LogIndex,

    /// Election / leadership state. `None` on followers; `Some`
    /// while a candidate or established leader. See
    /// [`LeaderState`] and `DESIGN.md` §8.4.
    leader: Option<LeaderState>,

    /// Sender side of the Core mailbox.
    mailbox_tx: UnboundedSender<Event>,

    /// Receiver side of the Core mailbox.
    mailbox: UnboundedReceiver<Event>,

    /// Sender side of the public metrics watch channel.
    metrics_tx: watch::Sender<Metrics>,
}

impl<S, N> Core<S, N>
where
    S: Storage,
    N: Network,
{
    /// Spawn the Core onto the current Tokio runtime and return its public handle.
    pub(crate) fn spawn(id: NodeId, membership: Membership, storage: S, network: Arc<N>) -> Raf {
        let (tx, rx) = unbounded_channel();
        let initial_metrics = Metrics::initial(id, membership.node_ids().to_vec());
        let (metrics_tx, metrics_rx) = watch::channel(initial_metrics);

        let core = Self {
            storage,
            network,
            id,
            membership,
            committed: 0,
            leader: None,
            mailbox_tx: tx.clone(),
            mailbox: rx,
            metrics_tx,
        };
        log::info!("spawning raf core node={id}");
        tokio::spawn(core.run());
        Raf::from_core(tx, metrics_rx)
    }

    /// Single-mailbox event loop. All inbound traffic — application
    /// commands, network requests, network responses — arrives here as
    /// an [`Event`] and is dispatched inline.
    async fn run(mut self) -> Result<(), io::Error> {
        self.publish_metrics().await;

        while let Some(event) = self.mailbox.recv().await {
            self.handle_event(event).await?;
            self.try_initialize_replication().await;
            self.publish_metrics().await;
        }

        log::info!("raf core mailbox closed node={}", self.id);
        Ok(())
    }

    /// Dispatch one mailbox event to its protocol handler.
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

    /// Start a local election attempt.
    async fn elect(&mut self) -> Result<(), io::Error> {
        self.do_elect().await
    }

    /// Create candidate state, reserve the local term slot, and send vote RPCs.
    async fn do_elect(&mut self) -> Result<(), io::Error> {
        let term = self.storage.terms_len().await;
        let mut replications = BTreeMap::new();
        replications.insert(self.id, ReplicationState::new(self.id, self.storage.cmds_len().await));

        log::info!("starting election node={} term={term}", self.id);

        self.leader = Some(LeaderState {
            term,
            granted_votes: std::iter::once(self.id).collect(), // grant self vote
            established: false,
            replications,
            pending_writes: Default::default(),
        });

        self.storage.update_terms(term, &[term]).await;

        self.spawn_request_vote_rpcs(term).await?;

        Ok(())
    }

    /// Spawn outbound vote requests for all peers.
    async fn spawn_request_vote_rpcs(&mut self, term: u64) -> Result<(), io::Error> {
        for peer in self.membership.node_ids() {
            if peer == &self.id {
                continue;
            }

            let last_log_id = self.last_log_id().await;
            let target = *peer;

            log::debug!(
                "sending RequestVote from={} to={target} term={term} last_log_id={last_log_id:?}",
                self.id
            );

            let req = RequestVote { term, last_log_id };

            let network = Arc::clone(&self.network);
            let reply_tx = self.mailbox_tx.clone();

            let sending_term = term;

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
                        log::warn!("failed to send RequestVote to peer {target}: {e}");
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
    /// 2. `req.last_log_id >= local.last_log_id` — the candidate's history is at least as fresh as
    ///    ours.
    async fn handle_request_vote(&mut self, req: RequestVote) -> Result<RequestVoteReply, io::Error> {
        let local_next_term_slot = self.storage.terms_len().await;
        let local_last_term = self.storage.last_term().await;

        let local_cmds_len = self.storage.cmds_len().await;
        let local_last_cmd_term = self.storage.read_one_term(local_cmds_len - 1).await;
        let local_last_log_id = LogId::new(local_last_cmd_term, local_cmds_len - 1);

        if req.term < local_last_term {
            log::debug!(
                "rejecting RequestVote node={} req_term={} local_last_term={local_last_term}",
                self.id,
                req.term
            );
            return Ok(RequestVoteReply {
                granted: false,
                next_term_slot: local_next_term_slot,
                last_log_id: local_last_log_id,
            });
        }

        if req.last_log_id < local_last_log_id {
            log::debug!(
                "rejecting RequestVote node={} req_last_log_id={:?} local_last_log_id={local_last_log_id:?}",
                self.id,
                req.last_log_id
            );
            return Ok(RequestVoteReply {
                granted: false,
                next_term_slot: local_next_term_slot,
                last_log_id: local_last_log_id,
            });
        }

        // reset all leader or candidate
        self.leader = None;

        let _len = self.storage.update_terms(local_next_term_slot, &[req.term]).await;

        log::info!(
            "granted RequestVote node={} req_term={} next_term_slot={local_next_term_slot}",
            self.id,
            req.term
        );

        Ok(RequestVoteReply {
            granted: true,
            next_term_slot: local_next_term_slot,
            last_log_id: local_last_log_id,
        })
    }

    /// Handle one vote reply for the current candidacy.
    async fn handle_request_vote_reply(
        &mut self,
        sending_term: Term,
        target: NodeId,
        reply: RequestVoteReply,
    ) -> Option<()> {
        let leader = self.leader.as_mut()?;

        if leader.term != sending_term {
            log::debug!(
                "ignoring stale RequestVoteReply node={} target={target} sending_term={sending_term} current_term={}",
                self.id,
                leader.term
            );
            return None;
        }

        if reply.granted {
            leader.granted_votes.insert(target);
            let granted_votes = leader.granted_votes.iter().cloned().collect::<Vec<_>>();
            log::info!(
                "received granted vote node={} from={target} term={} votes={}",
                self.id,
                leader.term,
                granted_votes.len()
            );
            if !leader.established && self.membership.is_quorum(&granted_votes) {
                self.establish_leader().await;
            }
        } else {
            log::warn!(
                "stepping down after rejected vote node={} target={target} term={} responder_next_term_slot={} responder_last_log_id={:?}",
                self.id,
                leader.term,
                reply.next_term_slot,
                reply.last_log_id
            );
            self.leader = None;
            // TODO: save max-term-len
        }
        None
    }

    /// Turn candidate state into established leader state.
    async fn establish_leader(&mut self) {
        let leader = self.leader.as_mut().unwrap();
        leader.established = true;

        let cmds_len = self.storage.cmds_len().await;

        for target in self.membership.node_ids().iter().cloned() {
            if target == self.id {
                continue;
            }

            let replication = ReplicationState::new(target, cmds_len);

            leader.replications.insert(target, replication);
        }

        let cmds_len = self.storage.cmds_len().await;
        let n = self.storage.terms_len().await - cmds_len;

        self.storage.fill_terms_gap(cmds_len).await;

        // Occupy all entries with no-op Cmd.
        let cmds = vec![Cmd::empty(); n as usize];
        self.storage.append_cmds(cmds).await;

        if let Some(replication) = leader.replications.get_mut(&self.id) {
            let cmds_len = self.storage.cmds_len().await;
            replication.matched = cmds_len - 1;
            replication.end = cmds_len;
        }

        log::info!(
            "established leader node={} term={} next_log_slot={}",
            self.id,
            leader.term,
            self.storage.cmds_len().await
        );
    }

    /// Try to dispatch one append RPC for every peer without an in-flight append.
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
            let terms = self.storage.read_terms(start..start + len).await.entries;
            let cmds = self.storage.read_cmds(start..start + len).await.entries;

            let append_request = AppendRequest {
                term: leader.term,
                assume_matched_at: start,
                terms,
                cmds,
            };
            let target = replication.target;
            log::debug!(
                "sending Append from={} to={target} term={} start={} terms={} cmds={}",
                self.id,
                leader.term,
                append_request.assume_matched_at,
                append_request.terms.len(),
                append_request.cmds.len()
            );

            tokio::spawn(async move {
                let _permit = permit;

                let reply = net.append(target, append_request).await;
                let Ok(reply) = reply else {
                    log::warn!("failed to send Append to peer {target}: {}", reply.err().unwrap());
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

    /// Handle an inbound append request from a leader.
    async fn handle_append(&mut self, append: AppendRequest) -> Result<AppendReply, io::Error> {
        let last_term = self.storage.last_term().await;
        if append.term > last_term {
            // TODO: save last-seen, instead of updating terms. Updating terms means accepting a
            // RequestVote.
        } else if append.term < last_term {
            log::debug!(
                "rejecting stale Append node={} append_term={} local_last_term={last_term}",
                self.id,
                append.term
            );
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
        let end = end.min(self.storage.cmds_len().await);

        // find the matches

        let local_terms = self.storage.read_terms(start..end).await.entries;
        let mut last_matched = None;

        for i in start..end {
            if local_terms[(i - start) as usize] == append.terms[(i - start) as usize] {
                last_matched = Some(i);
            } else {
                break;
            }
        }

        let Some(last_matched) = last_matched else {
            log::debug!("Append conflict node={} start={start}", self.id);
            return Ok(AppendReply {
                term: last_term,
                matched: None,
                conflict: Some(start),
            });
        };

        if last_matched < end - 1 {
            self.storage.truncate_cmds(last_matched + 1).await;
        }

        self.storage.update_terms(append.assume_matched_at, &append.terms).await;

        let append_from = self.storage.cmds_len().await.saturating_sub(append.assume_matched_at) as usize;
        if append_from < append.cmds.len() {
            self.storage.append_cmds(append.cmds[append_from..].to_vec()).await;
        }

        log::debug!(
            "accepted Append node={} term={} matched_index={}",
            self.id,
            append.term,
            append.assume_matched_at + append.terms.len() as u64 - 1
        );

        Ok(AppendReply {
            term: last_term,
            matched: Some(LogId::new(
                *append.terms.last().unwrap(),
                append.assume_matched_at + append.terms.len() as u64 - 1,
            )),
            conflict: None,
        })
    }

    /// Handle one append reply from a replication target.
    async fn handle_append_reply(&mut self, sending_term: Term, target: NodeId, reply: AppendReply) {
        let Some(leader) = self.leader.as_mut() else {
            return;
        };

        if leader.term != sending_term {
            return;
        }

        if reply.term > leader.term {
            // TODO: save last seen
            log::warn!(
                "stepping down after newer AppendReply term node={} target={target} current_term={} reply_term={}",
                self.id,
                leader.term,
                reply.term
            );
            self.leader = None;
            return;
        }

        let Some(replication) = leader.replications.get_mut(&target) else {
            // target is removed.
            return;
        };

        if let Some(conflict) = reply.conflict {
            log::debug!(
                "Append conflict reply node={} target={target} conflict={conflict}",
                self.id
            );
            replication.end = conflict;
            return;
        }

        if let Some(matched) = reply.matched {
            log::debug!(
                "Append matched reply node={} target={target} matched_index={}",
                self.id,
                matched.index
            );
            replication.matched = matched.index;

            self.try_update_committed().await;
        }
    }

    /// Advance the node commit index if a quorum has matched a newer index.
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

        if committed > self.committed {
            log::info!(
                "advanced commit node={} from={} to={committed}",
                self.id,
                self.committed
            );
            self.committed = committed;

            self.respond_write_replies(committed).await;
        }
    }

    /// Reply to pending writes whose log index has become committed.
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
            log::warn!("rejecting write on follower node={} app_id={}", self.id, req.id);
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return;
        };

        if !leader.established {
            log::warn!("rejecting write on candidate node={} app_id={}", self.id, req.id);
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
        req: WriteRequest,
        reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>,
    ) {
        let Some(leader) = self.leader.as_mut() else {
            log::warn!(
                "rejecting write without leader state node={} app_id={}",
                self.id,
                req.id
            );
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return;
        };

        self.storage.update_terms(self.storage.cmds_len().await, &[leader.term]).await;
        self.storage.append_cmds(vec![Cmd::empty()]).await;

        let index = self.storage.cmds_len().await - 1;
        if let Some(replication) = leader.replications.get_mut(&self.id) {
            replication.matched = index;
            replication.end = self.storage.cmds_len().await;
        }
        leader.pending_writes.push_back((index, reply_tx));
        log::info!(
            "accepted leader write node={} app_id={} term={} index={index}",
            self.id,
            req.id,
            leader.term
        );
    }

    /// Return the last log id derived from the command length and term array.
    async fn last_log_id(&self) -> LogId {
        let cmds_len = self.storage.cmds_len().await;
        let index = cmds_len - 1;
        let last_term = self.storage.read_one_term(index).await;
        LogId::new(last_term, index)
    }

    /// Publish a metrics snapshot if it differs from the last one.
    async fn publish_metrics(&self) {
        let metrics = self.metrics_snapshot().await;

        self.metrics_tx.send_if_modified(|current| {
            if current == &metrics {
                false
            } else {
                *current = metrics;
                true
            }
        });
    }

    /// Build the current metrics snapshot from Core-owned state.
    async fn metrics_snapshot(&self) -> Metrics {
        let (role, mut granted_votes, replications) = match self.leader.as_ref() {
            Some(leader) => {
                let role = if leader.established {
                    NodeRole::Leader
                } else {
                    NodeRole::Candidate
                };
                let granted_votes = leader.granted_votes.iter().copied().collect::<Vec<_>>();
                let replications = leader
                    .replications
                    .iter()
                    .map(|(target, replication)| (*target, ReplicationMetrics::from_replication_state(replication)))
                    .collect::<BTreeMap<_, _>>();

                (role, granted_votes, replications)
            }
            None => (NodeRole::Follower, Vec::new(), BTreeMap::new()),
        };

        granted_votes.sort_unstable();

        Metrics {
            id: self.id,
            membership: self.membership.node_ids().to_vec(),
            role,
            term: self.storage.last_term().await,
            committed: self.committed,
            next_term_slot: self.storage.terms_len().await,
            next_log_slot: self.storage.cmds_len().await,
            granted_votes,
            replications,
        }
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
    use crate::mem_storage::MemStorage;
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

    fn new_core(terms: Vec<Term>, cmds_len: usize, membership: Membership) -> Core<MemStorage, NoopNetwork> {
        let (mailbox_tx, mailbox) = unbounded_channel();
        let (metrics_tx, _) = watch::channel(Metrics::initial(1, membership.node_ids().to_vec()));

        Core {
            storage: MemStorage::from_arrays(TermArray::new(terms), CmdArray::new(vec![Cmd::empty(); cmds_len])),
            network: Arc::new(NoopNetwork),
            id: 1,
            membership,
            committed: 0,
            leader: None,
            mailbox_tx,
            mailbox,
            metrics_tx,
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

        assert_eq!(core.storage.terms_len().await, 4);
        assert_eq!(core.storage.cmds_len().await, 4);
        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));

        let append = AppendRequest {
            term: 1,
            assume_matched_at: 1,
            terms: vec![1, 1, 1],
            cmds: vec![Cmd::empty(); 3],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(core.storage.terms_len().await, 4);
        assert_eq!(core.storage.cmds_len().await, 4);
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

        assert_eq!(core.committed, 12);

        let metrics = core.metrics_snapshot().await;
        assert_eq!(metrics.role, NodeRole::Leader);
        assert_eq!(metrics.term, 1);
        assert_eq!(metrics.committed, 12);
        assert_eq!(metrics.granted_votes, vec![1, 2, 3]);
        assert_eq!(metrics.replications.len(), 4);
        assert_eq!(metrics.replications[&1].matched, 12);
    }
}
