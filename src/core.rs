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
    async fn run(mut self) {
        if let Err(e) = self.run_loop().await {
            log::error!("raf core stopped with error node={}: {e}", self.id);
        }
    }

    /// Fallible body of the single-mailbox event loop.
    async fn run_loop(&mut self) -> Result<(), io::Error> {
        self.publish_metrics().await?;

        while let Some(event) = self.mailbox.recv().await {
            self.handle_event(event).await?;
            self.try_initialize_replication().await?;
            self.publish_metrics().await?;
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
                self.handle_request_vote_reply(sending_term, target, reply).await?;
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
                self.handle_append_reply(sending_term, target, reply).await?;
            }
            Event::Write { req, reply_tx } => {
                self.handle_write(req, reply_tx).await?;
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
        let term = self.storage.terms_len().await?;
        let mut replications = BTreeMap::new();
        replications.insert(self.id, ReplicationState::new(self.id, self.storage.cmds_len().await?));

        log::info!("starting election node={} term={term}", self.id);

        self.leader = Some(LeaderState {
            term,
            granted_votes: std::iter::once(self.id).collect(), // grant self vote
            established: false,
            replications,
            pending_writes: Default::default(),
        });

        self.storage.update_terms(term, &[term]).await?;

        let granted_votes = self.leader.as_ref().unwrap().granted_votes.iter().copied().collect::<Vec<_>>();
        if self.membership.is_quorum(&granted_votes) {
            self.establish_leader().await?;
        } else {
            self.spawn_request_vote_rpcs(term).await?;
        }

        Ok(())
    }

    /// Spawn outbound vote requests for all peers.
    async fn spawn_request_vote_rpcs(&mut self, term: u64) -> Result<(), io::Error> {
        for peer in self.membership.node_ids() {
            if peer == &self.id {
                continue;
            }

            let last_log_id = self.last_log_id().await?;
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
        let local_next_term_slot = self.storage.terms_len().await?;
        let local_last_term = self.storage.last_term().await?;

        let local_cmds_len = self.storage.cmds_len().await?;
        let local_last_cmd_term = self.storage.read_one_term(local_cmds_len - 1).await?;
        let local_last_log_id = LogId::new(local_last_cmd_term, local_cmds_len - 1);

        let reply = RequestVoteReply {
            granted: false,
            next_term_slot: local_next_term_slot,
            last_log_id: local_last_log_id.clone(),
        };

        if req.term < local_last_term {
            log::debug!(
                "rejecting RequestVote node={} req_term={} local_last_term={local_last_term}",
                self.id,
                req.term
            );
            return Ok(reply.with_granted(false));
        }

        // A candidate term names the slot it wants to claim. If that
        // slot already exists locally, this voter has already observed
        // an owner for it and must not grant it again.
        if req.term < local_next_term_slot {
            log::debug!(
                "rejecting RequestVote node={} req_term={} local_next_term_slot={local_next_term_slot}",
                self.id,
                req.term
            );
            return Ok(reply.with_granted(false));
        }

        if req.last_log_id < local_last_log_id {
            log::debug!(
                "rejecting RequestVote node={} req_last_log_id={:?} local_last_log_id={local_last_log_id:?}",
                self.id,
                req.last_log_id
            );
            return Ok(reply.with_granted(false));
        }

        // reset all leader or candidate
        self.leader = None;

        self.storage.update_terms(local_next_term_slot, &[req.term]).await?;

        log::info!(
            "granted RequestVote node={} req_term={} next_term_slot={local_next_term_slot}",
            self.id,
            req.term
        );

        Ok(reply.with_granted(true))
    }

    /// Handle one vote reply for the current candidacy.
    async fn handle_request_vote_reply(
        &mut self,
        sending_term: Term,
        target: NodeId,
        reply: RequestVoteReply,
    ) -> Result<(), io::Error> {
        let Some(leader) = self.leader.as_mut() else {
            return Ok(());
        };

        if leader.term != sending_term {
            log::debug!(
                "ignoring stale RequestVoteReply node={} target={target} sending_term={sending_term} current_term={}",
                self.id,
                leader.term
            );
            return Ok(());
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
                self.establish_leader().await?;
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
        Ok(())
    }

    /// Turn candidate state into established leader state.
    async fn establish_leader(&mut self) -> Result<(), io::Error> {
        let leader = self.leader.as_mut().unwrap();
        leader.established = true;

        let cmds_len = self.storage.cmds_len().await?;

        for target in self.membership.node_ids().iter().cloned() {
            if target == self.id {
                continue;
            }

            let replication = ReplicationState::new(target, cmds_len);

            leader.replications.insert(target, replication);
        }

        let cmds_len = self.storage.cmds_len().await?;
        let n = self.storage.terms_len().await? - cmds_len;

        self.storage.fill_terms_gap(cmds_len).await?;

        // Occupy all entries with no-op Cmd.
        let cmds = vec![Cmd::empty(); n as usize];
        self.storage.append_cmds(cmds).await?;

        if let Some(replication) = leader.replications.get_mut(&self.id) {
            let cmds_len = self.storage.cmds_len().await?;
            replication.matched = cmds_len - 1;
            replication.end = cmds_len;
        }

        let cmds_len = self.storage.cmds_len().await?;
        log::info!(
            "established leader node={} term={} next_log_slot={}",
            self.id,
            leader.term,
            cmds_len
        );

        Ok(())
    }

    /// Try to dispatch one append RPC for every peer without an in-flight append.
    async fn try_initialize_replication(&mut self) -> Result<(), io::Error> {
        let Some(leader) = self.leader.as_mut() else {
            return Ok(());
        };

        if !leader.established {
            return Ok(());
        }

        let sending_term = leader.term;
        let commit_index = self.committed;

        for replication in leader.replications.values_mut() {
            if replication.target == self.id {
                continue;
            }

            let permit = replication.inflight.clone().try_acquire_owned();

            let permit = match permit {
                Ok(permit) => permit,
                Err(_) => continue, // already inflight; skip
            };

            let prev_index = (replication.matched + replication.end) / 2;
            let prev_term = self.storage.read_one_term(prev_index).await?;
            let prev_log_id = LogId::new(prev_term, prev_index);
            let start = prev_index + 1;
            let cmds_len = self.storage.cmds_len().await?;
            let len = 64.min(cmds_len.saturating_sub(start));
            let net = self.network.clone();
            let tx = self.mailbox_tx.clone();

            let terms = self.storage.read_terms(start..start + len).await?.entries;
            let cmds = self.storage.read_cmds(start..start + len).await?.entries;

            let append_request = AppendRequest {
                term: leader.term,
                commit_index,
                prev_log_id,
                terms,
                cmds,
            };
            let target = replication.target;
            log::debug!(
                "sending Append from={} to={target} term={} prev_log_id={:?} terms={} cmds={}",
                self.id,
                leader.term,
                append_request.prev_log_id,
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

        Ok(())
    }

    /// Handle an inbound append request from a leader.
    async fn handle_append(&mut self, append: AppendRequest) -> Result<AppendReply, io::Error> {
        assert_eq!(
            append.terms.len(),
            append.cmds.len(),
            "AppendRequest terms and cmds must have the same length"
        );

        let last_term = self.storage.last_term().await?;
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

        if self.leader.take().is_some() {
            log::info!(
                "stepping down after Append node={} append_term={}",
                self.id,
                append.term
            );
        }

        let prev_index = append.prev_log_id.index;
        let cmds_len = self.storage.cmds_len().await?;
        if prev_index >= cmds_len {
            log::debug!(
                "Append prev missing node={} prev_log_id={:?} cmds_len={cmds_len}",
                self.id,
                append.prev_log_id
            );
            return Ok(AppendReply {
                term: last_term,
                matched: None,
                conflict: Some(prev_index),
            });
        }

        let local_prev_term = self.storage.read_one_term(prev_index).await?;
        let local_prev_log_id = LogId::new(local_prev_term, prev_index);
        if local_prev_log_id != append.prev_log_id {
            log::debug!(
                "Append prev conflict node={} req_prev={:?} local_prev={local_prev_log_id:?}",
                self.id,
                append.prev_log_id
            );
            return Ok(AppendReply {
                term: last_term,
                matched: None,
                conflict: Some(prev_index),
            });
        }

        let start = prev_index + 1;
        let end = start + append.terms.len() as u64;
        let local_end = end.min(cmds_len);
        let local_terms = self.storage.read_terms(start..local_end).await?.entries;
        let mut append_from = local_terms.len();

        for i in start..local_end {
            let offset = (i - start) as usize;
            if local_terms[offset] != append.terms[offset] {
                self.storage.truncate_cmds(i).await?;
                append_from = offset;
                break;
            }
        }

        self.storage.update_terms(start, &append.terms).await?;

        if append_from < append.cmds.len() {
            self.storage.append_cmds(append.cmds[append_from..].to_vec()).await?;
        }

        let appended_last_index = prev_index + append.terms.len() as u64;
        if append.commit_index > self.committed && append.commit_index < appended_last_index {
            log::info!(
                "advanced follower commit node={} from={} to={} appended_last_index={appended_last_index}",
                self.id,
                self.committed,
                append.commit_index
            );
            self.committed = append.commit_index;
        }

        let matched = match append.terms.last() {
            Some(term) => LogId::new(*term, appended_last_index),
            None => append.prev_log_id,
        };

        log::debug!(
            "accepted Append node={} term={} matched_index={}",
            self.id,
            append.term,
            matched.index
        );

        Ok(AppendReply {
            term: last_term,
            matched: Some(matched),
            conflict: None,
        })
    }

    /// Handle one append reply from a replication target.
    async fn handle_append_reply(
        &mut self,
        sending_term: Term,
        target: NodeId,
        reply: AppendReply,
    ) -> Result<(), io::Error> {
        let Some(leader) = self.leader.as_mut() else {
            return Ok(());
        };

        if leader.term != sending_term {
            return Ok(());
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
            return Ok(());
        }

        let Some(replication) = leader.replications.get_mut(&target) else {
            // target is removed.
            return Ok(());
        };

        if let Some(conflict) = reply.conflict {
            log::debug!(
                "Append conflict reply node={} target={target} conflict={conflict}",
                self.id
            );
            replication.end = conflict;
            return Ok(());
        }

        if let Some(matched) = reply.matched {
            log::debug!(
                "Append matched reply node={} target={target} matched_index={}",
                self.id,
                matched.index
            );
            replication.matched = matched.index;
            replication.end = replication.end.max(matched.index + 1);

            self.try_update_committed().await;
        }

        Ok(())
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
    async fn handle_write(
        &mut self,
        req: WriteRequest,
        reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>,
    ) -> Result<(), io::Error> {
        let Some(leader) = self.leader.as_mut() else {
            log::warn!("rejecting write on follower node={} app_id={}", self.id, req.id);
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return Ok(());
        };

        if !leader.established {
            log::warn!("rejecting write on candidate node={} app_id={}", self.id, req.id);
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return Ok(());
        }

        self.dispatch_leader_write(req, reply_tx).await
    }

    /// Append a leader-side write and reply once it reaches quorum commit.
    async fn dispatch_leader_write(
        &mut self,
        req: WriteRequest,
        reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>,
    ) -> Result<(), io::Error> {
        let Some(leader) = self.leader.as_mut() else {
            log::warn!(
                "rejecting write without leader state node={} app_id={}",
                self.id,
                req.id
            );
            reply_tx.send(Err(io::Error::other("not a leader; cannot handle write requests"))).ok();
            return Ok(());
        };

        let cmds_len = self.storage.cmds_len().await?;
        self.storage.update_terms(cmds_len, &[leader.term]).await?;
        self.storage.append_cmds(vec![Cmd::empty()]).await?;

        let cmds_len = self.storage.cmds_len().await?;
        let index = cmds_len - 1;
        if let Some(replication) = leader.replications.get_mut(&self.id) {
            replication.matched = index;
            replication.end = cmds_len;
        }
        leader.pending_writes.push_back((index, reply_tx));
        log::info!(
            "accepted leader write node={} app_id={} term={} index={index}",
            self.id,
            req.id,
            leader.term
        );

        Ok(())
    }

    /// Return the last log id derived from the command length and term array.
    async fn last_log_id(&self) -> Result<LogId, io::Error> {
        let cmds_len = self.storage.cmds_len().await?;
        let index = cmds_len - 1;
        let last_term = self.storage.read_one_term(index).await?;
        Ok(LogId::new(last_term, index))
    }

    /// Publish a metrics snapshot if it differs from the last one.
    async fn publish_metrics(&self) -> Result<(), io::Error> {
        let metrics = self.metrics_snapshot().await?;

        self.metrics_tx.send_if_modified(|current| {
            if current == &metrics {
                false
            } else {
                *current = metrics;
                true
            }
        });

        Ok(())
    }

    /// Build the current metrics snapshot from Core-owned state.
    async fn metrics_snapshot(&self) -> Result<Metrics, io::Error> {
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

        Ok(Metrics {
            id: self.id,
            membership: self.membership.node_ids().to_vec(),
            role,
            term: self.storage.last_term().await?,
            committed: self.committed,
            next_term_slot: self.storage.terms_len().await?,
            next_log_slot: self.storage.cmds_len().await?,
            granted_votes,
            replications,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::ops::Range;
    use std::sync::Arc;

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::ArrayChunk;
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

    struct FailingUpdateStorage {
        inner: MemStorage,
    }

    impl Storage for FailingUpdateStorage {
        async fn update_terms(&mut self, _since: u64, _terms: &[Term]) -> io::Result<()> {
            Err(io::Error::other("term update failed"))
        }

        async fn read_terms(&self, range: Range<u64>) -> io::Result<ArrayChunk<Term>> {
            self.inner.read_terms(range).await
        }

        async fn append_cmds(&mut self, cmds: Vec<Cmd>) -> io::Result<()> {
            self.inner.append_cmds(cmds).await
        }

        async fn truncate_cmds(&mut self, after: u64) -> io::Result<()> {
            self.inner.truncate_cmds(after).await
        }

        async fn read_cmds(&self, range: Range<u64>) -> io::Result<ArrayChunk<Cmd>> {
            self.inner.read_cmds(range).await
        }
    }

    fn new_core(terms: Vec<Term>, cmds_len: usize, membership: Membership) -> Core<MemStorage, NoopNetwork> {
        new_core_with_storage(
            MemStorage::from_arrays(TermArray::new(terms), CmdArray::new(vec![Cmd::empty(); cmds_len])),
            membership,
        )
    }

    fn new_core_with_storage<S>(storage: S, membership: Membership) -> Core<S, NoopNetwork>
    where S: Storage {
        let (mailbox_tx, mailbox) = unbounded_channel();
        let (metrics_tx, _) = watch::channel(Metrics::initial(1, membership.node_ids().to_vec()));

        Core {
            storage,
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
            commit_index: 0,
            prev_log_id: LogId::new(1, 1),
            terms: vec![1, 1],
            cmds: vec![Cmd::empty(); 2],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(core.storage.terms_len().await.unwrap(), 4);
        assert_eq!(core.storage.cmds_len().await.unwrap(), 4);
        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));

        let append = AppendRequest {
            term: 1,
            commit_index: 0,
            prev_log_id: LogId::new(1, 1),
            terms: vec![1, 1],
            cmds: vec![Cmd::empty(); 2],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(core.storage.terms_len().await.unwrap(), 4);
        assert_eq!(core.storage.cmds_len().await.unwrap(), 4);
        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));
    }

    #[tokio::test]
    async fn handle_append_rejects_mismatched_prev_log_id() {
        let mut core = new_core(vec![0, 1, 1], 3, Membership::new(vec![1, 2, 3]));

        let append = AppendRequest {
            term: 1,
            commit_index: 0,
            prev_log_id: LogId::new(2, 2),
            terms: vec![1],
            cmds: vec![Cmd::empty()],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(reply.term, 1);
        assert!(reply.matched.is_none());
        assert_eq!(reply.conflict, Some(2));
        assert_eq!(core.storage.terms_len().await.unwrap(), 3);
        assert_eq!(core.storage.cmds_len().await.unwrap(), 3);
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

        let metrics = core.metrics_snapshot().await.unwrap();
        assert_eq!(metrics.role, NodeRole::Leader);
        assert_eq!(metrics.term, 1);
        assert_eq!(metrics.committed, 12);
        assert_eq!(metrics.granted_votes, vec![1, 2, 3]);
        assert_eq!(metrics.replications.len(), 4);
        assert_eq!(metrics.replications[&1].matched, 12);
    }

    #[tokio::test]
    async fn handle_append_reply_advances_end_after_match() {
        let mut core = new_core(vec![0, 1, 1, 1, 1, 1], 6, Membership::new(vec![1, 2, 3]));

        core.leader = Some(LeaderState {
            term: 1,
            granted_votes: [1, 2].into(),
            established: true,
            replications: Default::default(),
            pending_writes: Default::default(),
        });

        let leader = core.leader.as_mut().unwrap();
        leader.replications.insert(2, ReplicationState {
            target: 2,
            matched: 1,
            end: 3,
            inflight: Arc::new(tokio::sync::Semaphore::new(1)),
        });

        core.handle_append_reply(1, 2, AppendReply {
            term: 1,
            matched: Some(LogId::new(1, 5)),
            conflict: None,
        })
        .await
        .unwrap();

        let replication = &core.leader.as_ref().unwrap().replications[&2];
        assert_eq!(replication.matched, 5);
        assert_eq!(replication.end, 6);
    }

    #[tokio::test]
    async fn handle_write_drops_reply_on_storage_error() {
        let storage = FailingUpdateStorage {
            inner: MemStorage::from_arrays(TermArray::new(vec![0, 1]), CmdArray::new(vec![Cmd::empty(); 2])),
        };
        let mut core = new_core_with_storage(storage, Membership::new(vec![1, 2, 3]));

        core.leader = Some(LeaderState {
            term: 1,
            granted_votes: [1, 2].into(),
            established: true,
            replications: Default::default(),
            pending_writes: Default::default(),
        });

        let (reply_tx, reply_rx) = oneshot::channel();

        let err = core.handle_write(WriteRequest { id: 1 }, reply_tx).await.unwrap_err();

        assert_eq!(err.to_string(), "term update failed");
        assert!(reply_rx.await.is_err());
    }

    #[tokio::test]
    async fn request_vote_rejects_already_observed_term_slot() {
        let mut core = new_core(vec![0], 1, Membership::new(vec![1, 2, 3]));

        let req = RequestVote {
            term: 1,
            last_log_id: LogId::new(0, 0),
        };

        let reply = core.handle_request_vote(req.clone()).await.unwrap();
        assert!(reply.granted);
        assert_eq!(core.storage.terms_len().await.unwrap(), 2);

        let reply = core.handle_request_vote(req).await.unwrap();
        assert!(!reply.granted);
        assert_eq!(reply.next_term_slot, 2);
    }

    #[tokio::test]
    async fn single_node_election_establishes_leader_from_self_vote() {
        let mut core = new_core(vec![0], 1, Membership::new(vec![1]));

        core.do_elect().await.unwrap();

        let leader = core.leader.as_ref().unwrap();
        assert!(leader.established);
        assert!(leader.granted_votes.contains(&1));
        assert_eq!(core.storage.terms_len().await.unwrap(), 2);
        assert_eq!(core.storage.cmds_len().await.unwrap(), 2);

        let metrics = core.metrics_snapshot().await.unwrap();
        assert_eq!(metrics.role, NodeRole::Leader);
        assert_eq!(metrics.term, 1);
    }

    #[tokio::test]
    async fn handle_append_clears_local_leader_state() {
        let mut core = new_core(vec![0, 1], 1, Membership::new(vec![1, 2, 3]));

        core.leader = Some(LeaderState {
            term: 1,
            granted_votes: [1, 2].into(),
            established: true,
            replications: Default::default(),
            pending_writes: Default::default(),
        });

        let append = AppendRequest {
            term: 1,
            commit_index: 0,
            prev_log_id: LogId::new(0, 0),
            terms: vec![1],
            cmds: vec![Cmd::empty()],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(reply.matched.unwrap(), LogId::new(1, 1));
        assert!(core.leader.is_none());
        assert_eq!(core.storage.cmds_len().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn handle_append_advances_follower_commit_below_appended_last_index() {
        let mut core = new_core(vec![0, 1], 2, Membership::new(vec![1, 2, 3]));

        let append = AppendRequest {
            term: 1,
            commit_index: 2,
            prev_log_id: LogId::new(1, 1),
            terms: vec![1, 1],
            cmds: vec![Cmd::empty(); 2],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));
        assert_eq!(core.committed, 2);
    }

    #[tokio::test]
    async fn handle_append_does_not_commit_appended_last_index() {
        let mut core = new_core(vec![0, 1], 2, Membership::new(vec![1, 2, 3]));

        let append = AppendRequest {
            term: 1,
            commit_index: 3,
            prev_log_id: LogId::new(1, 1),
            terms: vec![1, 1],
            cmds: vec![Cmd::empty(); 2],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(reply.matched.unwrap(), LogId::new(1, 3));
        assert_eq!(core.committed, 0);
    }

    #[tokio::test]
    async fn handle_append_matches_prev_for_empty_window() {
        let mut core = new_core(vec![0, 1], 2, Membership::new(vec![1, 2, 3]));

        let append = AppendRequest {
            term: 1,
            commit_index: 0,
            prev_log_id: LogId::new(1, 1),
            terms: vec![],
            cmds: vec![],
        };

        let reply = core.handle_append(append).await.unwrap();

        assert_eq!(reply.term, 1);
        assert_eq!(reply.matched.unwrap(), LogId::new(1, 1));
        assert!(reply.conflict.is_none());
        assert_eq!(core.storage.terms_len().await.unwrap(), 2);
        assert_eq!(core.storage.cmds_len().await.unwrap(), 2);
    }

    #[tokio::test]
    #[should_panic(expected = "AppendRequest terms and cmds must have the same length")]
    async fn handle_append_panics_on_mismatched_window_lengths() {
        let mut core = new_core(vec![0, 1], 2, Membership::new(vec![1, 2, 3]));

        let append = AppendRequest {
            term: 1,
            commit_index: 0,
            prev_log_id: LogId::new(1, 1),
            terms: vec![1],
            cmds: vec![],
        };

        core.handle_append(append).await.unwrap();
    }
}
