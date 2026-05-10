//! Top-level entry point: [`Raf`].

use std::sync::Arc;

use crate::Membership;
use crate::NodeId;
use crate::clock_storage::TermArray;
use crate::core::Core;
use crate::handle::Handle;
use crate::history_storage::CmdArray;
use crate::network::Network;
use crate::request_vote::RequestVote;

/// A running `raf` node.
///
/// Owns the lifetime relationship to the internal Core task. The
/// application interacts with the node through cheap-clone [`Handle`]s
/// obtained via [`Raf::handle`].
pub struct Raf {
    handle: Handle,
}

impl Raf {
    /// Start a new `raf` node.
    ///
    /// Spawns the Core task on the current Tokio runtime. Must be
    /// called from within a Tokio runtime context. The `network` is
    /// wrapped in [`Arc`] internally so the Core can clone it cheaply
    /// when spawning outbound RPCs as parallel tasks
    /// (see `DESIGN.md` §15.1.3).
    pub fn new<N>(id: NodeId, membership: Membership, terms: TermArray, cmds: CmdArray, network: N) -> Self
    where N: Network {
        let mailbox_tx = Core::spawn(id, membership, terms, cmds, Arc::new(network));
        Self {
            handle: Handle::new(mailbox_tx),
        }
    }

    /// Get a cheap-clone control handle for the application.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    pub async fn handle_request_vote(
        &self,
        req: RequestVote,
    ) -> std::io::Result<crate::request_vote_reply::RequestVoteReply> {
        self.handle.request_vote(req).await
    }
}
