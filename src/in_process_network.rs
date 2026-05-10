//! In-process [`Network`] implementation.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::RwLock;

use crate::AppendReply;
use crate::AppendRequest;
use crate::Network;
use crate::NodeId;
use crate::Raf;
use crate::RequestVote;
use crate::RequestVoteReply;

/// In-process [`Network`] implementation backed by node handles.
///
/// This transport is intended for tests and single-process setups.
/// Each outbound RPC looks up the target [`Raf`] handle, clones it,
/// releases the routing-table lock, and then forwards the request
/// through the target Core mailbox.
#[derive(Clone, Default)]
pub struct InProcessNetwork {
    routes: Arc<RwLock<BTreeMap<NodeId, Raf>>>,
}

impl InProcessNetwork {
    /// Create an empty in-process network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an in-process network with the provided routes.
    pub fn with_routes(routes: BTreeMap<NodeId, Raf>) -> Self {
        Self {
            routes: Arc::new(RwLock::new(routes)),
        }
    }

    /// Register or replace the [`Raf`] handle for `target`.
    pub fn insert(&self, target: NodeId, raf: Raf) -> Option<Raf> {
        let mut routes = self.routes.write().unwrap();

        routes.insert(target, raf)
    }

    fn target_handle(&self, target: NodeId) -> io::Result<Raf> {
        let routes = self.routes.read().unwrap();

        routes.get(&target).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("target node {target} is not registered"),
            )
        })
    }
}

impl Network for InProcessNetwork {
    async fn request_vote(&self, target: NodeId, req: RequestVote) -> Result<RequestVoteReply, io::Error> {
        let target = self.target_handle(target)?;
        target.request_vote(req).await
    }

    async fn append(&self, target: NodeId, req: AppendRequest) -> Result<AppendReply, io::Error> {
        let target = self.target_handle(target)?;
        target.append(req).await
    }
}
