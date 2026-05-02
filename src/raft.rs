//! Top-level entry point: [`Raft`].

use crate::core::Core;
use crate::handle::Handle;
use crate::network::Network;
use crate::state_machine::StateMachine;
use crate::storage::Storage;

/// A running `raf` node.
///
/// Owns the lifetime relationship to the internal Core task. The
/// application interacts with the node through cheap-clone [`Handle`]s
/// obtained via [`Raft::handle`].
pub struct Raft {
    handle: Handle,
}

impl Raft {
    /// Start a new `raf` node.
    ///
    /// Spawns the Core task on the current Tokio runtime. Must be
    /// called from within a Tokio runtime context.
    pub fn new<S, N, M>(storage: S, network: N, state_machine: M) -> Self
    where
        S: Storage,
        N: Network,
        M: StateMachine,
    {
        let mailbox_tx = Core::spawn(storage, network, state_machine);
        Self {
            handle: Handle::new(mailbox_tx),
        }
    }

    /// Get a cheap-clone control handle for the application.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
}
