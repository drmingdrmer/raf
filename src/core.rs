//! Singleton event-loop core for a `raf` node.

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;

use crate::network::Network;
use crate::state_machine::StateMachine;
use crate::storage::Storage;

/// Internal mailbox event.
///
/// Variants will be filled in once the message, request, and response
/// types are settled.
#[allow(dead_code)]
pub(crate) enum Event {
    /// Placeholder so the type is non-empty during scaffolding.
    Placeholder,
}

#[allow(dead_code)]
pub(crate) struct Core<S, N, M>
where
    S: Storage,
    N: Network,
    M: StateMachine,
{
    storage: S,
    network: N,
    state_machine: M,
    mailbox: UnboundedReceiver<Event>,
}

impl<S, N, M> Core<S, N, M>
where
    S: Storage,
    N: Network,
    M: StateMachine,
{
    /// Spawn the Core onto the current Tokio runtime; return a sender
    /// to its mailbox.
    pub(crate) fn spawn(storage: S, network: N, state_machine: M) -> UnboundedSender<Event> {
        let (tx, rx) = unbounded_channel();
        let core = Self {
            storage,
            network,
            state_machine,
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
                Event::Placeholder => {
                    // TODO: dispatch real variants.
                }
            }
        }
    }
}
