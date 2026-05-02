//! Application-side control handle for a running [`crate::Raf`] node.

use tokio::sync::mpsc::UnboundedSender;

use crate::core::Event;

/// Cheap-clone control handle to a running `raf` node.
///
/// Internally just an `mpsc` sender into the Core's mailbox, so cloning
/// is the cost of an `Arc` bump.
#[derive(Clone)]
pub struct Handle {
    mailbox_tx: UnboundedSender<Event>,
}

impl Handle {
    pub(crate) fn new(mailbox_tx: UnboundedSender<Event>) -> Self {
        Self { mailbox_tx }
    }

    /// `true` once the Core task has terminated and is no longer
    /// accepting events.
    pub fn is_closed(&self) -> bool {
        self.mailbox_tx.is_closed()
    }
}
