//! Persistent state interface for a `raf` node.

/// Storage interface.
///
/// Concrete shape will be filled in once the log-entry and message types
/// are settled.
pub trait Storage: Send + Sync + 'static {}
