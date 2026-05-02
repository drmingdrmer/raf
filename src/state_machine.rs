//! State-machine interface — consumes committed log entries.

/// Application state machine.
///
/// Concrete shape will be filled in once the log-entry type is settled.
pub trait StateMachine: Send + Sync + 'static {}
