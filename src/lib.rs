//! `raf` — Raft without \[T\]erm.
//!
//! Experimental distributed consensus protocol that explores whether the
//! Raft *term* can be removed while preserving leader uniqueness and
//! log-matching guarantees. See `DESIGN.md` at the repository root for
//! the full design.

mod core;
mod handle;
mod log_state;
mod network;
mod raf;
mod request_vote;
mod storage;

pub use crate::handle::Handle;
pub use crate::log_state::LogState;
pub use crate::network::InProcessNetwork;
pub use crate::network::Network;
pub use crate::raf::Raf;
pub use crate::request_vote::RequestVote;
pub use crate::storage::Storage;
