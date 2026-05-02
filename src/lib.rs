//! `raf` — Raft without \[T\]erm.
//!
//! Experimental distributed consensus protocol that explores whether the
//! Raft *term* can be removed while preserving leader uniqueness and
//! log-matching guarantees. See `DESIGN.md` at the repository root for
//! the full design.

mod accepted_content;
mod core;
mod handle;
mod leader_state;
mod log_state;
mod network;
mod raf;
mod request_vote;
mod request_vote_reply;
mod storage;
mod write_reply;
mod write_request;

pub use crate::accepted_content::AcceptedContent;
pub use crate::handle::Handle;
pub use crate::log_state::LogState;
pub use crate::network::InProcessNetwork;
pub use crate::network::Network;
pub use crate::raf::Raf;
pub use crate::request_vote::RequestVote;
pub use crate::request_vote_reply::RequestVoteReply;
pub use crate::storage::Storage;
pub use crate::write_reply::WriteReply;
pub use crate::write_request::WriteRequest;
