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
mod payload;

pub use self::accepted_content::AcceptedContent;
pub use self::handle::Handle;
pub use self::log_state::LogState;
pub use self::network::InProcessNetwork;
pub use self::network::Network;
pub use self::raf::Raf;
pub use self::request_vote::RequestVote;
pub use self::request_vote_reply::RequestVoteReply;
pub use self::storage::Storage;
pub use self::write_reply::WriteReply;
pub use self::write_request::WriteRequest;
pub use self::payload::Payload;