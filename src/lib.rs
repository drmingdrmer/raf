//! `raf` — Raft without \[T\]erm.
//!
//! Experimental distributed consensus protocol that explores whether the
//! Raft *term* can be removed while preserving leader uniqueness and
//! log-matching guarantees. See `DESIGN.md` at the repository root for
//! the full design.

mod accepted_content;
mod array_chunk;
mod clock;
mod clock_storage;
mod cmd;
mod core;
mod handle;
mod hisotory_id;
mod history_storage;
mod index;
mod leader_state;
mod log_state;
mod network;
mod raf;
mod request_vote;
mod request_vote_reply;
mod write_reply;
mod write_request;
mod membership;

pub use self::accepted_content::AcceptedContent;
pub use self::array_chunk::ArrayChunk;
pub use self::clock::Clock;
pub use self::cmd::Cmd;
pub use self::handle::Handle;
pub use self::history_storage::HistoryStorage;
pub use self::index::Index;
pub use self::log_state::HistoryState;
pub use self::network::InProcessNetwork;
pub use self::network::Network;
pub use self::raf::Raf;
pub use self::request_vote::RequestVote;
pub use self::request_vote_reply::RequestVoteReply;
pub use self::write_reply::WriteReply;
pub use self::write_request::WriteRequest;
