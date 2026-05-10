//! `raf` — Raft without \[T\]erm.
//!
//! Experimental distributed consensus protocol that explores whether the
//! Raft *term* can be removed while preserving leader uniqueness and
//! log-matching guarantees. See `DESIGN.md` at the repository root for
//! the full design.

mod append_reply;
mod append_request;
mod array_chunk;
mod clock_storage;
mod cmd;
mod core;
mod history_storage;
mod index;
mod leader_state;
mod log_id;
mod log_state;
mod membership;
mod network;
mod node_id;
mod raf;
mod replication_state;
mod request_vote;
mod request_vote_reply;
mod term;
mod write_reply;
mod write_request;

pub use self::append_reply::AppendReply;
pub use self::append_request::AppendRequest;
pub use self::array_chunk::ArrayChunk;
pub use self::cmd::Cmd;
pub use self::index::LogIndex;
pub use self::membership::Membership;
pub use self::network::InProcessNetwork;
pub use self::network::Network;
pub use self::node_id::NodeId;
pub use self::raf::Raf;
pub use self::replication_state::ReplicationState;
pub use self::request_vote::RequestVote;
pub use self::request_vote_reply::RequestVoteReply;
pub use self::term::Term;
pub use self::write_reply::WriteReply;
pub use self::write_request::WriteRequest;
