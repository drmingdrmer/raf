//! Mailbox [`Event`] — the single sum type the Core dispatches on.

use std::io;

use tokio::sync::oneshot;

use crate::AppendReply;
use crate::NodeId;
use crate::Term;
use crate::append_request::AppendRequest;
use crate::request_vote::RequestVote;
use crate::request_vote_reply::RequestVoteReply;
use crate::write_reply::WriteReply;
use crate::write_request::WriteRequest;

/// Internal mailbox event.
///
/// Each variant is one inbound thing the Core has to react to. For
/// inbound RPCs and application requests the variant carries a
/// `oneshot::Sender` — the caller (transport or application) owns
/// the receiver and awaits the reply.
pub(crate) enum Event {
    /// Locally triggered leader election attempt.
    Elect {},

    /// Inbound `RequestVote` from a peer; the Core decides the vote
    /// per `DESIGN.md` §8.3 and ships the reply back through
    /// `reply_tx`.
    RequestVote {
        /// Vote request received from a peer.
        req: RequestVote,

        /// Channel used to return the vote reply to the transport.
        reply_tx: oneshot::Sender<RequestVoteReply>,
    },

    /// Reply to an outbound `RequestVote` sent by this node.
    RequestVoteReply {
        /// Local term used when the request was sent.
        sending_term: Term,

        /// Peer that produced the reply.
        target: NodeId,

        /// Vote reply returned by the peer.
        reply: RequestVoteReply,
    },

    /// Inbound `Append` from a peer.
    Append {
        /// Append request received from a peer.
        req: AppendRequest,

        /// Channel used to return the append reply to the transport.
        reply_tx: oneshot::Sender<AppendReply>,
    },

    /// Reply to an outbound `Append` sent by this node.
    AppendReply {
        /// Local term used when the append was sent.
        sending_term: Term,

        /// Peer that produced the reply.
        target: NodeId,

        /// Append reply returned by the peer.
        reply: AppendReply,
    },

    /// Application write request submitted via
    /// [`crate::Raf::write`] — handled per `DESIGN.md` §9.
    /// Only an established leader produces an `Ok` reply; everyone
    /// else returns an `io::Error`.
    Write {
        /// Application write request.
        req: WriteRequest,

        /// Channel used to return the write result to the caller.
        reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>,
    },
}
