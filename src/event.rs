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
    Elect {},
    /// Inbound `RequestVote` from a peer; the Core decides the vote
    /// per `DESIGN.md` §8.3 and ships the reply back through
    /// `reply_tx`.
    RequestVote {
        req: RequestVote,
        reply_tx: oneshot::Sender<RequestVoteReply>,
    },
    RequestVoteReply {
        sending_term: Term,
        target: NodeId,
        reply: RequestVoteReply,
    },
    Append {
        req: AppendRequest,
        reply_tx: oneshot::Sender<AppendReply>,
    },
    AppendReply {
        sending_term: Term,
        target: NodeId,
        reply: AppendReply,
    },

    /// Application write request submitted via
    /// [`crate::Raf::write`] — handled per `DESIGN.md` §9.
    /// Only an established leader produces an `Ok` reply; everyone
    /// else returns an `io::Error`.
    Write {
        req: WriteRequest,
        reply_tx: oneshot::Sender<Result<WriteReply, io::Error>>,
    },
}
