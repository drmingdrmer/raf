# raf

**`raf`** stands for **Raft without [T]erm** — an experimental exploration of
the [Raft][raft] distributed agreement protocol that eliminates the concept of
the *term*.

> ⚠️ **Experimental**: This project is not ready for production use.  It is a
> research prototype that explores whether removing the term from Raft yields a
> simpler foundation for building fault-tolerant, agreement-based applications.

---

## What Is a Term in Raft?

In the original Raft protocol a *term* is a monotonically increasing integer
that acts as a logical clock.  Each term begins with a leader election.  Every
message carries the sender's current term so that nodes can detect stale
leaders and safely ignore outdated requests.  While straightforward, the term
introduces bookkeeping that must be threaded through every part of the
implementation: log entries, RPC calls, persistence, and state-machine
transitions.

## The `raf` Approach

`raf` asks: *what if the term were not needed?*  By rethinking the invariants
that the term enforces — leader uniqueness and log freshness — it attempts to
preserve the same safety properties through alternative mechanisms.  The result
is a leaner protocol surface that is easier to reason about and, potentially,
easier to implement correctly.

Key ideas under exploration:

- **No term field** in log entries or network messages.
- **Equivalent safety**: leader uniqueness and log-matching guarantees are
  maintained through other invariants.
- **Simplified API**: fewer pieces of state for application authors to manage
  when building agreement-based services.

## Status

| Area | Status |
|------|--------|
| Core protocol | 🔬 Experimental |
| Production readiness | ❌ Not ready |
| API stability | ❌ Unstable |

This project is a work in progress.  Expect breaking changes, incomplete
features, and evolving design decisions.  Contributions, feedback, and
discussion are welcome.

## Motivation

Implementing correct distributed agreement is notoriously difficult.  Existing
libraries often expose a great deal of internal protocol state to application
code, making it hard to build clean, maintainable services on top of them.
`raf` explores whether stripping the term simplifies both the implementation
and the developer experience enough to lower the barrier to writing
agreement-based applications.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

[raft]: https://raft.github.io/
