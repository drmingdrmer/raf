# raf

> **Declaration**: This approach is just the same as saving terms in the
> separate first extra slot in the terms array. So it actually still stores the
> term and has no value at all. Please do not read this as a useful design; it
> is only a failed personal experiment.

**`raf`** stands for **Raft without a separate [T]erm field** — an experimental
exploration of the [Raft][raft] distributed agreement protocol that derives the
leader term from an election-reserved log index instead of persisting
`currentTerm` as an independent state field.

> ⚠️ **Experimental**: This project is not ready for production use.  It is a
> research prototype that explores whether removing the separately persisted
> `currentTerm` field from Raft yields a simpler foundation for building
> fault-tolerant, agreement-based applications.

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

`raf` asks: *what if the term did not need separate persistent storage?*  The
term still exists as logical time, and logs are still compared by `(term,
index)`.  What changes is where the term comes from: a candidate reserves a log
index during election, and that index becomes the leader term.

Key ideas under exploration:

- **No independent `currentTerm` field** in persistent state.
- **Index-derived leader terms**: a successful election binds the leader term to
  the reserved log index.
- **Equivalent safety target**: leader uniqueness and log-matching guarantees
  are maintained through the same quorum and freshness structure as Raft.
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
`raf` explores whether deriving the term from the log simplifies both the
implementation and the developer experience enough to lower the barrier to
writing agreement-based applications.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

[raft]: https://raft.github.io/
