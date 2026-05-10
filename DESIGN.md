# raf — Design

> Raft without [T]erm. A consensus protocol that preserves Raft's safety
> guarantees while eliminating the term as an explicit concept.

This document captures the design as it is described, conversation by
conversation. It will grow and be reorganized as the picture fills in.

---

## 1. Name

`raf` — *Raft without Term*.

## 2. Goals

- **Log replication.** Implement Raft-style log replication that
  preserves leader uniqueness and log-matching. Output: a durably
  ordered, committed sequence of log entries, each identified by a
  `(Term, LogIndex)` pair (a `LogId`) and carrying an opaque `Cmd`
  payload. The application interprets `Cmd`; no state machine is
  part of `raf` (see §6).
- **Term-as-implementation-detail.** The term is retained as an
  internal mechanism (for now) but the long-term design intent is
  to keep its surface minimal — the log identity exposed to the
  application is the `(Term, LogIndex)` pair, not the term alone.

## 3. Non-Goals

The following classical Raft features are explicitly out of scope:

- **Snapshots / log compaction.**
- **Membership configuration changes.**

These may be revisited later but are deliberately deferred so the
core protocol can be designed and validated in isolation.

## 4. Background: What the Term Does in Raft

*To be described — we will record which invariants the term enforces in
classical Raft, so that the replacement mechanisms can be evaluated against
the same properties.*

## 5. Core Idea

*To be described — the central mechanism that replaces the term.*

## 6. State

### 6.1 Log Model — Parallel Arrays + LogId

The log is a pair of parallel sequences indexed by the same
log-index space (a `u64`):

- a **term sequence** — at each index, the term of the leader
  that owns that slot;
- a **cmd sequence** — at each index, an opaque application
  payload. `raf` does not interpret it.

The pair `(term, index)` forms the **log identity** — the same
`(term, log_index)` shape standard Raft uses to compare freshness
and enforce log-matching. Conceptually:

```text
log[i] := (terms[i], cmds[i])
```

Both arrays are addressed by the same index space and grow in
lockstep — except for one transient state during candidacy
(§6.3).

### 6.2 Operations

**Term sequence.** Three operations matter at the protocol level:
read one slot, read a contiguous window, and overwrite a
contiguous window. The overwrite is used by both follower-side
append handling (replacing the speculative tail with the leader's
terms) and candidate-side voting (recording the new leader's term
at the next slot).

**Cmd sequence.** Three operations: append at the end, truncate
the tail past a confirmed-matching point, and read a contiguous
window. The cmd sequence is strictly grow-and-truncate — it does
not support arbitrary-position writes. Divergent tails are
repaired by truncating to the last matching index and then
appending the leader's payloads.

Persistence of these sequences is out of scope for the current
draft; the project will revisit a durable storage layer once the
protocol is fully fleshed out (see §15.1.4).

### 6.3 Steady State vs. Candidate State

In steady state — at any non-candidate, non-establishing node —
the two sequences have the same length: every stored payload has
an associated term, and every term has an associated payload.

A candidate that has issued (or granted) a `RequestVote(term = T)`
is briefly out of step. The term sequence is extended with one
extra entry at index `cmds.len()` recording term `T`, reserving
the slot for the new leader's first entry. The cmd sequence is
**not** yet extended; the slot has no payload.

This is the only legal source of `terms.len() > cmds.len()`. When
the candidate becomes an established leader, the cmd sequence is
padded out to match (with empty payloads at any reserved-but-not-
yet-written slots) so steady state is restored (§8.5).

If a higher-term grant arrives before establishment, the term
sequence's overwrite operation simply replaces the value at that
reserved slot — no special bookkeeping is needed to invalidate the
old reservation.

## 7. Messages / RPCs

`raf`'s wire protocol so far has two RPC pairs — `RequestVote` for
leader election (§7.1) and `Append` for replication and matching
(§7.2). More may be added as the design fills in.

### 7.1 RequestVote

Sent by a candidate to other nodes when it tries to become leader.
Modeled on standard Raft's `RequestVote` RPC.

| Field | Meaning |
|---|---|
| `term` | The candidate's term — the slot it is running to fill (§8.1). |
| `last_log_id` | The candidate's last log identity, `(term, index)` — the freshness comparator. |

**Reply:**

| Field | Meaning |
|---|---|
| `granted` | Whether the responder accepted the candidate's claim. |
| `term_len` | The responder's local term-sequence length — the smallest index past every term it has stored. The candidate uses this to skip ahead if it is behind. |
| `last_history` | The responder's local last log identity. The candidate uses this as the freshness comparator if the rejection was on freshness grounds. |

The reply ships local state regardless of `granted`, so a rejected
candidate can determine which condition fired and decide what to
do next — fall back to follower with a higher term, or retry with
a fresher log (per §8.3).

### 7.2 Append

Sent by an established leader to each peer to replicate log
entries and discover the longest matching prefix in a single
round-trip (§9.2).

| Field | Meaning |
|---|---|
| `term` | The leader's term. |
| `assume_matched_at` | The starting log index of the window the leader is shipping. |
| `terms` | The terms at `[assume_matched_at, assume_matched_at + terms.len())`. |
| `cmds` | The corresponding payloads, one per slot. |

The leader picks `assume_matched_at` by **bisection** between the
greatest log index known to match this peer and the smallest known
not to (§9.2.2). The `terms` array carries enough per-slot
identity for the follower to determine — slot by slot — exactly
how far it agrees with the leader.

**Reply:**

| Field | Meaning |
|---|---|
| `term` | The follower's most-recently-seen term. The leader uses this to detect that it is stale and step down. |
| `matched` | The greatest `LogId` at which the follower agrees with the leader, or `None` if the very first slot in the window did not match. |
| `conflict` | If the very first slot did not match, the index at which the disagreement was first observed; otherwise `None`. |

`matched` and `conflict` are mutually exclusive: a non-empty
`matched` means at least one slot agreed and the leader can
advance; a non-empty `conflict` means the leader's lower bound for
matching is wrong and it must retry with a smaller starting index.

## 8. Leader Election

A node decides to become a candidate when it observes that no
current leader is making progress (mechanism — timeout, heartbeat
starvation, etc. — TBD).

### 8.1 Choosing a Term

A candidate's term is `terms.len()` — the next slot past the end
of its term sequence. By writing its own term at index
`terms.len()` *before* issuing the `RequestVote`, the candidate
**reserves** that slot for itself: any later candidate observing
this state sees a term sequence that has already grown past it
and must claim a higher term to win freshness.

Each subsequent log entry the leader proposes (at indices
`terms.len()`, `terms.len() + 1`, …) carries this same term in
its term-sequence slot. Entries at indices below the candidate's
term are inherited from previous leaders and are not overwritten
unless they diverge from a more-up-to-date log (§9.2).

This is the standard Raft idea (the term is monotonically
increasing across leaders), but here the term and the log-index
space are coupled through the term sequence. The term is, in
effect, the index of the candidate's first prospective entry as
leader.

### 8.2 Issuing the Vote Request

The candidate sends a `RequestVote` (§7.1) to each other node,
carrying its term and its last log identity. It counts its own
vote in the initial tally — the candidate trivially satisfies the
grant rules for itself.

### 8.3 Voter Decision and Response

A voter receiving a `RequestVote` grants the vote **if and only if
both** conditions hold:

1. **Term not behind.** `req.term >= local.last_term`, where
   `local.last_term` is the term recorded at the last index of the
   voter's term sequence — i.e. the highest term the voter has
   stored or reserved.
2. **Log freshness.** `req.last_log_id > local.last_log_id`,
   comparing lexicographically `(term, index)`. The candidate's
   last log identity is strictly newer than the voter's own.

Both must hold. The freshness check is strict (`>`, not `>=`),
which forces simultaneous candidates with identical logs to
distinguish themselves by term.

#### On grant

The voter:

- Drops any in-memory leader state — granting is incompatible
  with continued candidacy or leadership on a different term
  (§15.1.1).
- Overwrites the term sequence at index `local.last_index + 1`
  with `req.term`, reserving that slot for the new leader.

The cmd sequence is **not** extended; the reservation is term-
only. The payload arrives later via `Append` (§7.2), and at
establishment the candidate pads any reserved-but-unwritten slot
with an empty payload (§8.5).

#### Response

The reply (§7.1) ships the voter's local state regardless of
`granted`, so a rejected candidate can read off which condition
fired: a `term_len` higher than the candidate's term means the
candidate is behind; a `last_history` greater-or-equal to the
candidate's `last_log_id` means the candidate's log is stale.

### 8.4 Tallying Votes and Establishing Leadership

A node that has issued a `RequestVote` for its own candidacy
holds **leader state** in memory until it either becomes an
established leader, steps down, or restarts. Followers carry no
leader state.

Leader state holds, at minimum:

- the candidate's term;
- the running set of granted votes (including this node itself);
- an `established` flag;
- per-peer replication state, populated at establishment (§9.2);
- the committed log index, advanced as quorum-matches arrive.

The candidate becomes an **established leader** when the granted
set reaches a quorum of the cluster. Establishment flips the flag
once and never reverses within the same leader-state instance.

Leader state is transient (in-memory only — see §15.1.1) and
distinct from the log itself. The in-memory exception is
deliberate: election outcomes don't need to survive a crash — a
restarted node simply re-runs the election.

#### Establishment is unique per term

At most one candidate becomes the established leader for any given
term. The argument is the standard Raft one:

- A voter that grants `RequestVote(term = T)` overwrites its term
  sequence at the next slot to record `T`. Its own
  `last_log_id` advances to `(T, slot)`.
- Any *subsequent* `RequestVote(term = T)` from a different
  candidate is then judged against this advanced state. The
  freshness check (§8.3.2) is strict, so the second candidate
  must offer a *newer* `last_log_id` than `(T, slot)` — which
  cannot happen at term `T` itself, since no one has produced an
  entry past `slot` at term `T`. The second candidate is
  rejected.
- Even if two candidates with identical `last_log_id` race for
  the same term, each voter's grant is exclusive — its grant set
  contains the first candidate it accepted, not the second. Two
  disjoint grant sets cannot both reach quorum in a cluster of
  size N.

Term reuse, by definition, does not occur: a candidate can claim
a term only by extending its own term sequence to that index, and
once an entry is recorded at that index by any candidate the next
candidate's `terms.len()` is at least one larger.

Only an established leader may serve application writes (§9). A
candidate still gathering votes refuses writes; so does any
follower.

### 8.5 Establishment Side-Effects

When a candidate flips to **established** for term T, the leader:

- Initializes per-peer replication state — `matched`, `end`, and
  an inflight bound — for each member of the cluster (§9.2.1).
- **Fills any term-sequence gap** between `cmds.len()` and
  `terms.len()` with no-op term values. (A gap arises because the
  candidate has reserved a slot via the term sequence but not yet
  appended a cmd. The leader fills the gap so subsequent writes
  start from a clean steady state.)
- **Pads the cmd sequence** with empty payloads up to
  `terms.len()`, restoring the steady-state invariant
  `cmds.len() == terms.len()`.
- Initiates the first round of `Append` RPCs (§9.2).

## 9. Log Replication

### 9.1 Write API

The application submits writes to a node via the control handle.
On the leader, the call returns when the entry has been committed
by quorum and carries the committed log index. On any other
node — follower, or candidate still gathering votes — the call
returns an error indicating that this node is not the leader.

The application interprets that error as a leader-redirect signal
and retries against another node. `raf` does not currently route
writes internally on the application's behalf; that is application
responsibility.

A node admits a write iff:

1. It holds leader state (§8.4), **and**
2. That leader state's `established` flag is set.

Both conditions are required. A candidate still tallying votes is
*not yet* a leader for write purposes; it must wait for quorum
before accepting writes.

### 9.2 Leader-Side Replication

The leader runs a **bisection-based** match-and-replicate loop per
peer: each `Append` RPC simultaneously narrows the leader's
estimate of where the peer's log diverges *and* ships the entries
in that window. There is no separate "find the matching index"
phase — discovery and replication share the same round-trip.

#### 9.2.1 Per-peer state

For each peer the leader tracks, in memory:

- **`matched`** — the greatest log index known to match this
  peer's log. Lower bound on the matching point. Starts at `0` at
  establishment and advances as positive replies arrive.
- **`end`** — the smallest index known *not* to match. Upper
  bound. Initialized at establishment to the leader's current
  `cmds.len()` (the optimistic assumption that the peer's log
  matches all the way through), and narrowed on each conflict
  reply.
- **`inflight`** — a single-permit gate. While an `Append` is
  outstanding to a peer, no second one is dispatched. The gate
  bounds parallelism per peer to one in-flight RPC.

#### 9.2.2 The Append window

The leader picks each `Append`'s starting index by bisection
between the current bounds:

```text
start = (matched + end) / 2
```

It then ships a fixed-size window — currently 64 slots — of
`(terms, cmds)` starting at `start`. The same RPC is therefore
both a probe (does the peer match at `start` … and if so, how
much further?) and a replication payload (here are the entries
to copy if you don't have them).

#### 9.2.3 Follower-side handling

A follower receiving an `Append`:

1. **Term check.** If `req.term < local.last_term`, reply with
   `matched = None`, `conflict = None`, and the local term — the
   leader is stale. (`req.term > local.last_term` is currently
   noted for future use; the slot reservation done by `RequestVote`
   already advanced the local term in normal operation.)
2. **Slot-by-slot match.** Walk the local term sequence against
   the request's `terms`, starting at `assume_matched_at`, and
   find the longest contiguous prefix where they agree.
   - If no slot agrees: reply `matched = None`,
     `conflict = Some(assume_matched_at)`. The leader's lower
     bound is wrong — the peer disagrees from the very first slot
     in the window.
   - If at least one slot agrees: truncate the local cmd sequence
     to drop any divergent suffix past the last matched slot,
     overwrite the term sequence with the request's `terms` over
     the window, and append the request's `cmds`. Reply
     `matched = Some((last_matched_term, last_matched_index))`.

The truncate-then-overwrite step is what enforces log-matching:
any entry past the matched boundary that disagrees with the
leader is discarded.

#### 9.2.4 Leader-side handling of reply

On reply:

- If `reply.term > leader.term`, the leader is stale; it clears
  its leader state and steps down.
- If `reply.matched` is set, the leader advances
  `replication.matched` for that peer and tries to advance the
  commit index (§9.2.5).
- If `reply.conflict` is set, the leader narrows
  `replication.end` to the conflict index. The next `Append` will
  probe a smaller starting index.

Either way, the inflight gate is released and the next round can
fire.

#### 9.2.5 Commit

A log index *I* is committed when a quorum of peers have
`matched >= I`, **and** the entry at *I* belongs to the leader's
own term. The own-term restriction is the standard Raft commit
rule: a leader may commit prior-term entries only by committing
one of its own that follows them — see §10.

The leader stores the committed index in its in-memory leader
state and reports it back to the application as the reply to a
successful `Write`.

## 10. Safety Argument

The current draft retains the term as an explicit value (the
project name notwithstanding — see §2). The safety argument
therefore reduces to the standard Raft proof, instantiated for
this design's term-as-slot-index choice:

- **Leader uniqueness per term** (§8.4). Each voter grants at
  most one `RequestVote` per term: granting reserves the term's
  slot in the term sequence and advances the voter's
  `last_log_id` to that slot, after which a second candidate at
  the same term cannot pass the strict freshness check. Two
  disjoint quorums cannot both reach majority.
- **Log matching** (§9.2). Followers truncate-and-overwrite any
  divergent tail on `Append`. The per-slot `(term, index)`
  identity lets the leader and follower compare slot-for-slot, so
  any agreement on slot *i* implies agreement on `[0..i]`.
- **Commit safety** (§9.2.5). An entry is committed only when a
  quorum has matched it *and* the entry is from the leader's own
  term. Older-term entries are committed transitively, by
  committing a newer-term entry that follows them.

The "no term" thesis remains aspirational: keeping the term as an
internal mechanism while exposing only `(term, index)` log
identities to the application is the current state. A future
revision may eliminate the term once an alternative ordering is
proven to carry the same invariants.

## 11. Membership Changes

*To be described.*

## 12. Open Questions

*To be described.*

---

## 13. Prior Art and Related Work

Before committing to this design, surveyed published Raft variants and
related consensus protocols for any that have already eliminated the term.
**Result: none found.** Every variant surveyed retains some equivalent of
the term, even when simplifying other parts of the protocol.

### 13.1 Variants That Rename the Term

These keep the term semantically intact, only changing the name:

- **Multi-Paxos** — *ballot number*.
- **Viewstamped Replication** — *view number*.
- **ZooKeeper / Zab** — *epoch*.
- **Apache Kafka KRaft** — *Leader Epoch*. Used both to fence zombies
  and to reconcile divergent logs ([KIP-595][kip-595]).

Renaming does not change the safety argument; the same monotone counter
serves the same role.

### 13.2 Variants That Keep the Term but Simplify Elsewhere

- **Logless Raft** ([Will Schultz, Aug 2025][logless-raft]) — keeps
  terms but eliminates incremental log management. The whole log
  becomes a single piece of state replicated in one shot, and
  `nextIndex` / `matchIndex` / `commitIndex` are removed. `(index,
  term)` pairs still identify unique log prefixes; terms are central,
  not eliminated.
- **MongoDB logless reconfig** — config identified by `(configVersion,
  configTerm)`; term retained.
- **Flexible Paxos** ([Howard et al.][fpaxos]) — relaxes quorum
  intersection rules. Orthogonal to terms.
- **Fast Raft** ([arXiv:2506.17793][fast-raft], 2025) — reduces
  message rounds in typical operation. Terms unchanged.

### 13.3 openraft's `(term, node_id, log_index)` LeaderId

The closest precedent is in the author's own prior work,
[`openraft`][openraft]. Standard Raft identifies a log entry by
`(term, log_index)` and enforces "at most one leader per term."
`openraft` instead uses `(term, node_id, log_index)` as the LeaderId,
permitting multiple leaders to be established within a single term;
the *latest* such leader (greatest `(node_id, log_index)` suffix) is
the valid one. The `single-term-leader` feature flag restores the
standard one-leader-per-term constraint.

That extension already weakens the term's role: in default openraft
mode, the term alone no longer determines leader uniqueness — the
`(node_id, log_index)` suffix does. `raf` is the logical next step:
drop the term entirely and let `(node_id, log_index)` (or another
replacement ordering) carry the full burden, then re-prove the same
safety invariants.

### 13.4 Other Simpler-Than-Raft Approaches

- **Compare-and-Swap Paxos** — models state evolution as CAS rather
  than log replication. Reportedly ~500 LOC for a full implementation.
  Different axis of simplification from term elimination.
- **Cassandra Accord** — leaderless, strictly serializable. No leader,
  so no term-equivalent for leader uniqueness; not a direct comparison.

### 13.5 Conclusion: Is `raf` Still Needed?

**Yes.** The published landscape contains:

1. Variants that **rename** the term (no real change to the design
   surface);
2. Variants that **simplify other parts** of Raft while retaining the
   term as the leader-uniqueness mechanism;
3. **Leaderless** alternatives that side-step the question.

A *leadered* consensus protocol that **eliminates the term as an
explicit field** does not appear in published literature or production
implementations. The closest precedent — openraft's relaxation of
"one leader per term" — is the author's own work, and `raf` is the
natural continuation: drop the term entirely and re-establish the
safety invariants on an alternative ordering.

[logless-raft]: https://will62794.github.io/distributed-systems/consensus/2025/08/25/logless-raft.html
[fpaxos]:       https://fpaxos.github.io/
[kip-595]:      https://cwiki.apache.org/confluence/display/KAFKA/KIP-595:+A+Raft+Protocol+for+the+Metadata+Quorum
[fast-raft]:    https://arxiv.org/abs/2506.17793

---

## 14. Implementation Conventions

The implementation follows the practices established in
[`openraft`][openraft], my prior Raft implementation. This section records
those practices so the development of `raf` stays consistent with them.
Reference path on this machine: `~/xp/vcs/github.com/drmingdrmer/openraft`.

[openraft]: https://github.com/drmingdrmer/openraft

### 14.1 Language and Repository Layout

- **Language**: Rust. Toolchain pinned via a `rust-toolchain` file.
- **Workspace layout**: a Cargo workspace with the core crate at the
  repository root (`raf/`) and any sub-crates (runtime adapters, stores,
  examples, integration tests) as members.
- **One main type per file**: each file contains a single primary trait or
  type with its impls. Applies to *new* code; existing files are not
  reorganized unless explicitly asked.
- **Public-API change annotation**: every change to a public type, trait,
  or associated type carries a `#[since(version = "X.Y.Z", change = "...")]`
  attribute, placed above any prior `#[since]`. Mechanical method-signature
  changes that follow from a parent generic-parameter change do not need
  one. `pub(crate)` items do not need one.

### 14.2 Makefile-Driven Workflow

The `Makefile` is the single entry point for build, test, and lint —
**`cargo fmt` and `cargo clippy` are never invoked directly**, only through
the Makefile, so settings stay consistent across all workspace crates.

Standard targets (mirroring openraft):

| Target | Purpose |
|---|---|
| `make lint` | `cargo fmt` + `cargo clippy --no-deps --all-targets -- -D warnings` across every workspace crate |
| `make test` | `cargo test` across the relevant feature combinations |
| `make basic_check` | fmt + clippy `--fix` + unit tests + integration tests + strict clippy + strict doc build |
| `make doc` | `RUSTDOCFLAGS="-D warnings" cargo doc --document-private-items --all --no-deps` |
| `make check` | `RUSTFLAGS="-D warnings" cargo check` for every crate |
| `make unused_dep` | `cargo machete` per crate |
| `make typos` | `typos` across docs and source |
| `make detsim` | deterministic-simulation test runner |
| `make clean` | `cargo clean` per crate |

Style configs:

- `rustfmt.toml`: `max_width = 120`, `comment_width = 100`,
  `imports_granularity = "Item"`, `group_imports = "StdExternalCrate"`,
  `reorder_imports = true`, `where_single_line = true`,
  `trailing_comma = "Vertical"`, `overflow_delimited_expr = true`,
  `merge_derives = false`, `inline_attribute_width = 0`,
  `chain_width = 100`.
- `clippy.toml`: `too-many-arguments-threshold = 10`,
  `cognitive-complexity-threshold = 25`.

### 14.3 GitHub Actions / CI

CI lives in `.github/workflows/`. The main file is `ci.yaml`, structured as
a set of focused matrix jobs rather than one monolithic job:

- **build (release)** — release build with all features enabled.
- **test core crates** — `cargo test` for the core crate(s), matrix over
  `stable` and `nightly`.
- **test integration crate** — `cargo test` for the integration-test
  crate, with a network-delay matrix variant.
- **store tests** — per-store crate, with defensive-store env flag
  (`*_STORE_DEFENSIVE=on`) enabled.
- **feature matrix** — core crate tested across each meaningful feature
  combination (`serde`, `bt`, `single-threaded`, etc.).
- **detsim** — deterministic simulation test (e.g. turmoil-based fuzz),
  toolchain pinned to a specific nightly date.
- **lint** — `cargo fmt --all -- --check`, full-workspace clippy with
  `-D warnings` across feature combinations, `cargo doc` with
  `RUSTDOCFLAGS="-D warnings"`, doc tests, `cargo audit`. Toolchain
  pinned to the same nightly date as `detsim`.
- **examples** — matrix per example, on both `stable` and `nightly`.

Cross-cutting CI conventions:

- Run on `push`, `pull_request`, and a nightly `schedule` cron.
- Always set `RUST_LOG=debug` and `RUST_BACKTRACE=full`.
- Use `RUST_TEST_THREADS: 2` for tests that contend on time/scheduling.
- On failure, upload the per-crate `_log/` directory as an artifact so
  the failure can be diagnosed offline.
- Toolchains driven by `actions-rust-lang/setup-rust-toolchain@v1`.

A separate `.github/workflows/commit-message-check.yml` enforces a
prefix on every commit subject. Allowed prefixes:

```
DataChange | Change/change | Feature/feat | Improve/improve |
Perf/perf | Dep/deps | Doc/docs | Test/test | CI/ci |
Refactor/refactor | Fix/fix | Fixdoc | Fixup | BumpVer | Chore/chore |
Build(deps) | Merge*
```

A `.mergify.yml` automates merge behavior; `dependabot.yml` keeps
dependencies current.

### 14.4 Coding Conventions

Style rules carried over from openraft, on top of standard `rustfmt` /
`clippy`:

- **`where` clauses for all trait bounds**, never inline:
  - Correct: `fn foo<T>(x: T) where T: RaftLeaderId`
  - Wrong:   `fn foo<T: RaftLeaderId>(x: T)`
- **Use trait names, not expanded bound lists.** If a trait already
  implies `Debug + Display + Clone + ...`, write the trait, not the
  expansion:
  - Correct: `struct Foo<T> where T: RaftLeaderId`
  - Wrong:   `struct Foo<T> where T: PartialOrd + Eq + Clone + Debug + Display + 'static`
- **One main trait/type per file** (see §14.1).
- **Where-bounds + `#[since]`** discipline for any public API change.

### 14.5 Git / PR Workflow

- Subject lines must start with one of the allowed commit prefixes
  (§14.3) or be a `Merge ...` commit.
- Commit messages follow the three-tier *subject / `# Summary` /
  `# Details`* structure (see top-level personal coding rules).
- **Rebase and squash** the branch onto the latest `main` *before*
  publishing a PR.
- **Do not rebase after publishing a PR** — only merge from `main`. This
  preserves stable commit hashes for review.
- Pre-PR checklist: `make lint` and `make basic_check` both pass
  locally; any documentation update is reflected in the user guide
  (`guide/`, mdBook).

---

## 15. Software Architecture

`raf` is a **single-crate Rust library** — one Cargo package, no
workspace sub-crates.

### 15.1 Components

#### 15.1.1 Core

- One singleton instance per wrapped node, owned by an internal task.
- Runs an event loop pulling events from a single mailbox.
- An event is one of: an inbound RPC (`RequestVote`, `Append`), a
  reply to an outbound RPC (`RequestVoteReply`, `AppendReply`), an
  application command (`Write`), or an internal trigger (`Elect`).
  Inbound RPC and application events carry a oneshot reply channel
  so the Core can produce replies inline.
- Replies are produced inline within the same loop, mirroring
  openraft's `RaftCore`.
- **Holds the term and cmd sequences directly in memory.** They
  are concrete in-process structures — there is no `Storage` trait
  abstraction at present (§15.1.4). Every handler reads from and
  writes to these sequences directly.
- **Holds in-memory leader state** when the node is a candidate or
  established leader (§8.4): the term, the granted-vote tally, the
  `established` flag, the per-peer replication state, and the
  committed index. Leader state is transient by design — it does
  not need to survive restart.

#### 15.1.2 Handle (Control Handle)

- Cheap to clone; the application clones it freely.
- The only API surface for the application *and* the inbound
  transport to talk to the Core. The application submits writes
  through it (§9.1); the inbound transport forwards peer RPCs
  through it.
- Internally a thin wrapper that fans application/transport calls
  into the Core's mailbox and awaits the inline reply.

#### 15.1.3 Network

- A single instance held by the Core, behind an `Arc` so outbound
  RPCs can be cloned into spawned subtasks. **No per-replicator
  parallel task** — the main runtime difference from openraft.
- Two outbound RPCs are exposed so far: `request_vote` (§7.1) and
  `append` (§7.2). More will be added as the protocol fills in.
- Pattern: the Core spawns an outbound RPC, awaits the reply on
  the spawned task, and posts the reply back to its own mailbox
  as a `*Reply` event (so the main loop can process the reply
  with full mailbox-ordered semantics). **Inbound RPCs *from*
  peers also arrive through the mailbox** (§7) — both directions
  flow through the Core's mailbox, but only the inbound direction
  carries an inline reply channel.

#### 15.1.4 Storage

A pluggable durability layer is anticipated but **not yet
present**. The current draft holds the term and cmd sequences as
concrete in-memory structures owned by the Core (§15.1.1); a
restart is not yet survivable. Once the protocol is fully fleshed
out, a storage abstraction will be reintroduced so application-
supplied backends can persist these sequences.

### 15.3 Construction

The top-level entry point spawns the Core task and exposes
cheap-clone `Handle`s for the application and inbound transport.
Construction takes the node's identity, the cluster membership,
the in-memory term and cmd sequences, and a `Network`
implementation. The exact constructor signature is the source of
truth (see `src/raf.rs`).

### 15.4 Differences From openraft

| Aspect | openraft | raf |
|---|---|---|
| Log id | `(term, node_id, log_index)` | `(term, log_index)` — standard Raft shape |
| Replication driver | per-target task running in parallel with `RaftCore` | single Network instance, all I/O via Core mailbox |
| Network trait | per-target factory + per-target send | one singleton |
| Replication probe | dedicated `next_index` walk | bisection-based `Append` window (§9.2.2) |
| Snapshots | supported | out of scope |
| Membership changes | supported | out of scope |
| State machine | required (`RaftStateMachine` trait) | not part of `raf`; application owns log application |
| Persistent storage | pluggable trait | not yet present (§15.1.4) |

