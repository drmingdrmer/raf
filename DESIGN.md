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
  preserves leader uniqueness and log-matching, but without the term
  as an explicit field. Output: a durably ordered, committed sequence
  of `u64` log identities. The application owns payload storage and
  payload application; no state machine is part of `raf` (see §6).

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

### 6.1 Log Model — Identity Without Payload

`raf` does **not** store log payloads. The log is a sequence of
opaque 64-bit identifiers — the *log identity*. The application maps
each identifier to whatever payload it represents and persists that
mapping in its own storage.

Conceptually:

```text
log: Vec<u64>
```

Rationale: the job of a consensus algorithm is to make a *sequence
of identities* durable and agreed-upon. Once identity persistence
is guaranteed, payload storage can be delegated to any external
system (database, object store, append-only file) without involving
the consensus core. Decoupling identity from payload is the central
simplification here — alongside dropping the term.

### 6.2 Storage Surface

The [`Storage`] trait exposes three operations:

| Method | Semantics |
|---|---|
| `append(from: u64, ids: &[u64])` | Write `ids` at positions `[from..from + ids.len())`, overwriting any prior values at those positions. Positions outside that range are unchanged. |
| `accept(idx: u64)` | Persist the accepted index — the boundary above which entries are not yet confirmed identical to the leader's. Monotone non-decreasing. See §6.3. |
| `read(range: Range<u64>)` | Read entries at positions in `range` (half-open, `start..end`). Returns a `LogState { entries: Vec<Option<u64>>, accepted_index, len }` — `None` entries are holes. |

Two notes on `append`:

**Append does not truncate.** Writing at a position never removes
entries at later positions. If old entries persist beyond
`from + ids.len()` after an `append`, they remain in the
speculative tail. The protocol relies on `accepted_index` (§6.3),
not on truncation, to demarcate trusted state.

**Holes are allowed.** `from` may exceed the highest
previously-written position; the intervening positions become
**holes** — empty slots that a later `append` may fill in. `read`
returns `None` at hole positions and `Some(id)` at written
positions. `len` is the highest written position plus one (or
zero if nothing has been written).

`append` and `accept` are on the steady-state hot path.
`read` is used **only** by the Core at startup to rebuild its
in-memory state from persistent storage; the running Core serves
replication out of its in-memory copy and never reads from `Storage`
again until the next restart.

Anything else — snapshots, payload retrieval, multi-range reads — is
out of scope; the application owns it. (See §3 *Non-Goals*.)

### 6.3 Accepted Content

Alongside the log array, `Storage` persists a single `u64` — the
**accepted index** — that splits the log into two regions:

- Positions `< accepted_index` are **decided**: entries here are
  confirmed byte-for-byte identical to the leader's log and will
  not be overwritten under normal protocol operation. The
  protocol must also never advance `accepted_index` past a hole,
  so a valid prefix is always contiguous and fully populated.
- Positions `>= accepted_index` are **speculative**: entries here
  have been written but not yet matched against the leader, so
  they may still be overwritten by a later `append(from, ids)` if
  a divergent prefix is detected.

The accepted index is monotone non-decreasing — once advanced, it
never regresses. `accept(idx)` is the only way to move it forward,
and `read` returns it together with the log so the Core can
reconstruct full state on startup in a single call.

#### Accepted-content cursor

Together with the leader_index recorded at log position
`accepted_index - 1`, the accepted index forms the
**accepted-content cursor** — exposed in code as the
[`AcceptedContent`] struct (`src/accepted_content.rs`, fields
`leader_index` and `index`). The leader_index half is *not*
persisted separately: it is derived from the log content at read
time.

The cursor is the freshness comparator used in leader election:
two cursors compare lexicographically by `leader_index` first,
then `index` — the same shape Raft uses for
`(lastTerm, lastLogIndex)`. See §7.1 and §8.

#### Validity of the persisted accepted index

The persisted `accepted_index` is **valid** — i.e., describes the
storage as-is — only when `accepted_index >= len`. Equivalently:
when there is no speculative tail.

- `accepted_index < len`: storage holds a speculative tail left
  behind by a previous leader. The persisted accepted_index
  describes a real prior state, but the trailing entries
  (positions `[accepted_index..len)`) have not yet been confirmed
  by the current leader.
- `accepted_index >= len`: every stored entry has been accepted
  by the current leader; the value is current and authoritative.

The Core consults this rule at startup. A valid accepted_index
means the entire stored log is safe to use as-is. An invalid one
means the speculative tail must be re-validated against the next
leader before it can be relied on. The check is exposed in code
as [`LogState::is_accepted_valid`].

## 7. Messages / RPCs

`raf`'s wire protocol is — for now — a single RPC pair, with more to
come as the design fills in.

### 7.1 RequestVote

Sent by a candidate to other nodes when it tries to become leader.
Modeled on standard Raft's `RequestVote` RPC. Defined in code as
[`RequestVote`] in `src/request_vote.rs`.

| Field | Meaning |
|---|---|
| `leader_index` | The candidate's chosen leader identity — the next index past the end of its local log. |
| `accepted` | The candidate's last known [`AcceptedContent`] — the freshness comparator (see §6.3). |

Each entry in the log is itself a `u64` leader_index (the identity
of the leader that proposed the entry at that position), so
`accepted.leader_index` is `log[accepted.index - 1]` — the
producer of the candidate's last accepted entry.

**Reply** — defined in code as [`RequestVoteReply`] in
`src/request_vote_reply.rs`:

| Field | Meaning |
|---|---|
| `granted` | Whether the responder accepted the candidate's claim. |
| `last_leader_index` | The responder's most-recently-seen leader_index. |
| `accepted` | The responder's local [`AcceptedContent`]. |

The reply always carries the responder's local state, so when
`granted == false` the candidate can determine which condition
caused the rejection — an occupied `leader_index`, or a stale
`accepted` cursor (per §8.3).

## 8. Leader Election

A node decides to become a candidate when it observes that no
current leader is making progress (mechanism — timeout, heartbeat
starvation, etc. — TBD).

### 8.1 Choosing an Identity

A candidate's identity is `len`, the next index past the end of its
local log. This index is what the candidate is "running for": if
elected, it will write its first entry as leader at this position.
Naming the identity by the position it claims removes the need for
a separate term — the position itself orders leaders.

Each subsequent entry the leader proposes (at positions `len`,
`len+1`, …) carries this same `leader_index` as its log value.
Entries at positions below `leader_index` are inherited from
previous leaders and remain unchanged unless they need to be
overwritten because they diverge from a more-up-to-date log
(§6.2).

### 8.2 Issuing the Vote Request

The candidate sends a [`RequestVote`] (§7.1) to each other node,
carrying:

- its candidate identity (`leader_index = len`);
- its last accepted content
  (`accepted_index`, `accepted_leader_index`).

### 8.3 Voter Decision and Response

*TBD — comparison rules between candidate's and voter's last
accepted content, and the shape of the response message.*

## 9. Log Replication

*To be described.*

## 10. Safety Argument

*To be described — how leader uniqueness and log-matching are preserved
without a term.*

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
- An event is one of: an inbound network message, an inbound network
  response, or an application command from a `Handle`.
- Replies are produced inline within the same loop, mirroring
  openraft's `RaftCore`.

#### 15.1.2 Handle (Control Handle)

- Cheap to clone; the application clones it freely.
- The only API surface for the application to talk to the Core
  (submit writes, query state, etc.).
- Internally a thin wrapper around an `mpsc::UnboundedSender` into
  the Core's mailbox.

#### 15.1.3 Network Instance

- A single instance held by the Core. **No per-replicator parallel
  task** — the main runtime difference from openraft.
- All outbound traffic flows through this one object.
- Pattern: the Core calls `network.send_*(target, req).await` and
  receives the reply directly. The Network instance owns the
  send-and-await round-trip; outbound replies do **not** loop back
  through the Core's mailbox. **Inbound RPCs *from* peers do
  arrive through the mailbox** (§7) — the asymmetry is deliberate:
  the Core's loop is already waiting on the mailbox, so routing
  inbound RPCs there is free; outbound calls are cleaner as
  direct awaits.

### 15.2 Traits

#### 15.2.1 `Storage`

- Trait, defined in `src/storage.rs`. Three methods —
  `append(from: u64, ids: &[u64])`, `accept(idx: u64)`, and
  `read(range: Range<u64>)` — all returning `io::Result<…>`. No
  associated error type; storage failures are I/O failures.
- `read` returns a [`LogState`] struct
  (`{ entries, accepted, len }`) defined in `src/log_state.rs`,
  where `accepted` is an [`AcceptedContent`] cursor — so the Core
  picks up entries, accepted cursor, and log length in one call.
- Methods are declared as `fn ... -> impl Future + Send` rather than
  `async fn`, so their returned futures are guaranteed `Send` and
  can be `.await`ed inside the Core's `tokio::spawn`'d task.
- See §6 for the log-model rationale, §6.2 for the storage surface,
  and §6.3 for accepted-index semantics.

#### 15.2.2 `Network`

- Trait, so the application can plug in its own transport.
- One method so far: `send_request_vote(target: u64, req: RequestVote)`
  — forwards a `RequestVote` to the node identified by `target`,
  awaits the reply, returns it. Returns `impl Future + Send` (not
  `async fn`) so the future stays `Send`. More RPCs will be added.
- A default `InProcessNetwork` ships with the crate; currently a
  stub returning `Err`, to be filled in once multi-node setup is
  wired.

### 15.3 Construction

```rust
let raf = Raf::new(storage, network);
let handle = raf.handle();
```

`Raf::new` spawns the Core task; the returned `Raf` exposes
`handle()` to produce cheap-clone `Handle`s.

### 15.4 Differences From openraft

| Aspect | openraft | raf |
|---|---|---|
| Log id | `(term, node_id, log_index)` | (no term — TBD) |
| Replication driver | per-target task running in parallel with `RaftCore` | single Network instance, all I/O via Core mailbox |
| Network trait | per-target factory + per-target send | one singleton |
| Snapshots | supported | out of scope |
| Membership changes | supported | out of scope |
| State machine | required (`RaftStateMachine` trait) | not part of `raf`; application owns log application |

