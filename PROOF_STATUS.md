# raf proof status

本文记录 `raf` 在 `v0.1.1` 之后的形式化证明状态：哪些 safety 结论目前可以成立，哪些前提仍然只是模型假设，哪些工程语义还需要继续补齐。

本文不是最终证明，而是后续逐项完善的工作清单。

## 直接结论

当前还不能说已经完整证明了 `raf` 实现的正确性。

更准确的说法是：在一个受限模型下，`raf-without-term` 的核心 safety 可以按 Raft 的标准证明结构成立。这个受限模型包括：

- 静态 membership。
- 非 Byzantine 节点和网络。
- 不考虑 snapshot 和 log compaction。
- 不考虑 membership change。
- 覆盖 crash 发生在多个 storage calls 之间的恢复语义。
- 不要求整个 `Core` handler 原子完成；只要求每个完成的 storage call 留下 well-formed array state。
- 不覆盖单个 storage call 内部 torn write 或介质损坏。
- application payload 暂不参与证明；当前 `WriteRequest.id` 没有写入 `Cmd`。

在这些前提下，`v0.1.1` 修正后的 gap backfill 规则恢复了 `(term, index)` 作为 `LogId` 的合法性：

- failed election 留下的 slot 在没有 command 时只是 election marker，不是完整 log entry。
- established leader 补齐 missing command 的 gap slot 时，必须把这些 slot 的 term 覆盖为当前 `leader.term`。
- 因此每个非零完整 log entry 的 term 都重新来自某个唯一 established leader。
- 基于这个前提，Log Matching、Leader Completeness 和 State Machine Safety 可以沿用 Raft 的标准证明结构。

## 已经修正的核心问题

旧版本的问题是：把 failed election 留下的 term slot 当成了后续可以直接补成 empty log entry 的稳定前缀。

这会导致一个危险情况：

- 一个节点在 index `i` 上可能已经有真实 user command。
- 另一个节点在同一个 index `i` 上可能后续补了 empty command。
- 如果补 empty command 时沿用 failed election slot 原来的 term，就可能出现同一个 `(term, index)` 对应不同 command 的情况。

一旦发生这种情况，`(term, index)` 就不能自动推出 prefix equality，也不能可靠支撑 freshness 比较。

`v0.1.1` 的修正是：leader established 后，在补齐 `[cmds.len(), terms.len())` 这段 missing command slots 时，把这些 slots 的 term 统一重写为当前 `leader.term`，再写入 empty command。

因此，`terms[i] <= i` 不是协议不变量。backfilled empty entry 可以出现 `terms[i] > i`。真正需要的不变量是：每个完整 log entry 的 term 都来自某个 established leader。

## 当前可以主张的证明结论

在上述受限模型下，可以主张以下 safety 结论：

- Leader Election Safety：同一个 term 至多有一个 established leader。
- Log Matching：如果两个节点在同一个 index 上有相同 `LogId`，则它们在该 index 及之前的完整 log prefix 相同。
- Leader Completeness：某个 entry 被提交后，后续更高 term 的 established leader 必须包含它。
- Commit Safety：已经提交的 entry 不会被未来合法 leader 覆盖成另一个 entry 并再次提交。
- State Machine Safety：任意两个节点提交同一个 index 时，提交的 entry 相同。

这些结论依赖的关键代码语义是：

- `RequestVote` 同一 term 单次授权。
- `RequestVote` 必须检查 candidate `last_log_id` freshness。
- `establish_leader()` 必须覆盖 gap terms 为当前 `leader.term`。
- `Append` 必须先匹配 `prev_log_id`，再复制后续 entries。
- leader 直接推进 commit 时，只考虑 `matched >= leader.term` 的节点。

## 已采用的证明模型

### 1. crash 和 partial storage operation

实际代码里有多个多步 storage 更新路径：

- `establish_leader()` 先覆盖 gap terms，再追加 empty commands。
- `dispatch_leader_write()` 先写 term，再追加 command。
- `handle_append()` 可能先 truncate commands，再覆盖 terms，再追加 commands。

这些路径不一定需要跨多个 storage calls 的 batch 原子性。更贴合当前设计的证明方式是：证明每一个单独 storage call 完成后的中间状态本身就是 safe recoverable state。

也就是说，crash 后恢复出来的状态不一定必须是整个 `Core` 状态转移的 pre-state 或 post-state；它可以是这个状态转移中间的某个持久化前缀。只要这个前缀状态仍然满足 storage well-formed，并且不会把 incomplete term slot 当成完整 log entry，就不破坏 safety。

关键解释是：

- 完整 log entry 只由 `cmds.len()` 决定；只有 `i < cmds.len()` 的位置才是 log entry。
- `i >= cmds.len()` 的 `terms[i]` 只是 durable term evidence，不是 log entry。
- crash 后如果只完成了 `update_terms()`，但还没有 `append_cmds()`，新增或改写的 term slot 不会参与 `last_log_id`。
- crash 后如果只完成了 `truncate_cmds()`，被截掉的 command 不再是完整 entry；残留在 `terms` 里的 suffix 只能作为 observed term evidence。
- 后续 established leader 补 gap 时，会重新覆盖 `[cmds.len(), terms.len())`，因此这些 incomplete suffix terms 不会作为原样 log entry 被提交。

这一点已经写入 `docs/formal_proof.md` 的 `Crash Closure 与可恢复中间态` 小节。当前采用的 proof model 是：

- crash 可以发生在任意两个 storage calls 之间。
- 不要求跨 calls 的 atomic batch。
- 单个完成的 storage call 必须留下 `0 < cmds.len() <= terms.len()` 的 well-formed array state。
- recovery 后允许 `terms[cmds.len()..]` 保存 incomplete term evidence。
- incomplete suffix 不参与 `last_log_id`、log matching、replication payload 或 commit。

incomplete term evidence 如何影响 future election term selection，归入下一节 observed term 语义继续收敛；它不再阻塞 crash closure 本身。

### 2. observed term 的记录语义

`observed_term` 定义为 `terms` 最后一个 slot 的值。

原因是：收到更高 term 的 RPC 时，节点必须先把 `terms[term] = term` 持久化，再继续处理后续逻辑。这样即使 Append 后续因为 `prev_log_id` 不匹配被拒绝，节点也已经像标准 Raft 一样更新了 durable observed term。

完整 log prefix 的 term 可以证明是非递减的，但整个 `terms` array 仍然不要求非递减。Append conflict repair 或 crash recovery 可以让较小的旧 suffix evidence 留在中间；关键不变量是最后一个 slot 保存最大 observed term，并且 `observed_term < terms.len()`。

当前采用的语义是：

- durable observed term 来自三类写入：`RequestVote` grant 写入的 term slot、收到更高 term Append 时先写入的 term slot、或接受非空 Append entries 后写入的 entry terms。
- 本地发起 election 时使用 `term = terms.len()`；由于每个 observed term `t` 都已经写入 `terms[t]`，所以 `terms.len()` 已经大于 observed term。
- voter 处理 RequestVote 时使用 `req.term > observed_term` 判断是否已经观察过该 term。

这一点已经写入 `docs/formal_proof.md` 的 `Observed Term 语义` 小节，并由 `append_records_higher_term_before_prev_log_check`、`request_vote_rejects_observed_term_in_incomplete_suffix` 与 `election_uses_next_term_slot_after_observed_suffix_term` 覆盖。

## 尚未封闭的问题

### 1. quorum API 的前提没有由类型强制

当前 `Membership::is_quorum()` 只按输入长度判断 quorum，没有检查：

- 输入 node id 是否属于 membership。
- 输入 node id 是否去重。

当前主要调用点通过 `HashSet` 和 `BTreeMap` 基本满足这个前提，但形式化证明里仍然要把它作为 P0 前提保留。

待完善方向：

- 让 `is_quorum()` 内部按 membership 过滤并去重。
- 或者引入一个只能由 membership 构造的 vote set / match set。
- 在 proof 中把 quorum intersection 从外部假设收敛到实现保证。

### 2. application payload 没有进入证明

当前 `WriteRequest.id` 没有写入 `Cmd`，leader write 追加的是 `Cmd::empty()`。

因此当前 proof 证明的是 log position safety 和 replicated log shape safety，不是完整的 application command 线性一致性。

待完善方向：

- 给 `WriteRequest` 增加真正的 payload。
- 让 `Cmd` 保存 application command。
- 把 state machine apply 顺序纳入模型。
- 证明 committed log prefix 相同可以推出 state machine output 相同。

### 3. liveness 没有证明

当前 proof 不覆盖 liveness。

已知 liveness 相关边界包括：

- 没有 automatic election timer。
- 没有 heartbeat。
- `RequestVote` retry 过于保守；没有持久化 `voted_for`，同一个 observed term 的重复请求会被拒绝。
- follower commit 推进偏保守。

这些点不直接破坏 safety，但会影响系统是否最终能选主、复制和提交。

待完善方向：

- 先保持 safety proof 与 liveness proof 分离。
- 明确定义当前实现只证明 safety。
- 等 heartbeat、timer、retry 语义稳定后，再单独写 liveness 条件。

### 4. proof 里还需要补两个关键 lemma

当前形式化证明草案的方向是对的，但还需要把两个隐含步骤写成独立 lemma。

第一个是 leader term boundary：

- leader term `t` 来自 election 时的 `terms.len()`。
- election 前有 `cmds.len() <= terms.len()`。
- 所以 leader 建立前已有完整 log 的最大 index 小于 `t`。
- 因此 leader 直接提交的 `i >= t` entry 一定是当前 leader term 的 entry。

第二个是 complete entry provenance：

- election marker 在没有 command 前不是完整 entry。
- gap slot 变成完整 entry 前会被 established leader 改写 term。
- 后续 user entry 也只由 established leader 写入当前 `leader.term`。
- follower 只能通过合法 Append 获得这些完整 entries。
- 结合同一 term 唯一 leader，可以推出完整 entry 的唯一 leader 来源。

待完善方向：

- 把这两个 lemma 加入 `formal_proof.md`。
- 用它们替换当前部分依赖直觉的文字说明。

## 建议完善顺序

建议按下面顺序逐一处理：

1. 把 proof obligations 分成 “代码保证” 和 “模型假设”。
2. 补上 leader term boundary lemma。
3. 补上 complete entry provenance lemma。
4. 收紧 `Membership::is_quorum()`，让 quorum 前提由代码保证。
5. 决定 application payload 是否进入当前 proof 范围。
6. 最后再考虑 liveness proof。

## 当前状态判断

`raf-without-term` 的核心修正已经解决了 `(term, index)` 合法性的关键漏洞。

但是当前证明仍是一个受限 safety proof，不是完整实现证明。它可以作为后续工作的基础，但在 quorum API、application payload 和 liveness 这些方面还需要继续收敛。
