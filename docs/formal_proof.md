# raf-without-term 形式化安全证明

本文针对 `2026-05-11-raf-without-term-cn.md` 描述的 `raf-without-term` 协议模型，以及 `raf` 源码当前实现，给出一个可审阅的 safety proof 草案。

直接结论：修正后的设计可以继续使用 `(term, index)` 作为 `LogId`。关键点是：失败 election 留下的 slot 在没有 command 时不是 log entry；当某个 established leader 补齐这些 slots 时，必须把这些 slots 的 term 覆盖为自己的 `leader.term`。因此每个完整 log entry 的 term 都重新拥有唯一 established leader 来源，标准 Raft 的 log matching 和 leader completeness 证明可以平移过来。

## 1. 证明范围

证明覆盖：

- 静态 membership 下的 leader election safety。
- 基于 `prev_log_id` 的 log matching。
- quorum commit 的 leader completeness。
- state machine safety，即任意两个节点在同一 index 上提交的 entry 相同。
- 不单独持久化 `currentTerm` 时，`terms` 与 `cmds` 分离存储的安全性。
- crash 发生在多个 storage calls 之间时的恢复安全性。

证明不覆盖：

- liveness。
- election timer、heartbeat、automatic election trigger。
- snapshot、log compaction、membership change。
- application payload 的持久化语义。当前代码里 `WriteRequest.id` 没有写入 `Cmd`，leader write 追加的是 `Cmd::empty()`。
- 单个 storage call 内部的 torn write 或介质损坏。本文假设每个完成的 storage call 都留下一个 well-formed array state；如果 call 没有完成，则恢复到该 call 之前的状态。

## 2. 源码对应关系

| 协议概念 | 源码位置 | 证明中使用的语义 |
|---|---|---|
| 节点串行状态机 | `Core::run_loop()` | 单节点内所有协议状态转移串行化。 |
| 发起 election | `Core::do_elect()` | 取 `term = terms_len()`，本地 self-vote，并写入 `terms[term] = term`。 |
| 处理 vote | `Core::handle_request_vote()` | 要求 `req.term > observed_term`、`req.term >= local_next_term_slot`，并检查 `last_log_id` freshness；grant 时写入 `terms[req.term] = req.term`，中间 gap 由 storage 按 index 补齐。 |
| 建立 leader | `Core::establish_leader()` | 达到 quorum 后设置 `established = true`；把 `[cmds_len, terms_len)` 覆盖为 `leader.term`；再追加 empty commands 补齐 `cmds`。 |
| 发送 replication | `Core::try_initialize_replication()` | 以 `prev_log_id` 为匹配点发送连续的 `terms` 和 `cmds` 窗口。 |
| 接收 Append | `Core::handle_append()` | 先检查 `prev_log_id`，再截断冲突 suffix，覆盖 `terms`，追加缺失 `cmds`。 |
| 处理 AppendReply | `Core::handle_append_reply()` | 维护每个 target 的 `matched` 和 `end`，收到更高 term 时退位。 |
| 推进 commit | `Core::try_update_committed()` | 只考虑 `matched >= leader.term` 的节点，并要求这些节点组成 quorum。 |

## 3. 模型和记号

设节点集合为 `N`，静态 membership 的 quorum 集合为 `Quorum(N)`。任意两个 quorum 必须相交。

每个节点 `n` 的持久化状态是两个有限序列：

- `T_n`：term array，对应源码中的 `terms`。
- `C_n`：command array，对应源码中的 `cmds`。

index 从 `0` 开始。约定：

- `T_n[0] = 0`。
- `C_n[0] = empty`。
- `0 < |C_n| <= |T_n|`。
- 当且仅当 `i < |C_n|` 时，节点 `n` 在 index `i` 有完整 log entry。
- `log_id_n(i) = (T_n[i], i)`。
- `last_log_id(n) = log_id_n(|C_n| - 1)`。

`LogId` 使用字典序比较：先比较 `term`，再比较 `index`。这与源码中 `LogId` 派生的 `Ord` 一致。

重要区分：

- `i >= |C_n|` 且 `i < |T_n|` 的位置只是 election marker，不是 log entry。
- election marker 可以记录为 `T_n[i] = i`，用于表示节点已经观察到这个 term slot。
- 当 leader `t` established 后，它会把本地 `[|C|, |T|)` 中的 election markers 改写为 `t`，再补 empty commands。这些位置从此成为 leader `t` 产生的完整 empty log entries。

因此，`T[i] <= i` 不是协议不变量。修正后的核心不变量是：每个非零完整 log entry 的 term 都对应唯一 established leader。

## 4. Crash Closure 与可恢复中间态

`Core` 的一个 handler 不要求作为整体原子完成。crash 可以发生在任意两个 storage calls 之间；恢复后只保留已经完成的 storage calls，内存态如 `leader`、`replications`、`pending_writes` 和 in-flight RPC 都丢失。

本文对单个 storage call 使用下面的最小契约：

- 已完成的 `update_terms(since, terms)` 会留下一个完整的 `T` array。它可以扩展 `T`，中间缺失 slots 按 storage 规则补齐。
- 已完成的 `truncate_cmds(after)` 会留下一个完整的 `C` array，长度变成不大于原长度的 prefix。
- 已完成的 `append_cmds(cmds)` 会把 commands 完整追加到 `C`。
- 任意完成的 storage call 之后都满足 `T[0] = 0`、`C[0] = empty`、`0 < |C| <= |T|`。

这个模型不要求 `update_terms()`、`truncate_cmds()` 和 `append_cmds()` 组成跨 call 的 atomic batch。

### C1：Incomplete suffix 不是 log entry

对任意可恢复状态，完整 log 只由 `C` 的长度决定：

- `i < |C|` 是完整 log entry。
- `|C| <= i < |T|` 是 incomplete term evidence，不是 log entry。
- `last_log_id` 总是 `log_id(|C| - 1)`。

因此，crash 后留在 `T[|C|..]` 中的 term evidence 可以影响后续 term observation 和 vote rejection，但不能直接参与 log freshness，也不能被提交。即使某个节点后来用一个不够新的本地 term 发起 election，它仍然必须取得 quorum vote；已观察到更新 term 的 quorum 交集节点会拒绝这个 stale election，所以这只影响可用性，不会把 incomplete suffix 提升为 committed log。

### C2：`establish_leader()` 的 crash closure

`establish_leader()` 的持久化顺序是：

1. `fill_terms_gap_with(cmds_len, leader.term)`。
2. `append_cmds(vec![empty; n])`。

如果 crash 发生在第 1 步完成后、第 2 步完成前，则 `[|C|, |T|)` 的 terms 已经被覆盖为 `leader.term`，但这些位置还没有 command。由 C1，它们不是 log entry。

恢复后可能发生两类后续行为：

- 如果这个节点重新发起 election，它会基于当前 durable `terms` 选择或拒绝 term；这只会让 election 更保守，不会提交 incomplete suffix。
- 如果未来某个 established leader 补齐 gap，它会再次覆盖 `[|C|, |T|)`，再追加 empty commands。

因此，`establish_leader()` 的中间 crash state 不会把 failed-election marker 或 overwritten gap term 原样变成 committed entry。

### C3：`dispatch_leader_write()` 的 crash closure

`dispatch_leader_write()` 的持久化顺序是：

1. `update_terms(cmds_len, &[leader.term])`。
2. `append_cmds(vec![cmd])`。

如果 crash 发生在第 1 步完成后、第 2 步完成前，则新增 term slot 位于 `i = |C|`，还没有对应 command。由 C1，这个 slot 不是 log entry；它不会进入 `last_log_id`，也不会被 commit。

同时，write reply 只存在于内存中的 `pending_writes`。crash 后 pending reply 丢失，client 不会收到已经 committed 的确认。因此，这个中间状态不会向外暴露一个未完成的 committed write。

### C4：`handle_append()` 的 crash closure

`handle_append()` 接受请求后，持久化顺序最多包含：

1. 如果发现本地 entry 与 leader suffix 分歧，先 `truncate_cmds(i)`。
2. `update_terms(start, append.terms)`。
3. 只对本地缺失部分执行 `append_cmds(...)`。

如果 crash 发生在 `truncate_cmds(i)` 之后，则 `C` 已经缩短；被截掉的 suffix 不再是完整 log。`T` 中残留的 suffix 只保留为 incomplete term evidence。

如果 crash 发生在 `update_terms(start, append.terms)` 之后、`append_cmds()` 之前，则 leader suffix 的 terms 可能已经写入 `T`，但尚未追加对应 commands 的部分仍然位于 `T[|C|..]`。由 C1，这部分不是 log entry，不参与 `last_log_id`，也不能被 commit。

只有 `append_cmds()` 完成后，对应 index 才成为完整 log entries。此时这些 entries 来自已经通过 `prev_log_id` 匹配的 leader append window，仍满足后文的 Log Matching 证明。

### C5：Crash-Closure Lemma

从任意满足 `0 < |C| <= |T|` 的可恢复状态开始，执行任意一个协议 handler。如果 crash 发生在任意两个 storage calls 之间，则恢复后的持久化状态仍满足：

- `T[0] = 0`。
- `C[0] = empty`。
- `0 < |C| <= |T|`。
- 完整 log entry 只存在于 `i < |C|`。
- `T[|C|..]` 中的 incomplete term evidence 不会作为 log entry 被比较、复制或提交。

证明由 C2、C3、C4 对所有多步 storage 路径逐一覆盖。其它 handler 要么不修改 storage，要么只执行单个 storage call，直接由 storage call contract 得到 closure。

因此，后续 Log Matching、Leader Completeness 和 Commit Safety 不需要假设整个 `Core` 状态转移原子完成；它们只需要在所有可恢复状态上解释完整 log prefix。

## 5. Observed Term 语义

`raf` 没有单独持久化 `currentTerm`，因此需要从 `terms` 中导出本地已经观察到的最大 logical time。

定义：

```text
observed_term(n) = T_n[|T_n| - 1]
```

这个定义依赖一个额外规则：任何 RPC 让节点观察到更高 term 时，节点必须先把 `T[term] = term` 持久化，再继续处理后续逻辑。`TermArray::update_terms()` 会把中间缺失 slots 补齐，因此完成这一步后，`T` 的最后一个 slot 就是本地最大 observed term，且 `observed_term < |T|`。

注意，这不要求整个 `T` array 非递减。完整 log prefix 中的 term 非递减；但 `T[|C|..]` 可以包含 incomplete suffix。Append conflict repair 或 crash recovery 可能让较小的旧 suffix evidence 留在中间。关键不变量是：最后一个 slot 保存最大 observed term。

### O1：完整 log prefix term 非递减

对任意节点，完整 log prefix `i < |C|` 中的 `T[i]` 非递减。

直觉上，这是标准 Raft log 的 term monotonicity：

- leader 建立时补齐的 gap entries 都使用同一个 `leader.term`。
- leader 后续追加 entries 也使用同一个 `leader.term`。
- 更高 term leader 获得 vote 时必须通过 freshness 检查，因此它的完整 log prefix 不会比 voter 已知的 committed prefix 更旧。
- follower 接受 Append 时先匹配 `prev_log_id`，再替换冲突 suffix。

这个性质只用于完整 log prefix。它不要求 `T[|C|..]` 中的 incomplete term evidence 非递减。

### O2：durable observed term

一个 term 被 durable observed，当且仅当它出现在 `T` 中。

当前实现有三类 durable observation：

- `RequestVote` grant 写入 `terms[req.term] = req.term`。
- 收到更高 term 的 Append 时，先写入 `terms[append.term] = append.term`。
- 接受非空 Append entries 时，Append window 的 terms 被写入 `T`。

Append term 的 durable observation 必须发生在 `prev_log_id` 检查之前。即使后续 Append 因为 prev-log mismatch 被拒绝，节点也已经像标准 Raft 一样观察并持久化了这个更高 term。

### O3：election term 必须超过 observed term

本地发起 election 时，candidate 选择：

```text
term = |T|
```

由于任意 durable observed term `t` 都已经写入 `T[t]`，必然有 `observed_term < |T|`。因此选择 `term = |T|` 已经保证本地 self-vote 使用的 candidate term 大于所有已观察 term。

voter 处理 `RequestVote(req.term)` 时要求：

```text
req.term > observed_term
req.term >= |T|
req.last_log_id >= local_last_log_id
```

第一条保证节点不会在已经观察 term `t` 后，再给 `t` 或更小 term 投票。第二条保证 candidate 要占用的 term slot 在本地还没有出现过。第三条是标准 Raft freshness。

因此 incomplete suffix 可以让 election 更保守，但不会让一个 stale term 获得该节点的 vote。

## 6. 证明义务

**P0：Quorum 相交。** `Membership` 中 node id 唯一，所有用于投票和 commit 的节点集合都是 membership 的子集。任意两个多数 quorum 相交。

**P1：单节点状态转移串行化。** 一个节点的 `RequestVote`、`Append`、`AppendReply`、`Write` 等事件由同一个 `Core` 顺序处理，不存在本地并发写坏协议状态。

**P2：存储 well-formed。** 任意可达、可恢复的持久化状态都满足 `T[0] = 0`、`C[0] = empty`、`0 < |C| <= |T|`。

**P3：term slot 唯一占用。** candidate 选择 `t = |T|` 发起 election 时，写入 `T[t] = t`。voter grant `req.term = t` 时，必须让本地 `T` 至少包含 index `t`，并写入 `T[t] = t`。

**P4：同一 term 单次授权。** 对任意节点 `v` 和任意 term `t`，`v` 最多向一个 candidate grant `t`。当前最保守的实现规则是：一旦 `t <= observed_term(v)`，就不再 grant `t`。这里的“观察到”包括 RequestVote term slot，也包括已接受 Append entry 中携带的 term。

**P5：投票 freshness。** voter 只在 `req.last_log_id >= local_last_log_id` 时 grant vote。

**P6：leader established 后覆盖 gap terms。** candidate 收到 quorum vote 后，必须在服务写入前把本地 `[cmds_len, terms_len)` 的 terms 覆盖成自己的 `leader.term`，再用 empty commands 补齐 `cmds`。

**P7：Append 只复制 leader 的完整 log。** `AppendRequest.terms.len() == AppendRequest.cmds.len()`，窗口从 `prev_log_id.index + 1` 开始，并且每个 entry 都来自 leader 本地完整 log。

**P8：Append 前缀匹配。** follower 只有在本地存在 `prev_log_id.index` 且本地 `log_id(prev_log_id.index) == prev_log_id` 时，才接受后续 entries。否则不修改 log。

**P9：冲突 suffix 可被覆盖，匹配 prefix 不变。** Append 被接受后，follower 只允许从第一个分歧 index 开始截断或覆盖；`prev_log_id.index` 及之前的 prefix 不变。

**P10：commit 只直接提交 leader election index 之后的 entry。** leader 只有在某个 index `i` 被 quorum matched，且 `i >= leader.term` 时，才把 `committed` 推进到 `i` 或更大。被补齐在 `leader.term` 之前的 empty entries 只能被间接提交。

**P11：storage call crash closure。** crash 可以发生在任意两个 storage calls 之间；每个已完成的 storage call 必须留下满足 P2 的状态，且 incomplete suffix 不被解释为 log entry。

**P12：observed term 派生规则。** 本地 stale term 判断使用 `T[|T|-1]`；任何更高 RPC term 都必须先写入 `T[term] = term`，使最后一个 slot 仍然保存最大 observed term，并保证 `T[|T|-1] < |T|`。

## 7. 基础不变量

### I1：存储形状不变量

任意可达、可恢复状态都满足 `0 < |C| <= |T|`。

证明：

- 初始 `MemStorage::new()` 创建 `T = [0]` 和 `C = [empty]`。
- election 只追加或覆盖 `T`，不会让 `C` 变长。
- established leader 先覆盖 `[|C|, |T|)` 的 terms，再追加同样数量的 empty commands，因此补齐后 `|C| = |T|`。
- leader write 在 `T[|C|]` 写入 `leader.term`，再追加一个 command。
- Append 接受时先覆盖 `T` 的请求窗口，再只追加缺失的 `C`；若发现分歧，先截断 `C` 到分歧 index，再追加 leader suffix。
- 如果 crash 发生在这些 storage calls 中间，由 C5，恢复状态仍满足同一形状约束。

### I2：完整 entry 的 leader provenance

对任意非零完整 log entry `i < |C|`，`T[i] = t` 意味着该 entry 由唯一 established leader `t` 创建，或由这个 leader 复制到该节点。

证明：

- candidate 发起 election 时写入的 `T[t] = t` 在 `C[t]` 存在之前只是 election marker，不是完整 log entry。
- candidate established 后，`establish_leader()` 把 `[|C|, |T|)` 统一改写为 `leader.term`，再补 empty commands。因此这些新完成的 entries 都由这个 established leader 创建。
- leader 后续 write 也只写入当前 `leader.term`。
- follower 只能通过接受该 leader 的 Append 获得这些完整 entries。
- 由 I3，同一个 term 不会有两个 established leaders，所以 provenance 唯一。

### I3：同一 term 至多一个 established leader

一个 term `t` 至多被一个 established leader 拥有。

证明：

- 任意 established leader 都必须得到一个 quorum 的 grants。
- 假设 term `t` 有两个不同 established leaders `L1` 和 `L2`。
- 设它们获得的 quorum 分别为 `Q1` 和 `Q2`。
- 由 quorum intersection，存在节点 `v` 同时属于 `Q1` 和 `Q2`。
- `v` 必须分别 grant `t` 给 `L1` 和 `L2`。
- 这违反 P4。

因此同一 term 不可能存在两个 established leaders。

## 8. Log Matching

**定理 T1：如果两个节点在同一 index `i` 上具有相同 `log_id = (t, i)`，则它们在 `i` 及之前的完整 log prefix 相同。**

证明按 `i` 归纳。

index `0` 是固定默认 entry，显然成立。

对 `i > 0`：

- 由 I2，完整 entry `(t, i)` 由唯一 established leader `t` 创建。
- leader `t` 在自己的本地 log 中，index `i` 只有一个 entry。
- 其它节点获得 `(t, i)` 的唯一方式是接受 leader `t` 或后继合法 leader 的 Append。
- Append 由 P8 要求 `prev_log_id` 匹配；由归纳假设，`prev_log_id.index` 及之前的 prefix 已相同。
- P9 保证接受 Append 时不会修改已匹配 prefix。

因此两个节点如果在 index `i` 有同一 `LogId`，它们在 `i` 及之前的完整 prefix 相同。

这里的关键修正是 P6：失败 election marker 在成为完整 entry 前会被 established leader 的 term 覆盖。因此完整 entry 不会保留“没有 leader owner 的 failed term”作为自己的 `LogId`。

## 9. Leader Completeness

**定理 T2：如果 entry `e` 在 index `i` 被 term `t` 的 leader 提交，则所有 term `u > t` 的 established leader 都包含 `e`。**

证明：

1. leader 只直接提交 `i >= t` 的 entry。因此 `e` 是 term `t` leader 在自己 election index 或之后创建并复制到 quorum 的 entry。

2. 设提交 `e` 的 quorum 为 `Qc`。每个 `Qc` 中的节点都 matched 至少到 `i`。

3. 假设存在最小的 `u > t`，使 term `u` 的 established leader `Lu` 不包含 `e`。

4. `Lu` 必须从某个 quorum `Qv` 获得 vote。由 quorum intersection，存在节点 `v` 同时属于 `Qc` 和 `Qv`。

5. `v` 在 `Qc` 中 matched 到 `i`，所以曾经拥有包含 `e` 的 prefix。由于 `u` 是第一个不包含 `e` 的 established leader term，在 `v` 给 `Lu` 投票前，不存在更早的合法 leader 可以覆盖 `e`。因此 `v` 投票时仍包含 `e`。

6. `v` 只会在 `Lu.last_log_id >= v.last_log_id` 时 grant vote。下面分情况说明如果 `Lu` 不包含 `e`，这个 freshness 条件不可能成立：

   - 如果 `Lu.last_log_id.term < t`，它落后于 `v` 中包含 `e` 的 log。
   - 如果 `Lu.last_log_id.term == t`，但 `Lu` 不包含 index `i` 的 `e`，则 `Lu.last_log_id.index < i`，仍落后。
   - 如果 `Lu.last_log_id.term > t`，这个更高 term 的完整 entry 由某个 established leader `w` 创建。由于 `u` 是第一个不包含 `e` 的 established leader term，`w` 必须包含 `e`；再由 T1，拥有 `w` 的后续 entry 也必须拥有包含 `e` 的 prefix。因此 `Lu` 不可能拥有该更高 term entry 却缺少 `e`。

7. 所以 `v` 不可能 grant vote 给不包含 `e` 的 `Lu`，与 `Lu` established 矛盾。

因此所有更高 term 的 established leader 都包含已提交 entry `e`。

## 10. Commit Safety

**定理 T3：一旦某个 entry `e` 在 index `i` 被提交，未来任何合法 Append 都不能在任何节点上把 index `i` 覆盖成不同 entry 并再次提交。**

证明：

- 由 T2，所有未来 established leader 都包含 `e`。
- leader 发送 Append 时，请求窗口来自自己的完整 log，因此未来 leader 在 index `i` 发送的 entry 只能是 `e`。
- follower 接受 Append 前必须匹配 `prev_log_id`。若 follower 已包含 `e` 之前的 prefix，Append 不会把已提交 prefix 改成另一个 entry。
- 若 follower 暂时缺少 `e`，它只能通过包含 `e` 的合法 leader Append 获得 index `i`。

所以 index `i` 上的已提交 entry 在所有未来合法历史中保持不变。

**定理 T4：State Machine Safety。若两个节点分别提交了 index `i` 的 entry，则这两个 entry 相同。**

证明：

- 取两个提交事件中较早发生的一个，提交 entry 为 `e`。
- 由 T3，之后任何合法提交 index `i` 的 entry 都只能是 `e`。
- 因此两个节点在同一 index 上提交的 entry 相同。

## 11. 为什么仍然使用 `index >= leader.term`

修正后的设计会把更早的 gap slots 覆盖成当前 `leader.term`，所以可能出现 `T[i] = leader.term` 且 `i < leader.term` 的 empty entry。

这些 entries 不应被 leader 直接用于推进 commit。原因是：quorum matched 到 `i < leader.term` 并不能证明 quorum 已经匹配到 leader 当选时占用的 election index。`raf` 因此仍然使用更保守的提交门槛：

```rust
if quorum_has_matched(index) && index >= leader.term {
    commit(index);
}
```

一旦 leader 提交了 `leader.term` 或之后的 entry，它当选时占用的 entry 也已经在同一个 committed prefix 中；更早 backfilled empty entries 会随这个 prefix 一起间接提交。

## 12. 当前实现边界

以下是实现边界，不是本文 safety proof 的反例：

- `RequestVote` retry 仍然偏保守。当前没有持久化 `voted_for`，同一个 term 已经被观察后，重复请求会被拒绝。可以用 in-memory `voted_for` 改善可用性。
- `Core::handle_append()` 收到 `append.term > observed_term` 时，会先持久化 `terms[append.term] = append.term`。因此即使空 heartbeat 没有 entries，也会推进 durable observed term。
- follower commit 推进是保守的：当前只有 `append.commit_index < appended_last_index` 时才推进 follower commit。这可能推迟 follower 可见 commit，但不破坏 safety。
- `Membership::is_quorum()` 假设输入节点已经去重且属于 membership。当前主要调用点通过 `HashSet` 和 `BTreeMap` 满足这个前提；形式化模型仍需显式保留 P0。
- Crash closure 依赖 storage primitive 的 postcondition：单个完成的 storage call 必须留下 well-formed array state。真实磁盘实现如果可能暴露 torn write，需要在 storage 层自行恢复到某个完整 call 之前或之后的状态。

## 13. 建议验证用例

建议实现或保留以下测试：

- `request_vote_fills_gap_to_requested_term`：voter 从 `terms.len() = 4` grant `req.term = 6` 后，`terms.len() = 7`，且 `terms[6] = 6`。
- `request_vote_rejects_repeated_gapped_term`：同一个落后 voter grant `term = 6` 一次后，第二个 candidate 请求同一 term 必须被拒绝。
- `request_vote_rejects_observed_term_without_term_slot`：本地完整 log 已经观察到 term `6`，即使 `terms[6]` slot 尚不存在，也必须拒绝 `req.term = 6`。
- `request_vote_rejects_observed_term_in_incomplete_suffix`：即使 observed term 只来自 incomplete suffix，voter 也必须拒绝同 term RequestVote。
- `append_records_higher_term_before_prev_log_check`：Append 即使后续 prev-log 检查失败，也必须先记录更高 term。
- `election_uses_next_term_slot_after_observed_suffix_term`：本地发起 election 时，`terms.len()` 必须已经大于最后一个 observed term slot。
- `establish_leader_overwrites_gap_terms_with_leader_term`：leader term `6` established 后，原来 `[4, 5]` 的 gap terms 被覆盖成 `[6, 6]`，并追加 empty commands。
- `establish_leader_crash_after_gap_term_fill_keeps_suffix_incomplete`：只完成 gap term 覆盖但没有追加 empty commands 时，`last_log_id` 仍由旧 `cmds` prefix 决定。
- `leader_write_crash_after_term_update_does_not_create_entry`：只完成 write term update 时，新增 term slot 不成为完整 log entry，也不会 ack write。
- `handle_append_crash_after_truncate_or_term_update_keeps_log_on_cmd_prefix`：Append 中间 crash 后，`terms` suffix 不参与 `last_log_id`。
- `same_term_two_candidates_cannot_both_establish`：三节点场景中两个 candidate 选择同一 gapped term，不能同时 established。
- `committed_current_term_entry_survives_later_leader`：一个 leader 提交 `index >= leader.term` 后，后续更高 term leader 必须包含该 index 的 entry。

## 14. 总结

`raf-without-term` 的安全核心不是“没有 term”，而是“不再单独持久化 `currentTerm`”。term 仍然存在，并且完整 log entry 仍然用 `(term, index)` 比较新旧。

修正后的 gap completion 规则消除了之前的歧义：失败 election 留下的 term slot 在没有 command 时不是 log entry；一旦它被补成完整 entry，它的 term 会被 established leader 覆盖。于是完整 entry 的 `LogId` 又能指向唯一 leader 历史，log matching 和 leader completeness 可以按 Raft 的标准结构成立。
