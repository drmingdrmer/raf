# raf 设计

`raf` 是一个 Raft 变体。它不把任期作为独立持久化时钟保存，而是让候选人在发起选举时占用一个日志 index，并把这个 index 的数值作为自己的 leader term。候选人当选后，这个 leader 产生的后续日志项都会记录同一个 term。

本文只描述当前核心协议和实现边界，避免背景性讨论。

## 目标

- 复制一条有序日志。
- 通过 quorum 选出唯一 leader。
- 用 `(term, index)` 标识日志项，其中 `term` 是 leader 当选时占用的 index。
- 保持核心实现小而直接：一个 `Core` task 串行处理所有事件。

当前不处理：

- snapshot / log compaction。
- membership change。
- payload 持久化语义。
- 定时 election 和 heartbeat；当前由应用显式触发 election，复制由事件循环继续驱动。

## 状态

每个节点持久化两段 Rust `Vec`，二者都用 `LogIndex` 作为下标：

- `terms: Vec<Term>`：`terms[i]` 记录 index `i` 这条日志的 leader term。
- `cmds: Vec<Cmd>`：`cmds[i]` 记录 index `i` 这条日志的应用命令；`raf` 不解释 `Cmd` 的内容。

index `0` 总是存在：

- `terms[0] = 0`。
- `cmds[0] = Cmd::empty()`。

当 `terms` 和 `cmds` 在同一个 index 上都有值时，该 index 对应的逻辑日志项是：

```text
log[index] = (terms[index], cmds[index])
log_id     = (terms[index], index)
```

`terms` 可以短暂比 `cmds` 长。这只用于选举：

1. 候选人用 `terms.len()` 作为新的 term。
2. 候选人先写入 `terms[term] = term`，表示这个 index 已经被该候选人占用。
3. 候选人成为 established leader 后，把 `[cmds.len(), terms.len())` 这段缺失 command 的 slots 改写为当前 `leader.term`，再追加 empty commands，让 `cmds.len()` 追上 `terms.len()`。

leader 之后追加的新日志项都写入同一个 `leader.term`。

注意：只有发起 election 时，candidate term 必须等于它占用的 log index。成为 established leader 后，它可以把更早的 incomplete slots 补成自己的 empty log entries，因此完整 log entry 上的 `terms[i]` 不再要求满足 `terms[i] <= i`。

## 组件

- `Raf`：公开 API。负责把本地 election、入站 RPC、application write 转成 `Event` 发给 `Core`。
- `Core`：单线程事件循环。所有协议状态变更都在这里串行执行。
- `Storage`：持久化 `terms` 和 `cmds` 两个 `Vec`；所有操作返回 `io::Result`。
- `Event`：`Core` 的内部事件，包括 `Elect`、`RequestVote`、`RequestVoteReply`、`Append`、`AppendReply`、`Write`。
- `Network`：`Core` 用它向其它节点发送 outbound `RequestVote` 和 `Append`。
- `InProcessNetwork`：进程内 `Network` 实现，用 `NodeId -> Raf` 路由表把 RPC 转发给目标节点。
- `Metrics`：公开 metrics 快照，应用可通过 `Raf::metrics()` 订阅 role、当前 term、commit index、`terms` 的下一个可写 index、`cmds` 的下一个可写 index 和 replication progress 变化。`term` 来自 `terms` 的最后一个元素。
- `LeaderState`：候选人或 leader 的内存态，包括当前 term、已获投票集合、是否 established、replication state 和 pending writes。
- `ReplicationState`：leader 对一个目标节点的复制状态，包括 `matched`、`end` 和该目标节点的 in-flight Append 限制。

`Core::run()` 每次从事件队列取一个 `Event`，调用 `handle_event()` 处理；处理完任何事件后，都会调用 `try_initialize_replication()` 尝试继续派发复制 RPC。若当前节点不是 established leader，该函数直接 no-op。

Storage IO error 是只在 `Core` 内处理的致命错误：`Core` 记录错误并退出，不把底层 IO error 作为 RPC 或 control handle 的返回值传播。调用者只会观察到 reply channel dropped 或 `Core` 的事件通道关闭。

## 消息

### RequestVote

候选人发给其它节点，用于请求选票。

| 字段 | 含义 |
|---|---|
| `term` | 候选人选择的 leader term，也就是它要占用的 log index。 |
| `last_log_id` | 候选人的最后一条日志身份 `(term, index)`，用于 freshness 比较。 |

### RequestVoteReply

| 字段 | 含义 |
|---|---|
| `granted` | 是否投票给候选人。 |
| `next_term_slot` | responder 的 `terms.len()`，也就是 responder 下一次可以写入 `terms` 的 index。候选人可用它判断自己是否落后。 |
| `last_log_id` | responder 的最后一条日志身份。候选人可用它判断日志 freshness 失败原因。 |

### Append

leader 发给其它节点，用于探测已匹配的日志前缀，并复制一段连续日志。

| 字段 | 含义 |
|---|---|
| `term` | leader term。 |
| `commit_index` | leader 已知 committed 的最大 index；follower 可用它推进本地 commit。 |
| `prev_log_id` | 本次请求携带 entries 之前的 log id。follower 必须先用它匹配自己的 log。 |
| `terms` | `prev_log_id.index + 1` 开始的真实 `Vec<Term>`。 |
| `cmds` | `prev_log_id.index + 1` 开始的真实 `Vec<Cmd>`，与 `terms` 等长且顺序一致。 |

### AppendReply

| 字段 | 含义 |
|---|---|
| `term` | responder 当前看到的最新 term。leader 用它判断自己是否 stale。 |
| `matched` | 如果 `prev_log_id` 匹配且 entries 被接受，返回最后一条已匹配日志的 `LogId`；如果 entries 为空，返回 `prev_log_id`。 |
| `conflict` | 如果 `prev_log_id` 不存在或 term 不匹配，返回冲突 index。 |

`matched` 和 `conflict` 语义上互斥。entries 为空的 `Append` 仍然会匹配 `prev_log_id`：匹配成功返回 `matched = Some(prev_log_id)`，匹配失败返回 `conflict = Some(prev_log_id.index)`。

## Election

### 发起 election

节点触发 `Raf::elect()` 后，`Core` 处理 `Event::Elect`：

1. 取 `term = terms.len()`。
2. 创建 `LeaderState`，并把自己的 node id 加入 `granted_votes`。
3. 写入 `terms[term] = term`，占用这个 index。
4. 立即检查 self-vote 是否已经达到 quorum；单节点集群会直接成为 established leader。
5. 如果还没有 quorum，向其它节点发送 `RequestVote { term, last_log_id }`。

### 处理 RequestVote

收到 `RequestVote` 后，voter：

1. 读取本地 `terms` 的最后一个元素，得到 `local_last_term`。
2. 读取本地最后一条 command 对应的 `local_last_log_id`。
3. 如果 `req.term <= local_last_term`，拒绝。当前没有持久化 `voted_for`，所以同一个已经观察过的 term 也保守拒绝。
4. 如果 `req.term < local_next_term_slot`，拒绝。候选人请求用作 term 的 log index 在本地已经存在，不能再次授予。
5. 如果 `req.last_log_id < local_last_log_id`，拒绝。
6. 否则清空本地 `leader` 内存态，把 `req.term` 写入本地 `terms[req.term]`。如果本地 `terms` 落后，中间缺失 slots 先按自己的 index 补齐。最后返回 `granted = true`。

当前实现和标准 Raft 一样接受相同 freshness：候选人的日志不落后即可。

### 处理 RequestVoteReply

候选人收到投票回复：

- 如果回复对应的 `sending_term` 不是当前 `leader.term`，忽略。
- 如果 `granted = true`，把该节点加入 `granted_votes`。
- 如果已达到 quorum，调用 `establish_leader()`。
- 如果 `granted = false`，清空 `leader` 状态，退回 follower。

### Establish leader

成为 established leader 时：

1. 设置 `leader.established = true`。
2. 为每个目标节点初始化 `ReplicationState`，也包含 leader 自己。
3. 将 `[cmds.len(), terms.len())` 中的 term 全部覆盖为当前 `leader.term`。
4. 用 empty command 补齐 `cmds`，让 `cmds.len()` 追上 `terms.len()`。

复制不在 `establish_leader()` 里直接展开，而是在当前事件处理完成后由 `try_initialize_replication()` 统一触发。

## Replication

leader 对每个目标节点保存：

- `matched`：已知该节点与 leader 匹配的最大 index。
- `end`：二分探测使用的上界；`end` 指向一个还没有确认匹配的 index。
- `inflight`：保证同一个目标节点同时最多一个 Append RPC。

### 发送 Append

`try_initialize_replication()` 对每个没有 inflight RPC 的目标节点：

1. 计算 `prev_index = (matched + end) / 2`。
2. 从 leader 本地 log 读取 `prev_log_id = log_id(prev_index)`。
3. 从 `prev_index + 1` 开始读取至多 64 条真实日志对应的 `terms` 和 `cmds`。
4. 发送 `Append { term, commit_index, prev_log_id, terms, cmds }`。

这个 RPC 同时用于二分探测匹配点和复制缺失日志。

### 处理 Append

收到 `Append` 后，follower：

1. 要求 `terms.len() == cmds.len()`；长度不等说明请求 malformed，直接 panic。
2. 如果 `append.term < local_last_term`，返回本地 term，并不匹配任何 index。
3. 否则清空本地 candidate/leader 内存态，退回 follower。
4. 用 `prev_log_id` 匹配本地完整 log：`prev_log_id.index` 必须存在于 `cmds`，并且本地 `log_id(prev_log_id.index)` 必须等于请求的 `prev_log_id`。
5. 如果 `prev_log_id` 不匹配，返回 `conflict = Some(prev_log_id.index)`。
6. 从 `prev_log_id.index + 1` 开始处理请求里的真实 entries：
   - 如果本地已有 entry 与 leader entry 分歧，从第一个分歧 index 截断本地 commands。
   - 覆盖本地 `terms` 中本次请求对应的连续范围。
   - 只追加本地缺失的 commands，避免重复追加已存在 command。
   - 如果 `commit_index < appended_last_index`，推进本地 `committed` 到 `commit_index`。
   - 如果 entries 非空，返回本次请求最后一个 index 的 `LogId`；如果 entries 为空，返回 `prev_log_id`。

### 处理 AppendReply

leader 收到 `AppendReply`：

- 如果 `reply.term > leader.term`，清空 `leader`，退回 follower。
- 如果 `reply.conflict = Some(index)`，设置该目标节点的 `end = index`。
- 如果 `reply.matched = Some(log_id)`，设置该目标节点的 `matched = log_id.index`，并把 `end` 至少推进到 `matched + 1`，然后尝试推进 commit。

每个事件处理结束后，`Core` 会再次调用 `try_initialize_replication()`，因此冲突或匹配回复都会驱动下一轮复制。

## Commit

leader 根据所有 replication state 的 `matched` 计算节点级 commit index。replication state 包括 leader 自己：

1. 只考虑 `matched >= leader.term` 的节点，避免直接提交旧 term 日志。
2. 从小到大扫描 matched index 集合。
3. 若某个 index 被 quorum 覆盖，则把它作为新的 committed candidate。
4. 若 candidate 大于当前 `Core.committed`，更新 committed。

`committed` 是 `Core` 的节点级状态，不属于 `LeaderState`。

leader 会把自己的 committed index 放进 `AppendRequest.commit_index`。follower 只在该 index 严格小于本次成功 `Append` 的最后 index 时接受它，避免提交还没有被当前 leader 的 `Append` 覆盖确认的日志。

## Write

`Raf::write()` 把 application write 发送到 `Core`。

当前规则：

- 非 established leader 返回 `not a leader`。
- established leader 在本地追加一条新日志，写入当前 `leader.term` 和 empty command。
- leader 更新自身 replication progress，并把 application reply 放入 `pending_writes`。
- 后续 `try_initialize_replication()` 把新日志复制给其它节点。
- `try_update_committed()` 观察到 quorum matched 后推进 `Core.committed`，再回复对应的 pending write。

当前 `Cmd` 仍是占位 command，`WriteRequest` 的 application payload 持久化语义不在本文范围内。
