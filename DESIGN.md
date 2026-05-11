# raf 设计

`raf` 是一个 Raft 变体。它不把任期作为独立持久化时钟，而是让候选人使用一个日志 index slot 作为自己的 leader term。候选人当选后，后续由这个 leader 产生的日志项都写入同一个 term 值，也就是它当选时占用的 index。

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
- 完整的 leader-side write replication；`write()` 入口目前仍返回未实现错误。

## 状态

每个节点维护两条同 index 空间的序列：

- `terms: TermArray`：每个日志 slot 的 leader term。
- `cmds: CmdArray`：每个日志 slot 的 opaque application command。

逻辑上：

```text
log[index] = (terms[index], cmds[index])
log_id     = (terms[index], index)
```

`terms` 可以短暂长于 `cmds`。这是 election 的保留 slot：

1. 候选人用 `terms.len()` 作为自己的 term。
2. 候选人先把这个 term 写入 `terms[term]`，表示占用该 index。
3. 当候选人成为 established leader 时，用 empty command 补齐 `cmds`。

leader 之后追加的新日志项都使用同一个 `leader.term`。

term array 必须保持一个关键不变量：

```text
i >= terms[i]
```

也就是说，一个 slot 里记录的 leader term 不能大于这个 slot 自身的 index。因为 term 本身就是 leader 当选时占用的 index slot。

## 组件

- `Raf`：公开 API。负责把本地 election、入站 RPC、application write 转成 `Event` 发给 `Core`。
- `Core`：单线程 mailbox event loop。所有协议状态变更都在这里串行执行。
- `Event`：Core 的内部事件，包括 `Elect`、`RequestVote`、`RequestVoteReply`、`Append`、`AppendReply`、`Write`。
- `Network`：Core 用它向其它节点发送 outbound `RequestVote` 和 `Append`。
- `InProcessNetwork`：进程内 `Network` 实现，用 `NodeId -> Raf` 路由表把 RPC 转发给目标节点。
- `Metrics`：公开 metrics 快照，应用可通过 `Raf::metrics()` 订阅 role、当前 term、commit index、term slot、log slot 和 replication progress 变化。`term` 来自 term array 最后一项。
- `LeaderState`：候选人或 leader 的内存态，包括当前 term、已获投票集合、是否 established、replication state 和 pending writes。
- `ReplicationState`：leader 对单个 peer 的复制状态，包括 `matched`、`end` 和 per-peer inflight gate。

`Core::run()` 每次从 mailbox 取一个 `Event`，调用 `handle_event()` 处理；处理完任何事件后，都会调用 `try_initialize_replication()` 尝试继续派发复制 RPC。若当前节点不是 established leader，该函数直接 no-op。

## 消息

### RequestVote

候选人发给 peer，用于请求选票。

| 字段 | 含义 |
|---|---|
| `term` | 候选人选择的 leader term，也就是它占用的 index slot。 |
| `last_log_id` | 候选人的最后一条日志身份 `(term, index)`，用于 freshness 比较。 |

### RequestVoteReply

| 字段 | 含义 |
|---|---|
| `granted` | 是否投票给候选人。 |
| `next_term_slot` | responder 的下一个可写 term slot。候选人可用它判断自己是否落后。 |
| `last_log_id` | responder 的最后一条日志身份。候选人可用它判断日志 freshness 失败原因。 |

### Append

leader 发给 peer，用于探测匹配点并复制日志窗口。

| 字段 | 含义 |
|---|---|
| `term` | leader term。 |
| `assume_matched_at` | 本次窗口起始 index。 |
| `terms` | 从 `assume_matched_at` 开始的 term window。 |
| `cmds` | 与 `terms` 对应的 command window。 |

### AppendReply

| 字段 | 含义 |
|---|---|
| `term` | responder 当前看到的最新 term。leader 用它判断自己是否 stale。 |
| `matched` | 如果窗口中至少一个 slot 匹配，返回最后一个匹配的 `LogId`。 |
| `conflict` | 如果窗口第一个 slot 就不匹配，返回冲突 index。 |

`matched` 和 `conflict` 语义上互斥。

## Election

### 发起 election

节点触发 `Raf::elect()` 后，Core 处理 `Event::Elect`：

1. 取 `term = terms.len()`。
2. 创建 `LeaderState`，并把自己的 node id 加入 `granted_votes`。
3. 写入 `terms[term] = term`，保留该 index slot。
4. 立即检查 self-vote 是否已经达到 quorum；单节点集群会直接成为 established leader。
5. 如果还没有 quorum，向其它节点发送 `RequestVote { term, last_log_id }`。

### 处理 RequestVote

收到 `RequestVote` 后，voter：

1. 读取本地最后一个 term slot，得到 `local_last_term`。
2. 读取本地最后一条 command 对应的 `local_last_log_id`。
3. 如果 `req.term < local_last_term`，拒绝。
4. 如果 `req.term < local_next_term_slot`，拒绝。候选人的 term slot 在本地已经存在，不能再次授予。
5. 如果 `req.last_log_id < local_last_log_id`，拒绝。
6. 否则清空本地 `leader` 内存态，写入新的 term slot，并返回 `granted = true`。

当前实现和标准 Raft 一样接受相同 freshness：候选人的日志不落后即可。

### 处理 RequestVoteReply

候选人收到投票回复：

- 如果回复对应的 `sending_term` 不是当前 `leader.term`，忽略。
- 如果 `granted = true`，把该 peer 加入 `granted_votes`。
- 如果已达到 quorum，调用 `establish_leader()`。
- 如果 `granted = false`，清空 `leader` 状态，退回 follower。

### Establish leader

成为 established leader 时：

1. 设置 `leader.established = true`。
2. 为每个 peer 初始化 `ReplicationState`。
3. 用 empty command 补齐 `cmds`，让 `cmds.len()` 追上 `terms.len()`。

复制不在 `establish_leader()` 里直接展开，而是在当前事件处理完成后由 `try_initialize_replication()` 统一触发。

## Replication

leader 对每个 peer 保存：

- `matched`：已知 peer 与 leader 匹配的最大 index。
- `end`：当前探测上界；`end` 指向一个 follower 没有 confirmed match 的 index。
- `inflight`：保证同一个 peer 同时最多一个 Append RPC。

### 发送 Append

`try_initialize_replication()` 对每个没有 inflight RPC 的 peer：

1. 计算 `start = (matched + end) / 2`。
2. 读取固定窗口，当前长度为 64。
3. 发送 `Append { term, assume_matched_at: start, terms, cmds }`。

这个 RPC 同时用于二分探测匹配点和复制缺失日志。

### 处理 Append

收到 `Append` 后，follower：

1. 要求 `terms.len() == cmds.len()`；长度不等说明请求 malformed，直接 panic。
2. 如果 `terms` 和 `cmds` 都为空，返回空回复：`matched = None` 且 `conflict = None`。
3. 如果 `append.term < local_last_term`，返回本地 term，并不匹配任何 index。
4. 否则清空本地 candidate/leader 内存态，退回 follower。
5. 从 `assume_matched_at` 开始逐 slot 比较本地 `terms` 与请求 `terms`。
6. 如果第一个 slot 就不匹配，返回 `conflict = Some(assume_matched_at)`。
7. 如果有匹配 prefix：
   - 如果本地 command tail 与 leader 分歧，截断到 `last_matched + 1`。
   - 覆盖本地 `terms` window。
   - 只追加本地缺失的 command suffix，避免重复追加已存在 command。
   - 返回最后匹配的 `LogId`。

### 处理 AppendReply

leader 收到 `AppendReply`：

- 如果 `reply.term > leader.term`，清空 `leader`，退回 follower。
- 如果 `reply.conflict = Some(index)`，设置该 peer 的 `end = index`。
- 如果 `reply.matched = Some(log_id)`，设置该 peer 的 `matched = log_id.index`，并把 `end` 至少推进到 `matched + 1`，然后尝试推进 commit。

每个事件处理结束后，Core 会再次调用 `try_initialize_replication()`，因此冲突或匹配回复都会驱动下一轮复制。

## Commit

leader 根据所有 replication state 的 `matched` 计算节点级 commit index。replication state 包括 leader 自身：

1. 只考虑 `matched >= leader.term` 的 peer，避免直接提交旧 term 日志。
2. 从小到大扫描 matched index 集合。
3. 若某个 index 被 quorum 覆盖，则把它作为新的 committed candidate。
4. 若 candidate 大于当前 `Core.committed`，更新 committed。

`committed` 是 `Core` 的节点级状态，不属于 `LeaderState`。

## Write

`Raf::write()` 把 application write 发送到 Core。

当前规则：

- 非 established leader 返回 `not a leader`。
- established leader 进入 `dispatch_leader_write()`。
- `dispatch_leader_write()` 尚未实现真实复制，目前返回 `leader-side write replication not yet implemented`。

## 当前已知问题

- fresh node 空日志路径会 panic；`terms.last()`、`read_one(cmds.len() - 1)` 等需要 empty case。
- `TermArray::fill_gap()` 当前实现会继续 append，而不是填补 `[since, len)`；需要重新定义并修复。
- `AppendRequest` 没验证 `terms.len() == cmds.len()`，空窗口会导致 `unwrap()` panic。
- durable storage、snapshot、membership change 都未实现。
