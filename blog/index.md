# Raf: 不存 Term 的 Raft：把 Log Index 变成逻辑时间

> 摘要：`raf` 是一个实验性的 Raft 变体。它不把 `currentTerm` 作为独立字段持久化，而是让候选人在发起选举时占用一个 log index，并把这个 index 作为 leader term。这样做不会取消 Raft 的逻辑时间模型，而是改变 term 在存储中的来源。

> 声明：这篇文章的想法来自 drdr.xp at gmail.com，代码为作者古法编程实现, 文章由 Codex 起草并改进。

## 导读：这个实验在问什么

这个仓库是一个实验项目，用来探索 Raft 协议是否可以在保持核心安全语义的前提下进一步简化。项目名 `raf` 表示 `Raft without [T]erm`：这里不是说完全删除 term，而是不把 `currentTerm` 作为独立字段持久化。它更像一个小型研究笔记，用代码验证一个问题：如果不单独持久化 `currentTerm`，而是把 leader term 绑定到 log index，Raft 的 election、replication 和 commit 是否还能自然地表达。

这篇文章解释 `raf` 的核心想法：term 仍然存在，日志仍然用 `(term, index)` 比较新旧，但 term 不再来自独立递增的持久化计数器，而是来自一次选举占用的 log index。这个实现的目标不是证明自己和标准 Raft 在所有工程行为上完全等价，而是观察这种存储表达是否仍能保留 Raft 最重要的安全直觉。后文会依次介绍存储模型、选举、复制、commit，以及仓库里的三节点示例。

本文假设读者已经了解 Raft 的基本流程，包括 leader election、AppendEntries、quorum commit 和 `(term, index)` 形式的 log id。

## 为什么 term 不能消失

在共识算法里，log 表示已经发生或将要发生的事件，term 表示这些事件所属的逻辑时间。只有 log index 是不够的，因为 index 只是某个节点本地历史的长度；在分布式系统里，一个节点看不到的更大 index 可能已经存在于其它副本上。

标准 Raft 用 term 来解决这个问题：

- leader election 先推进 term，再选择 leader。
- 日志比较先比较 term，再比较 index。
- leader 只能在自己的 term 内安全地推进 commit。

这个 term 与 Paxos 里的 ballot number 扮演的是类似角色。它让系统可以在不知道所有节点完整日志的情况下，仍然判断哪个历史更新、更有资格成为 leader。

**Log index 表示本地事件位置；term 表示跨节点比较历史时使用的逻辑时间**

`raf` 保留这个概念，但改变它的存储方式。

## 核心想法

标准 Raft 通常会持久化类似下面的状态：

```rust
struct StandardRaftStorage {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
}

struct LogEntry {
    term: Term,
    cmd: Cmd,
}
```

`raf` 把持久化状态压成两段按 log index 对齐的 `Vec`：

```rust
struct RafStorage {
    terms: Vec<Term>,
    cmds: Vec<Cmd>,
}
```

其中：

- `terms[i]` 是 index `i` 这条日志的 leader term。
- `cmds[i]` 是 index `i` 这条日志的应用命令。
- `log_id(i)` 仍然是 `(terms[i], i)`。

下面是一段可能出现的存储状态。`_` 表示 empty command；`cmds` 只到 index `7`，所以 index `8` 和 `9` 还不是完整日志项。

```text
Storage layout:

terms vector:  0  1  2  2  4  5  6  6  8  9
cmds  vector:  _  _  _  C3 _  _  _  C7
----------------------------------------------> index
               0  1  2  3  4  5  6  7  8  9
```

逐个 index 看，这段状态表示：

- index `0`：固定存在的默认项。`terms[0] = 0`，`cmds[0]` 是 empty command。
- index `1`：term `1` 对应的一次成功 election。这个 leader 当选时占用 index `1`，并写入第一条 empty command。
- index `2`：term `2` 对应的一次成功 election。新的 leader 占用 index `2`，第一条日志同样是 empty command。
- index `3`：term `2` 的 leader 写入的一条业务日志 `C3`，所以 `terms[3] = 2`，`cmds[3] = C3`。
- index `4`：term `4` 对应的一次 election attempt，但没有形成 established leader。后来 term `6` 的 leader 建立时，为了让 `cmds` 追上 `terms`，这里被补成 empty command。
- index `5`：term `5` 对应的一次 election attempt，同样没有形成 established leader；后来也被补成 empty command。
- index `6`：term `6` 对应的一次成功 election。这个 leader 当选时占用 index `6`，并写入第一条 empty command。
- index `7`：term `6` 的 leader 写入的一条业务日志 `C7`，所以 `terms[7] = 6`，`cmds[7] = C7`。
- index `8`：term `8` 对应的一次新的 election attempt。当前只看到 `terms[8] = 8`，还没有对应的 command，因此它还不是完整日志项。
- index `9`：term `9` 对应的另一次 election attempt。和 index `8` 一样，当前只有 term 记录，还没有对应的 command，是否最终形成 leader 还不能从这段状态判断。


_标准 Raft 持久化独立的 `current_term`；`raf` 把每个 index 的 term 和 command 拆成两段对齐的 `Vec`。_

这个结构并不是说日志项没有 term。相反，每个 log index 仍然可以找到对应的 leader term。它去掉的是单独的 `currentTerm` 存储项，并让一次选举占用的 index 同时承担 term 的身份。

## 存储模型

index `0` 是固定存在的默认项：

```text
terms[0] = 0
cmds[0]  = empty
```

当两个 `Vec` 在同一个 index 上都有值时，该 index 对应一条完整日志：

```text
log[index] = (terms[index], cmds[index])
log_id     = (terms[index], index)
```

_`terms` 和 `cmds` 共享同一个 log index；选举期间，`terms` 可以短暂领先于 `cmds`。_

在选举期间，`terms` 可以短暂比 `cmds` 长。原因是候选人会先占用一个 index 作为 term；只有当它成为 established leader 后，才会用一条 empty command 把 `cmds` 补齐。

因此这个实现需要维持两个基本事实：

- `cmds` 不应比 `terms` 长。
- 对所有 index `i`，都应满足 `i >= terms[i]`。

第二个不变量来自这个设计本身：term 是某次选举占用的 log index，所以一个日志项记录的 term 不能大于它自己的 index。

## 为什么拆成 `terms` 和 `cmds`

这个实现把 leader term 和 application command 分成两个 `Vec`，不是为了改变 Raft 的语义，而是为了让存储层有更清晰的优化空间。

例如，一个 leader 任期内可能连续写入大量日志，这些日志的 term 都相同。存储实现可以把连续相同的 term 压缩成更紧凑的表示，而 commands 则按应用需要存储。两者分开之后，term 的压缩、command 的持久化、payload 的编码都可以独立演进。

这也是这个项目的实验价值：它尝试把 Raft 中“逻辑时间”和“日志位置”的关系表达得更紧，同时观察这种表达能否简化持久化状态。

## 发起选举

候选人发起 election 时，不是读取并递增独立的 `currentTerm`，而是取 `terms` 的下一个 index 作为新 term：

```rust
let term = terms.len();
terms.push(term);
```

这一步有两个含义：

- 候选人声明自己要使用 `term` 作为 leader term。
- 本地持久化已经记录：这个 index 被一次选举占用。

随后候选人发送 `RequestVote`。请求中仍然携带标准 Raft 需要的两个关键信息：

- `term`：候选人要使用的 leader term。
- `last_log_id`：候选人最后一条完整日志的 `(term, index)`。

`last_log_id` 来自 `cmds` 的最后一个 index：

```rust
let last_log_index = cmds.len() - 1;
let last_log_id = (terms[last_log_index], last_log_index);
```

这里和标准 Raft 的含义一致：voter 用它判断候选人的日志是否至少和自己一样新。

## 处理投票请求

收到 `RequestVote` 后，voter 需要判断两件事：

1. 候选人请求用作 term 的 log index 是否尚未在本地存在。
2. 候选人的 log 是否不是 stale。

可以把核心判断理解成下面的伪代码：

```rust
let local_last_log_index = cmds.len() - 1;
let local_last_log_id = (terms[local_last_log_index], local_last_log_index);

let can_vote =
    req.term >= terms.len()
        && req.last_log_id >= local_last_log_id;
```

如果请求合法，voter 会把这个 term 记录进本地 `terms`。如果本地 `terms` 比 `req.term` 短，就用默认 index 补齐，直到本地已经包含 index `req.term`：

```rust
if can_vote {
    while terms.len() <= req.term {
        let index = terms.len();
        terms.push(index);
    }
}
```

这些默认项的值等于自己的 index，用来保持 `i >= terms[i]`。它们表示本地已经观察到对应 term index，并不表示这些 index 都已经有完整日志项，因为对应的 `cmds` 可能还不存在。循环最后一次写入时，`index == req.term`，因此这个 voter 已经观察并接受了该 term；后续它不会再接受旧 term 或已经存在于本地 `terms` 中的 term。

这部分替代了标准 Raft 中持久化 `currentTerm` 的角色，但它不完全等价于标准 Raft 的 `votedFor`。当前实现没有持久化“这个 term 投给了谁”，因此 RequestVote 重试和节点重启后的行为会更保守；后面的“当前边界”会单独说明这个取舍。

![Three node election flow](assets/election-flow.svg)

_候选人选择 `terms.len()` 作为 term；其它节点在本地 `terms` 中记录这个 term 并授予投票。_

当候选人收到 quorum 的 granted reply 后，它成为 established leader。此时它会追加一条 empty command，使 `cmds.len()` 追上 `terms.len()`。这条日志对应 leader 当选时占用的 index。

## 建立 leader 状态

当 candidate 变成 established leader 后，它需要在内存里保存这次 leadership 的核心状态。可以把它理解成下面的结构：

```rust
struct LeaderState {
    term: Term,
    granted_nodes: Vec<NodeId>,
    replications: BTreeMap<NodeId, ReplicationState>,
}

struct ReplicationState {
    matched: LogIndex,
    end: LogIndex,
    inflight: bool,
}
```

这里最重要的是三类信息：

- `term`：这个 leader 当选时占用的 log index。后续由它产生的日志都会写入这个 term。
- `granted_nodes`：已经授予这次 leadership 的节点 ID。它证明这个 leader 是由 quorum 选出来的。
- `replications`：leader 视角下每个节点的复制进度。`matched` 表示该节点已知匹配到的最大 index；`end` 是继续探测或复制时使用的上界；`inflight` 用来避免对同一个节点同时发送多个 Append 请求。

leader 自己也会有一份 replication state。这样计算 commit 时可以统一处理：只需要看哪些节点的 `matched` 覆盖了某个 index，并判断这些节点是否组成 quorum。

![Leader state](assets/leader-state.svg)

_Established leader 保存本次 leadership 的 term、已授予节点集合，以及每个节点的 replication progress。_

## 写入日志

成为 leader 后，新的 application write 会追加一条日志。term 不再变化，仍然使用 leader 当选时选择的 term：

```rust
terms.push(leader.term);
cmds.push(user_cmd);
```

所以在一个 leader 任期内，后续日志项的 `terms[i]` 都相同。这与标准 Raft 的行为一致，只是 term 的来源不同。

## 复制日志

leader 向其它节点发送 Append 请求。逻辑上，请求携带一段从 `start` 开始的连续日志：

```rust
struct Append {
    term: Term,
    start: LogIndex,
    terms: Vec<Term>,
    cmds: Vec<Cmd>,
}
```

这里 `terms` 和 `cmds` 必须等长，并且都从 `start` 这个 log index 开始。`start` 是本次请求的探测点，也是请求中第一条日志的 index：follower 会先检查这个 index 是否能和本地日志匹配；如果能匹配，就继续接受后面的连续日志。

标准 Raft 的 AppendEntries 会单独携带 `prevLogIndex` 和 `prevLogTerm`。`raf` 的实现没有把这个匹配点单独拆出来，而是让请求中的第一条日志同时承担匹配检查的角色：follower 从 `start` 开始逐个比较本地 `terms` 和请求里的 `terms`。

处理逻辑可以概括为：

1. 如果请求 term 比本地最后观察到的 term 更旧，拒绝。
2. 如果第一条日志就不匹配，返回 conflict index。
3. 如果存在匹配前缀，保留匹配部分。
4. 如果本地后续 commands 与 leader 分歧，截断本地 commands。
5. 覆盖本地 `terms` 中本次请求对应的范围。
6. 只追加本地缺失的 commands。

![Replication conflict repair](assets/replication-conflict.svg)

_Append 找到共同前缀，截断 follower 的冲突后缀，再复制 leader 缺失的日志。_

这个流程仍然是 Raft 的核心复制模型：leader 找到双方共同的日志前缀，然后用自己的后缀覆盖 follower 的分歧历史。

## 推进 commit

复制到 quorum 不等于立刻提交所有历史。标准 Raft 有一个重要规则：leader 只能直接提交自己当前 term 内的日志；旧 term 的日志需要被当前 term 的日志间接带上。

`raf` 保留这个规则。因为当前 leader 的日志从它当选时占用的 index 开始，所以 leader 在计算 commit 时只考虑已经进入当前 leader 历史范围的 matched index。

直观地说：

```rust
if quorum_has_matched(index) && index >= leader.term {
    commit(index);
}
```

这样做的目的和标准 Raft 一样：一旦某个 index 被提交，后续任何合法 leader 都必须包含它，不能再覆盖它。

![Quorum commit rule](assets/quorum-commit.svg)

_leader 只直接提交 quorum 覆盖且位于当前 leader term 范围内的 index。_

## 示例

仓库里有一个三节点进程内示例，用来演示本文描述的基本流程：创建三个 `Raf` 节点，通过 `InProcessNetwork` 连接它们，显式触发 node 1 的 election，然后通过 leader 写入几条日志，并通过 metrics 观察 role、term、commit index 和 replication progress 的变化。

示例源码在这里：

<https://github.com/drmingdrmer/raf/blob/main/examples/three_node.rs>

可以在仓库根目录运行：

```sh
cargo run --example three_node
```

这个示例不是生产部署模板，而是用于观察核心协议状态变化的最小演示。它把日志输出到 stderr，并且没有 election timer、heartbeat、snapshot 或 membership change。

## 当前边界

当前实现刻意保持很小，不包含一些完整生产系统通常需要的能力：

- automatic election trigger。
- snapshot 和 log compaction。
- membership change。
- heartbeat。
- RequestVote retry logic。
- application payload 的持久化语义。

Automatic election trigger 指的是标准 Raft 里的 election timer：节点周期性检查自己是否太久没有看到合法 leader，如果超时就发起新 election。这个机制可以由外部 timer 调用 `Raf::elect()` 实现，不需要放进 `raf` 的核心状态机，所以当前实现没有内置它。

RequestVote retry 有一个更细的边界：如果目标节点已经成功处理了 `RequestVote`，但 reply 在网络里丢失，candidate 重试同一个请求时，目标节点本地已经在 `terms[req.term]` 记录过这个 term。按当前规则，它会拒绝这个重试请求，因为这个 term index 已经存在。

一个可选修补方式是在内存中增加 `voted_for`，记录某个 term 属于哪个 candidate。这样同一个 candidate 对同一个 term 的重试可以被识别并再次返回 granted。这个字段不一定要持久化：如果节点重启后丢失了 `voted_for`，它可以保守地拒绝所有使用本地已存在 term 的 `RequestVote`。这会带来一个小的可用性问题，但只发生在节点重启之后；它不会改变已经持久化的日志和 term 关系。

![RequestVote retry after lost reply](assets/request-vote-retry.svg)

_如果 RequestVote reply 丢失，重试会遇到已存在的 term；可选的 in-memory `voted_for` 可以改善这个可用性问题。_

这些能力都可以在这个核心模型之外继续加入。本文关注的是最核心的问题：如果 term 来自 log index，Raft 的 election、replication 和 commit 是否仍然能用熟悉的方式表达。

## 总结

`raf` 并不是一个“没有 term 的 Raft”。它仍然有 term，也仍然用 `(term, index)` 比较日志新旧。它真正去掉的是独立持久化的 `currentTerm`，并把 leader term 绑定到一次选举占用的 log index。

这个变化让存储状态变成两段按 index 对齐的 `Vec`：

```rust
struct RafStorage {
    terms: Vec<Term>,
    cmds: Vec<Cmd>,
}
```

选举时，候选人用 `terms.len()` 选择 term；成为 leader 后，后续日志都写入这个 term；复制和提交仍然沿用 Raft 的基本规则。

这就是这个实验实现的核心：不改变 Raft 的逻辑时间模型，只改变这个逻辑时间在持久化状态中的来源。
