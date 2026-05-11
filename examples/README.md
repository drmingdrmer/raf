# raf examples

这个目录包含 `raf` 的可运行示例。当前只有一个三节点进程内集群示例：

- `three_node.rs`：创建三个 `Raf` 节点，用 `InProcessNetwork` 连接它们，显式触发 node 1 的 election，通过 leader 提交写入，并展示 follower 拒绝写入请求的行为。

## 运行

在仓库根目录执行：

```sh
cargo run --example three_node
```

示例会把日志输出到 stderr。输出中可以观察：

- node 1 从 follower/candidate 变成 leader。
- leader term 来自 election 时占用的 log index。
- leader write 被复制并推进 committed index。
- metrics 中的 `matched`、`end` 和 `inflight` 展示每个节点的 replication progress。
- 发给 follower 的 write 会被拒绝。

## 代码位置

示例源码：

<https://github.com/drmingdrmer/raf/blob/main/examples/three_node.rs>

## 边界

这个示例用于理解核心协议流程，不是生产部署模板。它刻意保持简单：

- 使用进程内网络，不打开真实 TCP 连接。
- 使用 `MemStorage`，不写入磁盘。
- 由应用显式调用 `elect()`，没有 election timer。
- 当前示例不覆盖 heartbeat、snapshot 和 membership change。
