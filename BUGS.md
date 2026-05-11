# 已知问题

这个文件记录当前 review 中发现的可疑 bug。优先级按对协议正确性的影响排序。

## 已修复

- P0：同一个 voter 可以在同一个 term 给多个 candidate 投票。当前修复通过拒绝 `req.term < local_next_term_slot` 的 `RequestVote`，避免已经存在的 term slot 被再次授予。

## 待修复

- P0：单节点集群永远不会成为 leader。当前 self-vote 后只在收到 peer vote reply 时检查 quorum；单节点没有 peer reply，因此会停在 candidate。

- P1：candidate 收到任意 rejected vote 就直接 step down。在多节点集群里，一个 follower 拒绝不代表 candidate 不能从其它节点获得 quorum；只有观察到更高 term 或等价的更强 stale 证据时才应退回 follower。

- P1：收到合法 `Append` 时没有清空本地 candidate/leader 状态。节点可能一边接受其它 leader 的复制，一边仍保留自己的 candidate/leader 内存态。

- P1：`AppendRequest` 没有校验窗口合法性。空 `terms` 会触发 `unwrap()` panic；`terms.len() != cmds.len()` 可能导致 term array 和 command array 分叉。

- P1：replication 的 `end` 在成功匹配后不更新。出现 `matched > end` 后，后续 bisection probe 会退回旧窗口，破坏复制进度 invariant。

- P2：follower 的 committed index 不会随 `Append` 推进。`AppendRequest` 还没有携带 leader commit index，因此 follower metrics 中的 `committed` 可能长期停在 0。

- P2：`StorageExt` 对自定义空 storage 不安全。`last_term()` 在 `terms_len() == 0` 时会下溢，`read_one_term()` 会直接索引空结果。
