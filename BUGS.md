# 已知问题

这个文件记录当前 review 中发现的可疑 bug。优先级按对协议正确性的影响排序。

## 已修复

- P0：同一个 voter 可以在同一个 term 给多个 candidate 投票。当前修复通过拒绝 `req.term < local_next_term_slot` 的 `RequestVote`，避免已经存在的 term slot 被再次授予。

- P0：单节点集群永远不会成为 leader。当前修复在 election 初始化 self-vote 后立即检查 quorum，单节点可以直接成为 established leader。

- P1：收到合法 `Append` 时没有清空本地 candidate/leader 状态。当前修复在 `append.term >= local_last_term` 时清空本地 leader state，让节点退回 follower。

- P1：`AppendRequest` 没有校验窗口合法性。当前修复要求 `terms.len() == cmds.len()`；如果两者都为空，返回 `matched = None` 且 `conflict = None` 的空回复。

- P1：replication 的 `end` 在成功匹配后不更新。当前修复在收到 matched reply 后把 `end` 至少推进到 `matched.index + 1`，避免 `matched >= end` 后下一轮 probe 退回旧窗口。

## 跳过

- P1：candidate 收到任意 rejected vote 就直接 step down。当前实现有意保持严格行为；任意拒票都会让 candidate 退回 follower。

## 待修复

- P2：follower 的 committed index 不会随 `Append` 推进。`AppendRequest` 还没有携带 leader commit index，因此 follower metrics 中的 `committed` 可能长期停在 0。

- P2：`StorageExt` 对自定义空 storage 不安全。`last_term()` 在 `terms_len() == 0` 时会下溢，`read_one_term()` 会直接索引空结果。
