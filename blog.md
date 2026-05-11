我们介绍 Raft 这个东西，它是一个 Raft 的实现，但是不在存储里面持久化 term。

并不是说完全没有 term，而是说它不需要专门的存储：它利用 log 的一个 index
作为它的 term，用 log 里面一个 index 的位置作为一个 term。这样的话，就可以去掉对
log 中 term 的存储。


整体来说，它跟 Raft 并没有本质的区别。

在一个分布式共识算法中，必须有 term
或者类似的东西，它表示分布式系统里的虚拟时间；而 log
则表示分布式共识中发生的事件。只有具备了“时间”和“事件”，才能构成一个正确的分布式共识。

时间的作用是表示事件的顺序。Log index
本身不足以表达时间上的顺序，尤其是在分布式环境或多副本环境下。为了达成一致，仅靠
index 是不够的，因为一个 log index
的最大值可能在某些节点上不可见。如果看不到当前最大的
index，你又不能覆盖已经提交的内容，就必须引入一个在不知道具体 index
时，能够决定更大历史、更大时间维度的概念。

也就是说，这里的 term 对应到 Paxos 算法中，其实就是 Ballot Number 的概念。


而在我们这个实现里面，只是做了一个简化，把 term 的概念放到了 log index 里面。

对于这个 Raft 的实现来说，我们需要持久化的东西主要有以下几个：

1. 独立的存储项
() term
() votedFor（也就是说当前这个节点认为谁是 leader）

2. log 序列
 log 序列包含两部分：一个是 index（即 log
 所在的位置）；另一个是这条日志对应的 term 及其内容（也就是 payload）。

 term 表示的是逻辑时间。在 log
 比较谁先谁后时，它是一个优先比较的概念，所以它代表了时间。

在本实现里，它的存储结构是下面这个样子，只包含两部分：

1. 一个 term 的数组
   也就是说，对应 index 的日志所属的 term 是谁，这个是没有办法省略的。

2. payload 的数组
   也就是对应 index 里面的日志内容是什么。


然后它的执行过程也跟标准的 Raft 很类似：
1. 先发起选举，确认一个 leader
2. 然后再由 leader 去 append log

之所以是这样，因为在分布式系统里面，要保证一致或者达成共识的一个原则，就是只有一个 process 来决定它写入的日志是什么。所以说在任何分布式系统里面，都必须有一个独立的 leader。


然后在执行过程中，我们就先把 term 和 payload（我们在项目里面叫做 cmd）这两个 array 整理出来，这是存储里面核心的数据结构。

```
Straoge:
	terms: Vec<u64>,
	cmds:  Vec<Cmd>,
```

注意这里，terms 和 commands 这两个数组的长度可能是不一样的。

terms 的长度通常要长于 commands，也就是说可能先有了 term，但是还没有写入任何日志。这也对应了分布式共识（Raft）里面的一个概念：我们先确定一个时间（即 term），然后再针对这个时间点写出具体的事件。

所以说：
1. terms 可能会比 commands 长
2. commands 绝不会比 terms 长

标准的 Raft 协议实现中通常不会有这个差异，因为它会单独存储 terms。


在这里我们可以说，我们把标准 Raft 里面的 log array 的长度做了一个简单的扩展，用它的选举时的长度作为一个 term。
简单来说，对于这两个数字，在发起选举的时候：

1. Terms 数字的长度是当前 Leader 所用的 Term
2. Commands 的长度是当前的最大历史

或者也可以理解为：Terms 的长度是当前的时间，而 Commands 的长度是当前的历史。



我们之所以采用 Terms 和 Commands 分开存储的结构，而没有使用一个数组元素同时包含 Term 和 Command 的方式，是因为在存储优化中，我们实际上可以对 Terms 进行针对性的优化。

例如，对于连续的一串 Terms，我们只需要存储一个起始位置即可。这样可以很大程度上为应用提供优化的空间。像有些 Adapter，为了获得更好的数据连续性，也会把 Terms 和 Commands 分成两个不同的数组，这正是为了在优化上提供更多的可能性。


然后这个实现里面不包含 Snapshot 和 Membership Change 这两个标准 Raft 里面提供的支持。

要加入它们也很简单，但是由于它们跟我们的核心实现没有关系，所以就暂时把它去掉了。


首先，我假设读者们已经对 Raft 比较了解，然后再来看这篇文章。如果不了解的话，可以参考以下文章，去了解 Raft 的运行机制。

这里下面给出几个链接（TODO）：
1. [链接待补充]

我现在就开始介绍我们这个算法的执行流程，并着重指出它跟标准 Raft 的差别。



首先就是发起选举。

发起选举的时候，我们要决定一个 term：
1. 在标准 Raft 里面，是把 term 这个单独的属性值加 1。
2. 在我们的这个实现里面，我们取 term 数组的下一个空的位置，它的 index 作为当前的 term。

```
let new_term = terms.len();
terms.push(new_term);
```

这就是我们 Leader 初始化一个选举的过程：他启用了一个新的 term，并把它加入到自己的 term 数组里面.


然后我们发起选举，选举的过程也跟标准 Raft 非常类似。它带有当前的 term（也就是上面我们决定的 term 数组中最后的元素），并带上 last log id。这与标准的 Raft 协议没有区别。

last log id 的定义如下：
它是 commands 数组中最后一个元素对应的 term 及其所在的 index。

```
let last_log_index = cmds.len()-1;

RequestVote:
	node_id: self.id,
	term: new_term,
	last_log_id: (terms[last_log_index], last_log_index),
```

可以看到 RequestVote 和标准 Raft 没有任何区别, 收到 RequestVote 请求的时候，一个节点也需要类似标准的 Raft 一样去处理这个请求。也就是说，它需要判断当前 Candidate 是否是最新的，并确认其 Leader 身份。

具体处理逻辑如下：
1. 比对 term 是否是最大的：确定它拥有最大的虚拟时间。
2. 比对 last_log_id 是否不小于自己的：确定它拥有最完整的历史记录。

这样一来，新的 Leader 在 propose 新的 log 时，就不会删除任何已经 committed 的事件。

而确定收到的 RequestVote 请求是否具有最大 Term，是用如下的方法去做：看本地的 terms array 里面对应的 term 的位置是否为空，若为空，就说明还没有收到这么大的 term，那么这个 term 就是合法的。

然后比对 last_log_id，也是类似的方法，跟标准的 Raft 是类似的，没有区别。

```
handle_request_vote(req: RequestVote):
	let last_log_index = self.cmds.len()-1;
	let last_log_id = (self.terms[last_log_index], last_log_index);

	if req.term >= self.terms.len() && req.last_log_id >= last_log_id:
		// Valid request.
```

如果申请合法的话，那么就把 Terms 写入到本地。

注意写入的时候，因为本地的 Terms Array 可能还有一段空洞，这时候要填充，填充的方法是填入和 Index 一样的: 

```
while self.terms.len() <= req.term {
	self.terms.push(self.terms.len());
}
```

这样就已经记录并确认了这个 leadership。

后续该节点就不会再接受当前 term 中其他节点的 request vote 请求了，也就是说，该节点已经完成了 request vote 的处理，承认了现在的 leadership。


然后完成 candidate 节点选举的标准也是一样的：当它收到多数（一个 quorum）返回的应答，就承认该节点当选。

这说明它现在在整个集群中，至少半数的节点有同样的 term，也就是同样长的 terms 数组。同时，它作为当前的 candidate，也有最大的 leader 和最大的 commands 数组。

也就是说，它拥有了最大的时间和最大的实践历史，可以开始进行 propose log, 也就是说，执行真正的写入


完成选举之后，当前的 TermsArray 和 CommandsArray 之间长度可能会有一个差异。

这时候就把它们都填成一个 No-Op 空的 Command 作为占位，新的日志都从下一个位置开始发起: `terms.len()`

写入的过程也很简单。当 Leader 接收到一个写请求的时候，它向 terms 数组里面追加一个元素，元素的值就是这个 leader 选举时使用的那个 term。也就是说，在后续一段时间内，只要这个 leader 不发生改变，那么 terms 数组里面被填充的值都是这个 leader 的 term，这跟标准 Raft 也没有区别。

然后是往 commands array 里面 push 一个 command，这是用户自定义的请求，即具体要做的事情。

```
self.terms.push(self.leader.term);
self.cmds.push(user_cmd);
```


然后 Leader 发起一个 replication 的请求发给所有的节点，然后等待其他人确认。

这个 replication, 它和标准的 Raft 完全一样，里面带有 leader_term 标记，用于验证 leader 的合法性。

后面包含一个 term 数组和一个 commands 数组，这里的 term 数组和 commands 数组长度是一样的。

```
Append:
	term: self.leader.term,
	since: u64, // the index of the first item of `terms` and `cmds`
	terms: Vec<u64>,
	cmds: Vec<Cmd>,
```

注意这里，我们为了逻辑上的简化，没有存、没有发那个 previous logID。我们认为 terms 数组里面第一个元素是在 Leader 和 Follower 上已经确定匹配的，也就是标准 Raft 里的 Previous LogID。

当第一个匹配成功后，就开始 append 后一个；后一个 append 成功之后，再继续 append 下一个，直至处理完成。

对于请求合法性的校验，跟标准 Raft 也是一样。如果 Term 匹配（即 Term 跟当前最大 Term 一致）的话，那么就说明当前的请求是合法的。


```
handle_append_request(append):
	let last_term = self.terms[self.terms.len()-1];
	if append.term == last_term {
		// valid
	} else {
		return error;
	}

	// push append.terms and append.cmds into local storage.
```

这样就完成了一个 append 的处理。

当集群中一个 quorum（或者说超半数的节点）已经返回应答，那么 leader 就可以认为某一段数据、某一段 log 已经安全地存储在多数派里面了。

这里和标准 Raft 一样，存储到多数派里面不代表提交。

因为提交的概念是说：下一个新的 Leader（有更大 term 的 Leader）必须能够选择这些已经写入的数据。而 Leader 发起选举的时候，它是选择最大 last log ID 的。

所以只有当 last log ID 复制到多数派，达到当前可能的最大值（也就是说在当前已经存在的 Leader 里面能成为最大值）的时候，它才是一个 committed 的状态。

因为 last log ID 是根据一个 term 和 log index 的 tuple 去比较的，所以这里就是 last log ID 的 term 必须是当前 leader term 的时候，才能认为是提交的。

```
if quorum_accepted_last_log_id.term == self.leader.term {
	// commit(quorum_accepted_last_log_id)
}
```

到此为止，就是我们简化过的 without term wrapped 的所有执行流程。

可以看到，它是用一个 log record 的 index 作为一个 term。


