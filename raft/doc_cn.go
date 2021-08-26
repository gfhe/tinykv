// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
raft 包负责 发送和接收 eraftpb.protoc 中定义的消息(Protocol Buffer 格式)。

Raft 是一个协议， 协议集群中的所优节点可以维护一个多副本状态机。状态机通过副本日志保持同步。
更多Raft协议的信息，参考Diego Ongaro 和 John Ousterhout的"In Search of an Understandable Consensus Algorithm"
(https://ramcloud.stanford.edu/raft.pdf)


Usage使用

Node是raft中的基本对象。要么使用raft.StartNode 从头启动一个Node，要么使用raft.RestartNode从一个初始状态启动Node。

从头启动一个Node:

  storage := raft.NewMemoryStorage()
  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
  }
  n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

从起一个状态重启Node:

  storage := raft.NewMemoryStorage()

  // 从持久化的快照、状态和数据项中恢复内存中的storage
  storage.ApplySnapshot(snapshot)
  storage.SetHardState(state)
  storage.Append(entries)

  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxInflightMsgs: 256,
  }

  // 在没有对等节点信息的情况下，重启raft 节点。
  // 对等节点信息在storage中。
  n := raft.RestartNode(c)

现在持有了一个Node，需要完成一些职责：

首先，必须从 Node.Ready() channel中读取消息，处理更新操作。除了步骤2中的操作，这些操作可以被并行处理。
1. 将非空的HardState, Entries, and Snapshot 写入持久化的storage；注意：在写入索引号为i的节点时， 任何之前持久化的索引号大于i的数据，必须被丢弃。
2. 将所有消息发送给 To列表中的节点。在最新的 HardState 被持久化到磁盘之前不会发送任何消息，并且所有entry都是由先前的Ready批次写入的 (可能会在同一批次中的entry被持久化时发送消息）
	注意：编组消息不是线程安全的； 确保在编组时不会持久化任何 新 entry 非常重要，实现这一点的最简单方法是直接在raft的主循环序列化消息
3. 将快照（如果有）和 CommittedEntries 应用到状态机。 如果任何已提交的entry具有EntryType_EntryConfChange类型，调用Node.ApplyConfChange() 将entry 应用到node。
此时可以通过在调用 ApplyConfChange 之前将 NodeId 字段设置为零来取消配置更改（但必须以一种或另一种方式调用 applyConfChange，取消的执行必须完全基于状态机，而不是外部信息，例如 观察到的节点健康状况）。
4. 调用 Node.Advance()发送准备好进行下一批更新的信号。这可能在步骤1后的任何时间完成， 尽管所有更新都必须按照 Ready 返回的顺序进行处理。

其次：所有持久化的日志entry都必须通过 Storage 接口的实现来提供。 提供的 MemoryStorage 类型可用于此（如果您在重新启动时重新填充其状态），或者您可以提供您自己的磁盘支持的实现。

第三，当你接收一条来自其他Node 的消息时，传递给 Node.Step：

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，你需要定期调用 Node.Tick() （可能通过 time.Ticker）。 Raft 有两个重要的超时时间：心跳和选举超时。 但是，在 raft 包的内部，时间是由抽象的“tick”表示的。

总的状态机处理循环可能会像这样：

  for {
    select {
    case <-s.Ticker:
      n.Tick()
    case rd := <-s.Node.Ready():
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
        if entry.Type == eraftpb.EntryType_EntryConfChange {
          var cc eraftpb.ConfChange
          cc.Unmarshal(entry.Data)
          s.Node.ApplyConfChange(cc)
        }
      }
      s.Node.Advance()
    case <-s.done:
      return
    }
  }

要从您的节点提出对状态机的更改，请获取您的应用程序数据，将其序列化为字节片并调用：

	n.Propose(data)

如果proposal commited， 数据会出现在commited entries，类型为 eraftpb.EntryType_EntryNormal。
并不保证一条发起的proposal 一定会commited， 你可能得在超时后，重新提出proposal。

要增加或者单处集群中的一个节点时，构建 ConfChange结构体对象cc，调到用：

	n.ProposeConfChange(cc)

在配置变更commited后，会返回一些类型为eraftpb.EntryType_EntryConfChange的commited entries， 必须将这些commited entries 应用到node：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：
1. 一个ID表示集群中唯一的Node，即使旧节点已被移除，给定的 ID 也必须仅使用一次。 这意味着例如 使用IP 地址作为节点 ID，是一个非常糟糕的注意，因为它们可能会被重复使用。
2. 节点 ID 必须非零。


Implementation notes

这个实现更新到了最终的 Raft 论文(https://ramcloud.stanford.edu/~ongaro/thesis.pdf)，尽管我们的实现成员变更协议与第四节有一点不一样
一次发生一个成员节点变更被保留，但在我们的实现中，成员更改在其entry被应用时生效，而不是在将entry添加到日志时生效（因此entry在旧成员角色下提交而不是 新成员角色）。
这在安全性方面是等效的，因为保证新旧配置重叠。

为了确保我们不会尝试通过匹配日志位置来一次提交两个成员资格更改（这将是不安全的，因为它们应该具有不同的quorum要求），当任何未提交的更改出现在领导者Leader的日志中时，我们禁止任何成员更改proposal。

当您尝试从两个成员的集群中删除成员时，此方法会引入一个问题：如果其中一个成员在另一个成员收到 confchange 条目的提交之前死亡，则该成员不能再被删除，因为该集群无法取得进展。
因此，强烈建议在每个集群中使用三个或更多节点。

MessageType

raft 包负责 发送和接收 eraftpb.protoc 中定义的消息(Protocol Buffer 格式)。
当基于给定的eraftpb.Message 导致状态变更时，每个状态(follower, candidate, leader) 实现了对应的 step 方法（'stepFollower', 'stepCandidate', 'stepLeader'）

每个step由其 eraftpb.MessageType 决定。
请注意，每一步都通过一种通用方法“Step”进行检查，该方法是安全检查方法，检查节点term和进来的消息以防止过时的日志entries：

	'MessageType_MsgHup' 用于选举. 如果节点是 follower 或 candidate, 'raft' 结构体的 'tick' 方法作为 'tickElection'.
	如果 follower 或 candidate 在选举超时前，没有收到任何心跳信息，节点会发送 'MessageType_MsgHup'到 Step 方法，成为candidate开始新的选举。

	'MessageType_MsgBeat' 内部类型，通知leader 发送 'MessageType_MsgHeartbeat' 类型的心跳。节点如果是leader, 它的'raft'结构体中的'tick' 方法被设置为 'tickHeartbeat'，并且触发leader 定期向follower 发送 'MessageType_MsgHeartbeat'消息。

	'MessageType_MsgPropose' 提议将data append到node的 log entries. 这是个特殊的消息类型，将proposal 重定向到leader。
	因此，send 方法用它的 HardState term 覆盖 eraftpb.Message 的term，以避免将其本地term附加到“MessageType_MsgPropose”
	当'MessageType_MsgPropose' 发送到leader的'Step'方法，leader首先调用 'appendEntry' 方法，将entries append到自己的 log，
	然后调用 'bcastAppend' 方法，发送那些entries 到对等节点。
	当发送到candidate，'MessageType_MsgPropose'会被丢弃.
	当发送到follower，'MessageType_MsgPropose' 通过 send方法 保存到follower's 的mailbox(msgs)，它和发送者ID一起保存，然后通过rafthttp包转发到leader。

	'MessageType_MsgAppend' contains log entries to replicate. A leader calls bcastAppend,
	which calls sendAppend, which sends soon-to-be-replicated logs in 'MessageType_MsgAppend'
	type. When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
	back to follower, because it indicates that there is a valid leader sending
	'MessageType_MsgAppend' messages. Candidate and follower respond to this message in
	'MessageType_MsgAppendResponse' type.

	'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend'). When
	'MessageType_MsgAppend' is passed to candidate or follower's Step method, it responds by
	calling 'handleAppendEntries' method, which sends 'MessageType_MsgAppendResponse' to raft
	mailbox.

	'MessageType_MsgRequestVote' requests votes for election. When a node is a follower or
	candidate and 'MessageType_MsgHup' is passed to its Step method, then the node calls
	'campaign' method to campaign itself to become a leader. Once 'campaign'
	method is called, the node becomes candidate and sends 'MessageType_MsgRequestVote' to peers
	in cluster to request votes. When passed to the leader or candidate's Step
	method and the message's Term is lower than leader's or candidate's,
	'MessageType_MsgRequestVote' will be rejected ('MessageType_MsgRequestVoteResponse' is returned with Reject true).
	If leader or candidate receives 'MessageType_MsgRequestVote' with higher term, it will revert
	back to follower. When 'MessageType_MsgRequestVote' is passed to follower, it votes for the
	sender only when sender's last term is greater than MessageType_MsgRequestVote's term or
	sender's last term is equal to MessageType_MsgRequestVote's term but sender's last committed
	index is greater than or equal to follower's.

	'MessageType_MsgRequestVoteResponse' contains responses from voting request. When 'MessageType_MsgRequestVoteResponse' is
	passed to candidate, the candidate calculates how many votes it has won. If
	it's more than majority (quorum), it becomes leader and calls 'bcastAppend'.
	If candidate receives majority of votes of denials, it reverts back to
	follower.

	'MessageType_MsgSnapshot' requests to install a snapshot message. When a node has just
	become a leader or the leader receives 'MessageType_MsgPropose' message, it calls
	'bcastAppend' method, which then calls 'sendAppend' method to each
	follower. In 'sendAppend', if a leader fails to get term or entries,
	the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.

	'MessageType_MsgHeartbeat' sends heartbeat from leader. When 'MessageType_MsgHeartbeat' is passed
	to candidate and message's term is higher than candidate's, the candidate
	reverts back to follower and updates its committed index from the one in
	this heartbeat. And it sends the message to its mailbox. When
	'MessageType_MsgHeartbeat' is passed to follower's Step method and message's term is
	higher than follower's, the follower updates its leaderID with the ID
	from the message.

	'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'. When 'MessageType_MsgHeartbeatResponse'
	is passed to the leader's Step method, the leader knows which follower
	responded.

*/
package raft
