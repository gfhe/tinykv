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

package raft

import (
	"errors"
	"log"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	//额外增加：实际选举超时时间，各节点不同，避免多次split vote
	randomElectionTimeout int

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	//NOTE!!!! TestLeaderElectionOverwriteNewerLogs2AB 需要从storage配置中恢复过来。
	hard, conf, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	// 恢复节点状态
	raftLog := newLog(c.Storage)
	raftLog.committed = hard.Commit

	// 恢复node 数量
	if len(c.peers) == 0 {
		c.peers = conf.Nodes
	}
	prs := make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		prs[peer] = &Progress{}
	}
	rand.Seed(time.Now().UnixNano())
	// 初始时，为Follower,Term为0， 无投票记录，心跳时间固定，选举超时时间增加随机变量，初始化底层RaftLog
	return &Raft{
		id:                    c.ID,
		State:                 StateFollower,
		Term:                  hard.Term,
		Vote:                  hard.Vote,
		votes:                 make(map[uint64]bool, 0),
		Prs:                   prs,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		RaftLog:               raftLog}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//TODO: 重置 heartbeat 定时

	// 当leader 与 follower match一致时，仅同步committed信息。获取match的Term作为LogTerm
	if r.Prs[to].Match == r.Prs[r.id].Match {
		log.Printf("[%v T_%v]:only sync committed=%d to follower %d", r.id, r.Term, r.RaftLog.committed, to)
		logTerm, err := r.RaftLog.Term(r.Prs[to].Match)
		if err != nil {
			panic(err)
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: logTerm,             // 用于判断数据是否一致
			Index:   r.Prs[to].Match,     // 用于判断数据是否一致
			Commit:  r.RaftLog.committed, // 用于更新follower的committed信息
		})
		return true
	}

	logTerm, err := r.RaftLog.Term(r.Prs[to].Match)
	if err != nil {
		panic(err)
	}

	// 思路：
	// 1. 通过Prs[i].Next 获取待发送entries的起始Index
	// 2. 通过Prs[i].Match 获取当前follower 已经成功复制的最后的logEntry 信息：Index 和LogTerm
	var ents []*pb.Entry
	toAppendEntries := r.RaftLog.entriesAfter(r.Prs[to].Next)
	//log.Printf("[%v T_%d]:to %d entries after %d:%v",r.id, r.Term, to, r.Prs[to].Next, toAppendEntries)
	for _, e := range toAppendEntries {
		var ent pb.Entry = e // NOTE：重新开辟存储，否则会导致append到ents中的为多个指向相同地址的指针。
		ents = append(ents, &ent)
	}

	log.Printf("sendAppend entries to node %d :%v", to, ents)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,             // 用于判断数据是否一致
		Index:   r.Prs[to].Match,     // 用于判断数据是否一致
		Commit:  r.RaftLog.committed, // 用于更新follower的committed信息
		Entries: ents,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// log.Printf("[%v T_%v]:send heartbeat to %v", r.id, r.Term, to)
	if to != r.id {
		logTerm, err := r.RaftLog.Term(r.Prs[to].Match)
		if err != nil {
			panic(err)
		}
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term, Index: r.Prs[to].Match, LogTerm: logTerm, Commit: r.RaftLog.committed})
	}
}

// sendRequestVote sends a requestVote RPC to the given peer
func (r *Raft) sendRequestVote(to uint64) {
	if to != r.id {
		// 获取当前节点的最后一个entry信息,用于争取投票
		log.Printf("[%v T_%d]: send reequest vote to %d", r.id, r.Term, to)
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: to, Term: r.Term, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastLogTerm()})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed += 1
	r.electionElapsed += 1

	// election 超时， 开始选举
	if r.electionElapsed >= r.randomElectionTimeout && (r.State == StateFollower || r.State == StateCandidate) {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, Term: r.Term})
	}

	// heartbeat timeout 发送空的entry心跳
	if r.heartbeatElapsed >= r.heartbeatTimeout && r.State == StateLeader {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, Term: r.Term})
	}
}

// 重置节点状态
func (r *Raft) reset() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	r.Lead = None
	r.votes = make(map[uint64]bool, 0)
	r.Vote = None // 重新变为Follower时重置投票情况
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset()
	log.Printf("[%v:->F] currentTerm=%v, messageTerm=%v", r.id, r.Term, term)
	if r.Term <= term {
		r.State = StateFollower
		r.Term = term
		r.Lead = lead
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset()
	r.State = StateCandidate
	r.Term = r.Term + 1
	log.Printf("[%v:->C] currentTerm=%v", r.id, r.Term)
	// 投票给了自己
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Printf("[%v:->L] currentTerm=%v", r.id, r.Term)
	if r.State != StateLeader {
		r.reset()
		r.State = StateLeader
		r.Lead = r.id

		// 更新所有节点的progress
		for _, progress := range r.Prs {
			progress.Match = r.RaftLog.LastIndex()
			progress.Next = r.RaftLog.LastIndex() + 1
		}

		// 发送确立leader的 noop propose
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgPropose,
			From:    r.id,
			To:      r.id,
			Entries: []*pb.Entry{{Term: r.Term}},
		})
	}
}

// makeProgress advanced current progress
func (r *Raft) makeProgress(p, index uint64) {
	r.Prs[p].Match = index
	r.Prs[p].Next = r.Prs[p].Match + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 如果term 不一致，则自动变为follower， leader 是谁等着心跳
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		// 仅被动处理日志， 超时后，转为candidate
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()

			// NOTE: 如果peer 节点仅有一个直接作为leader
			if len(r.Prs) <= 1 {
				r.becomeLeader() // 因为此处仅一个节点，不需要发送heartbeat 确立地位
			} else {
				// 成为candidate 需要立即发送VoteRequest RPC
				for peer := range r.Prs {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgPropose:
			// follower 收到propose 消息自动转发给leader
			r.msgs = append(r.msgs, pb.Message{MsgType: m.MsgType, To: r.Lead, Term: m.Term})
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			// candidate 竞选leader时，会发送此消息，
			// leader、candidate 收到此消息，且消息term比自身的大，则转为follower，然后判断投票
			// follower 则会判断后投票，或者拒绝
			r.handleRequestVote(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()

			// NOTE: 如果peer 节点仅有一个直接作为leader
			if len(r.Prs) <= 1 {
				r.becomeLeader() // 因为此处仅一个节点，不需选举，直接成为leader
			} else {
				// 成为candidate 需要立即发送VoteRequest RPC
				for peer := range r.Prs {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			// candidate 收到了append 消息，表明leader 已选出来，不是自己，转为follower 并处理消息
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			// candidate 竞选leader时，会发送此消息，
			// leader、candidate 收到此消息，且消息term比自身的大，则转为follower, 否则，直接拒绝
			// follower 则会判断后投票，或者拒绝
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
		case pb.MessageType_MsgRequestVoteResponse:
			log.Printf("[%v T_%d]: request vote response from %d: %v", r.id, r.Term, m.From, m)
			// 选举投票结果
			r.votes[m.From] = !m.Reject
			log.Printf("[%v_T%v]:votes: %v", r.id, r.Term, r.votes)
			// 如果大多数赞同，则leader；大多数反对，则follower
			count := make(map[bool]int, 3)
			for _, v := range r.votes {
				count[v] += 1
			}
			log.Printf("vote result: %v, peer length=%v", count, len(r.Prs))
			if count[true] > len(r.Prs)/2 {
				r.becomeLeader()
			} else if count[false] > len(r.Prs)/2 {
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgHeartbeat:
			// 来自leader的心跳，
			// 如果candidate 收到，且消息term大于自身，则转为follower；
			// 如果follower收到，且消息term大于自身， 则更新leaderId
			r.becomeFollower(m.Term, m.From)
		}
	case StateLeader:
		// 仅被动处理日志， 超时后，转为candidate
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			//只有leader可以处理该消息
			// leader 收到该消息，向follower 发送心跳
			log.Printf("[%v T_%v]: receive msg beat", r.id, r.Term)
			for peer := range r.Prs {
				r.sendHeartbeat(peer)
			}
		case pb.MessageType_MsgPropose:
			// follower 收到propose 消息自动转发给leader, leader 处理propose
			for _, e := range m.Entries {
				e.Term = r.Term
				e.Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
			//更新自己的Progress
			r.makeProgress(r.id, r.RaftLog.LastIndex())

			// NOTE: **仅有一个节点，此时直接commit，无需发送append到follower**
			if len(r.Prs) == 1 {
				r.RaftLog.committed += 1
			} else {
				// 同步到follower
				for peer := range r.Prs {
					if peer == r.id {
						continue
					}
					r.sendAppend(peer)
				}
			}
		case pb.MessageType_MsgAppendResponse:
			// 只有leader 对此消息处理，存在一下集中情况：
			// 1. reject=true
			//		1.1. leader发送的msg_append消息过小  -> follower RaftLog.lastIndex 不变  -> 不影响leader，**无需特殊处理**
			//		1.2. follower 发生截断 -> 发生log entry 冲突， follower RaftLog.lastIndex变化 -> 更新Prs，重试发送
			// 2. reject=false
			//		2.1. follower RaftLog.lastIndex 增加  -> 更新Prs， 更新leader committed，
			//      2.2. follower RaftLog.lastIndex 不变  ->  不影响leader，无需特殊处理
			log.Printf("[%v T_%v]: node %v appended msg, leader.commited=%d, index=%d, reject=%v, m=%v", r.id, r.Term, m.From, r.RaftLog.committed, m.Index, m.Reject, m)
			// follower RaftLog.lastIndex 不变, 且committed信息最新， 不需要更新 committed 信息
			if m.Index == r.Prs[m.From].Match && m.Commit == r.RaftLog.committed {
				break
			}
			//更新follower的progress
			r.makeProgress(m.From, m.Index)

			if m.Reject {
				r.sendAppend(m.From) //立即重试同步
				break
			}

			mLogTerm, err := r.RaftLog.Term(m.Index)
			if err != nil {
				panic(err)
			}

			if mLogTerm == r.Term && r.RaftLog.committed < m.Index { // NOTE：仅处理leader 的当前Term的 commit信息
				// 尝试更新Leader 的 commited
				// 统计大多数节点的progress，尝试更新自己的 committed 信息
				var count int
				for _, pg := range r.Prs {
					if pg.Match >= m.Index {
						count++
					}
				}
				// 多数节点已经同步，则可以commit
				if count > len(r.Prs)/2 {

					r.RaftLog.committed = m.Index
					// NOTE: 首次committed 前进时，发送给贡献committed 的node 更新committed信息， 后续的直接反馈给单个node即可。
					for p, _ := range r.Prs {
						if p == r.id || r.Prs[p].Match < r.RaftLog.committed {
							continue
						}
						r.sendAppend(p)
					}
				} else {

				}
			} else {
				r.sendAppend(m.From) // 因为此同步信息无法帮助advance committed信息，直接反馈给当前节点即可
			}
		case pb.MessageType_MsgRequestVote:
			// candidate 竞选leader时，会发送此消息，
			// leader、candidate 收到此消息，且消息term比自身的大，则转为follower, 否则，直接拒绝
			// follower 则会判断后投票，或者拒绝
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
		case pb.MessageType_MsgRequestVoteResponse:
			if m.Term == r.Term && m.Term == m.LogTerm { // split Vote or  一个node的logTerm较大
				log.Printf("[%v, T_%d]: m.Term == m.LogTerm, msg=%v", r.id, r.Term, m)
				r.becomeFollower(m.Term, None)
			}
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeatResponse:
			// MessageType_MsgHeartbeat 的消息响应， 只有leader处理该响应。
			// Leader 收到后，可以更新commit和Prs Progress, 不会直接appendEntries
			log.Printf("[%v T_%v]: node %v appended msg, leader.commited=%d, index=%d, reject=%v, m=%v", r.id, r.Term, m.From, r.RaftLog.committed, m.Index, m.Reject, m)
			// follower RaftLog.lastIndex 不变, 且committed信息最新， 不需要更新 committed 信息
			if m.Index == r.Prs[m.From].Match && m.Commit == r.RaftLog.committed {
				break
			}
			mLogTerm, err := r.RaftLog.Term(m.Index)
			if err != nil {
				log.Printf("get term(%d) error, raftLogLast=%d", m.Index, r.RaftLog.LastIndex())
				panic(err)
			}

			//更新follower的progress
			r.makeProgress(m.From, m.Index)

			//if m.Reject {
			//	r.sendAppend(m.From) //立即重试同步
			//	break
			//}

			if mLogTerm == r.Term && r.RaftLog.committed < m.Index { // NOTE：仅处理leader 的当前Term的 commit信息
				// 尝试更新Leader 的 commited
				// 统计大多数节点的progress，尝试更新自己的 committed 信息
				var count int
				for _, pg := range r.Prs {
					if pg.Match >= m.Index {
						count++
					}
				}
				// 多数节点已经同步，则可以commit
				if count > len(r.Prs)/2 {

					r.RaftLog.committed = m.Index
					// NOTE: 首次committed 前进时，发送给贡献committed 的node 更新committed信息， 后续的直接反馈给单个node即可。
					for p, _ := range r.Prs {
						if p == r.id || r.Prs[p].Match < r.RaftLog.committed {
							continue
						}
						r.sendAppend(p)
					}
				} else {

				}
			} else {
				r.sendAppend(m.From) // 因为此同步信息无法帮助advance committed信息，直接反馈给当前节点即可
			}
		}
	}
	return nil
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	log.Printf("[%v T_%d]: r.Vote=%v, m.From=%d, m.LogTerm(%d)>r.LastLogTerm(%d): %v, m.Index(%d)>=r.LastIndex(%d)=%v",
		r.id, r.Term, r.Vote, m.From, m.LogTerm, r.RaftLog.LastLogTerm(), m.LogTerm > r.RaftLog.LastLogTerm(),
		m.Index, r.RaftLog.LastIndex(), m.Index >= r.RaftLog.LastIndex())
	if r.Vote == None || r.Vote == m.From {
		// 判断candidate 是否up to date：
		// 1. Term 相等时，看candidate的Term是否大
		// 2. Term 不等时，看candidate的Index是否大于等于当前节点
		var upToDate bool
		if m.LogTerm != r.RaftLog.LastLogTerm() {
			upToDate = m.LogTerm > r.RaftLog.LastLogTerm()
		} else {
			upToDate = m.Index >= r.RaftLog.LastIndex()
		}

		if upToDate {
			reject = false
			r.Vote = m.From
		}
	}
	//if ((r.Vote == None) || r.Vote == m.From) &&
	//	(m.LogTerm > r.RaftLog.LastLogTerm() || (m.LogTerm == r.RaftLog.LastLogTerm() && m.Index >= r.RaftLog.LastIndex())) {
	//	reject = false
	//	r.Vote = m.From
	//}
	resp := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, LogTerm: r.RaftLog.LastLogTerm(), Reject: reject}
	r.msgs = append(r.msgs, resp)
	log.Printf("[%v T_%d]: handle requeset vote, response: %+v", r.id, r.Term, resp)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// NOTE!!!!：忽略掉不可能的消息
	if m.LogTerm > m.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	log.Printf("[%v T_%d]:has entries: %+v", r.id, r.Term, r.RaftLog.entries)
	log.Printf("handle append entries: %+v", m)
	if m.Term < r.Term || // term 过小， 直接拒绝
		r.RaftLog.LastIndex() < m.Index { // leader 记录的Progress 错误，需要告诉leader更新
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), Reject: true})
		return
	}
	r.becomeFollower(m.Term, m.From) // 可能同一个Term 中，其他 candidate 竞选成功，step中的简单term对比无法覆盖

	if le := len(m.Entries); le == 0 && m.Index == 0 { // 心跳
		// 更新commit
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	} else {
		// 获取m.Index对应的Entry Term， 判断是否冲突
		lt, err := r.RaftLog.Term(m.Index)
		log.Printf("get term(%d)=%d", m.Index, lt)
		if err != nil {
			panic(err)
		}

		// raft 在m.Index 上的Entry的LogTerm为m.LogTerm，则将所有entries加入到合适的位置， **更新commited信息**
		if lt != m.LogTerm { // raft 在m.Index 上的Entry的LogTerm != leader 上的同index的Entry LogTerm
			// raft log 中包含不一致的部分，直接截断重新append
			log.Printf("[%v T_%v]: current entry term=%d != msg term=%d", r.id, r.Term, lt, m.LogTerm)
			r.RaftLog.truncate(m.Index)
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), Reject: true})
			return
		}

		// 依次校验每个entry 是否可以append
		neli := m.Index // neli = new entries last index
		for _, e := range m.Entries {
			neli = e.Index
			if e.Index <= r.RaftLog.LastIndex() {
				// 判断同Index的Term 是否一致，不一致则截断
				elt, err := r.RaftLog.Term(e.Index)
				log.Printf("raftlog term(%d)=%d, msg term(%d)=%d", e.Index, elt, e.Index, e.Term)
				if err != nil {
					panic(err)
				}
				if elt != e.Term {
					// raft log 中包含不一致的部分，直接截断重新append
					r.RaftLog.truncate(e.Index)
					r.RaftLog.entries = append(r.RaftLog.entries, *e)
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
		}
		// NOTE!!!! raftlog 的committed 只能将m.Entries 中最后一个Index（默认为m.Index） 和leader.Commit 取最小。
		r.RaftLog.committed = min(m.Commit, neli)

		log.Printf("[%v T_%v]: after MsgAppend, committed=%d, raftlog.ents=%v", r.id, r.Term, r.RaftLog.committed, r.RaftLog.entries)
	}

	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), Commit: r.RaftLog.committed, Reject: false})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateLeader {
		log.Printf("[%v T_%d]: received heartbeat!!!! msg=%+v", r.id, r.Term, m)
	}
	r.becomeFollower(m.Term, m.From) // 可能同一个Term 中，其他 candidate 竞选成功，step中的简单term对比无法覆盖

	if m.Index != 0 && r.RaftLog.LastIndex() >= m.Index {
		lt, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		if lt == m.LogTerm {
			// 后面有多余的肯定是冲突的log entry 直接truncate
			r.RaftLog.truncate(m.Index+1)
			// 更新commit, 需要判断是否对齐和更新
			r.RaftLog.committed = min(m.Commit, m.Index)
		}
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), Commit: r.RaftLog.committed, Reject: false})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
