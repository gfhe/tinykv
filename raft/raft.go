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

	// 额外增加：存储所有对等节点
	// TODO: 后续看看是否可以和 Prs 复用
	peers []uint64

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
	// 初始时，为Follower,Term为0， 无投票记录，心跳时间固定，选举超时时间增加随机变量，初始化底层RaftLog
	return &Raft{
		id:                    c.ID,
		State:                 StateFollower,
		Term:                  0,
		Vote:                  None,
		votes:                 make(map[uint64]bool, 0),
		peers:                 c.peers,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		RaftLog:               newLog(c.Storage)}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if to != r.id {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term})
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
	log.Printf("[%v:->C] currentTerm=%v", r.id, r.Term)
	r.State = StateCandidate
	r.Term = r.Term + 1
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

	}
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
			if len(r.peers) <= 1 {
				r.becomeLeader() // 因为此处仅一个节点，不需要发送heartbeat 确立地位
			} else {
				// 成为candidate 需要立即发送VoteRequest RPC
				for _, peer := range r.peers {
					if peer != r.id {
						r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term})
					}
				}
			}
		case pb.MessageType_MsgPropose:
			// follower 收到propose 消息自动转发给leader
			r.msgs = append(r.msgs, pb.Message{MsgType: m.GetMsgType(), To: r.Lead, Term: m.GetTerm()})
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			// candidate 竞选leader时，会发送此消息，
			// leader、candidate 收到此消息，且消息term比自身的大，则转为follower
			// follower 则会判断后投票，或者拒绝
			r.handleRequestVote(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()

			// NOTE: 如果peer 节点仅有一个直接作为leader
			if len(r.peers) <= 1 {
				r.becomeLeader() // 因为此处仅一个节点，不需要发送heartbeat 确立地位
			} else {
				// 成为candidate 需要立即发送VoteRequest RPC
				for _, peer := range r.peers {
					if peer != r.id {
						r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term})
					}
				}
			}
		case pb.MessageType_MsgAppend:
			// candidate 收到了append 消息，表明leader 已选出来，不是自己，转为follower 并处理消息
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			// candidate 竞选leader时，会发送此消息，
			// leader、candidate 收到此消息，且消息term比自身的大，则转为follower
			// follower 则会判断后投票，或者拒绝
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			// 选举投票结果
			r.votes[m.GetFrom()] = !m.Reject
			log.Printf("[%v_T%v]:votes: %v", r.id, r.Term, r.votes)
			// 如果大多数赞同，则leader；大多数反对，则follower
			count := make(map[bool]int, 3)
			for _, v := range r.votes {
				count[v] += 1
			}
			log.Printf("vote result: %v, peer length=%v", count, len(r.peers))
			if count[true] > len(r.peers)/2 {
				r.becomeLeader()
				// 成为leader后，立即发送heartbeat，确立地位
				for _, peer := range r.peers {
					if peer != r.id {
						r.sendHeartbeat(peer)
					}
				}
			} else if count[false] > len(r.peers)/2 {
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
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
			for _, peer := range r.peers {
				if peer != r.id {
					r.sendHeartbeat(peer)
				}
			}
		case pb.MessageType_MsgPropose:
			// follower 收到propose 消息自动转发给leader, leader 处理propose
			// TODO：处理propose
			for _, peer := range r.peers {
				if peer != r.id {
					r.sendAppend(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			// leader 收到此消息，需要判断term，如果消息term较大，则状态出现问题，自动转为follower
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}
		case pb.MessageType_MsgAppendResponse:
			// 只有leader 对此消息处理
			log.Printf("[%v T_%v]: %v appended msg", r.id, r.Term, m.From)
		case pb.MessageType_MsgRequestVote:
			// candidate 竞选leader时，会发送此消息，
			// leader、candidate 收到此消息，且消息term比自身的大，则转为follower
			// follower 则会判断后投票，或者拒绝
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			// leader 忽略此消息
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeatResponse:
			// MessageType_MsgHeartbeat 的消息响应， 只有leader处理该响应。
			// Leader 收到后，leader知道follower 响应了。
			log.Printf("[HB_R]: %v", m)
		}
	}
	return nil
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	log.Printf("m.term=%v, m.from=%v,r.id=%v, r.Lead=%v,r.Vote=%v, r.RaftLog=%v, r.RaftLog.lastIndex=%v, r.RaftLog.lastTerm=%v", m.Term, m.From, r.id, r.Lead, r.Vote, r.RaftLog, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())
	if ((r.Vote == None && r.Lead == None) || r.Vote == m.From) &&
		(m.LogTerm > r.RaftLog.LastTerm() || (m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())) {
		reject = false
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: reject})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reject := false
	if m.Term < r.Term { // term 过小， 直接拒绝
		reject = true
	}
	r.becomeFollower(m.Term, m.From)
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Term: r.Term, Reject: reject})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
