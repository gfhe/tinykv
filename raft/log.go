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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 所有小于等于 committed 的index，都是应该持久化到storage的
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 所有小于等于applied的index，均应用到了状态机
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 所有小于等于stabled的所有index 均持久化到了storage
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//storage中的数据是stabled，但是不一定都commited 和applied。
	//storage 中ents的数据是未进入snapshot的数据，进入snapshot的数据默认是已经applied 和commited的数据。
	//所以默认storage.firstIndex-1 肯定是commited 和 applied。
	// 初始化时，如果storage中包含了未确定commit的数据和applied的数据，需要同步到RaftLog中。
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// storage 中存在未提交的entry，恢复到raftlog
	var ents []pb.Entry
	log.Printf("RaftLog: storage firstIndex=%d lastIndex=%d", firstIndex, lastIndex)
	if firstIndex <= lastIndex {
		ents, err = storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
	}
	log.Printf("new RaftLog entries: %v", ents)
	return &RaftLog{storage: storage, committed: firstIndex - 1, applied: firstIndex - 1, stabled: lastIndex, entries: ents}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	elen := len(l.entries)
	// raftlog 临时entries 的最后一个entry 的index  <= storage 的last entry index
	if elen == 0  || l.LastIndex() <= l.stabled{
		return []pb.Entry{}
	}

	//到此处，必有 raftLog.stabled < raftLog.LastIndex； 获取stabled对应raftLog.entries 的下标
	p, err := l.pos(l.stabled + 1)
	if err != nil {
		panic(err)
	}
	return l.entries[p:]
}

// entriesAfter 获取RaftLog.entries[i:]
func (l *RaftLog) entriesAfter(i uint64) []pb.Entry {
	p, err := l.pos(i)
	if err != nil {
		panic(err)
	}
	return l.entries[p:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).

	// storage.FirstIndex <= RaftLog.applied <= RaftLog.committed<=RaftLog.LastIndex,

	pc, err := l.pos(l.committed)
	if err != nil {
		panic(err)
	}

	var start uint64
	// 检查 l.applied = storage.FirstIndex 的情况（l.commited 不一定为l.LastIndex），此情况下，获取pos 会出现ErrCompact 异常，需要单独处理
	// 根本原因：storage. entry对RaftLog是透明的，需要特殊处理。RaftLog从storage恢复entries时，拿不到0号entry
	if l.applied == 0 {
		start = 0
	} else {
		// RaftLog.applied 不为0 时， 默认从applied 对应的下标开始找
		pa, err := l.pos(l.applied)
		if err != nil {
			panic(err)
		}
		start = pa + 1 // l.applied 不为0时，起点为l.applied 的下一个位置
	}
	return l.entries[start : pc+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if elen := len(l.entries); elen > 0 {
		return l.entries[elen-1].Index
	} else {
		return l.stabled
	}
}

// pos return the position of index i in RaftLog.entries
func (l *RaftLog) pos(i uint64) (uint64, error) {
	elen := len(l.entries)
	//log.Printf("pos: index=%d, entries length=%d, entries=%v", i, elen, l.entries)
	if elen == 0 {
		return 0, ErrCompacted
	}

	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	} else if i > offset+uint64(elen)-1 {
		return 0, ErrUnavailable
	} else {
		return i - offset, nil
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	pos, err := l.pos(i)
	//log.Printf("Term: index=%d, pos=%d, err=%v", i, pos, err)
	if err != nil {
		if err == ErrCompacted && i == 0 {
			return 0, nil
		}
		return 0, err
	}
	return l.entries[pos].Term, nil
}

// LastLogTerm return the last term of last index of log entry
func (l *RaftLog) LastLogTerm() uint64 {
	// Your Code Here (2A).
	lastIndex := l.LastIndex()
	term, err := l.Term(lastIndex)
	if err != nil {
		log.Printf("term of index=%v, error:%v", l.LastIndex(), err)
		panic(err)
	}
	return term
}

// truncate to delete entries after i, including i
func (l *RaftLog) truncate(i uint64) {
	log.Printf("truncate:delete entries after %d, now entries=%v", i, l.entries)

	p, err := l.pos(i)
	log.Printf("pos(%d)=%d, err=%v", i, p, err)
	if err != nil {
		if err == ErrCompacted && i == 0 {
			p = 0
		} else {
			panic(err)
		}
	}
	len_trun := len(l.entries[p:])
	l.entries = l.entries[:p]
	// 更新stabled， 可能未commited的，但是已经stabled了
	l.stabled = l.stabled - uint64(len_trun)
	log.Printf("truncate: lastIndex=%v, m.Index=%v, to:%v, p=%d, stabled=%d", l.LastIndex(), i, l.entries[:p], p, l.stabled)
}
