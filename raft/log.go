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
	return &RaftLog{storage: storage, committed: lastIndex, applied: lastIndex, stabled: lastIndex}
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
	if elen == 0 {
		return nil
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	if l.LastIndex() <= lastIndex {
		log.Fatalf("storage last index(%d) >= raft unstable entries last Index(%d)", lastIndex, l.LastIndex())
		return nil
	}
	if l.stabled != lastIndex {
		log.Fatalf("storage last index(%d) != raft stabled(%d)", lastIndex, l.stabled)
	}

	return l.entries[l.stabled+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	storageLastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	if elen := len(l.entries); elen > 0 {
		return l.entries[elen-1].Index
	} else {
		return storageLastIndex
	}
}

func (l *RaftLog) pos(i uint64) (uint64, error) {
	elen := len(l.entries)
	log.Printf("pos: index=%d, entries length=%d", i, elen)
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
	log.Printf("Term: index %d pos=%d, err=%v", i, pos, err)
	if err != nil {
		if err == ErrCompacted && i == l.stabled {
			return 0, nil
		}
		return 0, err
	}
	return l.entries[pos].Term, nil
}

// last term of last index of log entry
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
