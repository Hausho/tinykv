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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64 // 保存当前提交的日志数据索引

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64 //保存当前传入状态机的数据最高索引, applied <= committed

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	FirstIndex uint64 // 存储一个起始index，方便后面获取entries的下标
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil!")
	}
	log := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	log.entries = entries
	log.stabled = lastIndex
	// Initialize our committed and applied pointers to the time of the last compaction.
	// committed和applied从持久化的第一个index的前一个开始？为什么committed不是lastIndex？
	// 因为raft只是保证已经commit的一定持久化，但是不保证持久化的一定commit
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.FirstIndex = firstIndex
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.FirstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var index uint64
	if len(l.entries) > 0 { // 如果有未持久化的日志，返回未持久化日志的最后索引
		index = l.entries[len(l.entries)-1].Index
	} else { // 否则返回持久化日志的最后索引
		index, _ = l.storage.LastIndex()
	}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i-l.FirstIndex].Term, nil
	}
	return 0, nil
	//term, err := l.storage.Term(i)
	//if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
	//	if i == l.pendingSnapshot.Metadata.Index {
	//		term = l.pendingSnapshot.Metadata.Term
	//		err = nil
	//	} else if i < l.pendingSnapshot.Metadata.Index {
	//		err = ErrCompacted
	//	}
	//}
	//return term, err

}

// toSliceIndex return the idx of the given index in the entries
func (l *RaftLog) toSliceIndex(i uint64) int {
	idx := int(i - l.FirstIndex)
	if idx < 0 {
		panic("toSliceIndex: index < 0")
	}
	return idx
}

func (l *RaftLog) toEntryIndex(i int) uint64 {
	return uint64(i) + l.FirstIndex
}
