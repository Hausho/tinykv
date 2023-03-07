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
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sort"

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
	// Next: 为每个服务器，保存下一条将要发送到该服务器的日志条目的index（初始化为leader最后一条日志的index+1）
	// Match: 为每个服务器，保存该leader已知的该服务器已经复制了的日志条目的最大index（初始化为0，单调递增）
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress // 存储raft group其他节点(peers)的信息

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool // 作为candidate时收到的选票信息

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

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

	transferElapsed int

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

	// user-defined Used in 2A
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	peers := c.peers
	// todo: 为什么len(peers) > 0会报错
	if len(confState.Nodes) > 0 {
		if len(peers) > 0 {
			log.Panic("cannot specify both newRaft(peers) and ConfState.Nodes)")
		}
		peers = confState.Nodes
		c.peers = peers
	}

	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id { // 当前节点
			r.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			r.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}

	r.becomeFollower(0, None)
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term, r.Vote, r.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		// 如果发现结点落后太多以至于所需的日志已经被compact，则会调用sendSnapshot使用快照同步(给落后的节点进行日志同步)
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}
	var entries []*pb.Entry
	for i := r.RaftLog.toSliceIndex(prevLogIndex + 1); i < len(r.RaftLog.entries); i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to, term, index uint64, success bool) {
	// 这里的term和index是冲突日志的term和存储的那个任期的最早的index
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
		Reject:  !success,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to, lastLogIndex, lastLogTerm uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
	}
	// todo:这个msg是怎么发出去的？
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		//if r.leadTransferee != None {
		//	r.tickTransfer()
		//}
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	// 选举超时，重新选举
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	// 心跳超时，更新心跳并bcast心跳给所有追随者
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		//todo: 为什么不是MessageType_MsgHeartbeat
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Vote = None
	r.Lead = lead
	r.Term = term // todo:这个term？
	//log.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	//log.Infof("%x became Candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.Lead = r.id
	lastLogIndex := r.RaftLog.LastIndex()
	// 保证日志匹配特性
	for peerId := range r.Prs {
		if peerId == r.id {
			r.Prs[peerId].Next = lastLogIndex + 2
			r.Prs[peerId].Match = lastLogIndex + 1
		} else {
			r.Prs[peerId].Next = lastLogIndex + 1
		}
	}
	// 广播空AppendEntries通知其他node自己成为了leader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
	r.leadTransferee = None
	//log.Infof("%x became Leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 消息处理
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	// 只要发现发消息的term比自己大，立即变为follower
	if m.Term > r.Term {
		// todo: 3A
		//r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		// 如果MsgTransferLeader发送给非leader节点，需要转发给leader
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.doElection()
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		// todo
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		// 如果MsgTransferLeader发送给非leader节点，需要转发给leader
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		// 广播心跳给各follower
		r.bcastHeatbeat()
	case pb.MessageType_MsgPropose:
		// 实现领导人转移期间不允许接收新的请求
		if r.leadTransferee == None {
			//todo: 为什么这里不直接调用handleAppendEntries？因为propose是raft库使用者提议（propose）数据，只有leader才有处理的权限
			r.appendEntries(m.Entries)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// todo: leader将自己的最后一条提交的index和term与消息中的index和term进行比较。
		// 如果leader的提交日志较新，则发送Append。
		// 这里为什么没有进行比较？直接调用append，这样r.Prs[to].Next好像没有改变？
		r.sendAppend(m.From)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) doElection() {
	//if r.State != StateCandidate {
	//	r.becomeCandidate()
	//}
	// 不需要判断当前是否为Candidate，如果是Candidate也直接重新成为Candidate
	// 因为有可能是Candidate选举超时
	r.becomeCandidate()
	// 重置选举超时时间
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// 向其他所有node发送RequestVote RPCs
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}
	for peerId := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendRequestVote(peerId, lastLogIndex, lastLogTerm)
	}
}

func (r *Raft) bcastHeatbeat() {
	for peerId := range r.Prs {
		if r.id != peerId {
			r.sendHeartbeat(peerId)
		}
	}
}

func (r *Raft) bcastAppend() {
	for peerId := range r.Prs {
		if r.id != peerId {
			r.sendAppend(peerId)
		}
	}
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastLogIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastLogIndex + uint64(i) + 1
		// 保证 conf change 单步配置变更的安全性
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = entry.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	// 超过半数的人的match >= n，说明超过半数的人已经复制的index都到了n
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.bcastAppend()
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, None, None, false)
		return
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From
	lastLogIndex := r.RaftLog.LastIndex()
	// follower丢失了一些条目
	if m.Index > lastLogIndex {
		r.sendAppendResponse(m.From, None, lastLogIndex+1, false)
		return
	}
	// 一致性检查
	if m.Index >= r.RaftLog.FirstIndex {
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		// 优化减少AppendEntries RPC失败的次数
		// 优化之后，一个冲突的任期只需要一次AppendEntries，但是如果有多个冲突任期还需要继续多次AppendEntries
		if logTerm != m.LogTerm {
			index := r.RaftLog.toEntryIndex(sort.Search(r.RaftLog.toSliceIndex(m.Index+1),
				func(i int) bool { return r.RaftLog.entries[i].Term == logTerm }))
			r.sendAppendResponse(m.From, logTerm, index, false)
			return
		}
	}

	// todo: m.Index < r.RaftLog.FirstIndex?

	// 开始复制日志
	for i, entry := range m.Entries {
		// 这个已经保存到快照里面去了，注意raft只是保证已经commit的一定持久化，但是不保证持久化的一定commit
		if entry.Index < r.RaftLog.FirstIndex {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				idx := r.RaftLog.toSliceIndex(entry.Index)
				r.RaftLog.entries[idx] = *entry
				// 将冲突之后的日志条目全部删除
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				// todo:?没看太懂
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		}
	}
	if m.Commit > r.RaftLog.committed {
		// todo: 为什么还需要跟m.Index+uint64(len(m.Entries)做比较？
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, None, r.RaftLog.LastIndex(), true)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index
		if index == None {
			return
		}
		if m.LogTerm != None {
			logTerm := m.LogTerm
			// todo：在当前leader的日志中找到term=LogTerm+1的idx，why?
			sliceIndex := sort.Search(len(r.RaftLog.entries),
				func(i int) bool { return r.RaftLog.entries[i].Term > logTerm })
			if sliceIndex > 0 && r.RaftLog.entries[sliceIndex-1].Term == logTerm {
				index = r.RaftLog.toEntryIndex(sliceIndex)
			}
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
		r.leaderCommit()
		// 领导人转移资格检查
		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
			r.leadTransferee = None
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	// 如果消息中的任期更大，成为follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// 当前node已经给其他candidate投票了
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// 保证日志匹配原则
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		panic(err)
	}
	if lastLogTerm > m.LogTerm ||
		lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		//r.Term = m.Term
		return
	}
	r.votes[m.From] = !m.Reject

	votes := len(r.votes)
	grant := 0
	threshold := len(r.Prs) / 2
	for _, g := range r.votes {
		if g {
			grant++
		}
	}
	if grant > threshold {
		r.becomeLeader()
		//log.Infof("new leader:%d, current leader %d\n", r.Lead, r.id)
	} else if votes-grant > threshold { // todo：不判断拒绝人数超过半数可以吗？
		r.becomeFollower(r.Term, None)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	// 1. 判断发送的快照是否过期，即 raft 本身是否已经含有快照中的日志，如果已经过期回复 MessageType_MsgAppendResponse 消息
	if meta.Index < r.RaftLog.committed {
		r.sendAppendResponse(m.From, None, r.RaftLog.committed, true)
		return
	}
	// todo: 为什么要更新自己的term?可不可以不更新
	r.becomeFollower(max(r.Term, m.Term), m.From)
	// 2. 根据快照修改 raft 的信息，包括 firstIndex、committed、applied、stabled 等
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.FirstIndex = meta.Index + 1
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	// 3. 保存快照，同时将 raft 中已在快照中的日志进行压缩
	r.RaftLog.pendingSnapshot = m.Snapshot
	// 4. 回复 MessageType_MsgAppendResponse 消息
	r.sendAppendResponse(m.From, None, r.RaftLog.LastIndex(), true)
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// 转移给当前leader
	if m.From == r.id {
		return
	}
	// 已经转移给目标节点了
	if r.leadTransferee != None && r.leadTransferee == m.From {
		return
	}
	// 当前leader没有目标节点的信息
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	r.transferElapsed = 0
	// 检查transferee的资格（是否已经包含当前leader的所有日志）
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	// 清除 PendingConfIndex 表示当前没有未完成的配置更新
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		// todo: 删除结点之后需要判断是否有新的可以提交的entry?
		if r.State == StateLeader {
			r.leaderCommit()
		}
	}
	r.PendingConfIndex = None
}
