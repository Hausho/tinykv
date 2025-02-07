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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready 压缩了那些已经准备好被读取、存储到持久化设备、被提交或者被发送到其他peers的entries和msg，Ready中所有的字段都是只读的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// Entries 指定在发送消息之前需要保存到稳定存储的条目（说明还没有保存到稳定存储）
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// CommittedEntries 指定提交到存储/状态机的entries。这些之前都被提交到稳定存储了
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// Messages 指定在条目被提交到稳定存储之后发送的出站消息，
	// 如果它包含MessageType_MsgSnapshot消息，当快照已经被接收到或失败时，应用程序必须通过调用ReportSnapshot报告回raft，
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevSoftState *SoftState
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	r := newRaft(config)
	rn := &RawNode{
		Raft:          r,
		prevSoftState: r.softState(),
		prevHardState: r.hardState(),
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
// 调用该函数将驱动节点进入候选人状态，进而将竞争leader
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
// 提议写入数据到日志中，可能会返回错误
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
// 将消息msg灌入状态机中
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
// 这里是核心函数，将返回Ready的channel，应用层需要关注这个channel，当发生变更时将其中的数据进行操作
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	r := rn.Raft
	rd := Ready{
		// entries保存的是没有持久化的数据数组
		Entries: r.RaftLog.unstableEntries(),
		// 保存committed但是还没有applied的数据数组
		CommittedEntries: r.RaftLog.nextEnts(),
		// 保存待发送的消息
		Messages: r.msgs,
	}

	softState := r.softState()
	hardState := r.hardState()
	if !isSoftStateEqual(softState, rn.prevSoftState) {
		//rn.prevSoftState = softState
		rd.SoftState = softState
	}
	if !isHardStateEqual(hardState, rn.prevHardState) {
		//todo 为什么不需要修改prevHardState？
		// 在Advance中更新prevHardState
		//rn.prevHardState = hardState
		rd.HardState = hardState
	}
	// 待发送的消息置为空
	r.msgs = make([]pb.Message, 0)

	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		rd.Snapshot = *r.RaftLog.pendingSnapshot
		r.RaftLog.pendingSnapshot = nil
	}

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	r := rn.Raft
	if !isSoftStateEqual(r.softState(), rn.prevSoftState) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardState) {
		return true
	}
	if len(r.RaftLog.unstableEntries()) > 0 || len(r.RaftLog.nextEnts()) > 0 || len(r.msgs) > 0 {
		return true
	}

	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// Advance函数是当使用者已经将上一次Ready数据处理之后，调用该函数告诉raft库可以进行下一步的操作
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	if rd.SoftState != nil {
		rn.prevSoftState = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		// 更新stabled
		rn.Raft.RaftLog.stabled = e.Index
	}
	if len(rd.CommittedEntries) > 0 {
		e := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		// 更新applied
		rn.Raft.RaftLog.applied = e.Index
	}
	rn.Raft.RaftLog.maybeCompact()
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	// `TransferLeader` 实际上是一个不需要复制给其他peer的动作
	// 所以上层只需要调用 `RawNode` 的`TransferLeader()`方法而不是`Propose()` 来执行
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
