package raft_storage

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type ServerTransport struct {
	raftClient        *RaftClient
	raftRouter        message.RaftRouter
	resolverScheduler chan<- worker.Task
	snapScheduler     chan<- worker.Task
	resolving         sync.Map
}

func NewServerTransport(raftClient *RaftClient, snapScheduler chan<- worker.Task, raftRouter message.RaftRouter, resolverScheduler chan<- worker.Task) *ServerTransport {
	return &ServerTransport{
		raftClient:        raftClient,
		raftRouter:        raftRouter,
		resolverScheduler: resolverScheduler,
		snapScheduler:     snapScheduler,
	}
}

func (t *ServerTransport) Send(msg *raft_serverpb.RaftMessage) error {
	storeID := msg.GetToPeer().GetStoreId()
	t.SendStore(storeID, msg)
	return nil
}

func (t *ServerTransport) SendStore(storeID uint64, msg *raft_serverpb.RaftMessage) {
	addr := t.raftClient.GetAddr(storeID)
	if addr != "" {
		t.WriteData(storeID, addr, msg)
		return
	}
	if _, ok := t.resolving.Load(storeID); ok {
		log.Debugf("store address is being resolved, msg dropped. storeID: %v, msg: %s", storeID, msg)
		return
	}
	log.Debug("begin to resolve store address. storeID: %v", storeID)
	t.resolving.Store(storeID, struct{}{})
	t.Resolve(storeID, msg)
}

func (t *ServerTransport) Resolve(storeID uint64, msg *raft_serverpb.RaftMessage) {
	callback := func(addr string, err error) {
		// clear resolving
		t.resolving.Delete(storeID)
		if err != nil {
			log.Errorf("resolve store address failed. storeID: %v, err: %v", storeID, err)
			return
		}
		t.raftClient.InsertAddr(storeID, addr)
		t.WriteData(storeID, addr, msg)
		t.raftClient.Flush()
	}
	t.resolverScheduler <- &resolveAddrTask{
		storeID:  storeID,
		callback: callback,
	}
}

func (t *ServerTransport) WriteData(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) {
	// 截获 Snapshot 类型的消息，转而通过特殊的方式进行发送截获 Snapshot 类型的消息，转而通过特殊的方式进行发送
	if msg.GetMessage().GetSnapshot() != nil {
		t.SendSnapshotSock(addr, msg)
		return
	}
	if err := t.raftClient.Send(storeID, addr, msg); err != nil {
		log.Errorf("send raft msg err. err: %v", err)
	}
}

func (t *ServerTransport) SendSnapshotSock(addr string, msg *raft_serverpb.RaftMessage) {
	callback := func(err error) {
		regionID := msg.GetRegionId()
		toPeerID := msg.GetToPeer().GetId()
		toStoreID := msg.GetToPeer().GetStoreId()
		log.Debugf("send snapshot. toPeerID: %v, toStoreID: %v, regionID: %v, status: %v", toPeerID, toStoreID, regionID, err)
	}
	// 这里简单地把对应的 RaftMessage包装成一个 SnapTask::Send任务，并将其交给独立的 snap-worker去处理
	// 值得注意的是，这里的 RaftMessage只包含 Snapshot 的元信息，而不包括真正的快照数据
	// TiKV 中有一个单独的模块叫做 SnapManager，用来专门处理数据快照的生成与转存，
	// 稍后我们将会看到从 SnapManager模块读取 Snapshot 数据块并进行发送的相关代码
	t.snapScheduler <- &sendSnapTask{
		addr:     addr,
		msg:      msg,
		callback: callback,
	}
}

func (t *ServerTransport) Flush() {
	t.raftClient.Flush()
}
