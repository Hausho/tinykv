package raftstore

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry, handle func(*proposal)) {
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				handle(p)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

// notifyHeartbeatScheduler 帮助 region 快速创建 peer
func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) processAdminRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLog := req.GetCompactLog()
		applySt := d.peerStorage.applyState
		// apply包含`AdminCmdType_CompactLog`的日志项
		// 并修改`RaftApplyState`中的`RaftTruncatedState`的`index`和`term`（与消息中压缩位置匹配）
		if compactLog.CompactIndex >= applySt.TruncatedState.Index {
			applySt.TruncatedState.Index = compactLog.CompactIndex
			applySt.TruncatedState.Term = compactLog.CompactTerm
			wb.SetMeta(meta.ApplyStateKey(d.regionId), applySt)
			d.ScheduleCompactLog(applySt.TruncatedState.Index)
		}
	case raft_cmdpb.AdminCmdType_Split:
		splitReq := req.GetSplit()
		if len(d.Region().Peers) != len(splitReq.NewPeerIds) {
			// todo ugly code, fix bug
			log.Warningf("%s len(d.Region().Peers) != len(splitReq.NewPeerIds), region: %v, msg: %v", d.Tag, d.Region(), msg)
			return
		}
		if d.IsLeader() {
			log.Infof("%s start to commit split region request in store %d, split key is %v, msg %v", d.Tag, d.ctx.store.Id, hex.EncodeToString(splitReq.SplitKey), msg)
		}

		// copy region
		// 1. 检查
		originRegion := d.Region()

		err := util.CheckRegionEpoch(msg, originRegion, true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(errEpochNotMatching))
			})
			return
		}
		err = util.CheckKeyInRegion(splitReq.SplitKey, originRegion)
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return
		}
		// 2. clone两个region，并自增RegionEpoch.Version
		leftRegion := new(metapb.Region)
		if err := util.CloneMsg(originRegion, leftRegion); err != nil {
			panic(err)
		}
		rightRegion := new(metapb.Region)
		if err := util.CloneMsg(originRegion, rightRegion); err != nil {
			panic(err)
		}
		leftRegion.RegionEpoch.Version++
		rightRegion.RegionEpoch.Version++

		newPeers := make([]*metapb.Peer, len(splitReq.NewPeerIds))
		for i, peer := range leftRegion.Peers {
			newPeers[i] = &metapb.Peer{
				Id:      splitReq.NewPeerIds[i],
				StoreId: peer.StoreId,
			}
		}
		// 3. 修改克隆的两个region的StartKey和EndKey
		rightRegion.Id = splitReq.NewRegionId
		rightRegion.StartKey = splitReq.SplitKey
		rightRegion.EndKey = leftRegion.EndKey
		rightRegion.Peers = newPeers

		leftRegion.EndKey = splitReq.SplitKey

		// 4.持久化RegionState
		meta.WriteRegionState(wb, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(wb, rightRegion, rspb.PeerState_Normal)

		// 5.create new peer & register into router
		// 通过 createPeer() 方法创建新的 peer 并注册进 router，同时发送 message.MsgTypeStart 启动 peer；
		newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(newPeer)
		if err := d.ctx.router.send(rightRegion.Id, message.Msg{
			Type: message.MsgTypeStart,
		}); err != nil {
			panic("This should not happen")
		}

		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		defer storeMeta.Unlock()

		storeMeta.regionRanges.Delete(&regionItem{region: originRegion})
		storeMeta.setRegion(leftRegion, d.peer)
		storeMeta.setRegion(rightRegion, newPeer)
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})

		if d.IsLeader() {
			log.Infof("%s split region into [%s - %s] & [%s - %s] success.", d.Tag,
				hex.EncodeToString(leftRegion.GetStartKey()), hex.EncodeToString(leftRegion.GetEndKey()),
				hex.EncodeToString(rightRegion.GetStartKey()), hex.EncodeToString(rightRegion.GetEndKey()))
		}
		// 调用两次 d.notifyHeartbeatScheduler()，更新 scheduler 那里的 region 缓存；
		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, newPeer)

		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{leftRegion, rightRegion}},
				},
			})
		})
		// todo:新增加的 peer 是通过 leader 的心跳完成的
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}
}
func (d *peerMsgHandler) processRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.Requests[0]
	key := getRequestKey(req)
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
	}
	// apply to kv
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Put:
		wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	case raft_cmdpb.CmdType_Delete:
		wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
	case raft_cmdpb.CmdType_Snap:
	}
	// 生成response
	d.handleProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)
			value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				value = nil
			}
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: value}}}
			wb = new(engine_util.WriteBatch)
		case raft_cmdpb.CmdType_Put:
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}}
		case raft_cmdpb.CmdType_Snap:
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)
			resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			wb = new(engine_util.WriteBatch)
		}
		p.cb.Done(resp)
	})
	return wb
}

func (d *peerMsgHandler) processConfChange(entry *eraftpb.Entry, cc *eraftpb.ConfChange, wb *engine_util.WriteBatch) {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	region := d.Region()
	// 检查RegionEpoch是否一样
	err = util.CheckRegionEpoch(msg, region, true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(ErrResp(errEpochNotMatching))
		})
		return
	}
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// 查找当前peers中是否有要添加的peer，如果没有就添加
		n := searchRegionPeer(region, cc.NodeId)
		if n == len(region.Peers) {
			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.Meta.Id {
			d.destroyPeer()
			return
		}
		n := searchRegionPeer(region, cc.NodeId)
		if n < len(region.Peers) {
			// 删除下标为n的peer，写法注意一下
			region.Peers = append(region.Peers[:n], region.Peers[n+1:]...)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.removePeerCache(cc.NodeId)
		}
	}
	// 进行raft层面的更改
	d.RaftGroup.ApplyConfChange(*cc)
	// 处理完这个proposal，发送回调
	d.handleProposal(entry, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
			},
		})
	})
	// todo：干啥的？
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) process(entry *eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		if err := cc.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
		d.processConfChange(entry, cc, wb)
		return wb
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	if len(msg.Requests) > 0 {
		return d.processRequest(entry, msg, wb)
	}
	if msg.AdminRequest != nil {
		d.processAdminRequest(entry, msg, wb)
		return wb
	}
	// noop entry
	return wb
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B/2C).
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		// 1.持久化RaftLog和RaftLocalState，还有处理快照
		result, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		// ?干啥的
		if result != nil {
			if !reflect.DeepEqual(result.PrevRegion, result.Region) {
				d.peerStorage.SetRegion(result.Region)
				storeMeta := d.ctx.storeMeta
				storeMeta.Lock()
				storeMeta.regions[result.Region.Id] = result.Region
				storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
				storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
				storeMeta.Unlock()
			}
		}
		// 2.将raft中的消息转发出去，指定在条目被提交到稳定存储之后发送的出站消息
		d.Send(d.ctx.trans, rd.Messages)
		// 3.将Snapshot(如果有)已提交的log entries应用到状态机，并且通过cb回调让上层感知
		if len(rd.CommittedEntries) > 0 {
			oldProposals := d.proposals
			kvWB := new(engine_util.WriteBatch)
			for _, entry := range rd.CommittedEntries {
				// 3.1 modify kvDB
				kvWB = d.process(&entry, kvWB)
				// 节点有可能在 processCommittedEntry 返回之后就销毁了
				// 如果销毁了需要直接返回，保证对这个节点而言不会再 DB 中写入数据
				if d.stopped {
					return
				}
			}
			// 3.2 持久化RaftApplyState
			d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			//
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			// 如果之前有未apply的命令，那么需要更新d.proposals为最新的未apply的，上面已经apply了一些
			if len(oldProposals) > len(d.proposals) {
				proposals := make([]*proposal, len(d.proposals))
				copy(proposals, d.proposals)
				d.proposals = proposals
			}
		}
		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	return key
}

func searchRegionPeer(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return len(region.Peers)
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// TransferLeader是一个本地动作，不需要复制给其他peer，直接调用raft
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// 保证raft单步配置变更的safety：
		// 当前的applied是不是大于等于之前的PendingConfIndex，如果大于等于才可以propose这个配置消息
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			return
		}
		// 如果当前Raft层只有两个节点并且当前命令是删除领导人节点，那么会先进行领导人转移命令，然后返回一个错误
		// 这样做是为了保证单步配置变更的可用性
		if len(d.Region().Peers) == 2 && msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
			d.PeerId() == msg.AdminRequest.ChangePeer.Peer.Id {
			for _, p := range d.Region().Peers {
				if p.Id != d.PeerId() {
					d.RaftGroup.TransferLeader(p.Id)
					// todo：返回一个错误
					//cb.Done(ErrResp(err))
					break
				}
			}
		}
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		cc := eraftpb.ConfChange{ChangeType: req.ChangePeer.ChangeType, NodeId: req.ChangePeer.Peer.Id, Context: context}
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.ProposeConfChange(cc)
	case raft_cmdpb.AdminCmdType_Split:
		err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.Propose(data)
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	key := getRequestKey(msg.Requests[0])
	// 要操作的key不在当前peer所在的region中
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
	// peer还会在proposals数组中添加一个新的propose，表示此条指令已经被提交给raft（还未commit），等待应用
	d.proposals = append(d.proposals, p)
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	// 触发RaftLogGcCountLimit检查
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	// region大小检查
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		// 生成一个`recvSnapTask`请求到`snapWorker`中
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	// 当发现已经applied的日志超过RaftLogGcCountLimit，就会发送一个CompactLogRequest的AdminRequest
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	// todo: 为什么要-1？
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	// 申请其分配新的 region id 和 peer id。申请成功后其会发起一个 AdminCmdType_Split 的 AdminRequest 到 region 中
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
