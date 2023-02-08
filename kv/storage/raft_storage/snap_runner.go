package raft_storage

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type sendSnapTask struct {
	addr     string
	msg      *raft_serverpb.RaftMessage
	callback func(error)
}

type recvSnapTask struct {
	stream   tinykvpb.TinyKv_SnapshotServer
	callback func(error)
}

type snapRunner struct {
	config      *config.Config
	snapManager *snap.SnapManager
	router      message.RaftRouter
}

func newSnapRunner(snapManager *snap.SnapManager, config *config.Config, router message.RaftRouter) *snapRunner {
	return &snapRunner{
		config:      config,
		snapManager: snapManager,
		router:      router,
	}
}

func (r *snapRunner) Handle(t worker.Task) {
	switch t.(type) {
	case *sendSnapTask:
		r.send(t.(*sendSnapTask))
	case *recvSnapTask:
		r.recv(t.(*recvSnapTask))
	}
}

func (r *snapRunner) send(t *sendSnapTask) {
	t.callback(r.sendSnap(t.addr, t.msg))
}

const snapChunkLen = 1024 * 1024

func (r *snapRunner) sendSnap(addr string, msg *raft_serverpb.RaftMessage) error {
	//然后将 RaftMessage和 Snap一起封装进 SnapChunk结构，最后创建全新的 gRPC 连接及一个 Snapshot stream 并将 SnapChunk写入。
	//这里引入 SnapChunk是为了避免将整块 Snapshot 快照一次性加载进内存， 它 impl 了 futures::Stream这个 trait 来达成按需加载流式发送的效果。

	start := time.Now()
	// 1.先是用 Snapshot 元信息从 SnapManager取到待发送的快照数据，
	msgSnap := msg.GetMessage().GetSnapshot()
	snapKey, err := snap.SnapKeyFromSnap(msgSnap)
	if err != nil {
		return err
	}

	r.snapManager.Register(snapKey, snap.SnapEntrySending)
	defer r.snapManager.Deregister(snapKey, snap.SnapEntrySending)

	snap, err := r.snapManager.GetSnapshotForSending(snapKey)
	if err != nil {
		return err
	}
	if !snap.Exists() {
		return errors.Errorf("missing snap file: %v", snap.Path())
	}

	// 然后将 RaftMessage和 Snap一起封装进 SnapChunk结构
	// 3. 创建全新的 gRPC 连接及一个 Snapshot stream 并将 SnapChunk写入。
	// 这里引入 SnapChunk是为了避免将整块 Snapshot 快照一次性加载进内存， 它 impl 了 futures::Stream这个 trait 来达成按需加载流式发送的效果。
	// 3.1 连接服务器
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    3 * time.Second,
			Timeout: 60 * time.Second,
		}))
	if err != nil {
		return err
	}
	// 3.2 建立gRPC连接
	client := tinykvpb.NewTinyKvClient(cc)
	// 3.3 获取发送流发送数据
	stream, err := client.Snapshot(context.TODO())
	if err != nil {
		return err
	}
	// 3.3.1 先发送消息头
	err = stream.Send(&raft_serverpb.SnapshotChunk{Message: msg})
	if err != nil {
		return err
	}

	buf := make([]byte, snapChunkLen)
	for remain := snap.TotalSize(); remain > 0; remain -= uint64(len(buf)) {
		if remain < uint64(len(buf)) {
			buf = buf[:remain]
		}
		_, err := io.ReadFull(snap, buf)
		if err != nil {
			return errors.Errorf("failed to read snapshot chunk: %v", err)
		}
		// 3.3.2 写入 SnapChunk
		err = stream.Send(&raft_serverpb.SnapshotChunk{Data: buf})
		if err != nil {
			return err
		}
	}
	// 4.关闭stream并接收服务端响应
	_, err = stream.CloseAndRecv()
	if err != nil {
		return err
	}

	log.Infof("sent snapshot. regionID: %v, snapKey: %v, size: %v, duration: %s", snapKey.RegionID, snapKey, snap.TotalSize(), time.Since(start))
	return nil
}

func (r *snapRunner) recv(t *recvSnapTask) {
	// 接收到快照数据
	msg, err := r.recvSnap(t.stream)
	// 通知Raft更新自己的各种状态并设置`pendingSnapshot`
	if err == nil {
		r.router.SendRaftMessage(msg)
	}
	t.callback(err)
}

func (r *snapRunner) recvSnap(stream tinykvpb.TinyKv_SnapshotServer) (*raft_serverpb.RaftMessage, error) {
	head, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if head.GetMessage() == nil {
		return nil, errors.New("no raft message in the first chunk")
	}
	message := head.GetMessage().GetMessage()
	snapKey, err := snap.SnapKeyFromSnap(message.GetSnapshot())
	if err != nil {
		return nil, errors.Errorf("failed to create snap key: %v", err)
	}

	data := message.GetSnapshot().GetData()
	snapshot, err := r.snapManager.GetSnapshotForReceiving(snapKey, data)
	if err != nil {
		return nil, errors.Errorf("%v failed to create snapshot file: %v", snapKey, err)
	}
	if snapshot.Exists() {
		log.Infof("snapshot file already exists, skip receiving. snapKey: %v, file: %v", snapKey, snapshot.Path())
		stream.SendAndClose(&raft_serverpb.Done{})
		return head.GetMessage(), nil
	}
	r.snapManager.Register(snapKey, snap.SnapEntryReceiving)
	defer r.snapManager.Deregister(snapKey, snap.SnapEntryReceiving)

	// 1.for循环接收客户端发送的消息
	for {
		// 2. 通过 Recv() 不断获取客户端 send()推送的消息
		chunk, err := stream.Recv()
		if err != nil {
			// 3. err == io.EOF表示已经获取全部数据
			if err == io.EOF {
				break
			}
			return nil, err
		}
		data := chunk.GetData()
		if len(data) == 0 {
			return nil, errors.Errorf("%v receive chunk with empty data", snapKey)
		}
		_, err = bytes.NewReader(data).WriteTo(snapshot)
		if err != nil {
			return nil, errors.Errorf("%v failed to write snapshot file %v: %v", snapKey, snapshot.Path(), err)
		}
	}

	err = snapshot.Save()
	if err != nil {
		return nil, err
	}

	// 4.SendAndClose 返回并关闭连接。在客户端发送完毕后服务端即可返回响应
	stream.SendAndClose(&raft_serverpb.Done{})
	return head.GetMessage(), nil
}
