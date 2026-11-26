package remote

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/event"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// ErrNoRing 表示环上没有节点（D42、D50）：remote_nodes 缺省空表示不启用远端块服务，
// 空环上的下沉与远端读无目标可落。
var ErrNoRing = errors.New("remote ring has no node")

// ReasonSinkDiscard 是两侧写失败丢弃事件的原因取值（D47、D51）。
const ReasonSinkDiscard = "sink_fallback_failed"

// Dialer 建立到远端节点的传输连接（D50）：返回 BlockStore 客户端与连接释放函数。
// 远端节点复用 BlockStore 服务语义：对端收到 PutBlock 即同键幂等覆盖，GetBlock 读回。
// 真实跨机传输未定，测试注入 bufconn 连接，装配方注入 gRPC 连接。
type Dialer func(addr string) (api.BlockStoreClient, func() error, error)

// LocalPut 把块字节写回本机段层（D51）：远端写失败回退落本机由装配方接段层写入，
// 本包不触碰段层。
type LocalPut func(blockHash string, data []byte) error

// Outcome 是一次 Sink 的结果（D50、D51）。
type Outcome int

const (
	// SinkRemote 表示块已推到环上节点，同键幂等覆盖，全集群只留一份（D50、R26）。
	SinkRemote Outcome = iota + 1
	// KeepLocal 表示远端写失败回退落本机，块留本机（D51）。
	KeepLocal
	// Discard 表示远端与本机都写不进，块丢弃并记事件（D51）。
	Discard
)

// Options 是 Client 构造选项。
type Options struct {
	// NodeID 是本存储节点编号，随事件与远端请求携带（D47）。
	NodeID string
	// Nodes 是 remote_nodes 节点列表（D42、D50），空列表表示不启用远端块服务。
	Nodes []string
	// NodeSpec 是本节点 KV 口径（D44），随远端请求携带，全集群口径一致。
	NodeSpec spec.KVCacheSpec
	// Dial 为传输注入；nil 时用不加密 gRPC 连接（D50）。
	Dial Dialer
	// Emit 是事件发射回调，demote 与丢弃事件经它发布（D47）。
	Emit segment.EmitFunc
	// LocalPut 为回退落本机入口（D51）；nil 时远端写失败按丢弃处理。
	LocalPut LocalPut
	// Log 为结构化日志器，nil 时用默认 stdout 日志器。
	Log *telemetry.Logger
}

type peerConn struct {
	client api.BlockStoreClient
	close  func() error
}

// Client 是远端块服务客户端（D50、D51）：下沉推送、远端写入与远端读。
type Client struct {
	nodeID   string
	spec     spec.KVCacheSpec
	ring     *Ring
	dial     Dialer
	emit     segment.EmitFunc
	localPut LocalPut
	log      *telemetry.Logger

	mu    sync.Mutex
	conns map[string]*peerConn
}

// New 建立远端块服务客户端并按节点列表建环（D50、R27）：环成员只在启动时构建一次。
func New(opts Options) *Client {
	if opts.Log == nil {
		opts.Log = telemetry.New()
	}
	dial := opts.Dial
	if dial == nil {
		dial = defaultDial
	}
	return &Client{
		nodeID:   opts.NodeID,
		spec:     opts.NodeSpec,
		ring:     NewRing(opts.Nodes, opts.Log),
		dial:     dial,
		emit:     opts.Emit,
		localPut: opts.LocalPut,
		log:      opts.Log,
		conns:    make(map[string]*peerConn),
	}
}

// Close 释放全部远端连接。
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for target, pc := range c.conns {
		if err := pc.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, target)
	}
	return firstErr
}

// peer 取到目标节点的连接，未建过时经 Dialer 建立；建连失败不缓存，下次重试。
func (c *Client) peer(target string) (*peerConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pc, ok := c.conns[target]; ok {
		return pc, nil
	}
	cl, closeFn, err := c.dial(target)
	if err != nil {
		return nil, fmt.Errorf("dial remote node %s: %w", target, err)
	}
	pc := &peerConn{client: cl, close: closeFn}
	c.conns[target] = pc
	return pc, nil
}

// Sink 把块字节连同块哈希发到环上节点（D50）：对端按同键幂等覆盖，全集群只留一份（R26）。
// 远端写失败回退落本机，块留本机；本机也写不进则丢弃并记事件（D51）。
// 下沉由保留决策触发，决策侧触发器未实现，本方法即 Sink 调用入口，注入哈希与字节即可下沉。
// 下沉成功后发 demote 事件（D47），本机段按普通驱逐流程回收由装配方编排，本包不触碰段层。
func (c *Client) Sink(ctx context.Context, blockHash string, data []byte) (Outcome, error) {
	target := c.ring.Pick(blockHash)
	if target == "" {
		return 0, ErrNoRing
	}
	start := time.Now()
	var err error
	pc, perr := c.peer(target)
	if perr != nil {
		err = perr
	} else {
		_, err = pc.client.PutBlock(ctx, &api.PutBlockRequest{
			NodeId:    c.nodeID,
			BlockHash: blockHash,
			Spec:      toProtoSpec(c.spec),
			BlockData: data,
		})
	}
	if err == nil {
		c.log.Info("block sink completed", telemetry.Fields{
			NodeID:     c.nodeID,
			BlockHash:  blockHash,
			Target:     target,
			DurationMs: time.Since(start).Milliseconds(),
			Result:     "sunk",
			Layer:      "remote",
			Bytes:      int64(len(data)),
		})
		c.emitDemote(blockHash)
		return SinkRemote, nil
	}
	c.log.Warn("remote sink write failed", telemetry.Fields{
		NodeID:     c.nodeID,
		BlockHash:  blockHash,
		Target:     target,
		DurationMs: time.Since(start).Milliseconds(),
		Result:     "fallback",
		Reason:     err.Error(),
		Layer:      "remote",
	})
	if c.localPut != nil {
		if lerr := c.localPut(blockHash, data); lerr == nil {
			return KeepLocal, nil
		}
	}
	c.emitDiscard(blockHash)
	return Discard, err
}

// GetRemote 按键定位节点读回块字节（D50）：读失败或节点不可达按未命中返回，
// 不换节点重试（D51）。
func (c *Client) GetRemote(ctx context.Context, blockHash string) ([]byte, bool) {
	target := c.ring.Pick(blockHash)
	if target == "" {
		return nil, false
	}
	start := time.Now()
	pc, err := c.peer(target)
	if err != nil {
		c.log.Warn("remote read failed", telemetry.Fields{
			NodeID:     c.nodeID,
			BlockHash:  blockHash,
			Target:     target,
			DurationMs: time.Since(start).Milliseconds(),
			Result:     "miss",
			Reason:     err.Error(),
			Layer:      "remote",
		})
		return nil, false
	}
	resp, err := pc.client.GetBlock(ctx, &api.GetBlockRequest{BlockHash: blockHash, Spec: toProtoSpec(c.spec)})
	if err != nil {
		c.log.Warn("remote read failed", telemetry.Fields{
			NodeID:     c.nodeID,
			BlockHash:  blockHash,
			Target:     target,
			DurationMs: time.Since(start).Milliseconds(),
			Result:     "miss",
			Reason:     err.Error(),
			Layer:      "remote",
		})
		return nil, false
	}
	return resp.GetBlockData(), true
}

// emitDemote 发布下沉事件（D47）：demote 记下沉，经现有 EmitFunc 回调发布，登记层位转远端。
func (c *Client) emitDemote(blockHash string) {
	if c.emit == nil {
		return
	}
	c.emit(segment.Event{
		Kind:      event.KindDemote,
		NodeID:    c.nodeID,
		BlockHash: blockHash,
		Layer:     "remote",
		Reason:    "sink",
		TsMs:      time.Now().UnixMilli(),
	})
}

// emitDiscard 发布丢弃事件（D51）：远端与本机都写不进时块从集群消失，按 evict 记驱逐
// 让索引删除登记（D47）。
func (c *Client) emitDiscard(blockHash string) {
	if c.emit == nil {
		return
	}
	c.emit(segment.Event{
		Kind:      event.KindEvict,
		NodeID:    c.nodeID,
		BlockHash: blockHash,
		Layer:     "local",
		Reason:    ReasonSinkDiscard,
		TsMs:      time.Now().UnixMilli(),
	})
}

// defaultDial 建立到远端节点的 gRPC 连接（D50）：真实跨机传输协议未定前按网络地址直连。
func defaultDial(addr string) (api.BlockStoreClient, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	return api.NewBlockStoreClient(conn), conn.Close, nil
}

// toProtoSpec 把节点口径转成请求消息口径（D44）。
func toProtoSpec(s spec.KVCacheSpec) *api.KVSpec {
	return &api.KVSpec{
		BlockBytes:    s.BlockBytes,
		KvPrecision:   s.KVPrecision,
		BytesPerToken: s.BytesPerToken,
		AlignmentPad:  s.AlignmentPad,
	}
}
