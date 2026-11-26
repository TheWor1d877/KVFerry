package server

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/TheWor1d877/KVFerry/decision/internal/health"
	"github.com/TheWor1d877/KVFerry/decision/internal/index"
	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
	"github.com/TheWor1d877/KVFerry/storage/api"
)

// PinSource 提供待下发的 pin 变更（D41、R25）：pin 清单由后续实施接线，先接空实现。
type PinSource interface {
	Pending() []*api.PinChange
}

// NoPins 是空 pin 源（R25）：不下发 pin 变更。
type NoPins struct{}

// Pending 返回空变更列表。
func (NoPins) Pending() []*api.PinChange { return nil }

// Option 注入 Reporter 依赖。
type Option func(*Reporter)

// WithIndex 注入索引，心跳对账在其上补登与注销（D54）。
func WithIndex(ix *index.Index) Option {
	return func(r *Reporter) { r.ix = ix }
}

// WithApplier 注入事件应用器（D47）：事件经它按 seq 推进，重报收齐回调清除 resync。
func WithApplier(ap *index.Applier) Option {
	return func(r *Reporter) {
		r.applier = ap
		r.rebuild.SetNodeComplete(ap.MarkRebuilt)
	}
}

// WithPins 注入 pin 变更源（D41、R25）。
func WithPins(src PinSource) Option {
	return func(r *Reporter) { r.pins = src }
}

// WithHealth 注入失联状态机（D51）：流的建立与断开驱动状态迁移。
func WithHealth(h *health.Tracker) Option {
	return func(r *Reporter) { r.health = h }
}

// WithLog 注入日志器。
func WithLog(l *telemetry.Logger) Option {
	return func(r *Reporter) { r.log = l }
}

// nodeReconcile 记录单节点心跳对账的分片收集状态（D48、D54）：片 0 开新周期，
// 全部片收齐后做登记有而清单无的注销。
type nodeReconcile struct {
	total  int64
	shards map[int64]map[string]struct{}
}

// Reporter 实现 kvferry.decision.v1.StorageReporter（D41）：ReportManifest 逐片接收交给
// 索引重建，StreamEvents 接收事件交应用器并回确认，Heartbeat 做心跳对账并回 pin 变更
// 与全量重报请求。
type Reporter struct {
	api.UnimplementedStorageReporterServer
	rebuild *index.Rebuild
	ix      *index.Index
	applier *index.Applier
	pins    PinSource
	health  *health.Tracker
	log     *telemetry.Logger
	mu      sync.Mutex
	rc      map[string]*nodeReconcile
}

// NewReporter 建立 StorageReporter 服务端（D41），依赖经 Option 注入。
func NewReporter(rebuild *index.Rebuild, opts ...Option) *Reporter {
	r := &Reporter{
		rebuild: rebuild,
		log:     telemetry.New(),
		rc:      make(map[string]*nodeReconcile),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// ReportManifest 逐片接收清单并交给重建（D41、D49）：
// 分片形态非法按 InvalidArgument 拒绝，逐片收齐后由重建替换该节点登记。
func (r *Reporter) ReportManifest(ctx context.Context, req *api.ReportManifestRequest) (*api.ReportManifestResponse, error) {
	node := req.GetNodeId()
	if node == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	total, shardIdx := req.GetShardTotal(), req.GetShardIndex()
	if total <= 0 {
		return nil, status.Error(codes.InvalidArgument, "shard_total must be positive")
	}
	if shardIdx < 0 || shardIdx >= total {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("shard_index %d out of range [0, %d)", shardIdx, total))
	}
	records := make(map[string]index.Record, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		hash := e.GetBlockHash()
		if hash == "" {
			return nil, status.Error(codes.InvalidArgument, "entry block_hash is required")
		}
		records[hash] = index.Record{
			Layer:  e.GetLayer(),
			Offset: e.GetOffset(),
			Length: e.GetBlockBytes(),
		}
	}
	if err := r.rebuild.Shard(node, shardIdx, total, records); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &api.ReportManifestResponse{Accepted: true}, nil
}

// StreamEvents 接收节点事件流（D41、D47）：事件交应用器按 seq 推进，逐条回确认序号；
// 流绑定首条事件的节点，连接建立与断开驱动失联状态机（D51）。
func (r *Reporter) StreamEvents(stream api.StorageReporter_StreamEventsServer) error {
	var node string
	defer func() {
		if node != "" && r.health != nil {
			r.health.Disconnected(node)
		}
	}()
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		if node == "" {
			node = ev.GetNodeId()
			if node == "" {
				return status.Error(codes.InvalidArgument, "node_id is required")
			}
			if r.health != nil {
				r.health.Reconnected(node)
			}
		}
		if ev.GetNodeId() != node {
			continue
		}
		if r.applier != nil {
			r.applier.Apply(ev)
			ack := &api.EventAck{LastConfirmedSeq: r.applier.Applied(node)}
			if err := stream.Send(ack); err != nil {
				return err
			}
		}
	}
}

// Heartbeat 接收心跳清单片（D41、D48）：补登清单有而登记无的条目，该节点全部片收齐后
// 注销登记有而清单无的条目（D54）；响应回 pin 变更（R25）与全量重报请求（D49、D47）。
func (r *Reporter) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	node := req.GetNodeId()
	if node == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	r.reconcile(node, req)
	resp := &api.HeartbeatResponse{}
	if r.rebuild != nil && r.rebuild.Open() {
		resp.FullRereport = true
	}
	// 节点处于 resync 待重建时同样请求全量重报（D47）：跳号或缓冲溢出置位的 resync
	// 由重报逐片收齐后的完成回调清除，链路收敛。
	if r.applier != nil && r.applier.Resync(node) {
		resp.FullRereport = true
		if r.rebuild != nil {
			r.rebuild.Enqueue(node)
		}
	}
	if r.pins != nil {
		resp.PinChanges = r.pins.Pending()
	}
	return resp, nil
}

// reconcile 做心跳对账（D54）：补登按片即时进行，注销等该节点全部片收齐后按完整清单
// 差集执行，避免单片清空其余片的登记。
func (r *Reporter) reconcile(node string, req *api.HeartbeatRequest) {
	if r.ix == nil {
		return
	}
	seen := make(map[string]struct{}, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		hash := e.GetBlockHash()
		if hash == "" {
			continue
		}
		seen[hash] = struct{}{}
		if _, ok := r.ix.Lookup(node, hash); !ok {
			r.ix.Insert(node, hash, index.Record{
				Layer:  e.GetLayer(),
				Offset: e.GetOffset(),
				Length: e.GetBlockBytes(),
			})
		}
	}
	idx, total := req.GetShardIndex(), req.GetShardTotal()
	if idx < 0 || total <= 0 || idx >= total {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rc := r.rc[node]
	if idx == 0 || rc == nil || rc.total != total {
		rc = &nodeReconcile{total: total, shards: make(map[int64]map[string]struct{})}
		r.rc[node] = rc
	}
	rc.shards[idx] = seen
	if int64(len(rc.shards)) < total {
		return
	}
	full := make(map[string]struct{})
	for _, shard := range rc.shards {
		for hash := range shard {
			full[hash] = struct{}{}
		}
	}
	for _, hash := range r.ix.Hashes(node) {
		if _, ok := full[hash]; !ok {
			r.ix.Delete(node, hash)
		}
	}
	delete(r.rc, node)
}
