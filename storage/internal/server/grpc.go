package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/block"
	"github.com/TheWor1d877/KVFerry/storage/internal/cache"
	"github.com/TheWor1d877/KVFerry/storage/internal/pin"
	"github.com/TheWor1d877/KVFerry/storage/internal/remote"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// Deps 聚合 BlockStore 服务依赖，main 装配注入（D40）。
type Deps struct {
	NodeID          string
	Store           *segment.Store
	Cache           *cache.LRU
	NodeSpec        spec.KVCacheSpec
	MaxMessageBytes int64
	// Ready 是节点就绪标志，由 main 注入；未就绪按 Unavailable 拒绝（D46）。
	Ready func() bool
	// Emit 是事件发射回调，oversized 事件经它发出（R35、D47）。
	Emit segment.EmitFunc
	// Pins 是 pin 表实例，请求带 pin 时块写入完成后把键写进清单并落盘（R34）；
	// 取 nil 时 pin 请求被忽略，写入照常成功。
	Pins *pin.Table
	// Remote 是远端块服务客户端（D50）：本机未命中时按环取远端，远端不可达或读失败
	// 按未命中返回（D51）；取 nil 时行为与不启用远端块服务一致（D42）。
	Remote *remote.Client
	Log    *telemetry.Logger
}

// Server 实现 kvferry.storage.v1.BlockStore 两个方法（D40）。
type Server struct {
	api.UnimplementedBlockStoreServer
	deps Deps
	log  *telemetry.Logger
}

// New 建立 BlockStore 服务。
func New(d Deps) *Server {
	if d.Log == nil {
		d.Log = telemetry.New()
	}
	return &Server{deps: d, log: d.Log}
}

// MaxRecvMsgSize 由 rpc_max_message_mb 推导服务端接收上限（D40）。
// 额外加 1 MiB 余量，保证超限消息能进入应用层校验，按 InvalidArgument 拒收并记 oversized 事件（R35）。
func MaxRecvMsgSize(rpcMaxMessageMB int64) int {
	return int((rpcMaxMessageMB + 1) << 20)
}

func specFromProto(p *api.KVSpec) spec.KVCacheSpec {
	if p == nil {
		return spec.KVCacheSpec{}
	}
	return spec.KVCacheSpec{
		BlockBytes:    p.GetBlockBytes(),
		KVPrecision:   p.GetKvPrecision(),
		BytesPerToken: p.GetBytesPerToken(),
		AlignmentPad:  p.GetAlignmentPad(),
	}
}

func (s *Server) ready() bool {
	if s.deps.Ready == nil {
		return true
	}
	return s.deps.Ready()
}

func (s *Server) rejectFields(hash string, bytes int64, reason string, traceID string) telemetry.Fields {
	return telemetry.Fields{
		TraceID:   traceID,
		NodeID:    s.deps.NodeID,
		BlockHash: hash,
		Result:    "rejected",
		Reason:    reason,
		Bytes:     bytes,
		Layer:     "local",
	}
}

// PutBlock 写入一个满块：入口三项校验（口径一致、字节超限、节点就绪）后走段层完整路径（D40、D46）。
// 请求带 pin 时块写入完成后把键写进 pin 表并落盘，pin 添加失败只记日志，
// 不改变写入成功的事实与响应（R34）。水位驱逐由段层 Put 内部触发（D42）。
// 入口生成请求级 trace_id，完成与拒收打点同源（D47）。
func (s *Server) PutBlock(ctx context.Context, req *api.PutBlockRequest) (*api.PutBlockResponse, error) {
	hash := req.GetBlockHash()
	data := req.GetBlockData()
	traceID := telemetry.NewTraceID("put", hash, time.Now().String())

	if !s.ready() {
		s.log.Warn("putblock rejected", s.rejectFields(hash, int64(len(data)), block.ErrNotReady.Error(), traceID))
		return nil, status.Error(codes.Unavailable, block.ErrNotReady.Error())
	}

	err := block.ValidatePut(hash, data, specFromProto(req.GetSpec()), s.deps.NodeSpec, s.deps.MaxMessageBytes, func(bh string) {
		if s.deps.Emit != nil {
			s.deps.Emit(segment.Event{
				Kind:      "oversized",
				NodeID:    s.deps.NodeID,
				BlockHash: bh,
				Layer:     "local",
				Reason:    "block exceeds rpc_max_message_mb",
				TsMs:      time.Now().UnixMilli(),
			})
		}
	})
	if err != nil {
		s.log.Warn("putblock rejected", s.rejectFields(hash, int64(len(data)), err.Error(), traceID))
		return nil, status.Error(block.MapError(err), err.Error())
	}

	seq, isNew, err := s.deps.Store.Put(hash, data)
	if err != nil {
		s.log.Warn("putblock rejected", s.rejectFields(hash, int64(len(data)), err.Error(), traceID))
		return nil, status.Error(block.MapError(err), err.Error())
	}

	s.deps.Cache.Put(hash, data)

	// 请求带 pin 时块写入完成后把键写进 pin 表并落盘；pin 添加失败只记日志，
	// 块写入成功的事实与响应不变（R34）。
	if req.GetPin() && s.deps.Pins != nil {
		if err := s.deps.Pins.Add(hash, time.Duration(req.GetPinTtlS())*time.Second); err != nil {
			s.log.Warn("pin add failed", telemetry.Fields{
				TraceID:   traceID,
				NodeID:    s.deps.NodeID,
				BlockHash: hash,
				Result:    "pin_add_failed",
				Reason:    err.Error(),
				Layer:     "local",
			})
		}
	}

	s.log.Info("putblock saved", telemetry.Fields{
		TraceID:    traceID,
		NodeID:     s.deps.NodeID,
		BlockHash:  hash,
		SegmentSeq: seq,
		Bytes:      int64(len(data)),
		Result:     "saved",
		Layer:      "local",
	})

	return &api.PutBlockResponse{SegmentSeq: seq, IsNewWrite: isNew}, nil
}

// GetBlock 按块哈希读回块字节；先查读缓存，未命中走段层，读回后进读缓存（D40、D52）。
// 响应 bytes 字段承载块字节，file_descriptor 与 registered_memory 预留恒为零（D52、R30）。
// 入口生成请求级 trace_id，返回与失败打点同源（D47）。
func (s *Server) GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error) {
	start := time.Now()
	hash := req.GetBlockHash()
	traceID := telemetry.NewTraceID("get", hash, time.Now().String())

	if !s.ready() {
		s.log.Warn("getblock failed", telemetry.Fields{
			TraceID:    traceID,
			NodeID:     s.deps.NodeID,
			BlockHash:  hash,
			Result:     "failed",
			Reason:     block.ErrNotReady.Error(),
			DurationMs: time.Since(start).Milliseconds(),
		})
		return nil, status.Error(codes.Unavailable, block.ErrNotReady.Error())
	}

	if !specFromProto(req.GetSpec()).Equal(s.deps.NodeSpec) {
		err := fmt.Errorf("%w: getblock request spec differs from node spec", spec.ErrSpecMismatch)
		s.log.Warn("getblock failed", telemetry.Fields{
			TraceID:    traceID,
			NodeID:     s.deps.NodeID,
			BlockHash:  hash,
			Result:     "failed",
			Reason:     err.Error(),
			DurationMs: time.Since(start).Milliseconds(),
		})
		return nil, status.Error(block.MapError(err), err.Error())
	}

	if data, ok := s.deps.Cache.Get(hash); ok {
		s.log.Info("getblock hit", telemetry.Fields{
			TraceID:    traceID,
			NodeID:     s.deps.NodeID,
			BlockHash:  hash,
			Result:     "hit_cache",
			Layer:      "local",
			DurationMs: time.Since(start).Milliseconds(),
		})
		return &api.GetBlockResponse{BlockData: data, FileDescriptor: 0, RegisteredMemory: 0}, nil
	}

	data, seq, err := s.deps.Store.Get(hash)
	if err != nil {
		// 本机未命中且启用远端块服务时按环取远端（D50）：远端不可达或读失败按未命中返回，
		// 不重试其他节点（D51）；CRC 失败与段损坏类数据错误不转远端（D46）。
		if s.deps.Remote != nil && isLocalMiss(err) {
			if rdata, ok := s.deps.Remote.GetRemote(ctx, hash); ok {
				s.deps.Cache.Put(hash, rdata)
				s.log.Info("getblock returned", telemetry.Fields{
					TraceID:    traceID,
					NodeID:     s.deps.NodeID,
					BlockHash:  hash,
					Bytes:      int64(len(rdata)),
					Result:     "hit_remote",
					Layer:      "remote",
					DurationMs: time.Since(start).Milliseconds(),
				})
				return &api.GetBlockResponse{BlockData: rdata, FileDescriptor: 0, RegisteredMemory: 0}, nil
			}
			err = fmt.Errorf("%w: %s", segment.ErrNotFound, hash)
		}
		s.log.Warn("getblock failed", telemetry.Fields{
			TraceID:    traceID,
			NodeID:     s.deps.NodeID,
			BlockHash:  hash,
			Result:     "failed",
			Reason:     err.Error(),
			Layer:      "local",
			DurationMs: time.Since(start).Milliseconds(),
		})
		return nil, status.Error(block.MapError(err), err.Error())
	}

	s.deps.Cache.Put(hash, data)

	s.log.Info("getblock returned", telemetry.Fields{
		TraceID:    traceID,
		NodeID:     s.deps.NodeID,
		BlockHash:  hash,
		SegmentSeq: seq,
		Bytes:      int64(len(data)),
		Result:     "hit_nvme",
		Layer:      "local",
		DurationMs: time.Since(start).Milliseconds(),
	})

	return &api.GetBlockResponse{BlockData: data, FileDescriptor: 0, RegisteredMemory: 0}, nil
}

// isLocalMiss 判定本机读失败是否为未命中（D50）：映射无键与段被回收复用两类转远端，
// CRC 失败与段损坏等数据错误不转（D46、D51）。
func isLocalMiss(err error) bool {
	return errors.Is(err, segment.ErrNotFound) || errors.Is(err, segment.ErrHashMismatch)
}
