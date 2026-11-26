package server

import (
	"context"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// PinConsumer 消费心跳响应里的 pin 变更（D41、R25）：pin 清单由后续实施接线，
// 装配方注入实现，本服务不做 pin 存储。
type PinConsumer interface {
	ApplyPin(change *api.PinChange)
}

// PinConsumerFunc 适配函数为 pin 变更消费者。
type PinConsumerFunc func(*api.PinChange)

// ApplyPin 实现消费者接口。
func (f PinConsumerFunc) ApplyPin(c *api.PinChange) { f(c) }

// HeartbeatDeps 聚合节点侧心跳循环依赖（D41、D48）：清单条目与容量水位经 Source 注入，
// 传输连接经 Client 注入。
type HeartbeatDeps struct {
	NodeID      string
	Interval    time.Duration
	Client      api.StorageReporterClient
	ShardBlocks int
	// Source 返回当前清单条目与容量水位（0 到 1 的比例）（D48）。
	Source func() ([]manifest.Entry, float64, error)
	// Synced 报告事件流当前连接是否已完成首次补传（D47）：false 时跳过本轮清单上报，
	// 保证重连后先补传事件再上报清单；nil 视为已同步。
	Synced func() bool
	// Pins 消费响应 pin 变更，nil 时忽略（R25）。
	Pins PinConsumer
	// OnFullRereport 在响应带全量重报请求时调用，nil 时忽略；重报不动事件 seq（D49）。
	OnFullRereport func()
	Log            *telemetry.Logger
}

// Heartbeat 是节点侧心跳循环（D41、D48）：按 heartbeat_interval_s 周期上报清单片，
// 消费响应里的 pin 变更与全量重报请求；单次失败记日志不中断循环（D51）。
type Heartbeat struct {
	deps HeartbeatDeps
	log  *telemetry.Logger
}

// NewHeartbeat 建立心跳循环（D41、D48）。
func NewHeartbeat(deps HeartbeatDeps) *Heartbeat {
	if deps.Log == nil {
		deps.Log = telemetry.New()
	}
	return &Heartbeat{deps: deps, log: deps.Log}
}

// Run 按周期发心跳，ctx 取消即退出（D41、D48）；interval 非正时直接返回。
func (h *Heartbeat) Run(ctx context.Context) {
	if h.deps.Interval <= 0 {
		return
	}
	ticker := time.NewTicker(h.deps.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.Beat(ctx)
		}
	}
}

// Beat 发一轮心跳（D41、D48）：清单按 manifest_shard_blocks 切片逐片上报，每片带分片
// 序号与总片数；响应里的 pin 变更逐条消费，全量重报请求在一轮内去重只触发一次。
// 事件流首次补传未完成时跳过本轮，循环不阻塞，下轮再报（D47、D51）。
func (h *Heartbeat) Beat(ctx context.Context) {
	if h.deps.Synced != nil && !h.deps.Synced() {
		h.log.Info("heartbeat skipped until event stream synced", telemetry.Fields{
			NodeID: h.deps.NodeID,
			Result: "skipped_not_synced",
		})
		return
	}
	entries, watermark, err := h.deps.Source()
	if err != nil {
		h.log.Warn("heartbeat source failed", telemetry.Fields{NodeID: h.deps.NodeID, Reason: err.Error(), Result: "source_failed"})
		return
	}
	shards := manifest.Shards(entries, h.deps.ShardBlocks)
	total := int64(len(shards))
	rereported := false
	for i, shard := range shards {
		req := &api.HeartbeatRequest{
			NodeId:            h.deps.NodeID,
			CapacityWatermark: watermark,
			ShardIndex:        int64(i),
			ShardTotal:        total,
			Entries:           toProtoEntries(shard),
		}
		resp, err := h.deps.Client.Heartbeat(ctx, req)
		if err != nil {
			h.log.Warn("heartbeat failed, keep serving", telemetry.Fields{
				NodeID:     h.deps.NodeID,
				ShardIndex: int64(i),
				Reason:     err.Error(),
				Result:     "heartbeat_failed",
			})
			return
		}
		for _, change := range resp.GetPinChanges() {
			if h.deps.Pins != nil {
				h.deps.Pins.ApplyPin(change)
			}
		}
		if resp.GetFullRereport() && !rereported && h.deps.OnFullRereport != nil {
			rereported = true
			h.deps.OnFullRereport()
		}
	}
}

func toProtoEntries(entries []manifest.Entry) []*api.ManifestEntry {
	out := make([]*api.ManifestEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &api.ManifestEntry{
			BlockHash:  e.BlockHash,
			Layer:      e.Layer,
			Offset:     e.Offset,
			BlockBytes: e.BlockBytes,
		})
	}
	return out
}
