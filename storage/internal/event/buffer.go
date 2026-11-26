package event

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// 事件类别取值（D47），与段层与服务的 emit 回调约定一致。
const (
	KindRegister  = "register"
	KindDemote    = "demote"
	KindEvict     = "evict"
	KindOffline   = "offline"
	KindOversized = "oversized"
)

// DefaultCapacity 是环形缓冲默认容量（D47）：容量固定，写满覆盖最旧并置 resync 标记。
const DefaultCapacity = 4096

// watermarkFile 是事件 seq 高水位文件（D47）：独立小文件随段清单快照同频落盘，
// 先写临时文件再原子改名，节点重启从高水位加一继续分配，不回退。
const watermarkFile = "events.watermark"

// Buffer 是事件环形缓冲与节点内 seq 分配器（D47）：register、demote、evict、offline、
// oversized 五类共用一套节点内单调递增 seq；决策确认的最高连续 seq 推进续传位置，
// 乱序确认不回退；缓冲里最早 seq 大于待续传位置时置 resync，由决策发起全量重报。
// 环内事件以指针保存，Take 返回的指针对象由调用方独占修改，不得并发共享。
type Buffer struct {
	mu       sync.Mutex
	nodeID   string
	capacity int
	ring     []*api.StorageEvent
	head     int
	count    int
	lastSeq  int64
	acked    int64
	sent     int64
	resync   bool
	dataDir  string
	log      *telemetry.Logger
	now      func() time.Time
}

// NewBuffer 建立环形缓冲（D47）：启动时读 dataDir 高水位文件，读不到从零开始；
// 段层 emit 回调经 Consume 接入，容量取 DefaultCapacity 或调用方指定。
func NewBuffer(nodeID string, capacity int, dataDir string, log *telemetry.Logger) *Buffer {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	if log == nil {
		log = telemetry.New()
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Warn("event watermark dir missing", telemetry.Fields{NodeID: nodeID, Reason: err.Error(), Result: "watermark_load_failed"})
	}
	b := &Buffer{
		nodeID:   nodeID,
		capacity: capacity,
		ring:     make([]*api.StorageEvent, capacity),
		dataDir:  dataDir,
		log:      log,
		now:      time.Now,
	}
	b.lastSeq = loadWatermark(dataDir)
	return b
}

// Consume 把段层事件转成节点事件写入环形缓冲（D47）：seq 在此分配，写满覆盖最旧并置 resync。
// 回调在段层锁内调用，本方法不得回调回段层。
func (b *Buffer) Consume(e segment.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.appendLocked(&api.StorageEvent{
		Kind:       e.Kind,
		NodeId:     b.nodeID,
		SegmentSeq: e.SegmentSeq,
		BlockHash:  e.BlockHash,
		Layer:      layerOf(e.Layer),
		Reason:     e.Reason,
		TsMs:       e.TsMs,
	})
}

// Offline 把失联事件写入缓冲（D47、D51）：连接断开时由事件流客户端调用，随重连补传。
func (b *Buffer) Offline(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.appendLocked(&api.StorageEvent{
		Kind:   KindOffline,
		NodeId: b.nodeID,
		Reason: reason,
		TsMs:   b.now().UnixMilli(),
	})
}

func (b *Buffer) appendLocked(ev *api.StorageEvent) {
	b.lastSeq++
	ev.Seq = b.lastSeq
	if ev.TsMs == 0 {
		ev.TsMs = b.now().UnixMilli()
	}
	if b.count == b.capacity {
		b.head = (b.head + 1) % b.capacity
		b.count--
		b.resync = true
	}
	b.ring[(b.head+b.count)%b.capacity] = ev
	b.count++
}

// Take 取出待续传事件（D47）：返回 seq 大于已确认与已发送位点的全部事件指针，按 seq 升序，
// 以及 resync 标记（置位时由发送方写入事件副本的 Resync 字段）。发送后推进本连接发送位点，
// 断线重连经 ResetSent 回到确认位点。
func (b *Buffer) Take() ([]*api.StorageEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	from := b.acked
	if b.sent > from {
		from = b.sent
	}
	var out []*api.StorageEvent
	for i := 0; i < b.count; i++ {
		ev := b.ring[(b.head+i)%b.capacity]
		if ev.Seq > from {
			out = append(out, ev)
		}
	}
	if len(out) > 0 {
		b.sent = b.lastSeq
	}
	// 缓冲里最早 seq 大于待续传位置说明中间事件已被覆盖丢失（D47）。
	if b.count > 0 && b.ring[b.head].Seq > b.acked+1 {
		b.resync = true
	}
	return out, b.resync
}

// Confirm 记录决策确认的最高连续 seq（D47）：只前进不回退，乱序确认不回退续传位置。
func (b *Buffer) Confirm(seq int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if seq > b.acked {
		b.acked = seq
	}
}

// Acked 返回当前确认位点（D47）。
func (b *Buffer) Acked() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.acked
}

// ResetSent 把发送位点回到确认位点（D47）：重连建立后调用，从决策确认的 seq 之后补传。
func (b *Buffer) ResetSent() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = b.acked
}

// ResyncPending 报告 resync 标记是否置位（D47）。
func (b *Buffer) ResyncPending() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.resync
}

// ClearResync 清除 resync 标记（D47）：全量重报完成后由装配方调用。
func (b *Buffer) ClearResync() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resync = false
}

// LastSeq 返回当前 seq 高水位（D47）。
func (b *Buffer) LastSeq() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastSeq
}

// SaveWatermark 把 seq 高水位落 dataDir（D47）：临时文件写全后原子改名。
func (b *Buffer) SaveWatermark() error {
	b.mu.Lock()
	seq := b.lastSeq
	b.mu.Unlock()
	return writeWatermark(b.dataDir, seq)
}

// Run 周期落盘高水位，与段清单快照同频（D47、D49）；interval 非正时直接返回。
func (b *Buffer) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.SaveWatermark(); err != nil {
				b.log.Warn("event watermark save failed", telemetry.Fields{NodeID: b.nodeID, Reason: err.Error(), Result: "watermark_save_failed"})
			}
		}
	}
}

// Close 停机前落一次高水位（D47）。
func (b *Buffer) Close() {
	if err := b.SaveWatermark(); err != nil {
		b.log.Warn("event watermark save failed", telemetry.Fields{NodeID: b.nodeID, Reason: err.Error(), Result: "watermark_save_failed"})
	}
}

func writeWatermark(dataDir string, seq int64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(seq))
	tmp := filepath.Join(dataDir, watermarkFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open watermark temp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write watermark: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync watermark: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close watermark: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dataDir, watermarkFile)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename watermark: %w", err)
	}
	return nil
}

func loadWatermark(dataDir string) int64 {
	buf, err := os.ReadFile(filepath.Join(dataDir, watermarkFile))
	if err != nil || len(buf) != 8 {
		// 文件缺失或损坏从零开始：重放旧 seq 的重复事件会被决策按重复 seq 丢弃（D47）。
		return 0
	}
	return int64(binary.LittleEndian.Uint64(buf))
}

func layerOf(s string) api.BlockLayer {
	if s == "remote" {
		return api.BlockLayer_BLOCK_LAYER_REMOTE
	}
	return api.BlockLayer_BLOCK_LAYER_LOCAL
}
