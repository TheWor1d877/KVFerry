package index

import (
	"sync"

	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
	"github.com/TheWor1d877/KVFerry/storage/api"
)

// 事件类别取值（D47），与节点侧 kind 字符串一致。
const (
	KindRegister  = "register"
	KindDemote    = "demote"
	KindEvict     = "evict"
	KindOffline   = "offline"
	KindOversized = "oversized"
)

// Applier 是事件应用器（D47）：事件按节点按 seq 单调推进，seq 不大于已应用位置直接丢弃；
// 跳号置 resync 并等待节点全量重报，重报期间该节点新事件照常接收按待重建处理，
// 重报收齐替换登记后经 MarkRebuilt 清除 resync 恢复映射更新。
type Applier struct {
	mu      sync.Mutex
	ix      *Index
	applied map[string]int64
	resync  map[string]bool
	log     *telemetry.Logger
}

// NewApplier 建立事件应用器（D47）。
func NewApplier(ix *Index, logs ...*telemetry.Logger) *Applier {
	log := telemetry.New()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	return &Applier{
		ix:      ix,
		applied: make(map[string]int64),
		resync:  make(map[string]bool),
		log:     log,
	}
}

// Apply 应用一条节点事件（D47）：返回该事件是否按新 seq 处理，重复 seq 返回 false。
// 应用规则：register 插入登记，demote 更新层位，evict 只注销与事件 layer 同层的登记，
// offline 与 oversized 无登记变化（D46、D50）；块哈希为空时只推进确认位不动映射。
func (a *Applier) Apply(ev *api.StorageEvent) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	node := ev.GetNodeId()
	seq := ev.GetSeq()
	if node == "" || seq <= 0 {
		return false
	}
	last := a.applied[node]
	if seq <= last {
		return false
	}
	if seq > last+1 {
		a.resync[node] = true
	}
	if ev.GetResync() {
		// 节点缓冲不足或中间事件丢失时出网事件带 resync 标志，决策置该节点待重建，
		// 等该节点全量重报逐片收齐后经 MarkRebuilt 恢复（D47）。
		a.resync[node] = true
	}
	a.applied[node] = seq
	if a.resync[node] {
		return true
	}
	switch ev.GetKind() {
	case KindRegister:
		a.ix.Insert(node, ev.GetBlockHash(), Record{Layer: ev.GetLayer()})
	case KindDemote:
		if r, ok := a.ix.Lookup(node, ev.GetBlockHash()); ok {
			r.Layer = ev.GetLayer()
			a.ix.Insert(node, ev.GetBlockHash(), r)
		}
	case KindEvict:
		// evict 只注销与事件 layer 同层的登记（D47）：本机段回收的 evict(local)
		// 不注销已下沉的远端登记（D50）；普通本机驱逐删 local 登记行为不变。
		if r, ok := a.ix.Lookup(node, ev.GetBlockHash()); ok && r.Layer == ev.GetLayer() {
			a.ix.Delete(node, ev.GetBlockHash())
		}
	case KindOffline, KindOversized:
		// 失联由状态机摘调度池，超限拒收未产生块（D46、D51）。
	}
	return true
}

// Applied 返回节点已应用 seq 位，作为流确认的最高连续序号（D41、D47）。
func (a *Applier) Applied(node string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applied[node]
}

// Resync 报告节点是否处于 resync 待重建状态（D47）。
func (a *Applier) Resync(node string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.resync[node]
}

// MarkRebuilt 清除节点 resync（D47）：全量重报逐片收齐替换登记后调用，恢复映射更新。
func (a *Applier) MarkRebuilt(node string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.resync, node)
}
