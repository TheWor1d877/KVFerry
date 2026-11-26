package index

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
)

var (
	// ErrUnknownNode 表示上报节点不在登记节点集合内（D49）。
	ErrUnknownNode = errors.New("manifest report from unregistered node")
	// ErrShardTotal 表示同一节点的总片数在片间不一致（D49）。
	ErrShardTotal = errors.New("manifest shard_total differs between shards")
	// ErrBadShardIndex 表示分片序号越界（D49）。
	ErrBadShardIndex = errors.New("manifest shard_index out of range")
)

// nodeShards 记录单节点的分片接收状态（D49）：各片可乱序到达，按片记录。
type nodeShards struct {
	total  int64
	shards map[int64]map[string]Record
}

// Rebuild 管理全量重报的逐片重建与重建窗口（D49、R32）。
type Rebuild struct {
	mu             sync.Mutex
	ix             *Index
	nodes          map[string]*nodeShards
	open           bool
	totalNodes     int
	totalShards    int64
	totalEntries   int64
	started        time.Time
	onNodeComplete func(node string)
	log            *telemetry.Logger
}

// SetNodeComplete 注册单节点重报完成回调（D49）：该节点全部片收齐替换登记后调用；
// 回调在重建锁内执行，不得重入 Shard。
func (rb *Rebuild) SetNodeComplete(fn func(node string)) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.onNodeComplete = fn
}

// Enqueue 把节点纳入重报接收集合（D47、D49）：resync 触发的单节点重报与进程重启的
// 全量重建共用逐片接收路径；节点已在集合内（重建窗口收集中）时不动。
func (rb *Rebuild) Enqueue(node string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if _, ok := rb.nodes[node]; !ok {
		rb.nodes[node] = &nodeShards{shards: make(map[int64]map[string]Record)}
	}
}

// StartRebuild 开启重建窗口（D49）：登记节点集合固定，全部节点收齐后窗口才关闭。
// 日志器由构造方注入，未注入时用默认 stdout 日志器；重建开始打点记录节点数。
func StartRebuild(ix *Index, nodes []string, logs ...*telemetry.Logger) *Rebuild {
	log := telemetry.New()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	rb := &Rebuild{
		ix:         ix,
		nodes:      make(map[string]*nodeShards, len(nodes)),
		open:       true,
		totalNodes: len(nodes),
		started:    time.Now(),
		log:        log,
	}
	for _, node := range nodes {
		rb.nodes[node] = &nodeShards{shards: make(map[int64]map[string]Record)}
	}
	log.Info("rebuild started", telemetry.Fields{
		NodeID: "decision",
		Nodes:  int64(len(nodes)),
		Shards: 0,
		Result: "window_open",
	})
	return rb
}

// Shard 接收某节点某片（D49）：片数据按片暂存，节点全部片收齐后整体替换该节点登记；
// 全部登记节点的最后一片处理完才关闭重建窗口，部分节点先就绪不提前开闸（R32）。
// 分片接收打点记录 node_id、分片序号、条目数与耗时；最后一片关窗时打重建完成打点。
func (rb *Rebuild) Shard(node string, shardIndex, shardTotal int64, records map[string]Record) error {
	received := time.Now()
	rb.mu.Lock()
	defer rb.mu.Unlock()
	ns, ok := rb.nodes[node]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownNode, node)
	}
	if shardIndex < 0 || shardIndex >= shardTotal {
		return fmt.Errorf("%w: node %s index %d of %d", ErrBadShardIndex, node, shardIndex, shardTotal)
	}
	if ns.total == 0 {
		ns.total = shardTotal
	} else if ns.total != shardTotal {
		return fmt.Errorf("%w: node %s want %d got %d", ErrShardTotal, node, ns.total, shardTotal)
	}
	ns.shards[shardIndex] = records
	rb.totalShards++
	rb.totalEntries += int64(len(records))
	rb.log.Info("manifest shard received", telemetry.Fields{
		TraceID:    telemetry.NewTraceID(node, fmt.Sprintf("%d", shardIndex), time.Now().String()),
		NodeID:     node,
		ShardIndex: shardIndex,
		Entries:    int64(len(records)),
		DurationMs: time.Since(received).Milliseconds(),
		Result:     "shard_accepted",
	})
	if int64(len(ns.shards)) < ns.total {
		return nil
	}
	merged := make(map[string]Record)
	for _, shard := range ns.shards {
		for hash, r := range shard {
			merged[hash] = r
		}
	}
	rb.ix.Replace(node, merged)
	delete(rb.nodes, node)
	if rb.onNodeComplete != nil {
		rb.onNodeComplete(node)
	}
	if len(rb.nodes) == 0 {
		rb.open = false
		rb.log.Info("rebuild completed", telemetry.Fields{
			NodeID:     "decision",
			Nodes:      int64(rb.totalNodes),
			Shards:     rb.totalShards,
			Entries:    rb.totalEntries,
			DurationMs: time.Since(rb.started).Milliseconds(),
			Result:     "window_closed",
		})
	}
	return nil
}

// Open 报告重建窗口是否开着（D49）。
func (rb *Rebuild) Open() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.open
}
