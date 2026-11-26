package remote

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// virtualNodesPerNode 是每个物理节点在环上的虚拟节点数（D50）：散列键降低节点数少时的倾斜。
const virtualNodesPerNode = 160

// Ring 是远端块服务一致性哈希环（D50）：节点按 remote_nodes 列表建环，块哈希经哈希落到
// 环上一个节点，同键永远收敛同一处。环成员只在启动时构建一次，键到节点归属在进程
// 生命周期内固定；扩缩容靠重启全部远端节点重建同一个环，不提供在线变更路径（R27）。
type Ring struct {
	mu    sync.RWMutex
	nodes []string
	keys  []uint64
	owner map[uint64]string
	log   *telemetry.Logger
}

// NewRing 按节点列表建环（D50、R27）：构造即打一条环重建日志，节点数记入 node_count，
// 耗时记环构建耗时。环成员只在启动时经本函数构建，导出面不提供在线变更路径（R27）。
func NewRing(nodes []string, log *telemetry.Logger) *Ring {
	if log == nil {
		log = telemetry.New()
	}
	start := time.Now()
	r := &Ring{log: log, owner: make(map[uint64]string)}
	r.buildLocked(nodes)
	r.log.Info("remote ring rebuilt", telemetry.Fields{
		NodesCount: len(nodes),
		DurationMs: time.Since(start).Milliseconds(),
		Result:     "rebuilt",
	})
	return r
}

// rebuild 以新节点列表整环重建（D50、R27）：未导出，语义限定为测试模拟全部远端节点
// 重启后的整环一致重建；导出面不提供在线变更路径，成员变化只靠重启全部远端节点。
func (r *Ring) rebuild(nodes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	start := time.Now()
	r.buildLocked(nodes)
	r.log.Info("remote ring rebuilt", telemetry.Fields{
		NodesCount: len(nodes),
		DurationMs: time.Since(start).Milliseconds(),
		Result:     "rebuilt",
	})
}

// buildLocked 重建虚拟节点哈希序与归属表，调用方须持写锁。
func (r *Ring) buildLocked(nodes []string) {
	r.nodes = append([]string(nil), nodes...)
	r.keys = make([]uint64, 0, len(nodes)*virtualNodesPerNode)
	r.owner = make(map[uint64]string, len(nodes)*virtualNodesPerNode)
	for _, n := range r.nodes {
		for i := 0; i < virtualNodesPerNode; i++ {
			h := hashKey(fmt.Sprintf("%s#%d", n, i))
			r.keys = append(r.keys, h)
			r.owner[h] = n
		}
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// Pick 按块哈希返回归属节点（D50）；空环返回空串。
func (r *Ring) Pick(blockHash string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.keys) == 0 {
		return ""
	}
	h := hashKey(blockHash)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}
	return r.owner[r.keys[i]]
}

// Nodes 返回环成员列表副本（D50）。
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.nodes...)
}

// hashKey 取字节串的 SHA256 前 8 字节为大端 uint64，环上虚拟节点键与块键共用同一散列。
func hashKey(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
