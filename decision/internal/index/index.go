package index

import (
	"sort"
	"sync"

	"github.com/TheWor1d877/KVFerry/storage/api"
)

// Record 是登记条目，四项见 D53：层、节点、偏移与长度。
type Record struct {
	Layer  api.BlockLayer
	Node   string
	Offset int64
	Length int64
}

// Index 是块哈希到登记的映射，按节点分片保存，跨节点同键多份各记各的（R29、D53）。
type Index struct {
	mu    sync.RWMutex
	nodes map[string]map[string]Record
}

// New 建立空索引（D53）。
func New() *Index {
	return &Index{nodes: make(map[string]map[string]Record)}
}

// Insert 在节点登记内插入或覆盖一条（D53）。
func (ix *Index) Insert(node, hash string, r Record) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	shard := ix.nodes[node]
	if shard == nil {
		shard = make(map[string]Record)
		ix.nodes[node] = shard
	}
	r.Node = node
	shard[hash] = r
}

// Delete 删除节点登记内的一条，不存在时无动作（D53）。
func (ix *Index) Delete(node, hash string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if shard := ix.nodes[node]; shard != nil {
		delete(shard, hash)
	}
}

// Replace 整体替换节点登记（D49）：全量重报分片收齐后调用。
func (ix *Index) Replace(node string, records map[string]Record) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	shard := make(map[string]Record, len(records))
	for hash, r := range records {
		r.Node = node
		shard[hash] = r
	}
	ix.nodes[node] = shard
}

// Lookup 返回节点登记内的一条（D53）。
func (ix *Index) Lookup(node, hash string) (Record, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	r, ok := ix.nodes[node][hash]
	return r, ok
}

// Nodes 返回有登记分片的节点名升序（D53）。
func (ix *Index) Nodes() []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]string, 0, len(ix.nodes))
	for node := range ix.nodes {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// Count 返回节点登记条数，节点无分片返回零（D53）。
func (ix *Index) Count(node string) int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.nodes[node])
}

// Hashes 返回节点登记的块哈希升序，供心跳对账做注销差集（D54）。
func (ix *Index) Hashes(node string) []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]string, 0, len(ix.nodes[node]))
	for hash := range ix.nodes[node] {
		out = append(out, hash)
	}
	sort.Strings(out)
	return out
}
