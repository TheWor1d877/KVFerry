package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// LRU 是 DRAM 读缓存，按块哈希做键，容量按字节计（D42）。
// 命中移队首，容量满淘汰队尾；容量取 0 时整块关闭，所有读直接走 NVMe，命中计数恒为零。
type LRU struct {
	mu       sync.Mutex
	capBytes int64
	curBytes int64
	ll       *list.List
	entries  map[string]*list.Element
	hits     atomic.Int64
}

type entry struct {
	hash string
	data []byte
}

// New 建立读缓存，capBytes 取 0 时缓存关闭（D42）。
func New(capBytes int64) *LRU {
	return &LRU{
		capBytes: capBytes,
		ll:       list.New(),
		entries:  make(map[string]*list.Element),
	}
}

// Get 返回缓存命中结果；命中把该键移到队首并累加命中计数。
// 返回的切片与缓存内部共享，调用方只读不得修改。
// 关闭状态下恒未命中（D42）。
func (c *LRU) Get(hash string) ([]byte, bool) {
	if c.capBytes <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[hash]; ok {
		c.ll.MoveToFront(el)
		c.hits.Add(1)
		return el.Value.(*entry).data, true
	}
	return nil, false
}

// Put 写入缓存；容量满时从队尾淘汰，单块超过总容量的块不缓存（D42）。
// 关闭状态下为空操作。
func (c *LRU) Put(hash string, data []byte) {
	if c.capBytes <= 0 || int64(len(data)) > c.capBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[hash]; ok {
		old := el.Value.(*entry)
		c.curBytes += int64(len(data) - len(old.data))
		old.data = data
		c.ll.MoveToFront(el)
	} else {
		el := c.ll.PushFront(&entry{hash: hash, data: data})
		c.entries[hash] = el
		c.curBytes += int64(len(data))
	}
	for c.curBytes > c.capBytes {
		tail := c.ll.Back()
		if tail == nil {
			break
		}
		e := tail.Value.(*entry)
		c.ll.Remove(tail)
		delete(c.entries, e.hash)
		c.curBytes -= int64(len(e.data))
	}
}

// Hits 返回命中计数；关闭状态下恒为零（D42）。
func (c *LRU) Hits() int64 {
	return c.hits.Load()
}

// Len 返回缓存条目数。
func (c *LRU) Len() int {
	if c.capBytes <= 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Bytes 返回当前缓存字节数。
func (c *LRU) Bytes() int64 {
	if c.capBytes <= 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}
