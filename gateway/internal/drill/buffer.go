package drill

import "sync"

// Buffer 是事件环形缓冲，满时丢弃最旧一条（D22、R12）。
type Buffer struct {
	mu       sync.Mutex
	capacity int
	events   []Event
}

func NewBuffer(capacity int) *Buffer {
	if capacity < 0 {
		capacity = 0
	}
	return &Buffer{capacity: capacity}
}

// Record 非阻塞追加，容量已满时丢弃最旧一条。
func (b *Buffer) Record(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.capacity == 0 {
		return
	}
	if len(b.events) < b.capacity {
		b.events = append(b.events, e)
		return
	}
	copy(b.events, b.events[1:])
	b.events[len(b.events)-1] = e
}

// Snapshot 返回缓冲内全部事件的拷贝并清空，空缓冲返回非 nil 空切片。
func (b *Buffer) Snapshot() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]Event, len(b.events))
	copy(out, b.events)
	b.events = b.events[:0]
	return out
}
