package sticky

import "sync"

// Table 是会话到副本的映射，三个入口共用一把锁（D19）。
type Table struct {
	mu        sync.Mutex
	bySession map[string]string
}

func NewTable() *Table {
	return &Table{bySession: make(map[string]string)}
}

// Assign 写入或覆盖会话映射。
func (t *Table) Assign(session, node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bySession[session] = node
}

func (t *Table) Lookup(session string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node, ok := t.bySession[session]
	return node, ok
}

// DropNode 删除映射到该副本的全部条目（D19）。
func (t *Table) DropNode(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for session, mapped := range t.bySession {
		if mapped == node {
			delete(t.bySession, session)
		}
	}
}
