package cache

import "testing"

func TestEvictionOrder(t *testing.T) {
	lru := New(100)
	a := make([]byte, 40)
	b := make([]byte, 40)
	c := make([]byte, 40)
	lru.Put("a", a)
	lru.Put("b", b)
	lru.Put("c", c)
	if lru.Len() != 2 {
		t.Errorf("len = %d, want 2 after eviction", lru.Len())
	}
	if _, ok := lru.Get("a"); ok {
		t.Error("oldest entry a should be evicted first")
	}
	if _, ok := lru.Get("b"); !ok {
		t.Error("entry b should remain")
	}
	if _, ok := lru.Get("c"); !ok {
		t.Error("entry c should remain")
	}
}

func TestHitPromotes(t *testing.T) {
	lru := New(100)
	a := make([]byte, 40)
	b := make([]byte, 40)
	c := make([]byte, 40)
	lru.Put("a", a)
	lru.Put("b", b)
	if _, ok := lru.Get("a"); !ok {
		t.Fatal("a should hit")
	}
	lru.Put("c", c)
	if _, ok := lru.Get("a"); !ok {
		t.Error("promoted a should survive")
	}
	if _, ok := lru.Get("b"); ok {
		t.Error("unpromoted b should be evicted")
	}
	if lru.Hits() != 2 {
		t.Errorf("hits = %d, want 2", lru.Hits())
	}
}

func TestCapacityZeroDisabled(t *testing.T) {
	lru := New(0)
	lru.Put("a", []byte{1, 2, 3})
	if _, ok := lru.Get("a"); ok {
		t.Error("disabled cache should never hit")
	}
	if lru.Hits() != 0 {
		t.Errorf("hits = %d, want 0 when disabled", lru.Hits())
	}
	if lru.Len() != 0 {
		t.Errorf("len = %d, want 0 when disabled", lru.Len())
	}
	if lru.Bytes() != 0 {
		t.Errorf("bytes = %d, want 0 when disabled", lru.Bytes())
	}
}

func TestOversizedEntryNotCached(t *testing.T) {
	lru := New(8)
	lru.Put("big", make([]byte, 64))
	if lru.Len() != 0 {
		t.Errorf("entry bigger than capacity should not be cached, len = %d", lru.Len())
	}
	lru.Put("ok", make([]byte, 8))
	if lru.Len() != 1 {
		t.Errorf("len = %d, want 1", lru.Len())
	}
}

func TestUpdateExistingKey(t *testing.T) {
	lru := New(100)
	lru.Put("a", make([]byte, 40))
	lru.Put("a", make([]byte, 60))
	if lru.Len() != 1 {
		t.Errorf("len = %d, want 1", lru.Len())
	}
	if lru.Bytes() != 60 {
		t.Errorf("bytes = %d, want 60", lru.Bytes())
	}
	got, ok := lru.Get("a")
	if !ok || len(got) != 60 {
		t.Errorf("get = len %d ok %v, want 60 true", len(got), ok)
	}
}
