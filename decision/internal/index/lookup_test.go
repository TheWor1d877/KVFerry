package index

import (
	"testing"

	"github.com/TheWor1d877/KVFerry/storage/api"
)

// skipState 是节点状态注入桩（D51）：按集合决定节点是否跳过。
type skipState struct {
	skipped map[string]bool
}

func (s *skipState) Skip(node string) bool { return s.skipped[node] }

// TestHitReturnsLayerAndOffset 覆盖命中返回层位与偏移（D51、D53）。
func TestHitReturnsLayerAndOffset(t *testing.T) {
	ix := New()
	ix.Insert("node-1", "hash-a", Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 227, Length: 64})
	q := NewQuerier(ix, BuildForm(baseFormConfig()), func() bool { return false }, nil)
	rec, ok := q.Hit("hash-a", baseSpec())
	if !ok {
		t.Fatal("hit = miss, want hit")
	}
	if rec.Layer != api.BlockLayer_BLOCK_LAYER_LOCAL || rec.Offset != 227 || rec.Length != 64 || rec.Node != "node-1" {
		t.Errorf("record = %+v, want local/227/64/node-1", rec)
	}
}

// TestHitSkipsUnreachableNode 覆盖失联节点跳过，登记保留（D51）。
func TestHitSkipsUnreachableNode(t *testing.T) {
	ix := New()
	ix.Insert("node-1", "hash-a", Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64})
	state := &skipState{skipped: map[string]bool{"node-1": true}}
	q := NewQuerier(ix, BuildForm(baseFormConfig()), func() bool { return false }, state)
	if _, ok := q.Hit("hash-a", baseSpec()); ok {
		t.Error("hit on skipped node, want miss")
	}
	state.skipped["node-1"] = false
	if _, ok := q.Hit("hash-a", baseSpec()); !ok {
		t.Error("hit after node recovered, want hit")
	}
}

// TestMissReturnsEmpty 覆盖未命中返回空（D53）。
func TestMissReturnsEmpty(t *testing.T) {
	ix := New()
	q := NewQuerier(ix, BuildForm(baseFormConfig()), func() bool { return false }, nil)
	rec, ok := q.Hit("missing", baseSpec())
	if ok {
		t.Error("hit on empty index, want miss")
	}
	if rec != (Record{}) {
		t.Errorf("record on miss = %+v, want zero", rec)
	}
}

// TestCrossSpecMiss 覆盖跨口径块按未命中（D53）。
func TestCrossSpecMiss(t *testing.T) {
	ix := New()
	ix.Insert("node-1", "hash-a", Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64})
	q := NewQuerier(ix, BuildForm(baseFormConfig()), func() bool { return false }, nil)
	other := baseSpec()
	other.KVPrecision = "bf16"
	if _, ok := q.Hit("hash-a", other); ok {
		t.Error("hit with different spec, want miss")
	}
}
