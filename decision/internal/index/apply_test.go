package index

import (
	"testing"

	"github.com/TheWor1d877/KVFerry/storage/api"
)

func ev(kind, node string, seq int64, hash string, layer api.BlockLayer) *api.StorageEvent {
	return &api.StorageEvent{
		Kind:      kind,
		NodeId:    node,
		Seq:       seq,
		BlockHash: hash,
		Layer:     layer,
	}
}

// TestFiveKindsUpdateIndex 覆盖五类事件更新索引映射（D47）：
// register 插入，demote 更新层位，evict 只注销与事件 layer 同层的登记，
// offline 与 oversized 无登记变化。
func TestFiveKindsUpdateIndex(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	if !ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("register must be applied")
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("hash-a missing after register")
	}
	if !ap.Apply(ev(KindDemote, "node-1", 2, "hash-a", api.BlockLayer_BLOCK_LAYER_REMOTE)) {
		t.Fatal("demote must be applied")
	}
	r, ok := ix.Lookup("node-1", "hash-a")
	if !ok || r.Layer != api.BlockLayer_BLOCK_LAYER_REMOTE {
		t.Fatalf("after demote record = %+v, want remote layer", r)
	}
	if !ap.Apply(ev(KindOffline, "node-1", 3, "", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("offline must be processed")
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("offline must not change registration")
	}
	if !ap.Apply(ev(KindOversized, "node-1", 4, "hash-b", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("oversized must be processed")
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); ok {
		t.Fatal("oversized must not create registration")
	}
	if !ap.Apply(ev(KindRegister, "node-1", 5, "hash-c", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("register must be applied")
	}
	if !ap.Apply(ev(KindEvict, "node-1", 6, "hash-c", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("evict must be applied")
	}
	if _, ok := ix.Lookup("node-1", "hash-c"); ok {
		t.Fatal("hash-c must be removed after local evict")
	}
	// hash-a 登记已是远端，本机段回收的 evict(local) 不注销远端登记（D50）。
	if !ap.Apply(ev(KindEvict, "node-1", 7, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("evict must be applied")
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("remote registration must survive local evict")
	}
	if got := ap.Applied("node-1"); got != 7 {
		t.Fatalf("applied = %d, want 7", got)
	}
}

// TestEvictDifferentLayerKeepsRecord 覆盖 evict 只注销与事件 layer 同层的登记（D47、D50）：
// demote(remote) 把登记层位转远端后，本机段回收的 evict(local) 保留远端登记，查询仍命中。
func TestEvictDifferentLayerKeepsRecord(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	if !ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("register must be applied")
	}
	if !ap.Apply(ev(KindDemote, "node-1", 2, "hash-a", api.BlockLayer_BLOCK_LAYER_REMOTE)) {
		t.Fatal("demote must be applied")
	}
	if !ap.Apply(ev(KindEvict, "node-1", 3, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("local evict must be applied")
	}
	r, ok := ix.Lookup("node-1", "hash-a")
	if !ok {
		t.Fatal("remote registration must survive local segment evict")
	}
	if r.Layer != api.BlockLayer_BLOCK_LAYER_REMOTE {
		t.Errorf("layer = %v, want remote", r.Layer)
	}
}

// TestEvictSameLayerDeletesRecord 覆盖普通本机驱逐删 local 登记（D47，防回归）。
func TestEvictSameLayerDeletesRecord(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	if !ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("register must be applied")
	}
	if !ap.Apply(ev(KindEvict, "node-1", 2, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("evict must be applied")
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); ok {
		t.Fatal("local registration must be deleted by local evict")
	}
}

// TestDuplicateSeqIdempotent 覆盖重复 seq 幂等：seq 不大于已应用位置直接丢弃（D47）。
func TestDuplicateSeqIdempotent(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	if !ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("first register must be applied")
	}
	if ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("duplicate seq must be dropped")
	}
	if ap.Apply(ev(KindRegister, "node-1", 0, "hash-b", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("seq below applied position must be dropped")
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); ok {
		t.Fatal("dropped event must not create registration")
	}
	if got := ap.Applied("node-1"); got != 1 {
		t.Fatalf("applied = %d, want 1", got)
	}
}

// TestGapMarksResync 覆盖跳号置 resync 并等待全量重报（D47）：
// resync 期间新事件照常接收按待重建处理，重报收齐后经 MarkRebuilt 恢复映射更新。
func TestGapMarksResync(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL))
	if !ap.Apply(ev(KindRegister, "node-1", 5, "hash-b", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("gap event must be received")
	}
	if !ap.Resync("node-1") {
		t.Fatal("gap must set resync")
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); ok {
		t.Fatal("event during resync must not update registration")
	}
	if got := ap.Applied("node-1"); got != 5 {
		t.Fatalf("applied during resync = %d, want 5", got)
	}
	if !ap.Apply(ev(KindEvict, "node-1", 6, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL)) {
		t.Fatal("event during resync must be received")
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("event during resync must not update registration")
	}
	ap.MarkRebuilt("node-1")
	if ap.Resync("node-1") {
		t.Fatal("resync must be cleared after rebuild")
	}
	ap.Apply(ev(KindRegister, "node-1", 7, "hash-c", api.BlockLayer_BLOCK_LAYER_LOCAL))
	if _, ok := ix.Lookup("node-1", "hash-c"); !ok {
		t.Fatal("event after rebuild must update registration")
	}
	// resync 期间挂起的 evict 未应用，hash-a 保留到重报替换登记（D47）。
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("suspended evict must not have been applied during resync")
	}
}

// TestSeparateNodesTrackedIndependently 覆盖按节点分片推进 seq（D47、R29）。
func TestSeparateNodesTrackedIndependently(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	ap.Apply(ev(KindRegister, "node-1", 3, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL))
	ap.Apply(ev(KindRegister, "node-2", 1, "hash-b", api.BlockLayer_BLOCK_LAYER_LOCAL))
	if got := ap.Applied("node-1"); got != 3 {
		t.Errorf("node-1 applied = %d, want 3", got)
	}
	if got := ap.Applied("node-2"); got != 1 {
		t.Errorf("node-2 applied = %d, want 1", got)
	}
	if ap.Resync("node-2") {
		t.Error("node-2 must not be in resync")
	}
}

// TestResyncFlagEventPutsNodeIntoResync 覆盖带 resync 标志的事件把节点置入待重建（D47）：
// 标志事件只推进确认位不更新登记，重报收齐经 MarkRebuilt 后恢复映射更新。
func TestResyncFlagEventPutsNodeIntoResync(t *testing.T) {
	ix := New()
	ap := NewApplier(ix)
	ap.Apply(ev(KindRegister, "node-1", 1, "hash-a", api.BlockLayer_BLOCK_LAYER_LOCAL))
	if ap.Resync("node-1") {
		t.Fatal("node must not be in resync before flagged event")
	}
	if !ap.Apply(&api.StorageEvent{Kind: KindRegister, NodeId: "node-1", Seq: 2, BlockHash: "hash-b", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Resync: true}) {
		t.Fatal("flagged event must be received")
	}
	if !ap.Resync("node-1") {
		t.Fatal("flagged event must put node into resync")
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); ok {
		t.Fatal("flagged event must not update registration")
	}
	ap.MarkRebuilt("node-1")
	ap.Apply(ev(KindRegister, "node-1", 3, "hash-c", api.BlockLayer_BLOCK_LAYER_LOCAL))
	if _, ok := ix.Lookup("node-1", "hash-c"); !ok {
		t.Fatal("event after rebuild must update registration")
	}
}
