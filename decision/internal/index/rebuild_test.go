package index

import (
	"errors"
	"testing"

	"github.com/TheWor1d877/KVFerry/storage/api"
)

func baseSpec() Spec {
	return Spec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

func baseFormConfig() FormConfig {
	return FormConfig{
		KVShape:          "3d",
		BytesPerToken:    8,
		ChunkedRetrieval: true,
		MigrationUnit:    64,
		Spec:             baseSpec(),
	}
}

// TestInsertDeleteLookup 覆盖插入与删除登记后查询结果一致，跨节点同键多份各记各的（R29、D53）。
func TestInsertDeleteLookup(t *testing.T) {
	ix := New()
	rec := Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64}
	ix.Insert("node-1", "hash-a", rec)
	got, ok := ix.Lookup("node-1", "hash-a")
	if !ok {
		t.Fatal("lookup after insert = not found")
	}
	if got.Offset != 71 || got.Length != 64 || got.Node != "node-1" {
		t.Errorf("record = %+v, want offset 71 length 64 node node-1", got)
	}
	if got.Layer != api.BlockLayer_BLOCK_LAYER_LOCAL {
		t.Errorf("layer = %v, want local", got.Layer)
	}

	// 跨节点同键多份各记各的：另一节点的同键登记独立存在。
	ix.Insert("node-2", "hash-a", Record{Layer: api.BlockLayer_BLOCK_LAYER_REMOTE, Offset: 999, Length: 64})
	if got, ok := ix.Lookup("node-1", "hash-a"); !ok || got.Offset != 71 {
		t.Errorf("node-1 record = %+v ok=%v, want offset 71", got, ok)
	}
	if got, ok := ix.Lookup("node-2", "hash-a"); !ok || got.Offset != 999 {
		t.Errorf("node-2 record = %+v ok=%v, want offset 999", got, ok)
	}

	ix.Delete("node-1", "hash-a")
	if _, ok := ix.Lookup("node-1", "hash-a"); ok {
		t.Error("lookup after delete = found, want miss")
	}
	if _, ok := ix.Lookup("node-2", "hash-a"); !ok {
		t.Error("node-2 record must survive node-1 delete")
	}
	if ix.Count("node-1") != 0 {
		t.Errorf("node-1 count = %d, want 0", ix.Count("node-1"))
	}
}

// TestReplaceReplacesNodeShard 覆盖按节点整体替换登记（D49）。
func TestReplaceReplacesNodeShard(t *testing.T) {
	ix := New()
	ix.Insert("node-1", "hash-old", Record{Offset: 1, Length: 64})
	ix.Replace("node-1", map[string]Record{
		"hash-new": {Offset: 71, Length: 64},
	})
	if _, ok := ix.Lookup("node-1", "hash-old"); ok {
		t.Error("old record must be gone after replace")
	}
	if _, ok := ix.Lookup("node-1", "hash-new"); !ok {
		t.Error("new record missing after replace")
	}
}

// TestFormRebuildConsistency 覆盖注入同一份引擎配置后形态索引重建一致（D53）。
func TestFormRebuildConsistency(t *testing.T) {
	cfg := baseFormConfig()
	f1 := BuildForm(cfg)
	f2 := BuildForm(cfg)
	if !f1.Config().Equal(f2.Config()) {
		t.Errorf("rebuild configs differ: %+v vs %+v", f1.Config(), f2.Config())
	}
	if !f1.SameSpec(baseSpec()) {
		t.Error("same spec should match")
	}
	other := baseSpec()
	other.KVPrecision = "bf16"
	if f1.SameSpec(other) {
		t.Error("different spec should not match")
	}
	changed := baseFormConfig()
	changed.ChunkedRetrieval = false
	if BuildForm(changed).Config().Equal(f1.Config()) {
		t.Error("different config should rebuild to different form")
	}
}

// TestShardRebuild 覆盖全量重报逐片重建：乱序到达按片记录，节点收齐后替换登记，
// 全部节点收齐后才关闭重建窗口（D49、R32）。
func TestShardRebuild(t *testing.T) {
	ix := New()
	rb := StartRebuild(ix, []string{"node-1", "node-2"})
	if !rb.Open() {
		t.Fatal("window should be open right after start")
	}

	// node-1 片 1 先到（乱序），窗口仍开，登记未替换。
	err := rb.Shard("node-1", 1, 2, map[string]Record{"hash-b": {Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 227, Length: 64}})
	if err != nil {
		t.Fatalf("shard 1: %v", err)
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); ok {
		t.Error("single out-of-order shard must not replace registration yet")
	}
	if !rb.Open() {
		t.Error("window must stay open with node-2 pending")
	}

	// node-1 片 0 到达，收齐后替换该节点登记，窗口仍开（node-2 未收齐）。
	err = rb.Shard("node-1", 0, 2, map[string]Record{"hash-a": {Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64}})
	if err != nil {
		t.Fatalf("shard 0: %v", err)
	}
	if got, ok := ix.Lookup("node-1", "hash-a"); !ok || got.Offset != 71 {
		t.Errorf("node-1 record after complete = %+v ok=%v, want offset 71", got, ok)
	}
	if !rb.Open() {
		t.Error("window must stay open until every node completes")
	}

	// node-2 单片到齐，全部节点收齐后窗口才关闭。
	err = rb.Shard("node-2", 0, 1, map[string]Record{})
	if err != nil {
		t.Fatalf("node-2 shard: %v", err)
	}
	if rb.Open() {
		t.Error("window must close after all nodes complete")
	}
}

// TestShardRebuildErrors 覆盖未知节点、总片数不一致与序号越界的拒绝（D49）。
func TestShardRebuildErrors(t *testing.T) {
	ix := New()
	rb := StartRebuild(ix, []string{"node-1"})
	if err := rb.Shard("node-x", 0, 1, nil); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("unknown node err = %v, want ErrUnknownNode", err)
	}
	if err := rb.Shard("node-1", 0, 2, nil); err != nil {
		t.Fatalf("first shard: %v", err)
	}
	if err := rb.Shard("node-1", 1, 3, nil); !errors.Is(err, ErrShardTotal) {
		t.Errorf("total mismatch err = %v, want ErrShardTotal", err)
	}
	if err := rb.Shard("node-1", 2, 2, nil); !errors.Is(err, ErrBadShardIndex) {
		t.Errorf("index out of range err = %v, want ErrBadShardIndex", err)
	}
}

// TestRebuildWindowLookupMiss 覆盖重建窗口内命中查询一律未命中（D53、R32）。
func TestRebuildWindowLookupMiss(t *testing.T) {
	ix := New()
	ix.Insert("node-1", "hash-a", Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64})
	rb := StartRebuild(ix, []string{"node-1"})
	q := NewQuerier(ix, BuildForm(baseFormConfig()), rb.Open, nil)
	if _, ok := q.Hit("hash-a", baseSpec()); ok {
		t.Error("hit inside rebuild window, want miss")
	}
	if err := rb.Shard("node-1", 0, 1, map[string]Record{"hash-a": {Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64}}); err != nil {
		t.Fatalf("shard: %v", err)
	}
	if _, ok := q.Hit("hash-a", baseSpec()); !ok {
		t.Error("hit after window closed, want hit")
	}
}

// TestEnqueueAcceptsResyncRereport 覆盖 resync 触发的单节点重报：窗口关闭后经 Enqueue
// 纳入接收集合，逐片收齐替换登记并触发节点完成回调（D47、D49）。
func TestEnqueueAcceptsResyncRereport(t *testing.T) {
	ix := New()
	rb := StartRebuild(ix, []string{"node-0"})
	if err := rb.Shard("node-0", 0, 1, map[string]Record{}); err != nil {
		t.Fatalf("close window: %v", err)
	}
	if rb.Open() {
		t.Fatal("window must be closed before resync rereport")
	}
	completed := ""
	rb.SetNodeComplete(func(node string) { completed = node })
	rb.Enqueue("node-1")
	if err := rb.Shard("node-1", 0, 1, map[string]Record{"hash-a": {Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64}}); err != nil {
		t.Fatalf("shard after enqueue: %v", err)
	}
	if completed != "node-1" {
		t.Fatalf("node complete callback = %q, want node-1", completed)
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("registration must be replaced after resync rereport")
	}
}
