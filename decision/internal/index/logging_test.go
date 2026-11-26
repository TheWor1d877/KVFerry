package index

import (
	"strings"
	"sync"
	"testing"

	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
	"github.com/TheWor1d877/KVFerry/storage/api"
)

type capturedLine struct {
	level string
	msg   string
	f     telemetry.Fields
	line  string
}

// logCapture 用日志钩子捕获全部打点，供四处打点断言（D49、D53）。
type logCapture struct {
	mu    sync.Mutex
	lines []capturedLine
}

func (c *logCapture) hook(level, msg string, f telemetry.Fields, line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, capturedLine{level: level, msg: msg, f: f, line: string(line)})
}

func (c *logCapture) byMsg(msg string) []capturedLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []capturedLine
	for _, l := range c.lines {
		if l.msg == msg {
			out = append(out, l)
		}
	}
	return out
}

func TestRebuildLoggingCoversFourPoints(t *testing.T) {
	capture := &logCapture{}
	log := telemetry.NewWithHook(capture.hook)
	spec := Spec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}

	// 第一处：重建开始，记录节点数。
	ix := New()
	rb := StartRebuild(ix, []string{"node-1", "node-2"}, log)
	started := capture.byMsg("rebuild started")
	if len(started) != 1 {
		t.Fatalf("rebuild started logs = %d, want 1", len(started))
	}
	if started[0].f.NodeID != "decision" || started[0].f.Nodes != 2 {
		t.Errorf("started fields = node_id %q nodes %d, want decision/2", started[0].f.NodeID, started[0].f.Nodes)
	}

	// 第二处：分片接收，乱序两条，含 0 号片。
	if err := rb.Shard("node-1", 1, 2, map[string]Record{
		"hash-b": {Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 298, Length: 64},
	}); err != nil {
		t.Fatalf("shard 1: %v", err)
	}
	if err := rb.Shard("node-1", 0, 2, map[string]Record{
		"hash-a": {Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64},
	}); err != nil {
		t.Fatalf("shard 0: %v", err)
	}
	shards := capture.byMsg("manifest shard received")
	if len(shards) != 2 {
		t.Fatalf("shard received logs = %d, want 2", len(shards))
	}
	if shards[0].f.NodeID != "node-1" || shards[0].f.ShardIndex != 1 || shards[0].f.Entries != 1 {
		t.Errorf("shard fields = node_id %q shard_index %d entries %d, want node-1/1/1",
			shards[0].f.NodeID, shards[0].f.ShardIndex, shards[0].f.Entries)
	}
	if !strings.Contains(shards[1].line, `"shard_index":0`) {
		t.Errorf("first shard JSON line must carry shard_index 0, line = %s", shards[1].line)
	}
	if shards[1].f.NodeID != "node-1" || shards[1].f.Entries != 1 {
		t.Errorf("shard fields = node_id %q entries %d, want node-1/1", shards[1].f.NodeID, shards[1].f.Entries)
	}

	// 第三处：最后一片收齐关窗，记录节点数、分片数与条目数。
	if err := rb.Shard("node-2", 0, 1, map[string]Record{}); err != nil {
		t.Fatalf("node-2 shard: %v", err)
	}
	if rb.Open() {
		t.Error("window must close after all nodes complete")
	}
	completed := capture.byMsg("rebuild completed")
	if len(completed) != 1 {
		t.Fatalf("rebuild completed logs = %d, want 1", len(completed))
	}
	if completed[0].f.Nodes != 2 || completed[0].f.Shards != 3 || completed[0].f.Entries != 2 {
		t.Errorf("completed fields = nodes %d shards %d entries %d, want 2/3/2",
			completed[0].f.Nodes, completed[0].f.Shards, completed[0].f.Entries)
	}
	if completed[0].f.Result != "window_closed" {
		t.Errorf("completed result = %q, want window_closed", completed[0].f.Result)
	}

	// 空片打点的 entries=0 键存在；分片接收打点带非空 trace_id 且各片互不相同（D49）。
	allShards := capture.byMsg("manifest shard received")
	if len(allShards) != 3 {
		t.Fatalf("shard received logs = %d, want 3", len(allShards))
	}
	if !strings.Contains(allShards[2].line, `"entries":0`) {
		t.Errorf("empty shard JSON line must carry entries 0, line = %s", allShards[2].line)
	}
	ids := map[string]bool{}
	for _, l := range allShards {
		if l.f.TraceID == "" {
			t.Error("shard received log must carry non-empty trace_id")
		}
		if ids[l.f.TraceID] {
			t.Error("different shards must carry different trace_id")
		}
		ids[l.f.TraceID] = true
	}

	// 第四处：重建窗口内未命中，记录 result 与耗时。
	ix2 := New()
	rb2 := StartRebuild(ix2, []string{"node-3"}, log)
	q := NewQuerier(ix2, BuildForm(FormConfig{Spec: spec}), rb2.Open, nil, log)
	if _, ok := q.Hit("hash-a", spec); ok {
		t.Error("hit inside rebuild window, want miss")
	}
	misses := capture.byMsg("lookup missed in rebuild window")
	if len(misses) != 1 {
		t.Fatalf("window miss logs = %d, want 1", len(misses))
	}
	if misses[0].f.Result != "window_miss" {
		t.Errorf("miss result = %q, want window_miss", misses[0].f.Result)
	}
	if misses[0].f.TraceID == "" {
		t.Error("window miss log must carry non-empty trace_id")
	}
	if !strings.Contains(misses[0].line, "duration_ms") {
		t.Errorf("window miss JSON line must carry duration_ms, line = %s", misses[0].line)
	}

	// 四处打点种类齐全（rebuild started 两条来自两次开窗；分片接收三条含 node-2 空片）。
	for msg, want := range map[string]int{
		"rebuild started":                 2,
		"manifest shard received":         3,
		"rebuild completed":               1,
		"lookup missed in rebuild window": 1,
	} {
		if got := len(capture.byMsg(msg)); got != want {
			t.Errorf("msg %q count = %d, want %d", msg, got, want)
		}
	}
}

// TestQuerierWithoutLoggerUsesDefault 覆盖未注入日志器时查询行为不变（D53）。
func TestQuerierWithoutLoggerUsesDefault(t *testing.T) {
	ix := New()
	ix.Insert("node-1", "hash-a", Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, Length: 64})
	StartRebuild(ix, []string{"node-1"})
	q := NewQuerier(ix, BuildForm(FormConfig{Spec: Spec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}}), func() bool { return false }, nil)
	if _, ok := q.Hit("hash-a", Spec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}); !ok {
		t.Error("hit after window closed, want hit")
	}
}
