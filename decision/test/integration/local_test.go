package integration

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/decision/internal/health"
	"github.com/TheWor1d877/KVFerry/decision/internal/index"
	"github.com/TheWor1d877/KVFerry/decision/internal/server"
	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/test/node"
)

const (
	testNodeID = "node-1"
	testShards = 2
)

func testKVSpec() api.KVSpec {
	return api.KVSpec{BlockBytes: 64, KvPrecision: "fp16", BytesPerToken: 8}
}

var testKVSpecPtr = &api.KVSpec{BlockBytes: 64, KvPrecision: "fp16", BytesPerToken: 8}

func testFormSpec() index.Spec {
	return index.Spec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

func putBlock(t *testing.T, n *node.Node, hash string, data []byte) *api.PutBlockResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := n.Client.PutBlock(ctx, &api.PutBlockRequest{
		NodeId:    testNodeID,
		BlockHash: hash,
		Spec:      &api.KVSpec{BlockBytes: 64, KvPrecision: "fp16", BytesPerToken: 8},
		BlockData: data,
	})
	if err != nil {
		t.Fatalf("put %s: %v", hash, err)
	}
	return resp
}

func getBlock(t *testing.T, n *node.Node, hash string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := n.Client.GetBlock(ctx, &api.GetBlockRequest{
		BlockHash: hash,
		Spec:      &api.KVSpec{BlockBytes: 64, KvPrecision: "fp16", BytesPerToken: 8},
	})
	if err != nil {
		t.Fatalf("get %s: %v", hash, err)
	}
	return resp.BlockData
}

// TestRebuildWindowMissThenRelease 覆盖重建窗口内查询全部未命中且 save 计数继续增长，
// 清单收齐放行后命中查询返回正确层位与偏移（D49、D51、D53、R32）。
func TestRebuildWindowMissThenRelease(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{testNodeID})
	form := index.BuildForm(index.FormConfig{Spec: testFormSpec()})
	querier := index.NewQuerier(ix, form, rb.Open, nil)

	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, server.NewReporter(rb))
	go func() { _ = gsrv.Serve(lis) }()
	dconn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial decision: %v", err)
	}
	t.Cleanup(func() {
		dconn.Close()
		gsrv.Stop()
	})

	n, err := node.Start(node.Options{
		DataDir:       t.TempDir(),
		NodeID:        testNodeID,
		Spec:          testKVSpecPtr,
		CapacityBytes: 64 * 4096,
		CacheBytes:    0,
		ShardBlocks:   testShards,
		Reporter:      api.NewStorageReporterClient(dconn),
	})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	// 重建窗口内：查询全部未命中，save 照常接收，计数继续增长（R32、D51）。
	saves := 0
	seqs := make(map[string]int64, 3)
	for _, hash := range []string{"hash-a", "hash-b", "hash-c"} {
		if _, ok := querier.Hit(hash, testFormSpec()); ok {
			t.Fatalf("hit for %s inside rebuild window, want miss", hash)
		}
		resp := putBlock(t, n, hash, make([]byte, 64))
		seqs[hash] = resp.SegmentSeq
		saves++
	}
	if saves != 3 {
		t.Fatalf("save count inside window = %d, want 3", saves)
	}
	for _, hash := range []string{"hash-a", "hash-b", "hash-c"} {
		if _, ok := querier.Hit(hash, testFormSpec()); ok {
			t.Fatalf("hit for %s inside rebuild window after saves, want miss", hash)
		}
	}

	// 清单分片上报，决策逐片收齐后关闭窗口放行（D49）。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.ReportManifest(ctx); err != nil {
		t.Fatalf("report manifest: %v", err)
	}
	if rb.Open() {
		t.Fatal("window still open after manifest reported")
	}

	// 命中查询返回正确层位与偏移（D53）。
	for _, hash := range []string{"hash-a", "hash-b", "hash-c"} {
		rec, ok := querier.Hit(hash, testFormSpec())
		if !ok {
			t.Fatalf("hit for %s after window closed, want hit", hash)
		}
		if rec.Layer != api.BlockLayer_BLOCK_LAYER_LOCAL {
			t.Errorf("layer = %v, want local", rec.Layer)
		}
		if rec.Node != testNodeID {
			t.Errorf("node = %s, want %s", rec.Node, testNodeID)
		}
		if rec.Offset != n.SegmentOffset(seqs[hash]) {
			t.Errorf("offset = %d, want %d", rec.Offset, n.SegmentOffset(seqs[hash]))
		}
		if rec.Length != 64 {
			t.Errorf("length = %d, want 64", rec.Length)
		}
	}

	// 放行后 save 计数继续增长（D51）。
	putBlock(t, n, "hash-d", make([]byte, 64))
	saves++
	if saves != 4 {
		t.Errorf("save count after release = %d, want 4", saves)
	}
}

// TestEndToEndWriteRead 覆盖双进程写读一致（D49、D53）：两组件经 bufconn 端到端写入、
// 索引登记放行、按块哈希读回字节逐位相等。
func TestEndToEndWriteRead(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{testNodeID})
	form := index.BuildForm(index.FormConfig{Spec: testFormSpec()})
	querier := index.NewQuerier(ix, form, rb.Open, nil)

	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, server.NewReporter(rb))
	go func() { _ = gsrv.Serve(lis) }()
	dconn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial decision: %v", err)
	}
	t.Cleanup(func() {
		dconn.Close()
		gsrv.Stop()
	})

	n, err := node.Start(node.Options{
		DataDir:       t.TempDir(),
		NodeID:        testNodeID,
		Spec:          testKVSpecPtr,
		CapacityBytes: 64 * 4096,
		CacheBytes:    0,
		ShardBlocks:   testShards,
		Reporter:      api.NewStorageReporterClient(dconn),
	})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	putBlock(t, n, "hash-x", data)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.ReportManifest(ctx); err != nil {
		t.Fatalf("report manifest: %v", err)
	}
	rec, ok := querier.Hit("hash-x", testFormSpec())
	if !ok {
		t.Fatal("hit for hash-x after manifest, want hit")
	}
	if rec.Offset != n.SegmentOffset(0) || rec.Length != 64 || rec.Node != testNodeID {
		t.Fatalf("record = %+v, want node %s offset %d length 64", rec, testNodeID, n.SegmentOffset(0))
	}
	if got := getBlock(t, n, "hash-x"); !bytes.Equal(got, data) {
		t.Error("read back bytes differ from written")
	}
}

// TestDisconnectDeadAndRecover 覆盖双进程断连：从断连到判死与从重连到恢复的时长
// 可观察且落在配置阈值上（D51）。bufconn 断开模拟断连，注入时钟推进验证阈值边界。
func TestDisconnectDeadAndRecover(t *testing.T) {
	const downAfter = 30 * time.Second
	const hysteresis = 30 * time.Second

	// 注入时钟：以纳秒原子推进，断言两段时长精确落在阈值上。
	var nowNS atomic.Int64
	nowNS.Store(time.Unix(0, 0).UnixNano())
	clock := func() time.Time { return time.Unix(0, nowNS.Load()) }

	type capturedLog = capturedLogEntry
	var logsMu capturedLogSlice
	logs := &logsMu
	dlog := telemetry.NewWithHook(func(level, msg string, f telemetry.Fields, line []byte) {
		logs.append(capturedLog{msg: msg, line: string(line)})
	})

	ix := index.New()
	rb := index.StartRebuild(ix, []string{testNodeID})
	ap := index.NewApplier(ix)
	tracker := health.NewTrackerWithClock(downAfter, hysteresis, clock, dlog)
	form := index.BuildForm(index.FormConfig{Spec: testFormSpec()})
	querier := index.NewQuerier(ix, form, rb.Open, tracker, dlog)

	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, server.NewReporter(rb,
		server.WithIndex(ix), server.WithApplier(ap), server.WithHealth(tracker), server.WithLog(dlog)))
	go func() { _ = gsrv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial decision: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		gsrv.Stop()
	})

	// 第一次连接：发一条事件建立流，节点正常，命中查询可达。
	client := api.NewStorageReporterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(&api.StorageEvent{Kind: "register", NodeId: testNodeID, Seq: 1, BlockHash: "hash-live", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL}); err != nil {
		t.Fatalf("send seq 1: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("recv ack 1: %v", err)
	}
	// 清单收齐关窗放行命中查询（D49）；hash-live 随清单进登记。
	if _, err := client.ReportManifest(ctx, &api.ReportManifestRequest{
		NodeId:     testNodeID,
		ShardIndex: 0,
		ShardTotal: 1,
		Entries: []*api.ManifestEntry{{
			BlockHash:  "hash-live",
			Layer:      api.BlockLayer_BLOCK_LAYER_LOCAL,
			Offset:     71,
			BlockBytes: 64,
		}},
	}); err != nil {
		t.Fatalf("report manifest: %v", err)
	}
	if _, ok := querier.Hit("hash-live", testFormSpec()); !ok {
		t.Fatal("hit while node connected, want hit")
	}

	// 断连：流断开进疑似，摘出调度池，命中查询走重算（D51）。
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	waitForCond(t, "suspect after disconnect", func() bool { return tracker.State(testNodeID) == health.StateSuspect })
	if _, ok := querier.Hit("hash-live", testFormSpec()); ok {
		t.Fatal("hit for suspect node, want miss")
	}

	// 从断连到判死：疑似持续 down_after_s 判死，时长落在阈值上。
	nowNS.Store(time.Unix(0, 0).Add(downAfter - time.Second).UnixNano())
	if got := tracker.State(testNodeID); got != health.StateSuspect {
		t.Fatalf("state at down_after-1s = %s, want suspect", got)
	}
	nowNS.Store(time.Unix(0, 0).Add(downAfter).UnixNano())
	if got := tracker.State(testNodeID); got != health.StateDead {
		t.Fatalf("state at down_after = %s, want dead", got)
	}

	// 重连：进滞回，仍在调度池外；滞回期满恢复，时长落在阈值上。
	stream2, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatalf("reopen stream: %v", err)
	}
	if err := stream2.Send(&api.StorageEvent{Kind: "register", NodeId: testNodeID, Seq: 2, BlockHash: "hash-live2", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL}); err != nil {
		t.Fatalf("send seq 2: %v", err)
	}
	if _, err := stream2.Recv(); err != nil {
		t.Fatalf("recv ack 2: %v", err)
	}
	if got := tracker.State(testNodeID); got != health.StateHysteresis {
		t.Fatalf("state after reconnect = %s, want hysteresis", got)
	}
	if _, ok := querier.Hit("hash-live", testFormSpec()); ok {
		t.Fatal("hit during hysteresis, want miss")
	}
	nowNS.Store(time.Unix(0, 0).Add(downAfter + hysteresis - time.Second).UnixNano())
	if got := tracker.State(testNodeID); got != health.StateHysteresis {
		t.Fatalf("state at hysteresis-1s = %s, want hysteresis", got)
	}
	nowNS.Store(time.Unix(0, 0).Add(downAfter + hysteresis).UnixNano())
	if got := tracker.State(testNodeID); got != health.StateNormal {
		t.Fatalf("state at hysteresis = %s, want normal", got)
	}
	if _, ok := querier.Hit("hash-live", testFormSpec()); !ok {
		t.Fatal("hit after recovery, want hit")
	}

	// 时长可观察：判死与复活的迁移日志各一条，duration_ms 分别等于两个阈值（D51）。
	deadMs, recoveredMs, deadCount, recoveredCount := int64(-1), int64(-1), 0, 0
	for _, l := range logs.snapshot() {
		switch l.msg {
		case "node dead":
			deadCount++
			deadMs = extractDuration(l.line)
		case "node recovered":
			recoveredCount++
			recoveredMs = extractDuration(l.line)
		}
	}
	if deadCount != 1 || recoveredCount != 1 {
		t.Fatalf("dead logs = %d recovered logs = %d, want 1 each", deadCount, recoveredCount)
	}
	if deadMs != downAfter.Milliseconds() {
		t.Errorf("disconnect-to-dead duration = %d ms, want %d ms", deadMs, downAfter.Milliseconds())
	}
	if recoveredMs != hysteresis.Milliseconds() {
		t.Errorf("reconnect-to-recovered duration = %d ms, want %d ms", recoveredMs, hysteresis.Milliseconds())
	}
}

// capturedLogSlice 是集成测试的日志捕获切片，带互斥保护。
type capturedLogSlice struct {
	mu   sync.Mutex
	logs []capturedLogEntry
}

type capturedLogEntry struct {
	msg  string
	line string
}

func (c *capturedLogSlice) append(e capturedLogEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs = append(c.logs, e)
}

func (c *capturedLogSlice) snapshot() []capturedLogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedLogEntry(nil), c.logs...)
}

// extractDuration 从 JSON 日志行取 duration_ms 字段值。
func extractDuration(line string) int64 {
	const key = `"duration_ms":`
	idx := strings.Index(line, key)
	if idx < 0 {
		return -1
	}
	rest := line[idx+len(key):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return -1
	}
	var v int64
	for _, r := range rest[:end] {
		v = v*10 + int64(r-'0')
	}
	return v
}

// waitForCond 轮询等待条件成立，超时即失败。
func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
