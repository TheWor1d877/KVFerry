package server

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/cache"
	"github.com/TheWor1d877/KVFerry/storage/internal/pin"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

type env struct {
	Client  api.BlockStoreClient
	DataDir string
	Store   *segment.Store
	Events  *eventCapture
	Logs    *logCapture
	stop    func()
}

// logCapture 捕获服务端日志钩子，供请求级 trace_id 断言（D47）。
type logLine struct {
	msg string
	f   telemetry.Fields
}

type logCapture struct {
	mu    sync.Mutex
	lines []logLine
}

func (c *logCapture) record(level, msg string, f telemetry.Fields, line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, logLine{msg: msg, f: f})
}

func (c *logCapture) byMsg(msg string) []logLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []logLine
	for _, l := range c.lines {
		if l.msg == msg {
			out = append(out, l)
		}
	}
	return out
}

// eventCapture 捕获注入两侧的事件回调，供测试断言 oversized 链路（D46、D47）。
type eventCapture struct {
	mu     sync.Mutex
	events []segment.Event
}

func (c *eventCapture) record(e segment.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *eventCapture) snapshot() []segment.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]segment.Event(nil), c.events...)
}

func baseSpec() spec.KVCacheSpec {
	return spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

func startServer(t *testing.T, nodeSpec spec.KVCacheSpec, maxMessageBytes int64, capacity int64, ready bool, cacheBytes int64) *env {
	t.Helper()
	dir := t.TempDir()
	events := &eventCapture{}
	logs := &logCapture{}
	srvLog := telemetry.NewWithHook(logs.record)
	store, err := segment.Open(dir, "node-1", nodeSpec, capacity, segment.Options{Log: srvLog, Emit: events.record})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer(grpc.MaxRecvMsgSize(serverRecvSize(maxMessageBytes)))
	api.RegisterBlockStoreServer(gsrv, New(Deps{
		NodeID:          "node-1",
		Store:           store,
		Cache:           cache.New(cacheBytes),
		NodeSpec:        nodeSpec,
		MaxMessageBytes: maxMessageBytes,
		Ready:           func() bool { return ready },
		Emit:            events.record,
		Log:             srvLog,
	}))
	go func() { _ = gsrv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		gsrv.Stop()
		store.Close()
	})
	return &env{
		Client:  api.NewBlockStoreClient(conn),
		DataDir: dir,
		Store:   store,
		Events:  events,
		Logs:    logs,
		stop: func() {
			conn.Close()
			gsrv.Stop()
		},
	}
}

func serverRecvSize(maxMessageBytes int64) int {
	return int(maxMessageBytes + (1 << 20))
}

func putReq(hash string, data []byte, s spec.KVCacheSpec) *api.PutBlockRequest {
	return &api.PutBlockRequest{
		NodeId:    "node-1",
		BlockHash: hash,
		Spec:      &api.KVSpec{BlockBytes: s.BlockBytes, KvPrecision: s.KVPrecision, BytesPerToken: s.BytesPerToken, AlignmentPad: s.AlignmentPad},
		BlockData: data,
	}
}

func getReq(hash string, s spec.KVCacheSpec) *api.GetBlockRequest {
	return &api.GetBlockRequest{
		BlockHash: hash,
		Spec:      &api.KVSpec{BlockBytes: s.BlockBytes, KvPrecision: s.KVPrecision, BytesPerToken: s.BytesPerToken, AlignmentPad: s.AlignmentPad},
	}
}

// TestTraceIDPerRequest 覆盖请求级 trace_id：完成与拒收打点同源且非空，
// 不同请求的 trace_id 互不相同（D47）。
func TestTraceIDPerRequest(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 0)
	data := make([]byte, 64)
	if _, err := e.Client.PutBlock(context.Background(), putReq("hash-a", data, baseSpec())); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if _, err := e.Client.PutBlock(context.Background(), putReq("hash-b", data, baseSpec())); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if _, err := e.Client.PutBlock(context.Background(), putReq("", data, baseSpec())); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("rejected put = %v, want InvalidArgument", err)
	}
	saved := e.Logs.byMsg("putblock saved")
	if len(saved) != 2 {
		t.Fatalf("saved logs = %d, want 2", len(saved))
	}
	if saved[0].f.TraceID == "" || saved[1].f.TraceID == "" {
		t.Error("request-level logs must carry non-empty trace_id")
	}
	if saved[0].f.TraceID == saved[1].f.TraceID {
		t.Error("different requests must carry different trace_id")
	}
	rejected := e.Logs.byMsg("putblock rejected")
	if len(rejected) != 1 {
		t.Fatalf("rejected logs = %d, want 1", len(rejected))
	}
	if rejected[0].f.TraceID == "" {
		t.Error("rejected log must carry the trace_id generated at request entry")
	}
}

func TestPutBlockMessageShape(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 1<<20)
	data := make([]byte, 64)
	resp, err := e.Client.PutBlock(context.Background(), putReq("hash-a", data, baseSpec()))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if resp.SegmentSeq != 0 || !resp.IsNewWrite {
		t.Errorf("resp = seq %d isNew %v, want 0 true", resp.SegmentSeq, resp.IsNewWrite)
	}
	resp2, err := e.Client.PutBlock(context.Background(), putReq("hash-a", data, baseSpec()))
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if resp2.SegmentSeq != 0 || resp2.IsNewWrite {
		t.Errorf("overwrite resp = seq %d isNew %v, want 0 false", resp2.SegmentSeq, resp2.IsNewWrite)
	}
}

func TestGetBlockCarriesBytesAndReservedZero(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 1<<20)
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := e.Client.PutBlock(context.Background(), putReq("hash-a", data, baseSpec())); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := e.Client.GetBlock(context.Background(), getReq("hash-a", baseSpec()))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(resp.BlockData, data) {
		t.Error("block_data bytes differ")
	}
	if resp.FileDescriptor != 0 || resp.RegisteredMemory != 0 {
		t.Errorf("reserved fields = fd %d mem %d, want 0/0 (D52)", resp.FileDescriptor, resp.RegisteredMemory)
	}
}

func TestNotFound(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 0)
	_, err := e.Client.GetBlock(context.Background(), getReq("missing", baseSpec()))
	if status.Code(err) != codes.NotFound {
		t.Errorf("missing key = %v, want NotFound", err)
	}
}

func TestInvalidArgumentOnSpecMismatch(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 0)
	other := baseSpec()
	other.KVPrecision = "bf16"
	_, err := e.Client.GetBlock(context.Background(), getReq("hash-a", other))
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("spec mismatch get = %v, want InvalidArgument", err)
	}
	_, err = e.Client.PutBlock(context.Background(), putReq("hash-a", make([]byte, 64), other))
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("spec mismatch put = %v, want InvalidArgument", err)
	}
	_, err = e.Client.PutBlock(context.Background(), putReq("", make([]byte, 64), baseSpec()))
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty hash = %v, want InvalidArgument", err)
	}
}

func TestResourceExhausted(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256, true, 0)
	if _, err := e.Client.PutBlock(context.Background(), putReq("hash-a", make([]byte, 64), baseSpec())); err != nil {
		t.Fatalf("first put: %v", err)
	}
	_, err := e.Client.PutBlock(context.Background(), putReq("hash-b", make([]byte, 64), baseSpec()))
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("exhausted put = %v, want ResourceExhausted", err)
	}
}

func TestUnavailable(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, false, 0)
	_, err := e.Client.PutBlock(context.Background(), putReq("hash-a", make([]byte, 64), baseSpec()))
	if status.Code(err) != codes.Unavailable {
		t.Errorf("put not ready = %v, want Unavailable", err)
	}
	_, err = e.Client.GetBlock(context.Background(), getReq("hash-a", baseSpec()))
	if status.Code(err) != codes.Unavailable {
		t.Errorf("get not ready = %v, want Unavailable", err)
	}
}

func TestDataLossOnTamperedSegment(t *testing.T) {
	// 读缓存关闭，读请求直接走 NVMe。
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 0)
	data := make([]byte, 64)
	if _, err := e.Client.PutBlock(context.Background(), putReq("hash-a", data, baseSpec())); err != nil {
		t.Fatalf("put: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(e.DataDir, "blocks.dat"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, segment.FileHeaderSize+segment.HeaderSize); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	f.Close()
	_, err = e.Client.GetBlock(context.Background(), getReq("hash-a", baseSpec()))
	if status.Code(err) != codes.DataLoss {
		t.Errorf("tampered get = %v, want DataLoss", err)
	}
}

func TestOversizedMessageRejected(t *testing.T) {
	// 块字节 1.5 MB，rpc 上限 1 MB：gRPC 接收上限留 1 MiB 余量，超限进入应用层按 InvalidArgument 拒收。
	bigSpec := spec.KVCacheSpec{BlockBytes: (3 << 20) / 2, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
	e := startServer(t, bigSpec, 1<<20, ((3<<20)/2+256)*4, true, 0)
	data := make([]byte, (3<<20)/2)
	_, err := e.Client.PutBlock(context.Background(), putReq("hash-big", data, bigSpec))
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("oversized put = %v, want InvalidArgument (not grpc ResourceExhausted)", err)
	}
	// 端到端断言：oversized 事件经 Deps.Emit 到达钩子（D46、D47）。
	found := false
	for _, ev := range e.Events.snapshot() {
		if ev.Kind == "oversized" && ev.BlockHash == "hash-big" {
			found = true
		}
	}
	if !found {
		t.Errorf("oversized event should reach emit hook, events = %+v", e.Events.snapshot())
	}
}

func TestDeadlinePropagation(t *testing.T) {
	e := startServer(t, baseSpec(), 1<<20, 256*4, true, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.Client.GetBlock(ctx, getReq("missing", baseSpec())); status.Code(err) != codes.NotFound {
		t.Errorf("call within deadline = %v, want NotFound", err)
	}
}

// ---------- pin 接入 ----------

func startPinnedServer(t *testing.T, quotaBytes int64) (*env, *pin.Table) {
	t.Helper()
	nodeSpec := baseSpec()
	dir := t.TempDir()
	events := &eventCapture{}
	logs := &logCapture{}
	srvLog := telemetry.NewWithHook(logs.record)
	table := pin.New(pin.Options{
		DataDir:    dir,
		BlockBytes: nodeSpec.BlockBytes,
		QuotaBytes: quotaBytes,
		DefaultTTL: time.Hour,
	})
	store, err := segment.Open(dir, "node-1", nodeSpec, 256*4, segment.Options{Log: srvLog, Emit: events.record})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer(grpc.MaxRecvMsgSize(serverRecvSize(1 << 20)))
	api.RegisterBlockStoreServer(gsrv, New(Deps{
		NodeID:          "node-1",
		Store:           store,
		Cache:           cache.New(0),
		NodeSpec:        nodeSpec,
		MaxMessageBytes: 1 << 20,
		Ready:           func() bool { return true },
		Emit:            events.record,
		Pins:            table,
		Log:             srvLog,
	}))
	go func() { _ = gsrv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		gsrv.Stop()
		store.Close()
	})
	return &env{
		Client:  api.NewBlockStoreClient(conn),
		DataDir: dir,
		Store:   store,
		Events:  events,
		Logs:    logs,
		stop: func() {
			conn.Close()
			gsrv.Stop()
		},
	}, table
}

func putReqPinned(hash string, data []byte, s spec.KVCacheSpec, pin bool, ttlS int64) *api.PutBlockRequest {
	req := putReq(hash, data, s)
	req.Pin = pin
	req.PinTtlS = ttlS
	return req
}

func TestPutBlockWithPinWritesPinTable(t *testing.T) {
	e, table := startPinnedServer(t, 0)
	data := make([]byte, 64)
	if _, err := e.Client.PutBlock(context.Background(), putReqPinned("hash-pin", data, baseSpec(), true, 120)); err != nil {
		t.Fatalf("pinned put: %v", err)
	}
	if !table.Pinned("hash-pin") {
		t.Error("pinned put must write the key into the pin table")
	}
	if _, err := e.Client.PutBlock(context.Background(), putReqPinned("hash-plain", data, baseSpec(), false, 120)); err != nil {
		t.Fatalf("plain put: %v", err)
	}
	if table.Pinned("hash-plain") {
		t.Error("put without pin must not enter the pin table")
	}
	if _, err := os.Stat(filepath.Join(e.DataDir, "pin.manifest")); err != nil {
		t.Errorf("pin manifest should be persisted under data dir: %v", err)
	}
}

func TestPinQuotaExceededKeepsPutSuccess(t *testing.T) {
	// 配额 32 字节装不下 64 字节单块：pin 添加失败，写入成功的事实与响应不变（R34）。
	e, table := startPinnedServer(t, 32)
	resp, err := e.Client.PutBlock(context.Background(), putReqPinned("hash-big", make([]byte, 64), baseSpec(), true, 120))
	if err != nil {
		t.Fatalf("put with unaffordable pin = %v, want success", err)
	}
	if resp.SegmentSeq != 0 || !resp.IsNewWrite {
		t.Errorf("resp = seq %d isNew %v, want 0 true", resp.SegmentSeq, resp.IsNewWrite)
	}
	if table.Pinned("hash-big") {
		t.Error("rejected pin must not be recorded")
	}
	if _, err := e.Client.GetBlock(context.Background(), getReq("hash-big", baseSpec())); err != nil {
		t.Errorf("block after rejected pin = %v, want readable", err)
	}
	if got := e.Logs.byMsg("pin add failed"); len(got) != 1 {
		t.Errorf("pin add failed logs = %d, want 1", len(got))
	}
}
