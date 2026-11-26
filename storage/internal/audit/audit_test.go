package audit

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/block"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

func testSpec() spec.KVCacheSpec {
	return spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 128}
}

type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	sleepTotal time.Duration
}

func newAuditClock() *fakeClock { return &fakeClock{now: time.Unix(0, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleepTotal += d
	c.now = c.now.Add(d)
}

type evCapture struct {
	mu     sync.Mutex
	events []segment.Event
}

func (c *evCapture) emit(e segment.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *evCapture) snapshot() []segment.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]segment.Event(nil), c.events...)
}

type logCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (c *logCapture) record(level, msg string, f telemetry.Fields, line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, msg)
}

func (c *logCapture) count(msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.msgs {
		if m == msg {
			n++
		}
	}
	return n
}

type auditEnv struct {
	dir    string
	store  *segment.Store
	aud    *Auditor
	events *evCapture
	logs   *logCapture
}

func openAuditEnv(t *testing.T, capacity int64, pct int, bytesPerS int64, clk Clock, seed int64) *auditEnv {
	t.Helper()
	dir := t.TempDir()
	events := &evCapture{}
	logs := &logCapture{}
	st, err := segment.Open(dir, "node-1", testSpec(), capacity, segment.Options{
		Emit: events.emit,
		Log:  telemetry.NewWithHook(logs.record),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	aud := New(Options{
		NodeID:       "node-1",
		Store:        st,
		Emit:         events.emit,
		Log:          telemetry.NewWithHook(logs.record),
		SamplePct:    pct,
		MaxBytesPerS: bytesPerS,
		Clock:        clk,
		RandSeed:     seed,
	})
	return &auditEnv{dir: dir, store: st, aud: aud, events: events, logs: logs}
}

// writeRawSegment 直写数据大文件构造孤儿段：盘上有段头而内存映射无登记（D54）。
func (e *auditEnv) writeRawSegment(t *testing.T, seq int64, hash string, data []byte, corruptCRC bool) {
	t.Helper()
	off := segment.FileHeaderSize + seq*e.store.SegmentSize()
	hdr := segment.Header{
		Hash:       hash,
		SegmentSeq: seq,
		BlockBytes: int64(len(data)),
		State:      segment.StateWritten,
	}
	if corruptCRC {
		hdr.DataCRC = block.CRC32C(data) ^ 0xFFFFFFFF
	} else {
		hdr.DataCRC = block.CRC32C(data)
	}
	buf := make([]byte, segment.HeaderSize)
	if err := hdr.Encode(buf); err != nil {
		t.Fatalf("encode header: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(e.dir, "blocks.dat"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, off+segment.HeaderSize); err != nil {
		t.Fatalf("write data: %v", err)
	}
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("write header: %v", err)
	}
}

// overwriteHeaderState 覆写既有段的状态字节，构造损坏段头（D54）。
func (e *auditEnv) overwriteHeaderState(t *testing.T, seq int64, hash string, state segment.SegmentState) {
	t.Helper()
	off := segment.FileHeaderSize + seq*e.store.SegmentSize()
	hdr := segment.Header{Hash: hash, SegmentSeq: seq, BlockBytes: 64, State: state}
	buf := make([]byte, segment.HeaderSize)
	if err := hdr.Encode(buf); err != nil {
		t.Fatalf("encode header: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(e.dir, "blocks.dat"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("write header: %v", err)
	}
}

func TestOrphanSegmentRegistered(t *testing.T) {
	env := openAuditEnv(t, 256*4, 100, 0, nil, 7)
	data := make([]byte, 64)
	orphan := make([]byte, 64)
	orphan[0] = 9
	if _, _, err := env.store.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}
	env.writeRawSegment(t, 3, "hash-orphan", orphan, false)

	res, err := env.aud.RunOnce()
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.OrphansRegistered != 1 || res.CorruptReleased != 0 {
		t.Errorf("result = %+v, want 1 orphan registered, 0 released", res)
	}
	got, seq, err := env.store.Get("hash-orphan")
	if err != nil || seq != 3 {
		t.Errorf("orphan read = seq %d err %v, want seq 3 nil", seq, err)
	}
	if len(got) != 64 {
		t.Errorf("orphan data len = %d, want 64", len(got))
	}
	found := false
	for _, e := range env.events.snapshot() {
		if e.Kind == "register" && e.BlockHash == "hash-orphan" && e.Reason == "audit_orphan" {
			found = true
		}
	}
	if !found {
		t.Error("orphan registration must emit a register event")
	}
	if got := env.logs.count("audit orphan registered"); got != 1 {
		t.Errorf("orphan registered logs = %d, want 1", got)
	}
}

func TestOrphanBadCRCReleased(t *testing.T) {
	env := openAuditEnv(t, 256*4, 100, 0, nil, 7)
	data := make([]byte, 64)
	env.writeRawSegment(t, 2, "hash-bad", data, true)

	res, err := env.aud.RunOnce()
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.OrphansRegistered != 0 || res.CorruptReleased != 1 {
		t.Errorf("result = %+v, want 0 registered, 1 released", res)
	}
	if _, _, err := env.store.Get("hash-bad"); err == nil {
		t.Error("corrupt orphan must not be registered")
	}
}

func TestCorruptHeaderReleased(t *testing.T) {
	env := openAuditEnv(t, 256*4, 100, 0, nil, 7)
	data := make([]byte, 64)
	if _, _, err := env.store.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}
	env.overwriteHeaderState(t, 0, "hash-a", segment.StateCorrupt)

	res, err := env.aud.RunOnce()
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.CorruptReleased != 1 {
		t.Errorf("corrupt released = %d, want 1", res.CorruptReleased)
	}
	if _, _, err := env.store.Get("hash-a"); err == nil {
		t.Error("released corrupt block must be absent")
	}
	// 段号回位图：新键分配到最低空闲位。
	seq, _, err := env.store.Put("hash-new", data)
	if err != nil || seq != 0 {
		t.Errorf("reallocated seq = %d err %v, want 0", seq, err)
	}
}

func TestSampleCountAtLeastHundred(t *testing.T) {
	env := openAuditEnv(t, 256*160, 1, 0, nil, 7)
	data := make([]byte, 64)
	for i := 0; i < 150; i++ {
		hash := "hash-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		if _, _, err := env.store.Put(hash, data); err != nil {
			t.Fatalf("put %s: %v", hash, err)
		}
	}
	res, err := env.aud.RunOnce()
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	// 登记一百五十块抽百分之一不足下限，抽满一百块（D54）。
	if res.Sampled != 100 {
		t.Errorf("sampled = %d, want 100", res.Sampled)
	}
	if res.Deregistered != 0 {
		t.Errorf("deregistered = %d, want 0", res.Deregistered)
	}
}

func TestRateLimitHonorsBudget(t *testing.T) {
	clk := newAuditClock()
	env := openAuditEnv(t, 256*120, 100, 1000, clk, 7)
	data := make([]byte, 64)
	for i := 0; i < 100; i++ {
		hash := "hash-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		if _, _, err := env.store.Put(hash, data); err != nil {
			t.Fatalf("put %s: %v", hash, err)
		}
	}
	res, err := env.aud.RunOnce()
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.Sampled != 100 {
		t.Errorf("sampled = %d, want 100", res.Sampled)
	}
	// 注入时钟限速：读出字节数受每秒预算约束，时钟推进量与产出字节匹配（D54）。
	clk.mu.Lock()
	slept := clk.sleepTotal
	clk.mu.Unlock()
	totalBytes := int64(100) * 64
	if slept < 5*time.Second {
		t.Errorf("rate limiter slept %v, want at least 5s for %d bytes at 1000 B/s", slept, totalBytes)
	}
	if totalBytes > 1000*int64(slept/time.Second+1) {
		t.Errorf("produced %d bytes within %v, exceeds budget", totalBytes, slept)
	}
}

func TestRateLimitDisabledWhenZero(t *testing.T) {
	clk := newAuditClock()
	env := openAuditEnv(t, 256*120, 100, 0, clk, 7)
	data := make([]byte, 64)
	for i := 0; i < 100; i++ {
		hash := "hash-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		if _, _, err := env.store.Put(hash, data); err != nil {
			t.Fatalf("put %s: %v", hash, err)
		}
	}
	if _, err := env.aud.RunOnce(); err != nil {
		t.Fatalf("run once: %v", err)
	}
	clk.mu.Lock()
	defer clk.mu.Unlock()
	if clk.sleepTotal != 0 {
		t.Errorf("unlimited rate must not sleep, slept %v", clk.sleepTotal)
	}
}

func TestHashMismatchDeregisters(t *testing.T) {
	env := openAuditEnv(t, 256*4, 100, 0, nil, 7)
	a := make([]byte, 64)
	b := make([]byte, 64)
	b[1] = 5
	if _, _, err := env.store.Put("hash-a", a); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if _, _, err := env.store.Put("hash-b", b); err != nil {
		t.Fatalf("put b: %v", err)
	}
	off := int64(segment.FileHeaderSize + segment.HeaderSize)
	f, err := os.OpenFile(filepath.Join(env.dir, "blocks.dat"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open data file: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, off); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	f.Close()

	res, err := env.aud.RunOnce()
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if res.Sampled != 2 || res.Deregistered != 1 {
		t.Errorf("result = %+v, want sampled 2, deregistered 1", res)
	}
	if _, _, err := env.store.Get("hash-a"); err == nil {
		t.Error("deregistered block must be absent")
	}
	if _, _, err := env.store.Get("hash-b"); err != nil {
		t.Errorf("healthy block read = %v, want readable", err)
	}
	found := false
	for _, e := range env.events.snapshot() {
		if e.Kind == "evict" && e.BlockHash == "hash-a" && e.Reason == segment.ReasonAuditHash {
			found = true
		}
	}
	if !found {
		t.Error("deregistration must emit an evict event with audit reason")
	}
	if got := env.logs.count("audit block deregistered"); got != 1 {
		t.Errorf("deregistered logs = %d, want 1", got)
	}
}

func TestRunDisabledWithoutInterval(t *testing.T) {
	env := openAuditEnv(t, 256*4, 100, 0, nil, 7)
	done := make(chan struct{})
	go func() {
		env.aud.Run(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run without interval must return immediately")
	}
}
