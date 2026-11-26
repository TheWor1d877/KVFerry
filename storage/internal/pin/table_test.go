package pin

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(0, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTable(t *testing.T, dir string, blockBytes, quota int64, ttl time.Duration, clk Clock, onReclaim func(string)) *Table {
	t.Helper()
	return New(Options{
		DataDir:    dir,
		BlockBytes: blockBytes,
		QuotaBytes: quota,
		DefaultTTL: ttl,
		Clock:      clk,
		Log:        telemetry.New(),
		OnReclaim:  onReclaim,
	})
}

func TestAddRemoveAndPinned(t *testing.T) {
	dir := t.TempDir()
	tab := newTable(t, dir, 64, 0, 0, newFakeClock(), nil)
	if err := tab.Add("hash-a", 0); err != nil {
		t.Fatalf("add: %v", err)
	}
	if !tab.Pinned("hash-a") {
		t.Error("hash-a should be pinned after add")
	}
	if tab.Pinned("hash-b") {
		t.Error("unknown key must not be pinned")
	}
	if got := tab.UsedBytes(); got != 64 {
		t.Errorf("used bytes = %d, want 64", got)
	}
	if err := tab.Remove("hash-a"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if tab.Pinned("hash-a") {
		t.Error("hash-a should be released after remove")
	}
	if got := tab.UsedBytes(); got != 0 {
		t.Errorf("used bytes after remove = %d, want 0", got)
	}
}

func TestQuotaReclaimsOldest(t *testing.T) {
	dir := t.TempDir()
	var reclaimed []string
	tab := newTable(t, dir, 64, 128, 0, newFakeClock(), func(h string) { reclaimed = append(reclaimed, h) })
	for _, h := range []string{"hash-1", "hash-2", "hash-3"} {
		if err := tab.Add(h, 0); err != nil {
			t.Fatalf("add %s: %v", h, err)
		}
	}
	// 第三块加入后总量超配额，按 LRU 回收最旧的 hash-1（R34）。
	if len(reclaimed) != 1 || reclaimed[0] != "hash-1" {
		t.Errorf("reclaimed = %v, want [hash-1]", reclaimed)
	}
	if tab.Pinned("hash-1") {
		t.Error("hash-1 should be released after quota reclaim")
	}
	if !tab.Pinned("hash-2") || !tab.Pinned("hash-3") {
		t.Error("hash-2 and hash-3 should stay pinned")
	}
	if got := tab.UsedBytes(); got != 128 {
		t.Errorf("used bytes = %d, want 128", got)
	}
}

func TestQuotaSingleBlockExceeded(t *testing.T) {
	dir := t.TempDir()
	tab := newTable(t, dir, 64, 32, 0, newFakeClock(), nil)
	if err := tab.Add("hash-a", 0); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("add over quota = %v, want ErrQuotaExceeded", err)
	}
	if tab.Pinned("hash-a") {
		t.Error("rejected pin must not be recorded")
	}
	if len(tab.Hashes()) != 0 {
		t.Errorf("hashes = %v, want empty", tab.Hashes())
	}
}

func TestTTLExpiresAndSweep(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	tab := newTable(t, dir, 64, 0, 0, clk, nil)
	if err := tab.Add("hash-a", 1*time.Second); err != nil {
		t.Fatalf("add: %v", err)
	}
	clk.advance(2 * time.Second)
	if tab.Pinned("hash-a") {
		t.Error("expired pin must report unpinned before sweep")
	}
	released := tab.Sweep()
	if len(released) != 1 || released[0] != "hash-a" {
		t.Errorf("sweep released = %v, want [hash-a]", released)
	}
	if got := tab.UsedBytes(); got != 0 {
		t.Errorf("used bytes after sweep = %d, want 0", got)
	}

	// 过期条目不再进清单：新表恢复后为空。
	tab2 := newTable(t, dir, 64, 0, 0, clk, nil)
	if err := tab2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(tab2.Hashes()) != 0 {
		t.Errorf("hashes after reload = %v, want empty", tab2.Hashes())
	}
}

func TestManifestReload(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	tab := newTable(t, dir, 64, 0, 0, clk, nil)
	if err := tab.Add("hash-a", time.Hour); err != nil {
		t.Fatalf("add a: %v", err)
	}
	if err := tab.Add("hash-b", 0); err != nil {
		t.Fatalf("add b: %v", err)
	}
	if err := tab.Add("hash-c", 1*time.Second); err != nil {
		t.Fatalf("add c: %v", err)
	}
	clk.advance(2 * time.Second)

	// 重启先读 pin 清单恢复标记，过期条目不恢复（R34）。
	tab2 := newTable(t, dir, 64, 0, 0, clk, nil)
	if err := tab2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if !tab2.Pinned("hash-a") || !tab2.Pinned("hash-b") {
		t.Error("unexpired pins must be restored")
	}
	if tab2.Pinned("hash-c") {
		t.Error("expired pin must not be restored")
	}
	if got := tab2.UsedBytes(); got != 128 {
		t.Errorf("used bytes after reload = %d, want 128", got)
	}
}

func TestReloadKeepsLRUOrder(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	tab := newTable(t, dir, 64, 0, 0, clk, nil)
	for _, h := range []string{"hash-1", "hash-2", "hash-3"} {
		if err := tab.Add(h, 0); err != nil {
			t.Fatalf("add %s: %v", h, err)
		}
	}

	// 重启后配额回收仍取最旧标记：清单顺序颠倒会让 hash-3 被误回收（R34）。
	var reclaimed []string
	tab2 := newTable(t, dir, 64, 192, 0, clk, func(h string) { reclaimed = append(reclaimed, h) })
	if err := tab2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := tab2.Add("hash-4", 0); err != nil {
		t.Fatalf("add 4: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0] != "hash-1" {
		t.Errorf("reclaimed after reload = %v, want [hash-1]", reclaimed)
	}
}

type reclaimStoreLog struct {
	mu    sync.Mutex
	msgs  []string
	event segment.Event
	got   bool
}

func TestReclaimDrivesSegmentDropAndLog(t *testing.T) {
	dir := t.TempDir()
	events := make([]segment.Event, 0, 4)
	var logs reclaimStoreLog
	nodeSpec := spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 128}
	st, err := segment.Open(dir, "node-1", nodeSpec, 256*4, segment.Options{
		Emit: func(e segment.Event) { events = append(events, e) },
		Log: telemetry.NewWithHook(func(level, msg string, f telemetry.Fields, line []byte) {
			logs.mu.Lock()
			logs.msgs = append(logs.msgs, msg)
			logs.mu.Unlock()
		}),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	data := make([]byte, 64)
	if _, _, err := st.Put("hash-a", data); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if _, _, err := st.Put("hash-b", data); err != nil {
		t.Fatalf("put b: %v", err)
	}

	tab := newTable(t, dir, 64, 64, 0, newFakeClock(), func(h string) {
		if _, ok := st.DropBlock(h, segment.ReasonPinQuota); !ok {
			t.Errorf("drop block %s failed", h)
		}
	})
	if err := tab.Add("hash-a", 0); err != nil {
		t.Fatalf("add a: %v", err)
	}
	if err := tab.Add("hash-b", 0); err != nil {
		t.Fatalf("add b: %v", err)
	}

	// 超配额回收经装配回调释放段：键摘除、段号回位图、回收日志一条（R34）。
	if _, _, err := st.Get("hash-a"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("reclaimed block read = %v, want NotFound", err)
	}
	if _, _, err := st.Get("hash-b"); err != nil {
		t.Errorf("new block read = %v, want readable", err)
	}
	if st.UsedSegments() != 1 {
		t.Errorf("used segments = %d, want 1", st.UsedSegments())
	}
	logs.mu.Lock()
	defer logs.mu.Unlock()
	for _, m := range logs.msgs {
		if m == "pin reclaimed" {
			logs.got = true
		}
	}
	if !logs.got {
		t.Errorf("logs = %v, want one pin reclaimed entry", logs.msgs)
	}
	found := false
	for _, e := range events {
		if e.Kind == "evict" && e.BlockHash == "hash-a" && e.Reason == segment.ReasonPinQuota {
			found = true
		}
	}
	if !found {
		t.Errorf("events = %+v, want evict event with reason pin_quota_exceeded", events)
	}
	if err := tab.Remove("hash-b"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if tab.UsedBytes() != 0 {
		t.Errorf("used bytes = %d, want 0", tab.UsedBytes())
	}
	_ = filepath.Join // 保持导入对齐
}
