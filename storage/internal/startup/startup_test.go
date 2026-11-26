package startup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

const testCapacity = 256 * 4096

func baseSpec() spec.KVCacheSpec {
	return spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

func altSpec() spec.KVCacheSpec {
	s := baseSpec()
	s.BlockBytes = 128
	return s
}

func blockData(hash string) []byte {
	data := make([]byte, 64)
	for i := range data {
		data[i] = hash[i%len(hash)]
	}
	return data
}

type fakeRereporter struct {
	mu      sync.Mutex
	calls   int
	entries []manifest.Entry
	err     error
}

func (f *fakeRereporter) Rereport(ctx context.Context, entries []manifest.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.entries = entries
	return f.err
}

func (f *fakeRereporter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestFreshDiskStartsEmpty 覆盖数据大文件不存在时按节点口径新建文件头与段区（D55）。
func TestFreshDiskStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	store, verdict, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("open fresh disk: %v", err)
	}
	defer store.Close()
	if verdict != VerdictFresh {
		t.Errorf("verdict = %v, want fresh", verdict)
	}
	if store.TotalSegments() <= 0 {
		t.Errorf("total segments = %d, want positive", store.TotalSegments())
	}
	if store.UsedSegments() != 0 {
		t.Errorf("used segments = %d, want 0", store.UsedSegments())
	}
}

// TestSpecMatchPreservesBlocks 覆盖口径一致时段区原样保留且映射从段头重建（D44、D55）。
func TestSpecMatchPreservesBlocks(t *testing.T) {
	dir := t.TempDir()
	store, verdict, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if verdict != VerdictFresh {
		t.Fatalf("first verdict = %v, want fresh", verdict)
	}
	if _, _, err := store.Put("hash-a", blockData("hash-a")); err != nil {
		t.Fatalf("put hash-a: %v", err)
	}
	if _, _, err := store.Put("hash-b", blockData("hash-b")); err != nil {
		t.Fatalf("put hash-b: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, verdict, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if verdict != VerdictMatch {
		t.Fatalf("reopen verdict = %v, want match", verdict)
	}
	if reopened.UsedSegments() != 2 {
		t.Errorf("used segments = %d, want 2", reopened.UsedSegments())
	}
	for _, hash := range []string{"hash-a", "hash-b"} {
		data, _, err := reopened.Get(hash)
		if err != nil {
			t.Fatalf("get %s: %v", hash, err)
		}
		if !bytes.Equal(data, blockData(hash)) {
			t.Errorf("readback of %s differs from written bytes", hash)
		}
	}
}

// TestFileHeaderCorruptRebuilds 覆盖文件头缺失或损坏时清空数据大文件并重建（D55）。
func TestFileHeaderCorruptRebuilds(t *testing.T) {
	dir := t.TempDir()
	store, _, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, _, err := store.Put("hash-a", blockData("hash-a")); err != nil {
		t.Fatalf("put hash-a: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 文件头截断成垃圾字节，段头不再可用。
	if err := writeFileHeader(dir, make([]byte, segment.FileHeaderSize)); err != nil {
		t.Fatalf("corrupt file header: %v", err)
	}
	rebuilt, verdict, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("rebuild open: %v", err)
	}
	defer rebuilt.Close()
	if verdict != VerdictRebuild {
		t.Fatalf("verdict = %v, want rebuild", verdict)
	}
	if rebuilt.UsedSegments() != 0 {
		t.Errorf("used segments after rebuild = %d, want 0", rebuilt.UsedSegments())
	}
	if _, _, err := rebuilt.Get("hash-a"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("get after rebuild err = %v, want ErrNotFound", err)
	}
}

// TestSpecMismatchRebuilds 覆盖口径不一致时清空重建段区与位图（D44、D55）。
func TestSpecMismatchRebuilds(t *testing.T) {
	dir := t.TempDir()
	store, _, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, _, err := store.Put("hash-a", blockData("hash-a")); err != nil {
		t.Fatalf("put hash-a: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rebuilt, verdict, err := Open(dir, "node-1", altSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("rebuild open: %v", err)
	}
	defer rebuilt.Close()
	if verdict != VerdictRebuild {
		t.Fatalf("verdict = %v, want rebuild", verdict)
	}
	if rebuilt.SegmentSize() == store.SegmentSize() {
		t.Errorf("segment size %d unchanged after spec change, want resized", rebuilt.SegmentSize())
	}
	if rebuilt.UsedSegments() != 0 {
		t.Errorf("used segments after rebuild = %d, want 0", rebuilt.UsedSegments())
	}
	if _, _, err := rebuilt.Get("hash-a"); !errors.Is(err, segment.ErrNotFound) {
		t.Errorf("get after rebuild err = %v, want ErrNotFound", err)
	}
}

// TestRebuildRequestsFullRereport 覆盖清空重建后向决策服务逐片上报一次全量清单（D41、D55）。
func TestRebuildRequestsFullRereport(t *testing.T) {
	dir := t.TempDir()
	store, _, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, _, err := store.Put("hash-a", blockData("hash-a")); err != nil {
		t.Fatalf("put hash-a: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rebuilt, verdict, err := Open(dir, "node-1", altSpec(), testCapacity, Options{})
	if err != nil {
		t.Fatalf("rebuild open: %v", err)
	}
	defer rebuilt.Close()
	if verdict != VerdictRebuild {
		t.Fatalf("verdict = %v, want rebuild", verdict)
	}

	fake := &fakeRereporter{}
	if err := RereportRebuilt(context.Background(), dir, rebuilt, altSpec(), "node-1", fake, nil); err != nil {
		t.Fatalf("rereport: %v", err)
	}
	if fake.count() != 1 {
		t.Errorf("rereport calls = %d, want exactly 1", fake.count())
	}
	if len(fake.entries) != 0 {
		t.Errorf("rereport entries = %d, want 0 after rebuild", len(fake.entries))
	}

	// 重报入口失败按错误返回，调用方降级（D51）。
	failing := &fakeRereporter{err: errors.New("decision unreachable")}
	if err := RereportRebuilt(context.Background(), dir, rebuilt, altSpec(), "node-1", failing, nil); err == nil {
		t.Error("failing rereporter must surface error")
	}
}

// TestStartupLogging 覆盖启动、口径变更、重建完成、清单重报四处打点（D55）：
// 字段含 node_id、口径、耗时与结果。
func TestStartupLogging(t *testing.T) {
	capture := &logCapture{}
	log := telemetry.NewWithHook(capture.hook)
	dir := t.TempDir()

	store, _, err := Open(dir, "node-1", baseSpec(), testCapacity, Options{Log: log})
	if err != nil {
		t.Fatalf("fresh open: %v", err)
	}
	if _, _, err := store.Put("hash-a", blockData("hash-a")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rebuilt, verdict, err := Open(dir, "node-1", altSpec(), testCapacity, Options{Log: log})
	if err != nil {
		t.Fatalf("rebuild open: %v", err)
	}
	defer rebuilt.Close()
	if verdict != VerdictRebuild {
		t.Fatalf("verdict = %v, want rebuild", verdict)
	}

	fake := &fakeRereporter{}
	if err := RereportRebuilt(context.Background(), dir, rebuilt, altSpec(), "node-1", fake, log); err != nil {
		t.Fatalf("rereport: %v", err)
	}

	for msg, want := range map[string]int{
		"startup spec verified":                       1,
		"startup spec mismatch, rebuilding data file": 1,
		"startup rebuild completed":                   1,
		"manifest rereport completed":                 1,
	} {
		lines := capture.byMsg(msg)
		if len(lines) != want {
			t.Errorf("msg %q count = %d, want %d", msg, len(lines), want)
			continue
		}
		f := lines[0].f
		if f.NodeID != "node-1" {
			t.Errorf("msg %q node_id = %q, want node-1", msg, f.NodeID)
		}
		if f.Spec == "" || f.DurationMs < 0 || f.Result == "" {
			t.Errorf("msg %q fields = spec %q duration %d result %q, want all present", msg, f.Spec, f.DurationMs, f.Result)
		}
	}
	if lines := capture.byMsg("startup spec mismatch, rebuilding data file"); len(lines) == 1 && lines[0].f.Reason == "" {
		t.Error("spec mismatch log must carry the original mismatch reason")
	}
	if lines := capture.byMsg("manifest rereport completed"); len(lines) == 1 && lines[0].f.Entries != 0 {
		t.Errorf("rereport entries = %d, want 0 after rebuild", lines[0].f.Entries)
	}
}

// writeFileHeader 把数据大文件前 71 字节覆盖成给定内容，模拟文件头损坏（D55）。
func writeFileHeader(dir string, buf []byte) error {
	return os.WriteFile(filepath.Join(dir, dataFileName), buf, 0o644)
}

type logCapture struct {
	mu    sync.Mutex
	lines []capturedLine
}

type capturedLine struct {
	level string
	msg   string
	f     telemetry.Fields
}

func (c *logCapture) hook(level, msg string, f telemetry.Fields, line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, capturedLine{level: level, msg: msg, f: f})
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
