package segment

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/pin"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

func testSpec() spec.KVCacheSpec {
	return spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 128}
}

type recorder struct {
	events []string
}

func (r *recorder) record(name string) {
	r.events = append(r.events, name)
}

func openTestStore(t *testing.T, dir string, capacity int64, rec *recorder) *Store {
	t.Helper()
	opts := Options{
		Log: telemetry.NewWithHook(func(level, msg string, f telemetry.Fields, line []byte) {}),
	}
	if rec != nil {
		opts.Emit = func(e Event) { rec.record(e.Kind) }
		opts.AfterSync = func() { rec.record("sync") }
	}
	s, err := Open(dir, "node-1", testSpec(), capacity, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestFileHeaderSpecAndSegmentOffset(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256*4, nil)
	if got := s.SegmentSize(); got != 256 {
		t.Errorf("segment size = %d, want 256 (alignUp(71+92+64, 128))", got)
	}
	if got := s.TotalSegments(); got != 4 {
		t.Errorf("total segments = %d, want 4", got)
	}

	// 文件头落盘：重开时口径与段数逐项一致才成功。
	s.Close()
	s2, err := Open(dir, "node-1", testSpec(), 256*4, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	// 口径不一致按错误拒绝（清空重建不在本层）。
	other := testSpec()
	other.BlockBytes = 128
	if _, err := Open(dir, "node-1", other, 256*4, Options{}); !errors.Is(err, ErrFileHeaderMismatch) {
		t.Errorf("spec mismatch should be rejected, got %v", err)
	}
}

func TestPutGetByOffset(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256*4, nil)

	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	seq, isNew, err := s.Put("hash-a", data)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if seq != 0 || !isNew {
		t.Errorf("put = seq %d isNew %v, want 0 true", seq, isNew)
	}

	wantOffset := FileHeaderSize + seq*s.SegmentSize()
	if wantOffset != FileHeaderSize {
		t.Errorf("first segment offset = %d, want %d", wantOffset, FileHeaderSize)
	}

	got, gseq, err := s.Get("hash-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gseq != seq {
		t.Errorf("get seq = %d, want %d", gseq, seq)
	}
	if !bytes.Equal(got, data) {
		t.Error("read back bytes differ")
	}

	// 同键覆写复用原段号，位图不变。
	seq2, isNew2, err := s.Put("hash-a", data)
	if err != nil || seq2 != seq || isNew2 {
		t.Errorf("overwrite = seq %d isNew %v err %v, want same seq false nil", seq2, isNew2, err)
	}

	// 段号定位数据区按偏移读写：段偏移公式与文件布局一致。
	off := FileHeaderSize + seq*s.SegmentSize()
	if _, err := s.f.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	hbuf := make([]byte, HeaderSize)
	if _, err := s.f.ReadAt(hbuf, off); err != nil {
		t.Fatalf("read header: %v", err)
	}
	h, err := Decode(hbuf)
	if err != nil || h.State != StateWritten || h.Hash != "hash-a" {
		t.Errorf("header at offset = %+v err %v", h, err)
	}
}

func TestPutGetRejectsTamperedData(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256*4, nil)
	data := make([]byte, 64)
	if _, _, err := s.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}
	off := int64(FileHeaderSize + HeaderSize)
	if err := os.WriteFile(dir+"/probe", nil, 0o600); err != nil {
		t.Fatalf("probe: %v", err)
	}
	f, err := os.OpenFile(dir+"/blocks.dat", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte{0xFF}, off); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	_, _, err = s.Get("hash-a")
	if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("tampered read = %v, want DataLoss", err)
	}
}

func TestFsyncBeforeEvent(t *testing.T) {
	dir := t.TempDir()
	rec := &recorder{}
	s := openTestStore(t, dir, 256*4, rec)
	data := make([]byte, 64)
	if _, _, err := s.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}
	if len(rec.events) != 2 {
		t.Fatalf("recorded events = %v, want [sync register]", rec.events)
	}
	if rec.events[0] != "sync" || rec.events[1] != "register" {
		t.Errorf("order = %v, want fsync before emit", rec.events)
	}
}

func TestTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256*4, nil)
	data := make([]byte, 64)
	if _, _, err := s.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}
	s.Close()

	// 模拟掉电：段头状态停在写入中、CRC 未写全。
	f, err := os.OpenFile(dir+"/blocks.dat", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	torn := Header{Hash: "hash-a", SegmentSeq: 0, BlockBytes: 64, State: StateWriting, DataCRC: 0}
	buf := make([]byte, HeaderSize)
	if err := torn.Encode(buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := f.WriteAt(buf, FileHeaderSize); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	f.Close()

	s2, err := Open(dir, "node-1", testSpec(), 256*4, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, _, err := s2.Get("hash-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("torn block should be absent from index, got %v", err)
	}
	seq, ok := s2.bm.Allocate()
	if !ok || seq != 0 {
		t.Errorf("torn segment should return to bitmap, allocate = %d ok=%v", seq, ok)
	}
}

func TestReconcileBitmapHeaderAuthority(t *testing.T) {
	bm := NewBitmap(4)
	bm.Mark(1)
	heads := []Header{
		{Hash: "hash-a", State: StateWritten},
		{Hash: "hash-b", State: StateEmpty},
		{Hash: "hash-c", State: StateWritten},
		{Hash: "", State: StateEmpty},
	}
	bad := []bool{false, false, true, false}
	index := ReconcileBitmap(bm, heads, bad)
	if bm.IsOccupied(1) {
		t.Error("occupied bitmap bit with empty segment header should be released")
	}
	if !bm.IsOccupied(0) {
		t.Error("written segment with unmarked bitmap bit should be marked")
	}
	if bm.IsOccupied(2) {
		t.Error("bad segment should be released")
	}
	if _, ok := index["hash-a"]; !ok {
		t.Error("written segment should be indexed")
	}
	if _, ok := index["hash-b"]; ok {
		t.Error("empty segment should not be indexed")
	}
	if len(index) != 1 {
		t.Errorf("index size = %d, want 1", len(index))
	}

	// 撕裂写状态按损坏处理。
	bm2 := NewBitmap(2)
	bm2.Mark(0)
	heads2 := []Header{{Hash: "hash-t", State: StateWriting}, {}}
	bad2 := []bool{false, false}
	ReconcileBitmap(bm2, heads2, bad2)
	if bm2.IsOccupied(0) {
		t.Error("torn segment should be treated as corrupt and released")
	}
}

func TestReopenRebuildsIndex(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256*4, nil)
	a := make([]byte, 64)
	b := make([]byte, 64)
	b[0] = 1
	if _, _, err := s.Put("hash-a", a); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if _, _, err := s.Put("hash-b", b); err != nil {
		t.Fatalf("put b: %v", err)
	}
	s.Close()

	s2, err := Open(dir, "node-1", testSpec(), 256*4, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	gotA, _, err := s2.Get("hash-a")
	if err != nil || !bytes.Equal(gotA, a) {
		t.Errorf("get a after reopen = %v err %v", gotA, err)
	}
	gotB, _, err := s2.Get("hash-b")
	if err != nil || !bytes.Equal(gotB, b) {
		t.Errorf("get b after reopen = %v err %v", gotB, err)
	}
	if s2.HighWater() != 2 {
		t.Errorf("high water = %d, want 2", s2.HighWater())
	}
}

func TestNoFreeSegment(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256, nil)
	data := make([]byte, 64)
	if _, _, err := s.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}
	_, _, err := s.Put("hash-b", data)
	if !errors.Is(err, ErrNoFreeSegment) {
		t.Errorf("put on full store = %v, want ErrNoFreeSegment", err)
	}
	// 被拒的块不占段：原块仍可读。
	if _, _, err := s.Get("hash-a"); err != nil {
		t.Errorf("existing block should survive, err %v", err)
	}
}

func TestPutLengthGuard(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 256*4, nil)
	if _, _, err := s.Put("hash-a", make([]byte, 32)); !errors.Is(err, ErrBadPutLength) {
		t.Errorf("half block put = %v, want ErrBadPutLength", err)
	}
	if _, _, err := s.Put("", make([]byte, 64)); !errors.Is(err, ErrBadPutLength) {
		t.Errorf("empty hash put = %v, want ErrBadPutLength", err)
	}
}

// ---------- 水位驱逐与 pin 接入 ----------

type staticPins map[string]bool

func (p staticPins) Pinned(h string) bool { return p[h] }

type evRec struct {
	mu      sync.Mutex
	events  []Event
	onEvict func(Event)
}

func (r *evRec) emit(e Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
	if e.Kind == "evict" && r.onEvict != nil {
		r.onEvict(e)
	}
}

func (r *evRec) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

func (r *evRec) count(kind string) int {
	n := 0
	for _, e := range r.snapshot() {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

type logRec struct {
	mu     sync.Mutex
	msgs   []string
	fields []telemetry.Fields
}

func (c *logRec) record(level, msg string, f telemetry.Fields, line []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, msg)
	c.fields = append(c.fields, f)
}

func (c *logRec) count(msg string) int {
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

func openStoreFull(t *testing.T, dir string, capacity int64, watermark int, pins PinView, rec *evRec, logs *logRec) *Store {
	t.Helper()
	opts := Options{
		Log:          telemetry.NewWithHook(logs.record),
		Pins:         pins,
		WatermarkPct: watermark,
	}
	if rec != nil {
		opts.Emit = rec.emit
	}
	s, err := Open(dir, "node-1", testSpec(), capacity, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

type fakePinClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakePinClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakePinClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestWatermarkEvictionFillsAndDrains(t *testing.T) {
	dir := t.TempDir()
	rec := &evRec{}
	logs := &logRec{}
	s := openStoreFull(t, dir, 256*4, 50, nil, rec, logs)

	// 驱逐顺序断言：evict 事件发射时刻该键已摘除而段号尚未回位图（D43）。
	rec.onEvict = func(e Event) {
		if !s.bm.IsOccupied(e.SegmentSeq) {
			t.Errorf("evict event for %s fired after segment %d returned to bitmap", e.BlockHash, e.SegmentSeq)
		}
		if _, still := s.index[e.BlockHash]; still {
			t.Errorf("evict event for %s fired before key removal from index", e.BlockHash)
		}
	}

	data := make([]byte, 64)
	for _, h := range []string{"hash-a", "hash-b", "hash-c", "hash-d"} {
		if _, _, err := s.Put(h, data); err != nil {
			t.Fatalf("put %s: %v", h, err)
		}
	}
	// Put c 触发驱逐 a（75% 超水位，驱后 50%），Put d 触发驱逐 b。
	if got := s.UsedSegments(); got != 2 {
		t.Errorf("used segments = %d, want 2 after eviction to watermark", got)
	}
	for _, h := range []string{"hash-a", "hash-b"} {
		if _, _, err := s.Get(h); !errors.Is(err, ErrNotFound) {
			t.Errorf("evicted %s = %v, want NotFound", h, err)
		}
	}
	for _, h := range []string{"hash-c", "hash-d"} {
		if _, _, err := s.Get(h); err != nil {
			t.Errorf("kept %s = %v, want readable", h, err)
		}
	}
	if got := rec.count("evict"); got != 2 {
		t.Errorf("evict events = %d, want 2", got)
	}
	for _, e := range rec.snapshot() {
		if e.Kind == "evict" && e.Reason != ReasonWatermark {
			t.Errorf("evict reason = %q, want %q", e.Reason, ReasonWatermark)
		}
	}
	if got := logs.count("block evicted"); got != 2 {
		t.Errorf("eviction logs = %d, want 2", got)
	}
}

func TestWatermarkEvictionSkipsPinned(t *testing.T) {
	dir := t.TempDir()
	rec := &evRec{}
	logs := &logRec{}
	s := openStoreFull(t, dir, 256*4, 50, staticPins{"hash-a": true}, rec, logs)

	data := make([]byte, 64)
	for _, h := range []string{"hash-a", "hash-b", "hash-c", "hash-d"} {
		if _, _, err := s.Put(h, data); err != nil {
			t.Fatalf("put %s: %v", h, err)
		}
	}
	// pin 的段在不可驱逐集合：两次驱逐都跳过 hash-a，取次旧的 b 与 c。
	if got := s.UsedSegments(); got != 2 {
		t.Errorf("used segments = %d, want 2", got)
	}
	if _, _, err := s.Get("hash-a"); err != nil {
		t.Errorf("pinned %s = %v, want readable", "hash-a", err)
	}
	for _, h := range []string{"hash-b", "hash-c"} {
		if _, _, err := s.Get(h); !errors.Is(err, ErrNotFound) {
			t.Errorf("evicted %s = %v, want NotFound", h, err)
		}
	}
	if _, _, err := s.Get("hash-d"); err != nil {
		t.Errorf("kept %s = %v, want readable", "hash-d", err)
	}
	for _, e := range rec.snapshot() {
		if e.Kind == "evict" && e.BlockHash == "hash-a" {
			t.Error("pinned block must never be evicted")
		}
	}
}

func TestDropBlockPinReclaimed(t *testing.T) {
	dir := t.TempDir()
	rec := &evRec{}
	logs := &logRec{}
	s := openStoreFull(t, dir, 256*4, 0, nil, rec, logs)
	data := make([]byte, 64)
	if _, _, err := s.Put("hash-a", data); err != nil {
		t.Fatalf("put: %v", err)
	}

	seq, ok := s.DropBlock("hash-a", ReasonPinQuota)
	if !ok || seq != 0 {
		t.Fatalf("drop = seq %d ok %v, want 0 true", seq, ok)
	}
	if _, _, err := s.Get("hash-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("dropped read = %v, want NotFound", err)
	}
	if got := s.UsedSegments(); got != 0 {
		t.Errorf("used segments = %d, want 0", got)
	}
	if got, ok := s.bm.Allocate(); !ok || got != 0 {
		t.Errorf("reallocate = %d ok %v, want 0 true", got, ok)
	}
	if got := rec.count("evict"); got != 1 {
		t.Errorf("evict events = %d, want 1", got)
	}
	for _, e := range rec.snapshot() {
		if e.Kind == "evict" && e.Reason != ReasonPinQuota {
			t.Errorf("evict reason = %q, want %q", e.Reason, ReasonPinQuota)
		}
	}
	if got := logs.count("pin reclaimed"); got != 1 {
		t.Errorf("pin reclaim logs = %d, want 1", got)
	}
	if _, ok := s.DropBlock("hash-missing", ReasonPinQuota); ok {
		t.Error("drop of unknown key must report false")
	}
}

func TestReopenPinRecoveredGuardsEviction(t *testing.T) {
	dir := t.TempDir()
	clk := &fakePinClock{now: time.Unix(0, 0)}
	tab := pin.New(pin.Options{DataDir: dir, BlockBytes: 64, Clock: clk})
	logs := &logRec{}
	s1 := openStoreFull(t, dir, 256*4, 0, nil, nil, logs)
	data := make([]byte, 64)
	if _, _, err := s1.Put("hash-a", data); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if _, _, err := s1.Put("hash-b", data); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := tab.Add("hash-a", time.Hour); err != nil {
		t.Fatalf("pin a: %v", err)
	}
	s1.Close()

	// 重启先恢复 pin（pin 清单加载先于段头扫描重建），pin 的段重建后直接进不可驱逐集合（R34）。
	tab2 := pin.New(pin.Options{DataDir: dir, BlockBytes: 64, Clock: clk})
	if err := tab2.Load(); err != nil {
		t.Fatalf("load pin manifest: %v", err)
	}
	if !tab2.Pinned("hash-a") {
		t.Fatal("pin must be restored before segment scan rebuild")
	}
	rec := &evRec{}
	logs2 := &logRec{}
	s2 := openStoreFull(t, dir, 256*4, 50, tab2, rec, logs2)
	if _, _, err := s2.Put("hash-c", data); err != nil {
		t.Fatalf("put c: %v", err)
	}
	// 水位驱逐跳过 pin 的 hash-a，驱最旧未 pin 的 hash-b。
	if _, _, err := s2.Get("hash-a"); err != nil {
		t.Errorf("pinned block after reopen = %v, want readable", err)
	}
	if _, _, err := s2.Get("hash-b"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unpinned oldest = %v, want evicted", err)
	}
	if _, _, err := s2.Get("hash-c"); err != nil {
		t.Errorf("newest block = %v, want readable", err)
	}
	for _, e := range rec.snapshot() {
		if e.Kind == "evict" && e.BlockHash == "hash-a" {
			t.Error("restored pin must guard its segment from eviction")
		}
	}
}

func TestExpiredPinReleasedThenNotFound(t *testing.T) {
	dir := t.TempDir()
	clk := &fakePinClock{now: time.Unix(0, 0)}
	tab := pin.New(pin.Options{DataDir: dir, BlockBytes: 64, Clock: clk})
	logs := &logRec{}
	s1 := openStoreFull(t, dir, 256*2, 0, nil, nil, logs)
	data := make([]byte, 64)
	if _, _, err := s1.Put("hash-a", data); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := tab.Add("hash-a", 1*time.Second); err != nil {
		t.Fatalf("pin a: %v", err)
	}
	s1.Close()
	clk.advance(2 * time.Second)

	// 重启恢复时过期条目不恢复：标记释放回普通候选，随后驱逐轮转回收，读回 NotFound（R34）。
	tab2 := pin.New(pin.Options{DataDir: dir, BlockBytes: 64, Clock: clk})
	if err := tab2.Load(); err != nil {
		t.Fatalf("load pin manifest: %v", err)
	}
	if got := tab2.Hashes(); len(got) != 0 {
		t.Errorf("restored hashes = %v, want empty for expired pin", got)
	}
	rec := &evRec{}
	logs2 := &logRec{}
	s2 := openStoreFull(t, dir, 256*2, 50, tab2, rec, logs2)
	if _, _, err := s2.Put("hash-b", data); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if _, _, err := s2.Get("hash-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expired block read = %v, want NotFound", err)
	}
	if _, _, err := s2.Get("hash-b"); err != nil {
		t.Errorf("new block read = %v, want readable", err)
	}
	found := false
	for _, e := range rec.snapshot() {
		if e.Kind == "evict" && e.BlockHash == "hash-a" {
			found = true
		}
	}
	if !found {
		t.Error("released expired block should be evicted as normal candidate")
	}
}
