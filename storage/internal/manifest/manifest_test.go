package manifest

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

func nodeSpec() spec.KVCacheSpec {
	return spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

func openStore(t *testing.T, dir string) *segment.Store {
	t.Helper()
	store, err := segment.Open(dir, "node-1", nodeSpec(), 64*4096, segment.Options{Log: telemetry.New()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// reporterStub 是走真 gRPC 栈的清单接收服务端（D41）：逐片暂存并按片拼出登记哈希集。
type reporterStub struct {
	api.UnimplementedStorageReporterServer
	mu      sync.Mutex
	shards  map[int64][]*api.ManifestEntry
	totals  map[string]int64
	nodeIDs []string
}

func (s *reporterStub) ReportManifest(ctx context.Context, req *api.ReportManifestRequest) (*api.ReportManifestResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shards == nil {
		s.shards = make(map[int64][]*api.ManifestEntry)
		s.totals = make(map[string]int64)
	}
	s.shards[req.GetShardIndex()] = req.GetEntries()
	s.totals[req.GetNodeId()] = req.GetShardTotal()
	s.nodeIDs = append(s.nodeIDs, req.GetNodeId())
	return &api.ReportManifestResponse{Accepted: true}, nil
}

func (s *reporterStub) registrations() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64)
	for _, shard := range s.shards {
		for _, e := range shard {
			out[e.GetBlockHash()] = e.GetOffset()
		}
	}
	return out
}

func (s *reporterStub) shardTotal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, t := range s.totals {
		total = t
	}
	return total
}

func (s *reporterStub) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.nodeIDs)
}

func startReporterStub(t *testing.T) (api.StorageReporterClient, *reporterStub) {
	t.Helper()
	stub := &reporterStub{}
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, stub)
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
	})
	return api.NewStorageReporterClient(conn), stub
}

func TestSnapshotRoundtrip(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := Load(dir); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("missing snapshot = %v, want ErrNoSnapshot", err)
	}
	occupied := []int64{0, 2, 5}
	if err := Write(dir, occupied, 7); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, highWater, err := Load(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if highWater != 7 {
		t.Errorf("high_water = %d, want 7", highWater)
	}
	if len(got) != len(occupied) {
		t.Fatalf("occupied = %v, want %v", got, occupied)
	}
	for i := range occupied {
		if got[i] != occupied[i] {
			t.Errorf("occupied[%d] = %d, want %d", i, got[i], occupied[i])
		}
	}
}

func TestSnapshotInvalidRejected(t *testing.T) {
	dir := t.TempDir()
	if err := Write(dir, []int64{1}, 2); err != nil {
		t.Fatalf("write: %v", err)
	}
	path := dir + "/" + SnapshotFile
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if _, _, err := Load(dir); !errors.Is(err, ErrSnapshotInvalid) {
		t.Fatalf("tampered snapshot = %v, want ErrSnapshotInvalid", err)
	}
}

func TestSnapshotPeriodicWithHeartbeat(t *testing.T) {
	dir := t.TempDir()
	var current atomic.Int64
	current.Store(1)
	src := SourceFunc(func() ([]int64, int64, error) {
		return []int64{0}, current.Load(), nil
	})
	interval := 30 * time.Millisecond
	s := NewSnapshotter(dir, interval, src, telemetry.New())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	seen := map[int64]bool{}
	for time.Now().Before(deadline) {
		if _, hw, err := Load(dir); err == nil {
			seen[hw] = true
			if len(seen) >= 2 {
				break
			}
		}
		current.Add(1)
		time.Sleep(interval / 2)
	}
	if len(seen) < 2 {
		t.Fatalf("periodic writes = %v, want at least two distinct high waters", seen)
	}
}

func TestShardsIndexAndTotal(t *testing.T) {
	entries := make([]Entry, 7)
	for i := range entries {
		entries[i] = Entry{BlockHash: "hash-" + string(rune('a'+i)), SegmentSeq: int64(i)}
	}
	shards := Shards(entries, 3)
	if len(shards) != 3 {
		t.Fatalf("shards = %d, want 3", len(shards))
	}
	if len(shards[0]) != 3 || len(shards[1]) != 3 || len(shards[2]) != 1 {
		t.Fatalf("shard sizes = %d %d %d, want 3 3 1", len(shards[0]), len(shards[1]), len(shards[2]))
	}
	for i, shard := range shards {
		for j, e := range shard {
			want := int64(i*3 + j)
			if e.SegmentSeq != want {
				t.Errorf("shard %d entry %d seq = %d, want %d", i, j, e.SegmentSeq, want)
			}
		}
	}
	if got := Shards(nil, 3); len(got) != 1 || len(got[0]) != 0 {
		t.Errorf("empty entries shards = %v, want one empty shard", got)
	}
}

func TestReportAgainstRealServerMatchesSnapshot(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	data := make([]byte, 64)
	for i := 0; i < 5; i++ {
		if _, _, err := store.Put("hash-"+string(rune('a'+i)), data); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	occupied, highWater, err := gatherSource(dir, store)
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if err := Write(dir, occupied, highWater); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	snapOccupied, snapHighWater, err := Load(dir)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if len(snapOccupied) != 5 || snapHighWater != 5 {
		t.Fatalf("snapshot occupied %d high_water %d, want 5/5", len(snapOccupied), snapHighWater)
	}

	client, stub := startReporterStub(t)
	_, hashes, err := ScanSegmentHeads(dir, store.SegmentSize(), store.TotalSegments())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	entries := BuildEntries(hashes, store.SegmentSize(), 64)
	rep := NewReporter("node-1", 2, client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rep.Report(ctx, entries); err != nil {
		t.Fatalf("report: %v", err)
	}

	registrations := stub.registrations()
	if len(registrations) != len(snapOccupied) {
		t.Fatalf("decision registrations = %d, want snapshot blocks %d", len(registrations), len(snapOccupied))
	}
	for hash := range registrations {
		if _, ok := hashes[hash]; !ok {
			t.Errorf("registration %s not in scan result", hash)
		}
	}
	if stub.shardTotal() != 3 {
		t.Errorf("shard_total = %d, want 3", stub.shardTotal())
	}
	if stub.calls() != 3 {
		t.Errorf("report calls = %d, want 3", stub.calls())
	}
}

func TestSnapshotMatchesSegmentHeadsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	data := make([]byte, 64)
	for i := 0; i < 3; i++ {
		if _, _, err := store.Put("hash-"+string(rune('a'+i)), data); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	src := SourceFunc(func() ([]int64, int64, error) {
		occupied, _, err := ScanSegmentHeads(dir, store.SegmentSize(), store.TotalSegments())
		if err != nil {
			return nil, 0, err
		}
		return occupied, store.HighWater(), nil
	})
	s := NewSnapshotter(dir, time.Second, src, telemetry.New())
	if err := s.WriteOnce(); err != nil {
		t.Fatalf("write once: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := segment.Open(dir, "node-1", nodeSpec(), 64*4096, segment.Options{Log: telemetry.New()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	snapOccupied, snapHighWater, err := Load(dir)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	scanOccupied, hashes, err := ScanSegmentHeads(dir, reopened.SegmentSize(), reopened.TotalSegments())
	if err != nil {
		t.Fatalf("scan after restart: %v", err)
	}
	if len(snapOccupied) != len(scanOccupied) {
		t.Fatalf("snapshot occupied %d, segment heads %d", len(snapOccupied), len(scanOccupied))
	}
	for i := range snapOccupied {
		if snapOccupied[i] != scanOccupied[i] {
			t.Errorf("occupied[%d] = %d, want %d", i, snapOccupied[i], scanOccupied[i])
		}
	}
	if snapHighWater != reopened.HighWater() {
		t.Errorf("snapshot high_water = %d, reopened bitmap high_water = %d", snapHighWater, reopened.HighWater())
	}
	if len(hashes) != 3 {
		t.Errorf("hashes = %d, want 3", len(hashes))
	}
}

func gatherSource(dir string, store *segment.Store) ([]int64, int64, error) {
	occupied, _, err := ScanSegmentHeads(dir, store.SegmentSize(), store.TotalSegments())
	if err != nil {
		return nil, 0, err
	}
	return occupied, store.HighWater(), nil
}
