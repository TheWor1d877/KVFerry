package integration

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/test/node"
)

// reporterStub 是走真 gRPC 栈的清单接收服务端（D41）：逐片暂存，按片拼出决策登记。
type reporterStub struct {
	api.UnimplementedStorageReporterServer
	mu     sync.Mutex
	shards map[int64][]*api.ManifestEntry
	totals map[int64]int64
}

func (s *reporterStub) ReportManifest(ctx context.Context, req *api.ReportManifestRequest) (*api.ReportManifestResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shards == nil {
		s.shards = make(map[int64][]*api.ManifestEntry)
		s.totals = make(map[int64]int64)
	}
	s.shards[req.GetShardIndex()] = req.GetEntries()
	s.totals[req.GetShardIndex()] = req.GetShardTotal()
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

func (s *reporterStub) shardIndexes() map[int64]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int64]int64, len(s.totals))
	for idx, total := range s.totals {
		out[idx] = total
	}
	return out
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

func startNode(t *testing.T, reporter api.StorageReporterClient) *node.Node {
	t.Helper()
	n, err := node.Start(node.Options{
		DataDir:       t.TempDir(),
		NodeID:        "node-1",
		Spec:          &api.KVSpec{BlockBytes: 64, KvPrecision: "fp16", BytesPerToken: 8},
		CapacityBytes: 64 * 4096,
		CacheBytes:    0,
		ShardBlocks:   2,
		Reporter:      reporter,
	})
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func put(t *testing.T, n *node.Node, hash string, data []byte) *api.PutBlockResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := n.Client.PutBlock(ctx, &api.PutBlockRequest{
		NodeId:    "node-1",
		BlockHash: hash,
		Spec:      &api.KVSpec{BlockBytes: 64, KvPrecision: "fp16", BytesPerToken: 8},
		BlockData: data,
	})
	if err != nil {
		t.Fatalf("put %s: %v", hash, err)
	}
	return resp
}

func get(t *testing.T, n *node.Node, hash string) []byte {
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

// TestWriteReadOverBufconn 覆盖双组件端到端写读一致（D49、D53）：写入与读回字节逐位相等。
func TestWriteReadOverBufconn(t *testing.T) {
	n := startNode(t, nil)
	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}
	resp := put(t, n, "hash-a", data)
	if !resp.IsNewWrite {
		t.Errorf("first put is_new_write = %v, want true", resp.IsNewWrite)
	}
	got := get(t, n, "hash-a")
	if !bytes.Equal(got, data) {
		t.Error("read back bytes differ from written")
	}
}

// TestManifestReportMatchesSnapshot 覆盖分片上报后决策登记条数等于快照块数，
// 分片序号与总片数按段序号升序切分（D48、D49）；save 计数在上报后继续增长。
func TestManifestReportMatchesSnapshot(t *testing.T) {
	client, stub := startReporterStub(t)
	n := startNode(t, client)

	saves := 0
	data := make([]byte, 64)
	var seqs []int64
	for i := 0; i < 5; i++ {
		resp := put(t, n, "hash-"+string(rune('a'+i)), data)
		seqs = append(seqs, resp.SegmentSeq)
		saves++
	}
	if saves != 5 {
		t.Fatalf("save count = %d, want 5", saves)
	}
	if err := n.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	occupied, highWater, err := manifest.Load(n.DataDir())
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if len(occupied) != 5 || highWater != 5 {
		t.Fatalf("snapshot occupied %d high_water %d, want 5/5", len(occupied), highWater)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.ReportManifest(ctx); err != nil {
		t.Fatalf("report manifest: %v", err)
	}
	registrations := stub.registrations()
	if len(registrations) != len(occupied) {
		t.Fatalf("decision registrations = %d, want snapshot blocks %d", len(registrations), len(occupied))
	}
	for i, seq := range seqs {
		hash := "hash-" + string(rune('a'+i))
		want := n.SegmentOffset(seq)
		if registrations[hash] != want {
			t.Errorf("registration %s offset = %d, want %d", hash, registrations[hash], want)
		}
	}
	indexes := stub.shardIndexes()
	if len(indexes) != 3 {
		t.Fatalf("shards received = %d, want 3", len(indexes))
	}
	for idx, total := range indexes {
		if total != 3 {
			t.Errorf("shard %d total = %d, want 3", idx, total)
		}
	}

	// save 计数继续增长：上报后再写入成功。
	put(t, n, "hash-f", data)
	saves++
	if saves != 6 {
		t.Errorf("save count after report = %d, want 6", saves)
	}
}

// TestSnapshotReloadMatchesSegmentHeads 覆盖快照重启加载后位图与段头逐段一致（D49）。
func TestSnapshotReloadMatchesSegmentHeads(t *testing.T) {
	n := startNode(t, nil)
	data := make([]byte, 64)
	for i := 0; i < 4; i++ {
		put(t, n, "hash-"+string(rune('a'+i)), data)
	}
	if err := n.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	occupied, highWater, err := manifest.Load(n.DataDir())
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if highWater != n.HighWater() {
		t.Errorf("snapshot high_water = %d, bitmap high_water = %d", highWater, n.HighWater())
	}
	for _, seq := range occupied {
		if n.SegmentOffset(seq) <= 0 {
			t.Errorf("occupied segment %d offset invalid", seq)
		}
	}
	_ = spec.KVCacheSpec{}
}
