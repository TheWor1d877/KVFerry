package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/event"
	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
)

// fakeReporter 是心跳测试的决策端替身（D41）：记录请求，按脚本回 pin 变更与全量重报，
// 实现客户端接口供 HeartbeatDeps.Client 注入。
type fakeReporter struct {
	mu       sync.Mutex
	requests []*api.HeartbeatRequest
	pins     []*api.PinChange
	full     bool
}

func (f *fakeReporter) Heartbeat(ctx context.Context, req *api.HeartbeatRequest, _ ...grpc.CallOption) (*api.HeartbeatResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	pins, full := f.pins, f.full
	f.mu.Unlock()
	return &api.HeartbeatResponse{PinChanges: pins, FullRereport: full}, nil
}

func (f *fakeReporter) ReportManifest(ctx context.Context, req *api.ReportManifestRequest, _ ...grpc.CallOption) (*api.ReportManifestResponse, error) {
	return &api.ReportManifestResponse{Accepted: true}, nil
}

func (f *fakeReporter) StreamEvents(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[api.StorageEvent, api.EventAck], error) {
	return nil, context.Canceled
}

func (f *fakeReporter) recorded() []*api.HeartbeatRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*api.HeartbeatRequest(nil), f.requests...)
}

func hbEntries(n int) []manifest.Entry {
	entries := make([]manifest.Entry, n)
	for i := range entries {
		entries[i] = manifest.Entry{
			BlockHash:  "hash",
			SegmentSeq: int64(i),
			Layer:      api.BlockLayer_BLOCK_LAYER_LOCAL,
			Offset:     int64(100 + i),
			BlockBytes: 64,
		}
	}
	return entries
}

// TestHeartbeatRequestFields 覆盖心跳请求字段齐全（D41、D48）。
func TestHeartbeatRequestFields(t *testing.T) {
	f := &fakeReporter{}
	h := NewHeartbeat(HeartbeatDeps{
		NodeID:      "node-1",
		Client:      f,
		ShardBlocks: 4096,
		Source:      func() ([]manifest.Entry, float64, error) { return hbEntries(2), 0.5, nil },
	})
	h.Beat(context.Background())
	reqs := f.recorded()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	req := reqs[0]
	if req.GetNodeId() != "node-1" || req.GetCapacityWatermark() != 0.5 {
		t.Fatalf("request header = %+v", req)
	}
	if req.GetShardIndex() != 0 || req.GetShardTotal() != 1 {
		t.Fatalf("shard info = %d/%d, want 0/1", req.GetShardIndex(), req.GetShardTotal())
	}
	if len(req.GetEntries()) != 2 {
		t.Fatalf("entries = %d, want 2", len(req.GetEntries()))
	}
	e := req.GetEntries()[0]
	if e.GetBlockHash() != "hash" || e.GetLayer() != api.BlockLayer_BLOCK_LAYER_LOCAL || e.GetOffset() != 100 || e.GetBlockBytes() != 64 {
		t.Fatalf("entry = %+v", e)
	}
}

// TestHeartbeatSharding 覆盖条目超 manifest_shard_blocks 时按片切分（D48）。
func TestHeartbeatSharding(t *testing.T) {
	f := &fakeReporter{}
	h := NewHeartbeat(HeartbeatDeps{
		NodeID:      "node-1",
		Client:      f,
		ShardBlocks: 2,
		Source:      func() ([]manifest.Entry, float64, error) { return hbEntries(5), 0.25, nil },
	})
	h.Beat(context.Background())
	reqs := f.recorded()
	if len(reqs) != 3 {
		t.Fatalf("requests = %d, want 3 shards", len(reqs))
	}
	for i, req := range reqs {
		if req.GetShardIndex() != int64(i) || req.GetShardTotal() != 3 {
			t.Errorf("shard %d info = %d/%d, want %d/3", i, req.GetShardIndex(), req.GetShardTotal(), i)
		}
		if len(req.GetEntries()) > 2 {
			t.Errorf("shard %d carries %d entries, want at most 2", i, len(req.GetEntries()))
		}
	}
	total := 0
	for _, req := range reqs {
		total += len(req.GetEntries())
	}
	if total != 5 {
		t.Errorf("shards carry %d entries, want 5", total)
	}
}

// TestHeartbeatPinChanges 覆盖响应 pin 变更下发消费回调（D41、R25）。
func TestHeartbeatPinChanges(t *testing.T) {
	f := &fakeReporter{pins: []*api.PinChange{
		{BlockHash: "hash-a", Pinned: true, PinTtlS: 60},
		{BlockHash: "hash-b", Pinned: false},
	}}
	var got []*api.PinChange
	h := NewHeartbeat(HeartbeatDeps{
		NodeID: "node-1",
		Client: f,
		Source: func() ([]manifest.Entry, float64, error) { return hbEntries(1), 0, nil },
		Pins: PinConsumerFunc(func(c *api.PinChange) {
			got = append(got, c)
		}),
	})
	h.Beat(context.Background())
	if len(got) != 2 {
		t.Fatalf("consumed %d pin changes, want 2", len(got))
	}
	if got[0].GetBlockHash() != "hash-a" || !got[0].GetPinned() || got[0].GetPinTtlS() != 60 {
		t.Errorf("pin change 0 = %+v", got[0])
	}
	if got[1].GetBlockHash() != "hash-b" || got[1].GetPinned() {
		t.Errorf("pin change 1 = %+v", got[1])
	}
}

// TestHeartbeatFullRereport 覆盖全量重报请求触发重报回调且一轮去重一次（D41、D49）。
func TestHeartbeatFullRereport(t *testing.T) {
	f := &fakeReporter{full: true}
	calls := 0
	h := NewHeartbeat(HeartbeatDeps{
		NodeID:      "node-1",
		Client:      f,
		ShardBlocks: 2,
		Source:      func() ([]manifest.Entry, float64, error) { return hbEntries(5), 0, nil },
		OnFullRereport: func() {
			calls++
		},
	})
	h.Beat(context.Background())
	if calls != 1 {
		t.Fatalf("full rereport callback fired %d times, want 1 per beat", calls)
	}
}

// TestHeartbeatFailureKeepsServing 覆盖数据源与上报失败不中断心跳循环（D51）。
func TestHeartbeatFailureKeepsServing(t *testing.T) {
	f := &fakeReporter{}
	h := NewHeartbeat(HeartbeatDeps{
		NodeID: "node-1",
		Client: f,
		Source: func() ([]manifest.Entry, float64, error) { return nil, 0, context.DeadlineExceeded },
	})
	h.Beat(context.Background())
	if len(f.recorded()) != 0 {
		t.Fatal("no request must be sent when source fails")
	}
}

// sequenceDecision 是顺序联测的决策端替身（D41）：按到达顺序统一记录事件与心跳请求，
// 可在指定条数后断流并拒绝后续连接（模拟决策不可达）。
type sequenceDecision struct {
	api.UnimplementedStorageReporterServer
	mu        sync.Mutex
	arrivals  []string
	count     int
	dropAfter int
	dropped   bool
	reject    bool
}

func (f *sequenceDecision) StreamEvents(stream api.StorageReporter_StreamEventsServer) error {
	f.mu.Lock()
	reject := f.reject
	f.mu.Unlock()
	if reject {
		return errors.New("decision unreachable")
	}
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.arrivals = append(f.arrivals, fmt.Sprintf("event:%d", ev.GetSeq()))
		f.count++
		drop := f.dropAfter > 0 && !f.dropped && f.count >= f.dropAfter
		if drop {
			f.dropped = true
			f.reject = true
		}
		f.mu.Unlock()
		if err := stream.Send(&api.EventAck{LastConfirmedSeq: ev.GetSeq()}); err != nil {
			return err
		}
		if drop {
			return errors.New("connection dropped")
		}
	}
}

func (f *sequenceDecision) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	f.mu.Lock()
	f.arrivals = append(f.arrivals, "heartbeat")
	f.mu.Unlock()
	return &api.HeartbeatResponse{}, nil
}

func (f *sequenceDecision) setReject(reject bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reject = reject
}

func (f *sequenceDecision) heartbeatCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, a := range f.arrivals {
		if a == "heartbeat" {
			n++
		}
	}
	return n
}

func (f *sequenceDecision) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.arrivals...)
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestHeartbeatAwaitsEventResync 覆盖重连后先补传事件再上报清单的顺序（D47）：
// 事件流首次补传未完成时心跳跳过本轮清单上报且循环不阻塞，补传完成后事件到达先于心跳。
func TestHeartbeatAwaitsEventResync(t *testing.T) {
	f := &sequenceDecision{dropAfter: 3}
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, f)
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
	client := api.NewStorageReporterClient(conn)

	buf := event.NewBuffer("node-1", 0, t.TempDir(), nil)
	stream := event.NewStream("node-1", buf, client, nil, event.StreamOptions{Backoff: 20 * time.Millisecond, Tick: 5 * time.Millisecond})
	hb := NewHeartbeat(HeartbeatDeps{
		NodeID:      "node-1",
		Client:      client,
		ShardBlocks: 4096,
		Synced:      stream.Synced,
		Source:      func() ([]manifest.Entry, float64, error) { return hbEntries(1), 0, nil },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go stream.Run(ctx)

	// 首次连接：三条事件补传完成后心跳才上报，事件到达先于心跳（D47）。
	for i := 0; i < 3; i++ {
		buf.Consume(segment.Event{Kind: event.KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	waitUntil(t, "first connection synced", func() bool { return stream.Synced() })
	hb.Beat(ctx)
	waitUntil(t, "first heartbeat recorded", func() bool { return f.heartbeatCount() == 1 })
	arr := f.snapshot()
	firstHb := -1
	for i, a := range arr {
		if a == "heartbeat" {
			firstHb = i
			break
		}
	}
	if firstHb < 0 || !strings.HasPrefix(arr[firstHb-1], "event:") {
		t.Fatalf("heartbeat must come after events, arrivals = %v", arr)
	}

	// 注入断流：决策端断开并拒绝后续连接，补传未完成，心跳跳过本轮且循环不阻塞（D47、D51）。
	buf.Consume(segment.Event{Kind: event.KindRegister, BlockHash: "h", SegmentSeq: 9})
	waitUntil(t, "stream desynced after drop", func() bool { return !stream.Synced() })
	hb.Beat(ctx)
	if got := f.heartbeatCount(); got != 1 {
		t.Fatalf("heartbeat count while unsynced = %d, want 1 (skipped)", got)
	}

	// 解除拒绝：重连补传积压事件与 offline 事件，补传完成后心跳再上报（D47）。
	f.setReject(false)
	waitUntil(t, "reconnected and synced", func() bool { return stream.Synced() })
	hb.Beat(ctx)
	waitUntil(t, "second heartbeat recorded", func() bool { return f.heartbeatCount() == 2 })

	// 顺序断言：重连批次的积压事件先于第二次心跳到达（D47）。
	arr = f.snapshot()
	hbIndexes := []int{}
	for i, a := range arr {
		if a == "heartbeat" {
			hbIndexes = append(hbIndexes, i)
		}
	}
	if len(hbIndexes) != 2 {
		t.Fatalf("heartbeat arrivals = %d, want 2: %v", len(hbIndexes), arr)
	}
	foundReplay := false
	for i := 0; i < hbIndexes[1]; i++ {
		if arr[i] == "event:4" {
			foundReplay = true
			break
		}
	}
	if !foundReplay {
		t.Fatalf("buffered events must be replayed before second heartbeat: %v", arr)
	}
	// 第二次心跳之前最后到达的是补传事件而非心跳（D47）。
	lastBefore := arr[hbIndexes[1]-1]
	if !strings.HasPrefix(lastBefore, "event:") {
		t.Fatalf("event must arrive immediately before second heartbeat, got %q", lastBefore)
	}
}
