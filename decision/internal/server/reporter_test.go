package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/decision/internal/health"
	"github.com/TheWor1d877/KVFerry/decision/internal/index"
	"github.com/TheWor1d877/KVFerry/storage/api"
)

func startReporter(t *testing.T, rb *index.Rebuild, opts ...Option) (api.StorageReporterClient, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, NewReporter(rb, opts...))
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
	return api.NewStorageReporterClient(conn), conn
}

func shardReq(node string, idx, total int64, entries ...*api.ManifestEntry) *api.ReportManifestRequest {
	return &api.ReportManifestRequest{
		NodeId:     node,
		ShardIndex: idx,
		ShardTotal: total,
		Entries:    entries,
	}
}

func entry(hash string, offset, blockBytes int64) *api.ManifestEntry {
	return &api.ManifestEntry{
		BlockHash:  hash,
		Layer:      api.BlockLayer_BLOCK_LAYER_LOCAL,
		Offset:     offset,
		BlockBytes: blockBytes,
	}
}

func reporterSpec() index.Spec {
	return index.Spec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

// TestShardsReplaceRegistration 覆盖逐片收齐替换登记（D41、D49）。
func TestShardsReplaceRegistration(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	client, _ := startReporter(t, rb)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.ReportManifest(ctx, shardReq("node-1", 0, 2, entry("hash-a", 71, 64), entry("hash-b", 298, 64))); err != nil {
		t.Fatalf("shard 0: %v", err)
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); ok {
		t.Error("single shard must not replace registration yet")
	}
	if _, err := client.ReportManifest(ctx, shardReq("node-1", 1, 2, entry("hash-c", 525, 64))); err != nil {
		t.Fatalf("shard 1: %v", err)
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Error("hash-a missing after shards complete")
	}
	if _, ok := ix.Lookup("node-1", "hash-c"); !ok {
		t.Error("hash-c missing after shards complete")
	}
	if rb.Open() {
		t.Error("window must close after the only node completes")
	}
}

// TestOutOfOrderShards 覆盖乱序到达按片记录（D49）。
func TestOutOfOrderShards(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	client, _ := startReporter(t, rb)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.ReportManifest(ctx, shardReq("node-1", 1, 2, entry("hash-b", 298, 64))); err != nil {
		t.Fatalf("shard 1: %v", err)
	}
	if _, err := client.ReportManifest(ctx, shardReq("node-1", 0, 2, entry("hash-a", 71, 64))); err != nil {
		t.Fatalf("shard 0: %v", err)
	}
	for _, hash := range []string{"hash-a", "hash-b"} {
		if _, ok := ix.Lookup("node-1", hash); !ok {
			t.Errorf("%s missing after out-of-order completion", hash)
		}
	}
	if rb.Open() {
		t.Error("window must close after out-of-order completion")
	}
}

// TestWindowOpensAfterAllNodes 覆盖全部节点收齐后放行命中查询（D49、R32）。
func TestWindowOpensAfterAllNodes(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1", "node-2"})
	form := index.BuildForm(index.FormConfig{Spec: reporterSpec()})
	q := index.NewQuerier(ix, form, rb.Open, nil)
	client, _ := startReporter(t, rb)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.ReportManifest(ctx, shardReq("node-1", 0, 1, entry("hash-a", 71, 64))); err != nil {
		t.Fatalf("node-1 shard: %v", err)
	}
	if _, ok := q.Hit("hash-a", reporterSpec()); ok {
		t.Error("hit while node-2 pending, want miss")
	}
	if _, err := client.ReportManifest(ctx, shardReq("node-2", 0, 1)); err != nil {
		t.Fatalf("node-2 shard: %v", err)
	}
	if _, ok := q.Hit("hash-a", reporterSpec()); !ok {
		t.Error("hit after all nodes complete, want hit")
	}
}

// TestInvalidShardFormRejected 覆盖分片形态非法按 InvalidArgument 拒绝（D41）。
func TestInvalidShardFormRejected(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	client, _ := startReporter(t, rb)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, req := range []*api.ReportManifestRequest{
		{NodeId: "node-1", ShardIndex: 0, ShardTotal: 0},
		{NodeId: "node-1", ShardIndex: -1, ShardTotal: 2},
		{NodeId: "node-1", ShardIndex: 2, ShardTotal: 2},
		{NodeId: "", ShardIndex: 0, ShardTotal: 1},
		{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1, Entries: []*api.ManifestEntry{{Offset: 71, BlockBytes: 64}}},
	} {
		if _, err := client.ReportManifest(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("invalid form %v should be InvalidArgument, got %v", req, err)
		}
	}
	if _, err := client.ReportManifest(ctx, shardReq("node-x", 0, 1)); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unregistered node should be InvalidArgument, got %v", err)
	}
}

// fakePins 是 pin 变更源的测试替身（D41、R25）。
type fakePins struct {
	mu    sync.Mutex
	pends []*api.PinChange
}

func (f *fakePins) Pending() []*api.PinChange {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*api.PinChange(nil), f.pends...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
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

// TestStreamEventsAckAndDisconnect 覆盖事件按 seq 推进确认、断流进疑似、重连续传（D41、D47、D51）。
func TestStreamEventsAckAndDisconnect(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	ap := index.NewApplier(ix)
	tracker := health.NewTracker(30*time.Second, 30*time.Second)
	client, _ := startReporter(t, rb, WithIndex(ix), WithApplier(ap), WithHealth(tracker))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	send := func(seq int64, hash string) int64 {
		t.Helper()
		if err := stream.Send(&api.StorageEvent{Kind: index.KindRegister, NodeId: "node-1", Seq: seq, BlockHash: hash, Layer: api.BlockLayer_BLOCK_LAYER_LOCAL}); err != nil {
			t.Fatalf("send seq %d: %v", seq, err)
		}
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv ack for seq %d: %v", seq, err)
		}
		return ack.GetLastConfirmedSeq()
	}
	if got := send(1, "hash-a"); got != 1 {
		t.Fatalf("ack 1 = %d, want 1", got)
	}
	if got := send(2, "hash-b"); got != 2 {
		t.Fatalf("ack 2 = %d, want 2", got)
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("hash-a must be registered by event")
	}

	// 断流：服务端观测到流断开，节点进疑似（D51）。
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	waitFor(t, "suspect after stream break", func() bool { return tracker.State("node-1") == health.StateSuspect })

	// 重连补传：确认位保持在 2，新事件继续推进（D47）。
	stream2, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatalf("reopen stream: %v", err)
	}
	if err := stream2.Send(&api.StorageEvent{Kind: index.KindRegister, NodeId: "node-1", Seq: 3, BlockHash: "hash-c", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL}); err != nil {
		t.Fatalf("send seq 3: %v", err)
	}
	ack, err := stream2.Recv()
	if err != nil {
		t.Fatalf("recv ack for seq 3: %v", err)
	}
	if ack.GetLastConfirmedSeq() != 3 {
		t.Fatalf("ack after reconnect = %d, want 3", ack.GetLastConfirmedSeq())
	}
	if _, ok := ix.Lookup("node-1", "hash-c"); !ok {
		t.Fatal("hash-c must be registered by event after reconnect")
	}
	if err := stream2.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
}

// TestHeartbeatReconcileAndRespond 覆盖心跳对账补登与注销、pin 变更下发（D41、D48、D54）。
func TestHeartbeatReconcileAndRespond(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	// 悬空登记：盘上已无对应段，等心跳收齐后注销（D54）。
	ix.Insert("node-1", "hash-stale", index.Record{Layer: api.BlockLayer_BLOCK_LAYER_LOCAL})
	pins := &fakePins{pends: []*api.PinChange{{BlockHash: "hash-p", Pinned: true, PinTtlS: 30}}}
	client, _ := startReporter(t, rb, WithIndex(ix), WithPins(pins))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mk := func(idx, total int64, hashes ...string) *api.HeartbeatRequest {
		req := &api.HeartbeatRequest{NodeId: "node-1", CapacityWatermark: 0.4, ShardIndex: idx, ShardTotal: total}
		for _, h := range hashes {
			req.Entries = append(req.Entries, &api.ManifestEntry{BlockHash: h, Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, BlockBytes: 64})
		}
		return req
	}
	// 两片心跳：片 0 补登 hash-a，片 1 补登 hash-b；收齐后注销悬空的 hash-stale（D54）。
	if _, err := client.Heartbeat(ctx, mk(0, 2, "hash-a")); err != nil {
		t.Fatalf("heartbeat shard 0: %v", err)
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("hash-a must be inserted by heartbeat reconciliation")
	}
	resp, err := client.Heartbeat(ctx, mk(1, 2, "hash-b"))
	if err != nil {
		t.Fatalf("heartbeat shard 1: %v", err)
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); !ok {
		t.Fatal("hash-b must be inserted by heartbeat reconciliation")
	}
	if _, ok := ix.Lookup("node-1", "hash-stale"); ok {
		t.Fatal("hash-stale must be deleted after all shards arrived")
	}
	if !resp.GetFullRereport() {
		t.Error("full rereport must be true while rebuild window is open")
	}
	if len(resp.GetPinChanges()) != 1 || resp.GetPinChanges()[0].GetBlockHash() != "hash-p" {
		t.Errorf("pin changes = %+v, want one change for hash-p", resp.GetPinChanges())
	}
	// 下一周期单片：收齐即注销，清单里没有的 hash-b 被删除（D54）。
	if _, err := client.Heartbeat(ctx, mk(0, 1, "hash-a")); err != nil {
		t.Fatalf("heartbeat next cycle: %v", err)
	}
	if _, ok := ix.Lookup("node-1", "hash-b"); ok {
		t.Fatal("hash-b must be deleted after single-shard cycle")
	}
}

// TestHeartbeatFullRereportDuringRebuild 覆盖重建窗口开着时心跳响应带全量重报请求（D49）。
func TestHeartbeatFullRereportDuringRebuild(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	client, _ := startReporter(t, rb, WithIndex(ix))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &api.HeartbeatRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1}
	resp, err := client.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !resp.GetFullRereport() {
		t.Error("full rereport must be requested while rebuild window is open")
	}
}

// TestResyncEventTriggersFullRereportFlow 覆盖缓冲溢出置 resync 的全链路（D47、D49）：
// 事件带 resync 标志出网，决策置节点待重建并在下一次心跳响应回全量重报请求，节点重报
// 逐片收齐后 resync 清除、MarkRebuilt 生效、命中查询恢复，链路收敛。
func TestResyncEventTriggersFullRereportFlow(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	ap := index.NewApplier(ix)
	client, _ := startReporter(t, rb, WithIndex(ix), WithApplier(ap))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 空片清单收齐关窗，让后续全量重报请求只能由 resync 驱动（D49）。
	if _, err := client.ReportManifest(ctx, &api.ReportManifestRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1}); err != nil {
		t.Fatalf("close window: %v", err)
	}
	if resp, err := client.Heartbeat(ctx, &api.HeartbeatRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1}); err != nil || resp.GetFullRereport() {
		t.Fatalf("heartbeat before resync err=%v full=%v, want no rereport request", err, resp.GetFullRereport())
	}

	stream, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	send := func(kind string, seq int64, hash string, resync bool) int64 {
		t.Helper()
		if err := stream.Send(&api.StorageEvent{Kind: kind, NodeId: "node-1", Seq: seq, BlockHash: hash, Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Resync: resync}); err != nil {
			t.Fatalf("send seq %d: %v", seq, err)
		}
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv ack for seq %d: %v", seq, err)
		}
		return ack.GetLastConfirmedSeq()
	}
	if got := send(index.KindRegister, 1, "hash-a", false); got != 1 {
		t.Fatalf("ack 1 = %d, want 1", got)
	}
	// 注入缓冲溢出：事件带 resync 标志出网，决策置节点待重建（D47）。
	if got := send(index.KindRegister, 5, "hash-b", true); got != 5 {
		t.Fatalf("ack 5 = %d, want 5 (flagged event still acked)", got)
	}
	if !ap.Resync("node-1") {
		t.Fatal("flagged event must put node into resync")
	}
	// 重报期间该节点新事件照常接收按待重建处理：evict 只推进确认位不更新登记（D47）。
	if got := send(index.KindEvict, 6, "hash-a", false); got != 6 {
		t.Fatalf("ack 6 = %d, want 6", got)
	}
	if _, ok := ix.Lookup("node-1", "hash-a"); !ok {
		t.Fatal("event during resync must not update registration")
	}

	// 下一次心跳响应携带全量重报请求（D47）。
	resp, err := client.Heartbeat(ctx, &api.HeartbeatRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1})
	if err != nil {
		t.Fatalf("heartbeat after resync: %v", err)
	}
	if !resp.GetFullRereport() {
		t.Fatal("heartbeat must request full rereport while node is in resync")
	}

	// 节点全量重报逐片上报，收齐后 resync 清除、MarkRebuilt 生效（D47、D49）。
	if _, err := client.ReportManifest(ctx, &api.ReportManifestRequest{
		NodeId: "node-1", ShardIndex: 0, ShardTotal: 2,
		Entries: []*api.ManifestEntry{{BlockHash: "hash-a", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 71, BlockBytes: 64}},
	}); err != nil {
		t.Fatalf("rereport shard 0: %v", err)
	}
	if _, err := client.ReportManifest(ctx, &api.ReportManifestRequest{
		NodeId: "node-1", ShardIndex: 1, ShardTotal: 2,
		Entries: []*api.ManifestEntry{{BlockHash: "hash-b", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL, Offset: 298, BlockBytes: 64}},
	}); err != nil {
		t.Fatalf("rereport shard 1: %v", err)
	}
	if ap.Resync("node-1") {
		t.Fatal("resync must be cleared after full rereport completed")
	}

	// 命中查询恢复，登记以重报清单为准（D51）。
	form := index.BuildForm(index.FormConfig{Spec: reporterSpec()})
	q := index.NewQuerier(ix, form, rb.Open, nil)
	for _, hash := range []string{"hash-a", "hash-b"} {
		if _, ok := q.Hit(hash, reporterSpec()); !ok {
			t.Fatalf("hit for %s after rereport, want hit", hash)
		}
	}
	// 链路收敛：resync 清除后心跳不再请求重报（D47）。
	if resp, err := client.Heartbeat(ctx, &api.HeartbeatRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1}); err != nil || resp.GetFullRereport() {
		t.Fatalf("heartbeat after rereport err=%v full=%v, want no rereport request", err, resp.GetFullRereport())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
}

// TestHeartbeatFullRereportOnGap 覆盖决策观测跳号后下一次心跳响应回全量重报请求（D47）：
// 跳号由应用器自置 resync，心跳响应据此逐节点判定回全量重报请求。
func TestHeartbeatFullRereportOnGap(t *testing.T) {
	ix := index.New()
	rb := index.StartRebuild(ix, []string{"node-1"})
	ap := index.NewApplier(ix)
	client, _ := startReporter(t, rb, WithIndex(ix), WithApplier(ap))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 关窗后全量重报请求只能由跳号自置的 resync 驱动（D49）。
	if _, err := client.ReportManifest(ctx, &api.ReportManifestRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1}); err != nil {
		t.Fatalf("close window: %v", err)
	}

	stream, err := client.StreamEvents(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for _, seq := range []int64{1, 3} {
		if err := stream.Send(&api.StorageEvent{Kind: index.KindRegister, NodeId: "node-1", Seq: seq, BlockHash: "hash-a", Layer: api.BlockLayer_BLOCK_LAYER_LOCAL}); err != nil {
			t.Fatalf("send seq %d: %v", seq, err)
		}
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv ack for seq %d: %v", seq, err)
		}
		if ack.GetLastConfirmedSeq() != seq {
			t.Fatalf("ack for seq %d = %d, want %d", seq, ack.GetLastConfirmedSeq(), seq)
		}
	}
	if !ap.Resync("node-1") {
		t.Fatal("gap must set resync")
	}
	resp, err := client.Heartbeat(ctx, &api.HeartbeatRequest{NodeId: "node-1", ShardIndex: 0, ShardTotal: 1})
	if err != nil {
		t.Fatalf("heartbeat after gap: %v", err)
	}
	if !resp.GetFullRereport() {
		t.Fatal("heartbeat must request full rereport after gap")
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
}
