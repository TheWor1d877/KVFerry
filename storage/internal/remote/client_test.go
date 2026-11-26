package remote_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/event"
	"github.com/TheWor1d877/KVFerry/storage/internal/remote"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// failingPeer 是读写都失败的远端传输桩（D51）。
type failingPeer struct {
	mu       sync.Mutex
	getCalls int
}

func (f *failingPeer) PutBlock(ctx context.Context, in *api.PutBlockRequest, opts ...grpc.CallOption) (*api.PutBlockResponse, error) {
	return nil, errors.New("peer write failed")
}

func (f *failingPeer) GetBlock(ctx context.Context, in *api.GetBlockRequest, opts ...grpc.CallOption) (*api.GetBlockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return nil, errors.New("peer read failed")
}

func (f *failingPeer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}

// TestSinkPushesToPeerAndEmitsDemote 覆盖下沉推送成功与 demote 事件发布（D50、D47）：
// bufconn 对端收到且读回字节一致，事件 Kind 为 demote、层位转远端。
func TestSinkPushesToPeerAndEmitsDemote(t *testing.T) {
	nodeSpec := testSpec()
	events := &eventSpy{}
	peer := startTestNode(t, "remote-1", nodeSpec, 64*4096, nil)
	cl := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    []string{"remote-1"},
		NodeSpec: nodeSpec,
		Dial:     peer.dialOf(),
		Emit:     events.record,
	})
	defer func() { _ = cl.Close() }()
	data := bytes.Repeat([]byte{0x5A}, testBlockBytes)
	out, err := cl.Sink(context.Background(), "hash-demote", data)
	if err != nil || out != remote.SinkRemote {
		t.Fatalf("sink = %v, %v; want SinkRemote", out, err)
	}
	resp, err := peer.Client.GetBlock(context.Background(), &api.GetBlockRequest{BlockHash: "hash-demote", Spec: kvSpecProto(nodeSpec)})
	if err != nil {
		t.Fatalf("peer get: %v", err)
	}
	if !bytes.Equal(resp.BlockData, data) {
		t.Error("peer bytes differ from pushed block")
	}
	if events.count(event.KindDemote) != 1 {
		t.Fatalf("demote events = %d, want 1", events.count(event.KindDemote))
	}
	e := events.snapshot()[0]
	if e.BlockHash != "hash-demote" || e.Layer != "remote" || e.NodeID != "node-1" || e.Reason != "sink" {
		t.Errorf("demote event = %+v", e)
	}
}

// TestSinkFallsBackToLocalOnRemoteWriteFailure 覆盖远端写失败回退落本机（D51）：
// 块留本机，不发 demote 也不发丢弃事件，写失败 warn 打点一条。
func TestSinkFallsBackToLocalOnRemoteWriteFailure(t *testing.T) {
	events := &eventSpy{}
	logs := &logSpy{}
	localCalls := 0
	cl := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    []string{"remote-dead"},
		NodeSpec: testSpec(),
		Dial: func(addr string) (api.BlockStoreClient, func() error, error) {
			return nil, nil, errors.New("peer unreachable")
		},
		Emit: events.record,
		LocalPut: func(hash string, data []byte) error {
			localCalls++
			return nil
		},
		Log: telemetry.NewWithHook(logs.record),
	})
	defer func() { _ = cl.Close() }()
	out, err := cl.Sink(context.Background(), "hash-fallback", bytes.Repeat([]byte{0x9}, testBlockBytes))
	if err != nil || out != remote.KeepLocal {
		t.Fatalf("sink outcome = %v, %v; want KeepLocal", out, err)
	}
	if localCalls != 1 {
		t.Fatalf("local put calls = %d, want 1 (block kept on local node)", localCalls)
	}
	failed := logs.byMsg("remote sink write failed")
	if len(failed) != 1 {
		t.Fatalf("remote write failed logs = %d, want 1", len(failed))
	}
	if failed[0].f.BlockHash != "hash-fallback" || failed[0].f.Target != "remote-dead" || failed[0].f.Reason == "" {
		t.Errorf("write failed log fields = %+v", failed[0].f)
	}
	if events.count(event.KindDemote) != 0 || events.count(event.KindEvict) != 0 {
		t.Errorf("fallback must not emit events, got %+v", events.snapshot())
	}
}

// TestSinkDiscardsWhenBothWritesFail 覆盖两侧写失败丢弃并事件计数加一（D51）：
// 远端与本机都写不进时按 evict 记丢弃事件一条。
func TestSinkDiscardsWhenBothWritesFail(t *testing.T) {
	events := &eventSpy{}
	peer := &failingPeer{}
	cl := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    []string{"remote-1"},
		NodeSpec: testSpec(),
		Dial: func(addr string) (api.BlockStoreClient, func() error, error) {
			return peer, func() error { return nil }, nil
		},
		Emit: events.record,
		LocalPut: func(hash string, data []byte) error {
			return errors.New("local segment full")
		},
	})
	defer func() { _ = cl.Close() }()
	out, err := cl.Sink(context.Background(), "hash-discard", make([]byte, testBlockBytes))
	if out != remote.Discard || err == nil {
		t.Fatalf("sink outcome = %v, %v; want Discard with error", out, err)
	}
	if events.count(event.KindEvict) != 1 {
		t.Fatalf("discard events = %d, want 1", events.count(event.KindEvict))
	}
	e := events.snapshot()[0]
	if e.Reason != remote.ReasonSinkDiscard || e.Layer != "local" || e.BlockHash != "hash-discard" || e.NodeID != "node-1" {
		t.Errorf("discard event = %+v", e)
	}
}

// TestGetRemoteMissOnFailureSingleCall 覆盖远端读失败按未命中返回（D51）：
// 返回未命中，远端调用只发生一次，不换节点重试。
func TestGetRemoteMissOnFailureSingleCall(t *testing.T) {
	peer := &failingPeer{}
	cl := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    []string{"remote-1"},
		NodeSpec: testSpec(),
		Dial: func(addr string) (api.BlockStoreClient, func() error, error) {
			return peer, func() error { return nil }, nil
		},
	})
	defer func() { _ = cl.Close() }()
	data, ok := cl.GetRemote(context.Background(), "hash-miss")
	if ok || data != nil {
		t.Errorf("remote read on failure = ok %v, want miss", ok)
	}
	if n := peer.callCount(); n != 1 {
		t.Fatalf("remote get calls = %d, want 1 (no retry on same or other node)", n)
	}
}

// TestRemoteLogHooksCaptureFourPoints 覆盖下沉、远端写失败、远端读失败、环重建四处
// 打点各捕获一条，四条均带 duration_ms（D50、D51）。
func TestRemoteLogHooksCaptureFourPoints(t *testing.T) {
	nodeSpec := testSpec()
	logs := &logSpy{}
	peer := startTestNode(t, "remote-1", nodeSpec, 64*4096, nil)

	baseDial := peer.dialOf()
	ok := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    []string{"remote-1"},
		NodeSpec: nodeSpec,
		Dial: func(addr string) (api.BlockStoreClient, func() error, error) {
			cl, closeFn, err := baseDial(addr)
			if err != nil {
				return nil, nil, err
			}
			// 注入延迟让耗时进入毫秒刻度，duration_ms 断言不依赖机器快慢。
			time.Sleep(3 * time.Millisecond)
			return cl, closeFn, nil
		},
		Log: telemetry.NewWithHook(logs.record),
	})
	defer func() { _ = ok.Close() }()
	// 环重建：构造即打一条，node_count 记节点数（D50）。
	rings := logs.byMsg("remote ring rebuilt")
	if len(rings) != 1 {
		t.Fatalf("ring logs after first client = %d, want 1", len(rings))
	}
	if rings[0].f.NodesCount != 1 || !strings.Contains(rings[0].line, `"node_count":1`) {
		t.Errorf("ring log fields = %+v, line = %s", rings[0].f, rings[0].line)
	}
	// 下沉：info，块哈希、目标节点、耗时与结果（D50）。
	if _, err := ok.Sink(context.Background(), "hash-log", bytes.Repeat([]byte{0x7}, testBlockBytes)); err != nil {
		t.Fatalf("sink: %v", err)
	}
	sunk := logs.byMsg("block sink completed")
	if len(sunk) != 1 {
		t.Fatalf("sink logs = %d, want 1", len(sunk))
	}
	if sunk[0].level != "info" || sunk[0].f.Target != "remote-1" || sunk[0].f.Result != "sunk" || sunk[0].f.BlockHash != "hash-log" {
		t.Errorf("sink log = %+v", sunk[0].f)
	}
	if sunk[0].f.DurationMs < 3 || !strings.Contains(sunk[0].line, `"duration_ms":`) {
		t.Errorf("sink log duration = %d, line = %s", sunk[0].f.DurationMs, sunk[0].line)
	}
	if !strings.Contains(sunk[0].line, `"target":"remote-1"`) {
		t.Errorf("sink log line missing target: %s", sunk[0].line)
	}

	// 远端写失败与读失败：第二个客户端的传输不可达，节点数放大让环构建耗时进入毫秒刻度（D51）。
	deadNodes := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		deadNodes = append(deadNodes, fmt.Sprintf("dead-%03d", i))
	}
	bad := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    deadNodes,
		NodeSpec: nodeSpec,
		Dial: func(addr string) (api.BlockStoreClient, func() error, error) {
			time.Sleep(3 * time.Millisecond)
			return nil, nil, errors.New("peer unreachable")
		},
		Log: telemetry.NewWithHook(logs.record),
	})
	defer func() { _ = bad.Close() }()
	if _, err := bad.Sink(context.Background(), "hash-fail", make([]byte, testBlockBytes)); err == nil {
		t.Fatal("sink on dead peer = success, want error")
	}
	failed := logs.byMsg("remote sink write failed")
	if len(failed) != 1 {
		t.Fatalf("write failed logs = %d, want 1", len(failed))
	}
	if failed[0].level != "warn" || failed[0].f.Target == "" || failed[0].f.Reason == "" || failed[0].f.BlockHash != "hash-fail" {
		t.Errorf("write failed log = %+v", failed[0].f)
	}
	if failed[0].f.DurationMs < 3 || !strings.Contains(failed[0].line, `"duration_ms":`) {
		t.Errorf("write failed log duration = %d, line = %s", failed[0].f.DurationMs, failed[0].line)
	}
	if _, ok := bad.GetRemote(context.Background(), "hash-fail"); ok {
		t.Error("remote read on dead peer = hit, want miss")
	}
	reads := logs.byMsg("remote read failed")
	if len(reads) != 1 {
		t.Fatalf("read failed logs = %d, want 1", len(reads))
	}
	if reads[0].level != "warn" || reads[0].f.Target == "" || reads[0].f.Reason == "" {
		t.Errorf("read failed log = %+v", reads[0].f)
	}
	if reads[0].f.DurationMs < 3 || !strings.Contains(reads[0].line, `"duration_ms":`) {
		t.Errorf("read failed log duration = %d, line = %s", reads[0].f.DurationMs, reads[0].line)
	}
	// 环重建日志随第二个客户端再打一条：两次构建各一条，大环的构建耗时进入毫秒刻度（D50）。
	rings = logs.byMsg("remote ring rebuilt")
	if len(rings) != 2 {
		t.Fatalf("ring logs total = %d, want 2 (one per client build)", len(rings))
	}
	if rings[1].f.NodesCount != 500 || rings[1].f.DurationMs <= 0 || !strings.Contains(rings[1].line, `"duration_ms":`) {
		t.Errorf("ring rebuild log = %+v, line = %s", rings[1].f, rings[1].line)
	}
}

// TestCrossNodeSinkAndReadOverBufconn 覆盖双进程加临时目录加注入传输模拟跨机：
// 两个节点进程组件完成一次下沉与跨机读回，读回字节与写入一致（D50、D51）。
func TestCrossNodeSinkAndReadOverBufconn(t *testing.T) {
	nodeSpec := testSpec()
	events := &eventSpy{}
	// 远端存储机节点进程组件（D55），独立临时目录。
	nodeB := startTestNode(t, "remote-b", nodeSpec, 64*4096, nil)
	rc := remote.New(remote.Options{
		NodeID:   "node-a",
		Nodes:    []string{"remote-b"},
		NodeSpec: nodeSpec,
		Dial:     nodeB.dialOf(),
		Emit:     events.record,
	})
	defer func() { _ = rc.Close() }()
	// 引擎同机存储节点进程组件（D55），服务端挂远端客户端（D50）。
	nodeA := startTestNode(t, "node-a", nodeSpec, 64*4096, rc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := make([]byte, testBlockBytes)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := nodeA.Client.PutBlock(ctx, &api.PutBlockRequest{
		NodeId:    "node-a",
		BlockHash: "hash-cross",
		Spec:      kvSpecProto(nodeSpec),
		BlockData: data,
	}); err != nil {
		t.Fatalf("local put: %v", err)
	}
	// 决策侧触发器未实现，经 Sink 入口注入哈希与字节（D50）。
	out, err := rc.Sink(ctx, "hash-cross", data)
	if err != nil || out != remote.SinkRemote {
		t.Fatalf("sink = %v, %v; want SinkRemote", out, err)
	}
	got, ok := rc.GetRemote(ctx, "hash-cross")
	if !ok {
		t.Fatal("cross-node read = miss, want hit")
	}
	if !bytes.Equal(got, data) {
		t.Error("cross-node read back bytes differ from written")
	}
	resp, err := nodeB.Client.GetBlock(ctx, &api.GetBlockRequest{BlockHash: "hash-cross", Spec: kvSpecProto(nodeSpec)})
	if err != nil {
		t.Fatalf("peer direct get: %v", err)
	}
	if !bytes.Equal(resp.BlockData, data) {
		t.Error("peer stored bytes differ from written")
	}
	// 本机段保留：回收按普通驱逐流程由装配方编排，本机读回仍成功（D50、D51）。
	if _, _, err := nodeA.Store.Get("hash-cross"); err != nil {
		t.Errorf("local segment after sink = %v, want retained", err)
	}
	if events.count(event.KindDemote) != 1 {
		t.Errorf("demote events = %d, want 1", events.count(event.KindDemote))
	}
}
