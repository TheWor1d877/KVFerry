package remote_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/cache"
	"github.com/TheWor1d877/KVFerry/storage/internal/remote"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/server"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// testBlockBytes 是测试块字节，口径与段层测试一致（D44）。
const testBlockBytes = 64

// testSpec 返回测试口径（D44）。
func testSpec() spec.KVCacheSpec {
	return spec.KVCacheSpec{BlockBytes: testBlockBytes, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
}

// kvSpecProto 把测试口径转成请求消息口径（D44）。
func kvSpecProto(s spec.KVCacheSpec) *api.KVSpec {
	return &api.KVSpec{BlockBytes: s.BlockBytes, KvPrecision: s.KVPrecision, BytesPerToken: s.BytesPerToken, AlignmentPad: s.AlignmentPad}
}

// eventSpy 捕获注入的事件回调（D47）。
type eventSpy struct {
	mu     sync.Mutex
	events []segment.Event
}

func (s *eventSpy) record(e segment.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *eventSpy) snapshot() []segment.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]segment.Event(nil), s.events...)
}

func (s *eventSpy) count(kind string) int {
	n := 0
	for _, e := range s.snapshot() {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// spyLine 是捕获的一条日志。
type spyLine struct {
	level string
	msg   string
	f     telemetry.Fields
	line  string
}

// logSpy 捕获日志钩子输出（D50）。
type logSpy struct {
	mu    sync.Mutex
	lines []spyLine
}

func (s *logSpy) record(level, msg string, f telemetry.Fields, raw []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, spyLine{level: level, msg: msg, f: f, line: string(raw)})
}

func (s *logSpy) byMsg(msg string) []spyLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []spyLine
	for _, l := range s.lines {
		if l.msg == msg {
			out = append(out, l)
		}
	}
	return out
}

// testNode 是带真 BlockStore 服务端的节点进程组件（D40、D50）：bufconn 传输与独立
// 临时目录模拟跨机部署；rc 非空时服务端 Deps 挂远端客户端，Sink 链路直达。
type testNode struct {
	NodeID string
	Dir    string
	Spec   spec.KVCacheSpec
	Store  *segment.Store
	Client api.BlockStoreClient
	gsrv   *grpc.Server
	conn   *grpc.ClientConn
}

func startTestNode(t *testing.T, nodeID string, nodeSpec spec.KVCacheSpec, capacity int64, rc *remote.Client) *testNode {
	t.Helper()
	dir := t.TempDir()
	store, err := segment.Open(dir, nodeID, nodeSpec, capacity, segment.Options{Log: telemetry.New()})
	if err != nil {
		t.Fatalf("open segment store: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer(grpc.MaxRecvMsgSize(2 << 20))
	api.RegisterBlockStoreServer(gsrv, server.New(server.Deps{
		NodeID:          nodeID,
		Store:           store,
		Cache:           cache.New(0),
		NodeSpec:        nodeSpec,
		MaxMessageBytes: 1 << 20,
		Ready:           func() bool { return true },
		Log:             telemetry.New(),
		Remote:          rc,
	}))
	go func() { _ = gsrv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial node: %v", err)
	}
	n := &testNode{
		NodeID: nodeID,
		Dir:    dir,
		Spec:   nodeSpec,
		Store:  store,
		Client: api.NewBlockStoreClient(conn),
		gsrv:   gsrv,
		conn:   conn,
	}
	t.Cleanup(n.Close)
	return n
}

// Close 释放节点连接与服务端。
func (n *testNode) Close() {
	_ = n.conn.Close()
	n.gsrv.Stop()
	_ = n.Store.Close()
}

// dialOf 返回连接到本节点的 Dialer（D50）：真实跨机传输由装配方注入，测试注入 bufconn。
func (n *testNode) dialOf() remote.Dialer {
	return func(addr string) (api.BlockStoreClient, func() error, error) {
		if addr != n.NodeID {
			return nil, nil, fmt.Errorf("unknown remote node %s", addr)
		}
		return n.Client, func() error { return nil }, nil
	}
}
