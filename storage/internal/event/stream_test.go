package event

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
)

// fakeDecision 是事件流测试的决策端替身（D41）：按确认上限回 ack，可在指定条数后断流。
type fakeDecision struct {
	api.UnimplementedStorageReporterServer
	client    api.StorageReporterClient
	mu        sync.Mutex
	acked     int64
	ackLimit  int64
	dropAt    int
	dropped   bool
	conns     [][]*api.StorageEvent
	connReady chan struct{}
}

func (f *fakeDecision) StreamEvents(stream api.StorageReporter_StreamEventsServer) error {
	f.mu.Lock()
	idx := len(f.conns)
	f.conns = append(f.conns, nil)
	f.mu.Unlock()
	if f.connReady != nil {
		f.connReady <- struct{}{}
	}
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.conns[idx] = append(f.conns[idx], ev)
		// dropAt 只对首次连接生效，断流一次后后续连接完整收事件。
		drop := f.dropAt > 0 && !f.dropped && len(f.conns[idx]) >= f.dropAt
		if drop {
			f.dropped = true
		}
		// ackLimit 非正表示全部确认，正数表示确认上限。
		if ev.GetSeq() > f.acked && (f.ackLimit <= 0 || ev.GetSeq() <= f.ackLimit) {
			f.acked = ev.GetSeq()
		}
		ackVal := f.acked
		f.mu.Unlock()
		if err := stream.Send(&api.EventAck{LastConfirmedSeq: ackVal}); err != nil {
			return err
		}
		if drop {
			return errors.New("connection dropped")
		}
	}
}

func (f *fakeDecision) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

func (f *fakeDecision) connEvents(i int) []*api.StorageEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i >= len(f.conns) {
		return nil
	}
	return append([]*api.StorageEvent(nil), f.conns[i]...)
}

func startFake(t *testing.T, f *fakeDecision) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, f)
	go func() { _ = gsrv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		gsrv.Stop()
	})
	f.client = api.NewStorageReporterClient(conn)
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

// TestAckAdvancesConfirm 覆盖决策回确认序号推进续传位点（D41、D47）。
func TestAckAdvancesConfirm(t *testing.T) {
	f := &fakeDecision{}
	startFake(t, f)
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	s := NewStream("node-1", b, f.client, nil, StreamOptions{Backoff: 10 * time.Millisecond, Tick: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	for i := 0; i < 3; i++ {
		b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	waitFor(t, "ack 3", func() bool { return b.Acked() == 3 })
	if got, _ := b.Take(); len(got) != 0 {
		t.Fatalf("pending after ack 3 = %d events, want 0", len(got))
	}
}

// TestReconnectResumesFromAck 覆盖断线从确认 seq 续传与断线写 offline 事件（D41、D47、D51）。
func TestReconnectResumesFromAck(t *testing.T) {
	f := &fakeDecision{ackLimit: 2, dropAt: 3, connReady: make(chan struct{}, 8)}
	startFake(t, f)
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	s := NewStream("node-1", b, f.client, nil, StreamOptions{Backoff: 10 * time.Millisecond, Tick: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	for i := 0; i < 5; i++ {
		b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	waitFor(t, "ack 2", func() bool { return b.Acked() == 2 })
	// 第一连接收到 seq 1、2、3 后被决策端断流；断线后 offline 事件进入缓冲（seq 6）。
	waitFor(t, "offline event buffered", func() bool { return b.LastSeq() == 6 })
	waitFor(t, "second connection", func() bool { return f.connCount() >= 2 })
	waitFor(t, "second connection events", func() bool {
		evs := f.connEvents(1)
		return len(evs) > 0 && evs[0].Seq == 3
	})
	evs := f.connEvents(1)
	if evs[0].Seq != 3 {
		t.Fatalf("resume starts at seq %d, want 3", evs[0].Seq)
	}
	for _, ev := range evs {
		if ev.Seq <= 2 {
			t.Errorf("confirmed event seq %d resent after reconnect", ev.Seq)
		}
	}
	last := evs[len(evs)-1]
	if last.Kind != KindOffline || last.Seq != 6 {
		t.Fatalf("last resent event = kind %s seq %d, want offline at seq 6", last.Kind, last.Seq)
	}
}

// TestResyncFlagPropagates 覆盖缓冲不足置 resync 后出网事件带标记，清除后状态复位（D47）。
func TestResyncFlagPropagates(t *testing.T) {
	f := &fakeDecision{}
	startFake(t, f)
	b := NewBuffer("node-1", 2, t.TempDir(), nil)
	s := NewStream("node-1", b, f.client, nil, StreamOptions{Backoff: 10 * time.Millisecond, Tick: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	for i := 0; i < 3; i++ {
		b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	waitFor(t, "flagged events delivered", func() bool {
		for _, ev := range f.connEvents(0) {
			if ev.Resync {
				return true
			}
		}
		return false
	})
	waitFor(t, "all events acked", func() bool { return b.Acked() == 3 })
	// 环已满，之后任何新写入都覆盖最旧并按契约重新置位；此处只验证清除生效（D47）。
	b.ClearResync()
	if b.ResyncPending() {
		t.Fatal("resync must be cleared after clear")
	}
}
