package remote_test

import (
	"bytes"
	"context"
	"errors"

	"testing"

	"github.com/TheWor1d877/KVFerry/storage/internal/remote"
)

// TestRingEmptyHasNoTarget 覆盖空环行为（D42、D50）：remote_nodes 缺省空表示不启用
// 远端块服务，Pick 无目标，Sink 报错，远端读按未命中。
func TestRingEmptyHasNoTarget(t *testing.T) {
	ring := remote.NewRing(nil, nil)
	if got := ring.Pick("hash-a"); got != "" {
		t.Errorf("empty ring pick = %q, want empty", got)
	}
	if got := ring.Nodes(); len(got) != 0 {
		t.Errorf("empty ring nodes = %v, want none", got)
	}
	cl := remote.New(remote.Options{NodeID: "node-1", NodeSpec: testSpec()})
	defer func() { _ = cl.Close() }()
	if _, err := cl.Sink(context.Background(), "hash-a", make([]byte, testBlockBytes)); !errors.Is(err, remote.ErrNoRing) {
		t.Errorf("sink on empty ring = %v, want ErrNoRing", err)
	}
	if _, ok := cl.GetRemote(context.Background(), "hash-a"); ok {
		t.Error("remote read on empty ring = hit, want miss")
	}
}

// TestRingSameKeySingleSegmentOnPeer 覆盖同键两次写只留一段（D50、R26）：经 bufconn
// 对端验证幂等覆盖，全集群只留一份。
func TestRingSameKeySingleSegmentOnPeer(t *testing.T) {
	nodeSpec := testSpec()
	peer := startTestNode(t, "remote-1", nodeSpec, 64*4096, nil)
	cl := remote.New(remote.Options{
		NodeID:   "node-1",
		Nodes:    []string{"remote-1"},
		NodeSpec: nodeSpec,
		Dial:     peer.dialOf(),
	})
	defer func() { _ = cl.Close() }()
	ctx := context.Background()
	first := bytes.Repeat([]byte{0x11}, testBlockBytes)
	second := bytes.Repeat([]byte{0x22}, testBlockBytes)
	if out, err := cl.Sink(ctx, "hash-sink", first); err != nil || out != remote.SinkRemote {
		t.Fatalf("first sink = %v, %v; want SinkRemote", out, err)
	}
	if out, err := cl.Sink(ctx, "hash-sink", second); err != nil || out != remote.SinkRemote {
		t.Fatalf("second sink = %v, %v; want SinkRemote", out, err)
	}
	if used := peer.Store.UsedSegments(); used != 1 {
		t.Fatalf("peer segments = %d, want 1 (idempotent overwrite keeps one copy)", used)
	}
	got, ok := cl.GetRemote(ctx, "hash-sink")
	if !ok {
		t.Fatal("read back after overwrite = miss, want hit")
	}
	if !bytes.Equal(got, second) {
		t.Error("read back after overwrite differs from second write")
	}
}
