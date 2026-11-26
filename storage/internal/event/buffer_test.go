package event

import (
	"testing"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
)

// TestFiveKindsSeqMonotonic 覆盖五类事件共用一套节点内单调递增 seq（D47）。
func TestFiveKindsSeqMonotonic(t *testing.T) {
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h1", SegmentSeq: 1, Layer: "local"})
	b.Offline("stream broken")
	b.Consume(segment.Event{Kind: KindDemote, BlockHash: "h1", Layer: "remote"})
	b.Consume(segment.Event{Kind: KindEvict, BlockHash: "h1"})
	b.Consume(segment.Event{Kind: KindOversized, BlockHash: "h2"})

	pending, _ := b.Take()
	if len(pending) != 5 {
		t.Fatalf("pending = %d events, want 5", len(pending))
	}
	kinds := []string{KindRegister, KindOffline, KindDemote, KindEvict, KindOversized}
	for i, ev := range pending {
		if ev.Seq != int64(i+1) {
			t.Errorf("event %d seq = %d, want %d", i, ev.Seq, i+1)
		}
		if ev.Kind != kinds[i] {
			t.Errorf("event %d kind = %s, want %s", i, ev.Kind, kinds[i])
		}
		if ev.NodeId != "node-1" {
			t.Errorf("event %d node = %s, want node-1", i, ev.NodeId)
		}
	}
}

// TestConsumeMapsSegmentEvent 覆盖段层事件到节点事件的字段映射（D47）。
func TestConsumeMapsSegmentEvent(t *testing.T) {
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	b.Consume(segment.Event{Kind: KindRegister, BlockHash: "hash-a", SegmentSeq: 7, Layer: "local", Reason: "", TsMs: 1234})
	b.Consume(segment.Event{Kind: KindDemote, BlockHash: "hash-b", SegmentSeq: 8, Layer: "remote", TsMs: 2345})
	pending, _ := b.Take()
	if len(pending) != 2 {
		t.Fatalf("pending = %d events, want 2", len(pending))
	}
	if pending[0].Layer != api.BlockLayer_BLOCK_LAYER_LOCAL {
		t.Errorf("layer = %v, want local", pending[0].Layer)
	}
	if pending[1].Layer != api.BlockLayer_BLOCK_LAYER_REMOTE {
		t.Errorf("layer = %v, want remote", pending[1].Layer)
	}
	if pending[0].SegmentSeq != 7 || pending[0].BlockHash != "hash-a" || pending[0].TsMs != 1234 {
		t.Errorf("event 0 = %+v", pending[0])
	}
}

// TestSeqResumesFromWatermark 覆盖重启后 seq 从高水位加一继续，不回退（D47）。
func TestSeqResumesFromWatermark(t *testing.T) {
	dir := t.TempDir()
	b1 := NewBuffer("node-1", 0, dir, nil)
	for i := 0; i < 3; i++ {
		b1.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	if err := b1.SaveWatermark(); err != nil {
		t.Fatalf("save watermark: %v", err)
	}
	b2 := NewBuffer("node-1", 0, dir, nil)
	b2.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: 9})
	pending, _ := b2.Take()
	if len(pending) != 1 || pending[0].Seq != 4 {
		t.Fatalf("seq after restart = %d, want 4", pending[0].Seq)
	}
}

// TestConfirmAdvancesPending 覆盖确认位点续传：断线重连后从决策确认的 seq 之后补传（D47）。
func TestConfirmAdvancesPending(t *testing.T) {
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	for i := 0; i < 5; i++ {
		b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	pending, _ := b.Take()
	if len(pending) != 5 {
		t.Fatalf("first take = %d events, want 5", len(pending))
	}
	b.Confirm(2)
	b.ResetSent()
	pending, _ = b.Take()
	if len(pending) != 3 || pending[0].Seq != 3 {
		t.Fatalf("resume from ack = %d events starting at %d, want 3 starting at 3", len(pending), pending[0].Seq)
	}
	b.Confirm(5)
	b.ResetSent()
	if got, _ := b.Take(); len(got) != 0 {
		t.Fatalf("take after full ack = %d events, want 0", len(got))
	}
}

// TestConfirmNeverRecedes 覆盖乱序确认不回退续传位置（D47）。
func TestConfirmNeverRecedes(t *testing.T) {
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	b.Confirm(3)
	b.Confirm(1)
	if got := b.Acked(); got != 3 {
		t.Fatalf("acked = %d after out-of-order confirm, want 3", got)
	}
}

// TestOverflowOverwritesOldestAndMarksResync 覆盖写满覆盖最旧并置 resync 标记（D47）：
// 覆盖后出网事件带标记，确认推进到衔接点并清除标记后新事件不再带标记。
func TestOverflowOverwritesOldestAndMarksResync(t *testing.T) {
	b := NewBuffer("node-1", 3, t.TempDir(), nil)
	for i := 0; i < 5; i++ {
		b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	if !b.ResyncPending() {
		t.Fatal("resync must be set after oldest events were overwritten")
	}
	pending, flagged := b.Take()
	if len(pending) != 3 {
		t.Fatalf("pending = %d events, want 3 after overwrite", len(pending))
	}
	if !flagged {
		t.Fatal("take after overwrite must report resync")
	}
	for i, ev := range pending {
		if ev.Seq != int64(i+3) {
			t.Errorf("pending %d seq = %d, want %d", i, ev.Seq, i+3)
		}
	}
	// 确认推进到衔接点（acked+1 == 缓冲里最早 seq）后清除标记（D47）。
	b.Confirm(2)
	b.ClearResync()
	if b.ResyncPending() {
		t.Fatal("resync must be cleared after clear")
	}
	if got, flagged := b.Take(); len(got) != 0 || flagged {
		t.Fatalf("take after clear = %+v flagged=%v, want empty without resync", got, flagged)
	}
}

// TestContiguousBufferNoResync 覆盖缓冲与确认位点衔接时不置 resync（D47）。
func TestContiguousBufferNoResync(t *testing.T) {
	b := NewBuffer("node-1", 3, t.TempDir(), nil)
	for i := 0; i < 3; i++ {
		b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: int64(i)})
	}
	if b.ResyncPending() {
		t.Fatal("resync must not be set without overwrite")
	}
	_, _ = b.Take()
	b.Confirm(2)
	b.ResetSent()
	pending, flagged := b.Take()
	if len(pending) != 1 || pending[0].Seq != 3 || flagged {
		t.Fatalf("contiguous pending = %+v flagged=%v, want single event at seq 3 without resync", pending, flagged)
	}
}

// TestWatermarkSurvivesMissingFile 覆盖高水位文件缺失从零开始（D47）。
func TestWatermarkSurvivesMissingFile(t *testing.T) {
	b := NewBuffer("node-1", 0, t.TempDir(), nil)
	b.Consume(segment.Event{Kind: KindRegister, BlockHash: "h", SegmentSeq: 0})
	pending, _ := b.Take()
	if len(pending) != 1 || pending[0].Seq != 1 {
		t.Fatalf("seq without watermark = %d, want 1", pending[0].Seq)
	}
}
