package telemetry

import (
	"bytes"
	"strings"
	"testing"
)

type capture struct {
	fields []Fields
	lines  [][]byte
	kinds  []string
}

func newCapture() (*capture, *Logger) {
	c := &capture{}
	lg := NewWithHook(func(level, msg string, f Fields, line []byte) {
		c.fields = append(c.fields, f)
		c.lines = append(c.lines, append([]byte(nil), line...))
		c.kinds = append(c.kinds, level+":"+msg)
	})
	return c, lg
}

func TestStructuredFields(t *testing.T) {
	c, lg := newCapture()
	lg.Info("putblock saved", Fields{
		NodeID:     "node-1",
		BlockHash:  "hash-a",
		SegmentSeq: 3,
		Bytes:      64,
		Result:     "saved",
		Layer:      "local",
	})
	if len(c.fields) != 1 {
		t.Fatalf("captured %d entries, want 1", len(c.fields))
	}
	f := c.fields[0]
	if f.NodeID != "node-1" || f.BlockHash != "hash-a" || f.SegmentSeq != 3 || f.Bytes != 64 || f.Result != "saved" || f.Layer != "local" {
		t.Errorf("fields = %+v", f)
	}
	line := string(c.lines[0])
	for _, key := range []string{
		"component", "storage", "node_id", "block_hash", "segment_seq", "bytes", "result", "layer",
	} {
		if !strings.Contains(line, key) {
			t.Errorf("json line %q should contain %q", line, key)
		}
	}
}

func TestAllFieldNames(t *testing.T) {
	c, lg := newCapture()
	lg.Warn("getblock failed", Fields{
		NodeID:       "node-1",
		BlockHash:    "hash-b",
		DurationMs:   12,
		Result:       "failed",
		Reason:       "block not found",
		Layer:        "local",
		Seq:          7,
		WatermarkPct: 85,
	})
	line := string(c.lines[0])
	for _, key := range []string{"duration_ms", "reason", "seq", "watermark_pct", "node_id", "block_hash"} {
		if !strings.Contains(line, key) {
			t.Errorf("json line %q should contain %q", line, key)
		}
	}
}

func TestBlockBytesNeverLogged(t *testing.T) {
	c, lg := newCapture()
	payload := "SECRET-BLOCK-CONTENT-xyzzy"
	data := bytes.Repeat([]byte(payload), 4)
	lg.Info("putblock saved", Fields{
		NodeID:     "node-1",
		BlockHash:  "hash-a",
		SegmentSeq: 0,
		Bytes:      int64(len(data)),
		Result:     "saved",
	})
	if len(c.lines) != 1 {
		t.Fatalf("captured %d lines, want 1", len(c.lines))
	}
	if strings.Contains(string(c.lines[0]), payload) {
		t.Error("block content must never appear in log line")
	}
	for _, f := range c.fields {
		_ = f
	}
}

func TestTraceIDCarriedAndOptional(t *testing.T) {
	c, lg := newCapture()
	lg.Info("putblock saved", Fields{TraceID: "abc123def4567890", NodeID: "node-1", Result: "saved"})
	if !strings.Contains(string(c.lines[0]), `"trace_id":"abc123def4567890"`) {
		t.Errorf("json line should carry trace_id, got %s", c.lines[0])
	}
	lg.Info("startup spec verified", Fields{NodeID: "node-1", Result: "spec_match"})
	if strings.Contains(string(c.lines[1]), "trace_id") {
		t.Errorf("trace_id must be omitted when empty, got %s", c.lines[1])
	}
}

func TestNewTraceIDDistinct(t *testing.T) {
	a := NewTraceID("hash-a", "2026-01-01T00:00:00.000000001")
	b := NewTraceID("hash-a", "2026-01-01T00:00:00.000000002")
	if a == "" || len(a) != 16 {
		t.Errorf("trace id = %q, want 16 hex chars", a)
	}
	if a == b {
		t.Error("different request elements must yield different trace ids")
	}
}

func TestLevels(t *testing.T) {
	c, lg := newCapture()
	lg.Info("startup spec verified", Fields{Result: "spec_match"})
	lg.Warn("putblock rejected", Fields{Reason: "oversized"})
	lg.Error("segment crc verify failed", Fields{SegmentSeq: 2, Result: "crc_mismatch"})
	if len(c.kinds) != 3 {
		t.Fatalf("captured %d entries, want 3", len(c.kinds))
	}
	if c.kinds[0] != "info:startup spec verified" {
		t.Errorf("entry 0 = %s", c.kinds[0])
	}
	if c.kinds[1] != "warn:putblock rejected" {
		t.Errorf("entry 1 = %s", c.kinds[1])
	}
	if c.kinds[2] != "error:segment crc verify failed" {
		t.Errorf("entry 2 = %s", c.kinds[2])
	}
	if !strings.Contains(string(c.lines[2]), "crc_mismatch") {
		t.Errorf("error line should carry result, got %s", c.lines[2])
	}
}
func TestRetryFieldCarriedAndOmitted(t *testing.T) {
	c, lg := newCapture()
	lg.Warn("event stream disconnected", Fields{NodeID: "node-1", Seq: 7, Retry: 3, Result: "disconnected"})
	if !strings.Contains(string(c.lines[0]), `"retry":3`) {
		t.Errorf("json line should carry retry, got %s", c.lines[0])
	}
	if !strings.Contains(string(c.lines[0]), `"seq":7`) {
		t.Errorf("json line should carry confirmed seq, got %s", c.lines[0])
	}
	lg.Info("event stream connected", Fields{NodeID: "node-1", Result: "connected"})
	if strings.Contains(string(c.lines[1]), "retry") {
		t.Errorf("retry must be omitted when zero, got %s", c.lines[1])
	}
}
func TestShardIndexFieldCarriedAndOmitted(t *testing.T) {
	c, lg := newCapture()
	lg.Info("heartbeat manifest shard sent", Fields{NodeID: "node-1", ShardIndex: 2, Result: "shard_sent"})
	if !strings.Contains(string(c.lines[0]), `"shard_index":2`) {
		t.Errorf("json line should carry shard_index, got %s", c.lines[0])
	}
	lg.Info("audit orphan registered", Fields{NodeID: "node-1", Result: "orphan_registered"})
	if strings.Contains(string(c.lines[1]), "shard_index") {
		t.Errorf("shard_index must be omitted when zero, got %s", c.lines[1])
	}
}
