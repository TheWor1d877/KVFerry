package spec

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileReaderParse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "engine-spec.json")
	content := `{"block_bytes": 4096, "kv_precision": "fp16", "bytes_per_token": 512, "alignment_pad": 0}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write spec file: %v", err)
	}
	got, err := FileReader{Path: path}.ReadKVCacheSpec()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := KVCacheSpec{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: 0}
	if got != want {
		t.Errorf("spec = %+v, want %+v", got, want)
	}
}

func TestFileReaderFailure(t *testing.T) {
	dir := t.TempDir()
	r := FileReader{Path: filepath.Join(dir, "missing.json")}
	if _, err := r.ReadKVCacheSpec(); err == nil {
		t.Fatal("missing file should fail")
	}
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	r2 := FileReader{Path: path}
	if _, err := r2.ReadKVCacheSpec(); err == nil {
		t.Fatal("bad json should fail")
	}
}

func TestEqual(t *testing.T) {
	base := KVCacheSpec{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: 0}
	if !base.Equal(base) {
		t.Fatal("identical specs should be equal")
	}
	tests := []KVCacheSpec{
		{BlockBytes: 8192, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: 0},
		{BlockBytes: 4096, KVPrecision: "bf16", BytesPerToken: 512, AlignmentPad: 0},
		{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: 256, AlignmentPad: 0},
		{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: 64},
	}
	for i, other := range tests {
		if base.Equal(other) {
			t.Errorf("case %d: differing specs should not be equal", i)
		}
	}
}

func TestValidateRejectsUnsupportedPrecision(t *testing.T) {
	good := KVCacheSpec{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: 0}
	if err := good.Validate(); err != nil {
		t.Errorf("fp16 should pass: %v", err)
	}
	good.KVPrecision = "bf16"
	if err := good.Validate(); err != nil {
		t.Errorf("bf16 should pass: %v", err)
	}

	tests := []struct {
		name string
		spec KVCacheSpec
	}{
		{"unsupported precision", KVCacheSpec{BlockBytes: 4096, KVPrecision: "fp32", BytesPerToken: 512, AlignmentPad: 0}},
		{"empty precision", KVCacheSpec{BlockBytes: 4096, KVPrecision: "", BytesPerToken: 512, AlignmentPad: 0}},
		{"zero block bytes", KVCacheSpec{BlockBytes: 0, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: 0}},
		{"negative bytes per token", KVCacheSpec{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: -1, AlignmentPad: 0}},
		{"negative alignment", KVCacheSpec{BlockBytes: 4096, KVPrecision: "fp16", BytesPerToken: 512, AlignmentPad: -8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.spec.Validate(); err == nil {
				t.Fatalf("%s should be rejected", tt.name)
			}
		})
	}
}

func TestSpecMismatchSentinel(t *testing.T) {
	if !errors.Is(ErrSpecMismatch, ErrSpecMismatch) {
		t.Fatal("sentinel should match itself")
	}
}
