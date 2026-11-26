package block

import (
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
)

var (
	nodeSpec = spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "fp16", BytesPerToken: 8, AlignmentPad: 0}
	reqSpec  = nodeSpec
	fullData = make([]byte, 64)
)

func TestValidatePutAcceptsFullBlock(t *testing.T) {
	if err := ValidatePut("hash-1", fullData, reqSpec, nodeSpec, 1<<20, nil); err != nil {
		t.Errorf("full block should pass: %v", err)
	}
}

func TestValidatePutSpecMismatch(t *testing.T) {
	other := spec.KVCacheSpec{BlockBytes: 64, KVPrecision: "bf16", BytesPerToken: 8, AlignmentPad: 0}
	err := ValidatePut("hash-1", fullData, other, nodeSpec, 1<<20, nil)
	if err == nil {
		t.Fatal("spec mismatch should be rejected")
	}
	if got := MapError(err); got != codes.InvalidArgument {
		t.Errorf("map = %s, want InvalidArgument", got)
	}
}

func TestValidatePutEmptyHash(t *testing.T) {
	err := ValidatePut("", fullData, reqSpec, nodeSpec, 1<<20, nil)
	if err == nil {
		t.Fatal("empty hash should be rejected")
	}
	if got := MapError(err); got != codes.InvalidArgument {
		t.Errorf("map = %s, want InvalidArgument", got)
	}
}

func TestValidatePutOversized(t *testing.T) {
	var gotHash string
	called := false
	maxBytes := int64(32)
	data := make([]byte, 64)
	err := ValidatePut("hash-big", data, reqSpec, nodeSpec, maxBytes, func(h string) {
		called = true
		gotHash = h
	})
	if err == nil {
		t.Fatal("oversized block should be rejected")
	}
	if got := MapError(err); got != codes.InvalidArgument {
		t.Errorf("map = %s, want InvalidArgument", got)
	}
	if !called {
		t.Error("oversized callback should fire")
	}
	if gotHash != "hash-big" {
		t.Errorf("callback hash = %q, want hash-big", gotHash)
	}
}

func TestValidatePutHalfBlock(t *testing.T) {
	err := ValidatePut("hash-half", fullData[:32], reqSpec, nodeSpec, 1<<20, nil)
	if err == nil {
		t.Fatal("half block should be rejected")
	}
	if got := MapError(err); got != codes.InvalidArgument {
		t.Errorf("map = %s, want InvalidArgument", got)
	}
}

func TestValidatePutLongerThanSpec(t *testing.T) {
	err := ValidatePut("hash-long", make([]byte, 128), reqSpec, nodeSpec, 1<<20, nil)
	if err == nil {
		t.Fatal("block longer than spec should be rejected")
	}
	if got := MapError(err); got != codes.InvalidArgument {
		t.Errorf("map = %s, want InvalidArgument", got)
	}
}

func TestCRC32C(t *testing.T) {
	data := []byte("hello kvferry")
	sum := CRC32C(data)
	if !VerifyCRC32C(data, sum) {
		t.Error("intact data should verify")
	}
	if VerifyCRC32C([]byte("hello kvferrx"), sum) {
		t.Error("modified data should not verify")
	}
}

func TestMapError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"spec mismatch", spec.ErrSpecMismatch, codes.InvalidArgument},
		{"empty hash", ErrEmptyHash, codes.InvalidArgument},
		{"length mismatch", ErrLengthMismatch, codes.InvalidArgument},
		{"oversized", ErrOversized, codes.InvalidArgument},
		{"not ready", ErrNotReady, codes.Unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MapError(tt.err); got != tt.want {
				t.Errorf("map = %s, want %s", got, tt.want)
			}
		})
	}
	if got := MapError(nil); got != codes.Unknown {
		t.Errorf("nil error should map to Unknown, got %s", got)
	}
}
