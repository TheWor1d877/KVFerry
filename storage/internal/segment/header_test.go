package segment

import (
	"errors"
	"testing"

	"github.com/TheWor1d877/KVFerry/storage/internal/block"
)

func roundTrip(t *testing.T, h Header) Header {
	t.Helper()
	buf := make([]byte, HeaderSize)
	if err := h.Encode(buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Hash:       "deadbeef01",
		SegmentSeq: 12345,
		BlockBytes: 4096,
		DataCRC:    0xCAFEBABE,
		State:      StateWritten,
	}
	got := roundTrip(t, h)
	if got.Hash != h.Hash || got.SegmentSeq != h.SegmentSeq || got.BlockBytes != h.BlockBytes || got.DataCRC != h.DataCRC || got.State != h.State {
		t.Errorf("round trip mismatch: got %+v want %+v", got, h)
	}
}

func TestHeaderRejectsBadMagic(t *testing.T) {
	buf := make([]byte, HeaderSize)
	h := Header{Hash: "abc", SegmentSeq: 1, BlockBytes: 8, State: StateWritten}
	if err := h.Encode(buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := Decode(buf); !errors.Is(err, ErrBadMagic) {
		t.Errorf("bad magic should be rejected with ErrBadMagic, got %v", err)
	}
}

func TestHeaderRejectsBadBufAndVersion(t *testing.T) {
	if _, err := Decode(make([]byte, HeaderSize-1)); !errors.Is(err, ErrBadBuf) {
		t.Errorf("truncated buffer should be rejected, got %v", err)
	}
	buf := make([]byte, HeaderSize)
	h := Header{Hash: "abc", SegmentSeq: 1, BlockBytes: 8, State: StateWritten}
	if err := h.Encode(buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	buf[4] = 99
	if _, err := Decode(buf); !errors.Is(err, ErrBadVersion) {
		t.Errorf("bad version should be rejected, got %v", err)
	}
}

func TestHeaderCRC32CVerify(t *testing.T) {
	data := []byte("kvferry block payload")
	crc := block.CRC32C(data)
	h := Header{Hash: "hash-1", SegmentSeq: 7, BlockBytes: int64(len(data)), DataCRC: crc, State: StateWritten}
	got := roundTrip(t, h)
	if got.DataCRC != crc {
		t.Fatalf("crc field lost in round trip: got %d want %d", got.DataCRC, crc)
	}
	if !block.VerifyCRC32C(data, got.DataCRC) {
		t.Fatal("intact data should pass crc verify")
	}
	tampered := append([]byte{}, data...)
	tampered[0] ^= 0x01
	if block.VerifyCRC32C(tampered, got.DataCRC) {
		t.Error("tampered data should fail crc verify")
	}
}

func TestHeaderStateTransitions(t *testing.T) {
	states := []SegmentState{StateEmpty, StateWriting, StateWritten, StateCorrupt}
	for _, st := range states {
		h := Header{Hash: "h", SegmentSeq: 1, BlockBytes: 4, State: st}
		got := roundTrip(t, h)
		if got.State != st {
			t.Errorf("state %s lost in round trip, got %s", st, got.State)
		}
	}
	for _, st := range states {
		if st.String() == "unknown" {
			t.Errorf("state %d should have a name", st)
		}
	}
}

func TestHeaderRejectsInvalidState(t *testing.T) {
	buf := make([]byte, HeaderSize)
	h := Header{Hash: "h", SegmentSeq: 1, BlockBytes: 4, State: SegmentState(200)}
	if err := h.Encode(buf); !errors.Is(err, ErrBadState) {
		t.Errorf("encoding invalid state should fail, got %v", err)
	}
	buf2 := make([]byte, HeaderSize)
	ok := Header{Hash: "h", SegmentSeq: 1, BlockBytes: 4, State: StateWritten}
	if err := ok.Encode(buf2); err != nil {
		t.Fatalf("encode: %v", err)
	}
	buf2[5] = 9
	if _, err := Decode(buf2); !errors.Is(err, ErrBadState) {
		t.Errorf("decoding invalid state should fail, got %v", err)
	}
}

func TestHeaderHashSlot(t *testing.T) {
	long := make([]byte, HashMaxSize)
	for i := range long {
		long[i] = byte('a' + i%26)
	}
	h := Header{Hash: string(long), SegmentSeq: 2, BlockBytes: 16, State: StateWritten}
	got := roundTrip(t, h)
	if got.Hash != h.Hash {
		t.Error("max length hash should round trip")
	}
	if err := (Header{Hash: string(make([]byte, HashMaxSize+1)), State: StateEmpty}).Encode(make([]byte, HeaderSize)); !errors.Is(err, ErrHashTooLong) {
		t.Errorf("overlong hash should be rejected, got %v", err)
	}
}
