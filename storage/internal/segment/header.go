package segment

import (
	"encoding/binary"
	"errors"
)

// SegmentState 是段头状态，四种取值见 D43。
type SegmentState uint8

const (
	StateEmpty   SegmentState = iota
	StateWriting              // 写入中，撕裂写按损坏处理
	StateWritten              // 已写，CRC32C 已写全
	StateCorrupt              // 损坏
)

func (s SegmentState) String() string {
	switch s {
	case StateEmpty:
		return "empty"
	case StateWriting:
		return "writing"
	case StateWritten:
		return "written"
	case StateCorrupt:
		return "corrupt"
	}
	return "unknown"
}

const (
	// HeaderSize 是段头定长编码长度（D43）。
	HeaderSize = 92
	// HashMaxSize 是块哈希定长槽长度，超长哈希拒绝。
	HashMaxSize = 64
	// segmentMagic 与 headerVersion 标识段头格式；魔数不符一律拒绝（D43）。
	segmentMagic  uint32 = 0x46537631
	headerVersion uint8  = 1
)

var (
	ErrBadMagic    = errors.New("segment header magic mismatch")
	ErrBadVersion  = errors.New("segment header version mismatch")
	ErrBadBuf      = errors.New("segment header buffer size mismatch")
	ErrHashTooLong = errors.New("block hash exceeds header slot")
	ErrBadHashLen  = errors.New("segment header hash length invalid")
	ErrBadState    = errors.New("segment header state invalid")
)

// Header 是段头自描述信息，六项见 D43：魔数、块哈希、段序号、CRC32C、块字节与状态。
type Header struct {
	Hash       string
	SegmentSeq int64
	BlockBytes int64
	DataCRC    uint32
	State      SegmentState
}

// Encode 把段头编码进定长缓冲，布局与 HeaderSize 对应（D43）。
func (h Header) Encode(buf []byte) error {
	if len(buf) != HeaderSize {
		return ErrBadBuf
	}
	if len(h.Hash) > HashMaxSize {
		return ErrHashTooLong
	}
	if h.State > StateCorrupt {
		return ErrBadState
	}
	for i := range buf {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint32(buf[0:4], segmentMagic)
	buf[4] = headerVersion
	buf[5] = byte(h.State)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(len(h.Hash)))
	copy(buf[8:8+HashMaxSize], h.Hash)
	binary.LittleEndian.PutUint64(buf[72:80], uint64(h.SegmentSeq))
	binary.LittleEndian.PutUint64(buf[80:88], uint64(h.BlockBytes))
	binary.LittleEndian.PutUint32(buf[88:92], h.DataCRC)
	return nil
}

// Decode 解析段头，魔数不符、版本不符、缓冲截断、哈希长度或状态非法一律拒绝（D43）。
func Decode(buf []byte) (Header, error) {
	if len(buf) != HeaderSize {
		return Header{}, ErrBadBuf
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != segmentMagic {
		return Header{}, ErrBadMagic
	}
	if buf[4] != headerVersion {
		return Header{}, ErrBadVersion
	}
	state := SegmentState(buf[5])
	if state > StateCorrupt {
		return Header{}, ErrBadState
	}
	hashLen := int(binary.LittleEndian.Uint16(buf[6:8]))
	if hashLen > HashMaxSize {
		return Header{}, ErrBadHashLen
	}
	return Header{
		Hash:       string(buf[8 : 8+hashLen]),
		SegmentSeq: int64(binary.LittleEndian.Uint64(buf[72:80])),
		BlockBytes: int64(binary.LittleEndian.Uint64(buf[80:88])),
		DataCRC:    binary.LittleEndian.Uint32(buf[88:92]),
		State:      state,
	}, nil
}
