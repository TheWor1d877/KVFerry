package block

import (
	"errors"
	"fmt"
	"hash/crc32"

	"google.golang.org/grpc/codes"

	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
)

var (
	// ErrEmptyHash 表示块哈希字段缺失，存储不自算键（D45、D46）。
	ErrEmptyHash = errors.New("block hash is empty")
	// ErrLengthMismatch 表示块字节长度与口径不符，含半块拒收（D44、R31）。
	ErrLengthMismatch = errors.New("block size mismatch")
	// ErrOversized 表示块字节超过 rpc_max_message_mb（D46、R35）。
	ErrOversized = errors.New("block exceeds rpc_max_message_mb")
	// ErrNotReady 表示节点未就绪（D46）。
	ErrNotReady = errors.New("node not ready")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C 计算块字节的 CRC32C 校验值（D46）。
func CRC32C(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

// VerifyCRC32C 校验块字节与段头记录的 CRC32C 是否一致（D46）。
func VerifyCRC32C(data []byte, sum uint32) bool {
	return crc32.Checksum(data, castagnoliTable) == sum
}

// ValidatePut 做 PutBlock 入口校验：口径一致、哈希非空、块字节与口径一致、不超过消息上限（D44、D45、D46）。
// 校验通过返回 nil；超限时调用 onOversized 记 oversized 事件，回调由参数传入，本函数不建包级全局状态（R35）。
func ValidatePut(blockHash string, data []byte, reqSpec, nodeSpec spec.KVCacheSpec, maxMessageBytes int64, onOversized func(blockHash string)) error {
	if !reqSpec.Equal(nodeSpec) {
		return fmt.Errorf("%w: request spec differs from node spec", spec.ErrSpecMismatch)
	}
	if blockHash == "" {
		return ErrEmptyHash
	}
	if int64(len(data)) != nodeSpec.BlockBytes {
		return fmt.Errorf("%w: want %d bytes, got %d; half blocks are rejected", ErrLengthMismatch, nodeSpec.BlockBytes, len(data))
	}
	if int64(len(data)) > maxMessageBytes {
		if onOversized != nil {
			onOversized(blockHash)
		}
		return fmt.Errorf("%w: %d bytes exceeds limit %d", ErrOversized, len(data), maxMessageBytes)
	}
	return nil
}

// CodedError 是段层错误实现的状态码接口，MapError 据此取 gRPC 错误码（D46）。
type CodedError interface {
	error
	BlockCode() codes.Code
}

// MapError 把段层与块校验错误映射到五个 gRPC 错误码（D46）。
func MapError(err error) codes.Code {
	var ce CodedError
	if errors.As(err, &ce) {
		return ce.BlockCode()
	}
	switch {
	case errors.Is(err, spec.ErrSpecMismatch),
		errors.Is(err, ErrEmptyHash),
		errors.Is(err, ErrLengthMismatch),
		errors.Is(err, ErrOversized):
		return codes.InvalidArgument
	case errors.Is(err, ErrNotReady):
		return codes.Unavailable
	}
	return codes.Unknown
}
