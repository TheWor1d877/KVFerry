package spec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// KVCacheSpec 是引擎 KV 口径四项，语义见 D44：块字节、KV 精度、每 token 字节数、对齐修正。
// 存储节点按声明切段，不按公式自算。
type KVCacheSpec struct {
	BlockBytes    int64  `json:"block_bytes"`
	KVPrecision   string `json:"kv_precision"`
	BytesPerToken int64  `json:"bytes_per_token"`
	AlignmentPad  int64  `json:"alignment_pad"`
}

// SpecReader 向同机引擎读 KVCacheSpec（D44）。真实引擎协议未定，实现由 main 装配注入。
type SpecReader interface {
	ReadKVCacheSpec() (KVCacheSpec, error)
}

// supportedPrecisions 是存储节点支持的 KV 精度集合；集合外的精度在 Validate 拒绝。
var supportedPrecisions = map[string]bool{
	"fp16": true,
	"bf16": true,
}

// Validate 校验口径自洽：块字节与每 token 字节数须为正，精度须在支持集合内（D44）。
func (s KVCacheSpec) Validate() error {
	if s.BlockBytes <= 0 {
		return fmt.Errorf("block_bytes must be positive, got %d", s.BlockBytes)
	}
	if s.BytesPerToken <= 0 {
		return fmt.Errorf("bytes_per_token must be positive, got %d", s.BytesPerToken)
	}
	if !supportedPrecisions[s.KVPrecision] {
		return fmt.Errorf("unsupported kv_precision %q", s.KVPrecision)
	}
	if s.AlignmentPad < 0 {
		return fmt.Errorf("alignment_pad must not be negative, got %d", s.AlignmentPad)
	}
	return nil
}

// Equal 比对两组口径逐项一致，不一致时调用方按口径不一致标记处理（D44、D55）。
func (s KVCacheSpec) Equal(other KVCacheSpec) bool {
	return s.BlockBytes == other.BlockBytes &&
		s.KVPrecision == other.KVPrecision &&
		s.BytesPerToken == other.BytesPerToken &&
		s.AlignmentPad == other.AlignmentPad
}

// FileReader 从本地 JSON 文件读 KVCacheSpec，是引擎协议落地前的注入实现（D44）。
// 读取失败时返回错误，调用方不得让进程进入服务状态。
type FileReader struct {
	Path string
}

// ReadKVCacheSpec 读取并解析口径文件，解析后先 Validate，口径不合法按错误返回。
func (r FileReader) ReadKVCacheSpec() (KVCacheSpec, error) {
	raw, err := os.ReadFile(r.Path)
	if err != nil {
		return KVCacheSpec{}, fmt.Errorf("read engine spec: %w", err)
	}
	var s KVCacheSpec
	if err := json.Unmarshal(raw, &s); err != nil {
		return KVCacheSpec{}, fmt.Errorf("parse engine spec: %w", err)
	}
	if err := s.Validate(); err != nil {
		return KVCacheSpec{}, fmt.Errorf("invalid engine spec: %w", err)
	}
	return s, nil
}

// ErrSpecMismatch 标记口径不一致，语义见 D44。
var ErrSpecMismatch = errors.New("kv spec mismatch")
