// Package startup 做存储节点启动口径校验与清空重建编排（D44、D55）：
// 口径校验复用段层文件头比对，文件头缺失、损坏或与节点口径任一项不一致时清空数据
// 大文件按新口径重建段区与位图，再向决策服务逐片上报一次全量清单。
package startup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// dataFileName 与段层数据大文件同名（D43）；清空重建按文件粒度删除后由段层重建。
const dataFileName = "blocks.dat"

// Verdict 是启动口径校验结论（D44、D55）。
type Verdict int

const (
	// VerdictFresh 表示数据大文件不存在，本次启动按节点口径新建文件头与段区。
	VerdictFresh Verdict = iota
	// VerdictMatch 表示文件头记录口径与节点口径一致，段区原样保留。
	VerdictMatch
	// VerdictRebuild 表示文件头缺失、损坏或口径不一致，已清空数据大文件按新口径重建。
	VerdictRebuild
)

// String 返回结论名，用于日志 result 字段（D55）。
func (v Verdict) String() string {
	switch v {
	case VerdictFresh:
		return "fresh"
	case VerdictRebuild:
		return "rebuild"
	default:
		return "match"
	}
}

// Rereporter 接收全量清单条目并逐片上报决策服务（D41、D55）。
type Rereporter interface {
	Rereport(ctx context.Context, entries []manifest.Entry) error
}

// RereporterFunc 适配函数为重报入口。
type RereporterFunc func(ctx context.Context, entries []manifest.Entry) error

// Rereport 实现重报入口。
func (f RereporterFunc) Rereport(ctx context.Context, entries []manifest.Entry) error {
	return f(ctx, entries)
}

// Options 是启动编排选项。
type Options struct {
	// Emit 为事件发射回调，透传给段层（D47）。
	Emit segment.EmitFunc
	// Log 为结构化日志器，nil 时用默认 stdout 日志器。
	Log *telemetry.Logger
	// Pins 为 pin 标记视图；pin 清单恢复须先于段头扫描（R34），由调用方在 Open 前完成。
	Pins segment.PinView
	// WatermarkPct 为 NVMe 驱逐水位百分比（D42）。
	WatermarkPct int
}

// Open 装配段层并完成启动口径校验编排（D44、D55）：数据大文件不存在时按节点口径
// 新建文件头与段区（VerdictFresh）；文件头缺失、损坏或任一项不一致时清空数据大文件，
// 按新口径重建段区与位图（VerdictRebuild）；其余原样打开（VerdictMatch）。
// 位图与段头对账、映射重建由段层完成，快照缺失时段头自描述兜底（D49），本包不重复实现。
func Open(dataDir, nodeID string, kvSpec spec.KVCacheSpec, capacityBytes int64, opts Options) (*segment.Store, Verdict, error) {
	start := time.Now()
	if opts.Log == nil {
		opts.Log = telemetry.New()
	}
	fresh := !dataFileExists(dataDir)
	store, err := segment.Open(dataDir, nodeID, kvSpec, capacityBytes, segment.Options{
		Emit:         opts.Emit,
		Log:          opts.Log,
		Pins:         opts.Pins,
		WatermarkPct: opts.WatermarkPct,
	})
	if err == nil {
		verdict := VerdictMatch
		if fresh {
			verdict = VerdictFresh
		}
		opts.Log.Info("startup spec verified", telemetry.Fields{
			NodeID:     nodeID,
			Spec:       DescribeSpec(kvSpec),
			Segments:   store.TotalSegments(),
			DurationMs: time.Since(start).Milliseconds(),
			Result:     verdict.String(),
		})
		return store, verdict, nil
	}
	if !isRebuildRequired(err) {
		return nil, VerdictFresh, err
	}
	// 文件头缺失、损坏或口径不一致：清空数据大文件按新口径重建（D55）。
	opts.Log.Warn("startup spec mismatch, rebuilding data file", telemetry.Fields{
		NodeID:     nodeID,
		Spec:       DescribeSpec(kvSpec),
		DurationMs: time.Since(start).Milliseconds(),
		Result:     "spec_mismatch",
		Reason:     err.Error(),
	})
	if rmErr := os.Remove(filepath.Join(dataDir, dataFileName)); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
		return nil, VerdictRebuild, fmt.Errorf("remove stale data file: %w", rmErr)
	}
	rebuilt, err := segment.Open(dataDir, nodeID, kvSpec, capacityBytes, segment.Options{
		Emit:         opts.Emit,
		Log:          opts.Log,
		Pins:         opts.Pins,
		WatermarkPct: opts.WatermarkPct,
	})
	if err != nil {
		return nil, VerdictRebuild, err
	}
	opts.Log.Info("startup rebuild completed", telemetry.Fields{
		NodeID:     nodeID,
		Spec:       DescribeSpec(kvSpec),
		Segments:   rebuilt.TotalSegments(),
		DurationMs: time.Since(start).Milliseconds(),
		Result:     "rebuild_completed",
	})
	return rebuilt, VerdictRebuild, nil
}

// RereportRebuilt 在清空重建后向决策服务逐片上报一次全量清单（D41、D55）：
// 扫段头生成条目交重报入口分片上报，打点记条目数与耗时；上报失败按错误返回，
// 调用方降级记日志不阻塞服务，等心跳对账收敛（D51）。
func RereportRebuilt(ctx context.Context, dataDir string, store *segment.Store, kvSpec spec.KVCacheSpec, nodeID string, rp Rereporter, log *telemetry.Logger) error {
	if log == nil {
		log = telemetry.New()
	}
	start := time.Now()
	_, hashes, err := manifest.ScanSegmentHeads(dataDir, store.SegmentSize(), store.TotalSegments())
	if err != nil {
		log.Warn("manifest rereport failed", telemetry.Fields{
			NodeID:     nodeID,
			Spec:       DescribeSpec(kvSpec),
			DurationMs: time.Since(start).Milliseconds(),
			Result:     "manifest_rereport_failed",
			Reason:     err.Error(),
		})
		return err
	}
	entries := manifest.BuildEntries(hashes, store.SegmentSize(), kvSpec.BlockBytes)
	if err := rp.Rereport(ctx, entries); err != nil {
		log.Warn("manifest rereport failed", telemetry.Fields{
			NodeID:     nodeID,
			Spec:       DescribeSpec(kvSpec),
			Entries:    int64(len(entries)),
			DurationMs: time.Since(start).Milliseconds(),
			Result:     "manifest_rereport_failed",
			Reason:     err.Error(),
		})
		return err
	}
	log.Info("manifest rereport completed", telemetry.Fields{
		NodeID:     nodeID,
		Spec:       DescribeSpec(kvSpec),
		Entries:    int64(len(entries)),
		DurationMs: time.Since(start).Milliseconds(),
		Result:     "manifest_rereported",
	})
	return nil
}

// DescribeSpec 把口径四项转成日志紧凑描述（D44）。
func DescribeSpec(s spec.KVCacheSpec) string {
	return fmt.Sprintf("block_bytes=%d,precision=%s,bytes_per_token=%d,alignment_pad=%d",
		s.BlockBytes, s.KVPrecision, s.BytesPerToken, s.AlignmentPad)
}

// isRebuildRequired 判定段层错误是否要求清空重建（D55）：文件头缺失、损坏或口径不一致。
func isRebuildRequired(err error) bool {
	return errors.Is(err, segment.ErrFileHeader) || errors.Is(err, segment.ErrFileHeaderMismatch)
}

func dataFileExists(dataDir string) bool {
	_, err := os.Stat(filepath.Join(dataDir, dataFileName))
	return err == nil
}
