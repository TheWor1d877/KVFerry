package manifest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/block"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

const (
	// SnapshotFile 是段清单快照文件名（D49）。
	SnapshotFile = "manifest.snapshot"
	// snapshotTempFile 是快照写入的临时文件名，写全后原子改名。
	snapshotTempFile = "manifest.snapshot.tmp"
	// dataFile 是数据大文件名，首部文件头与段区布局见 D43。
	dataFile = "blocks.dat"
	// snapshotHeaderSize 是快照定长首部：魔数、版本、高水位、占用段数。
	snapshotHeaderSize = 21
	// snapshotMagic 与 snapshotVersion 标识快照格式。
	snapshotMagic   uint32 = 0x4B46534E
	snapshotVersion uint8  = 1
)

var (
	// ErrNoSnapshot 表示数据目录没有快照，调用方按段头兜底（D49）。
	ErrNoSnapshot = errors.New("manifest snapshot missing")
	// ErrSnapshotInvalid 表示快照损坏或不合法，调用方按段头兜底（D49）。
	ErrSnapshotInvalid = errors.New("manifest snapshot invalid")
)

// Write 把位图占用段号与高水位序列号编码落 dataDir 快照并 fsync（D49）。
// 先写临时文件再原子改名，读方不会遇到写半截的快照。
func Write(dataDir string, occupied []int64, highWater int64) error {
	buf := encodeSnapshot(occupied, highWater)
	tmp := filepath.Join(dataDir, snapshotTempFile)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open snapshot temp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dataDir, SnapshotFile)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

// Load 读快照，返回占用段号升序与高水位序列号（D49）。
// 快照缺失或损坏按错误返回，调用方按段头兜底（D49）。
func Load(dataDir string) ([]int64, int64, error) {
	buf, err := os.ReadFile(filepath.Join(dataDir, SnapshotFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, ErrNoSnapshot
		}
		return nil, 0, fmt.Errorf("read snapshot: %w", err)
	}
	return decodeSnapshot(buf)
}

func encodeSnapshot(occupied []int64, highWater int64) []byte {
	buf := make([]byte, snapshotHeaderSize+8*len(occupied)+4)
	binary.LittleEndian.PutUint32(buf[0:4], snapshotMagic)
	buf[4] = snapshotVersion
	binary.LittleEndian.PutUint64(buf[5:13], uint64(highWater))
	binary.LittleEndian.PutUint64(buf[13:21], uint64(len(occupied)))
	for i, seq := range occupied {
		binary.LittleEndian.PutUint64(buf[snapshotHeaderSize+8*i:], uint64(seq))
	}
	crc := block.CRC32C(buf[:snapshotHeaderSize+8*len(occupied)])
	binary.LittleEndian.PutUint32(buf[snapshotHeaderSize+8*len(occupied):], crc)
	return buf
}

func decodeSnapshot(buf []byte) ([]int64, int64, error) {
	if len(buf) < snapshotHeaderSize+4 {
		return nil, 0, fmt.Errorf("%w: buffer %d bytes too short", ErrSnapshotInvalid, len(buf))
	}
	body := len(buf) - 4
	if binary.LittleEndian.Uint32(buf[0:4]) != snapshotMagic {
		return nil, 0, fmt.Errorf("%w: magic mismatch", ErrSnapshotInvalid)
	}
	if buf[4] != snapshotVersion {
		return nil, 0, fmt.Errorf("%w: version mismatch", ErrSnapshotInvalid)
	}
	if !block.VerifyCRC32C(buf[:body], binary.LittleEndian.Uint32(buf[body:])) {
		return nil, 0, fmt.Errorf("%w: crc mismatch", ErrSnapshotInvalid)
	}
	count := int64(binary.LittleEndian.Uint64(buf[13:21]))
	if snapshotHeaderSize+8*count+4 != int64(len(buf)) {
		return nil, 0, fmt.Errorf("%w: entry count %d mismatches buffer %d bytes", ErrSnapshotInvalid, count, len(buf))
	}
	highWater := int64(binary.LittleEndian.Uint64(buf[5:13]))
	occupied := make([]int64, count)
	for i := range occupied {
		occupied[i] = int64(binary.LittleEndian.Uint64(buf[snapshotHeaderSize+8*i:]))
	}
	sort.Slice(occupied, func(i, j int) bool { return occupied[i] < occupied[j] })
	return occupied, highWater, nil
}

// Source 是快照数据源（D49）：位图占用段号与高水位序列号。
type Source interface {
	Snapshot() (occupied []int64, highWater int64, err error)
}

// SourceFunc 适配函数为快照数据源。
type SourceFunc func() ([]int64, int64, error)

// Snapshot 实现数据源接口。
func (f SourceFunc) Snapshot() ([]int64, int64, error) { return f() }

// Snapshotter 按周期落快照，写入周期与心跳同频（D49）。
type Snapshotter struct {
	dataDir  string
	interval time.Duration
	src      Source
	log      *telemetry.Logger
}

// NewSnapshotter 建立周期快照器（D49）：interval 由调用方取心跳周期注入。
func NewSnapshotter(dataDir string, interval time.Duration, src Source, log *telemetry.Logger) *Snapshotter {
	if log == nil {
		log = telemetry.New()
	}
	return &Snapshotter{dataDir: dataDir, interval: interval, src: src, log: log}
}

// WriteOnce 落一次快照（D49）：数据源失败按错误返回，周期调度不受单次失败影响。
func (s *Snapshotter) WriteOnce() error {
	occupied, highWater, err := s.src.Snapshot()
	if err != nil {
		return fmt.Errorf("gather snapshot: %w", err)
	}
	return Write(s.dataDir, occupied, highWater)
}

// Run 按周期落快照，ctx 取消即退出（D49）。
func (s *Snapshotter) Run(ctx context.Context) {
	if s.interval <= 0 {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.WriteOnce(); err != nil {
				s.log.Warn("manifest snapshot failed", telemetry.Fields{Reason: err.Error(), Result: "snapshot_failed"})
			}
		}
	}
}

// ScanSegmentHeads 扫全部段头，返回占用段号升序与块哈希到段号的映射（D43、D49）：
// 段头是唯一权威，状态非已写或解码失败的段不计占用。
// 高水位序列号不在段头里，由调用方从位图单独取。
func ScanSegmentHeads(dataDir string, segmentSize, totalSegments int64) ([]int64, map[string]int64, error) {
	f, err := os.Open(filepath.Join(dataDir, dataFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("%w: %s", ErrNoSnapshot, dataFile)
		}
		return nil, nil, fmt.Errorf("open data file: %w", err)
	}
	defer f.Close()

	var occupied []int64
	hashes := make(map[string]int64)
	buf := make([]byte, segment.HeaderSize)
	for seq := int64(0); seq < totalSegments; seq++ {
		off := segment.FileHeaderSize + seq*segmentSize
		if _, err := f.ReadAt(buf, off); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, nil, fmt.Errorf("read segment header %d: %w", seq, err)
		}
		h, err := segment.Decode(buf)
		if err != nil || h.State != segment.StateWritten {
			continue
		}
		occupied = append(occupied, seq)
		if _, exists := hashes[h.Hash]; !exists {
			hashes[h.Hash] = seq
		}
	}
	return occupied, hashes, nil
}

// Entry 是清单块条目（D48）。
type Entry struct {
	BlockHash  string
	SegmentSeq int64
	Layer      api.BlockLayer
	Offset     int64
	BlockBytes int64
}

// BuildEntries 从哈希到段号的映射生成清单条目（D48）：层位本机、偏移按布局、块字节取口径，
// 按段序号升序输出。
func BuildEntries(hashes map[string]int64, segmentSize, blockBytes int64) []Entry {
	entries := make([]Entry, 0, len(hashes))
	for hash, seq := range hashes {
		entries = append(entries, Entry{
			BlockHash:  hash,
			SegmentSeq: seq,
			Layer:      api.BlockLayer_BLOCK_LAYER_LOCAL,
			Offset:     segment.FileHeaderSize + seq*segmentSize,
			BlockBytes: blockBytes,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].SegmentSeq < entries[j].SegmentSeq })
	return entries
}

// Shards 按段序号升序把条目切成片（D48）：单次条目数超过 shardBlocks 时分片，
// 各片带分片序号与总片数；无条目时回单片空片，保证接收侧节点完成判定成立。
func Shards(entries []Entry, shardBlocks int) [][]Entry {
	if shardBlocks <= 0 || shardBlocks >= len(entries) {
		if len(entries) == 0 {
			return [][]Entry{{}}
		}
		return [][]Entry{entries}
	}
	var shards [][]Entry
	for start := 0; start < len(entries); start += shardBlocks {
		end := start + shardBlocks
		if end > len(entries) {
			end = len(entries)
		}
		shards = append(shards, entries[start:end])
	}
	return shards
}

// Reporter 把清单分片逐片上报决策侧（D41、D48）。
type Reporter struct {
	nodeID      string
	shardBlocks int
	client      api.StorageReporterClient
}

// NewReporter 建立清单上报客户端（D41）：客户端连接由调用方注入。
func NewReporter(nodeID string, shardBlocks int, client api.StorageReporterClient) *Reporter {
	return &Reporter{nodeID: nodeID, shardBlocks: shardBlocks, client: client}
}

// Report 把条目按 D48 切片并逐片调用决策侧 ReportManifest（D41）。
// 任一片失败即返回错误，调用方降级记日志，不阻塞写入主链路（D51）。
func (r *Reporter) Report(ctx context.Context, entries []Entry) error {
	shards := Shards(entries, r.shardBlocks)
	total := int64(len(shards))
	for i, shard := range shards {
		req := &api.ReportManifestRequest{
			NodeId:     r.nodeID,
			ShardIndex: int64(i),
			ShardTotal: total,
			Entries:    make([]*api.ManifestEntry, 0, len(shard)),
		}
		for _, e := range shard {
			req.Entries = append(req.Entries, &api.ManifestEntry{
				BlockHash:  e.BlockHash,
				Layer:      e.Layer,
				Offset:     e.Offset,
				BlockBytes: e.BlockBytes,
			})
		}
		if _, err := r.client.ReportManifest(ctx, req); err != nil {
			return fmt.Errorf("report manifest shard %d/%d: %w", i, total, err)
		}
	}
	return nil
}
