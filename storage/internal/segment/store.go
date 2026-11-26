package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/TheWor1d877/KVFerry/storage/internal/block"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

const (
	// FileHeaderSize 是数据大文件首部定长编码长度（D43）。
	FileHeaderSize = 71
	// fileHeaderMagic 与 fileHeaderVersion 标识文件头格式。
	fileHeaderMagic   uint32 = 0x4B564648
	fileHeaderVersion uint8  = 1
	// precisionSlotSize 是 KV 精度字符串的定长槽。
	precisionSlotSize = 32
	dataFileName      = "blocks.dat"
)

// 驱逐原因取值，写入事件 Reason 字段与日志 reason 字段（D47、D54、R34）。
const (
	// ReasonWatermark 是水位超限触发的水位驱逐（D42）。
	ReasonWatermark = "watermark_exceeded"
	// ReasonPinQuota 是 pin 配额超限回收最旧 pin 的段（R34）。
	ReasonPinQuota = "pin_quota_exceeded"
	// ReasonAuditHash 是抽读校验不符的注销（D54）。
	ReasonAuditHash = "audit_hash_mismatch"
)

var (
	// ErrNotFound 表示映射里没有该块哈希（D46）。
	ErrNotFound = &segError{code: codes.NotFound, msg: "block not found"}
	// ErrHashMismatch 表示段头记录的块哈希与请求键不符，段已被回收并复用（D46）。
	ErrHashMismatch = &segError{code: codes.NotFound, msg: "segment hash mismatch"}
	// ErrNoFreeSegment 表示位图耗尽，无空闲段（D46）。
	ErrNoFreeSegment = &segError{code: codes.ResourceExhausted, msg: "no free segment"}
	// ErrCRCMismatch 表示块字节 CRC32C 校验失败（D46）。
	ErrCRCMismatch = &segError{code: codes.DataLoss, msg: "block crc mismatch"}
	// ErrCorrupt 表示段头损坏或状态非已写，撕裂写按损坏处理（D43、D46）。
	ErrCorrupt = &segError{code: codes.DataLoss, msg: "segment corrupt"}
	// ErrBadBlockLength 表示读回长度与口径不符，按丢弃处理（D44）。
	ErrBadBlockLength = &segError{code: codes.DataLoss, msg: "segment block size differs from spec"}
	// ErrBadPutLength 表示写入块字节长度与口径不符，含半块（D44、R31）。
	ErrBadPutLength = &segError{code: codes.InvalidArgument, msg: "put size differs from spec"}
	// ErrFileHeader 表示文件头缺失、损坏或格式不符（D43）。
	ErrFileHeader = &segError{code: codes.DataLoss, msg: "file header invalid"}
	// ErrFileHeaderMismatch 表示文件头口径与节点口径不一致；本层按错误返回，
	// 清空重建编排由启动包负责（D55）。
	ErrFileHeaderMismatch = &segError{code: codes.DataLoss, msg: "file header spec mismatch"}
	// ErrCapacityTooSmall 表示容量上限装不下一个段（D42）。
	ErrCapacityTooSmall = &segError{code: codes.InvalidArgument, msg: "capacity smaller than one segment"}
)

// segError 是段层错误，携带五个错误码语义（D46）。
type segError struct {
	code codes.Code
	msg  string
}

func (e *segError) Error() string { return e.msg }

func (e *segError) BlockCode() codes.Code { return e.code }

// Is 支持错误包装后的 errors.Is 匹配。
func (e *segError) Is(target error) bool {
	t, ok := target.(*segError)
	return ok && t == e
}

// Event 是发给事件回调的结构，种类与 D47 五类命名一致。
// seq 由事件环形缓冲后续分配，本结构不带。
type Event struct {
	Kind       string
	NodeID     string
	BlockHash  string
	SegmentSeq int64
	Layer      string
	Reason     string
	TsMs       int64
}

// EmitFunc 是事件发射钩子；回调在 Store 内部锁内调用，不得回调回 Store。
type EmitFunc func(Event)

// PinView 是 pin 标记视图，驱逐候选生成时查询，标记的块跳过驱逐（R34）。
// pin 表由装配方注入，本包不依赖任何具体 pin 实现。
type PinView interface {
	Pinned(blockHash string) bool
}

// Orphan 是盘上已写而内存映射无登记的孤儿段（D54）。
type Orphan struct {
	Hash       string
	SegmentSeq int64
}

// Options 是 Store 构造选项。
type Options struct {
	// Emit 为事件发射回调，由 main 或测试注入。
	Emit EmitFunc
	// AfterSync 在数据文件 fsync 完成后调用，供测试断言 fsync 先于事件发射。
	AfterSync func()
	// Log 为结构化日志器，nil 时用默认 stdout 日志器。
	Log *telemetry.Logger
	// Pins 为 pin 标记视图，驱逐跳过标记块；nil 表示无 pin 保护（R34）。
	// pin 清单加载先于 Open 的段头扫描完成，pin 的段重建后即不可驱逐（R34）。
	Pins PinView
	// WatermarkPct 为 NVMe 占用水位百分比，每轮写入后超过即触发驱逐；取 0 关闭驱逐（D42）。
	WatermarkPct int
}

// Store 是数据大文件与段层管理器（D43）。
// 段偏移等于文件头长度加段序号乘段长，段长取段头长度加块字节后向上对齐。
type Store struct {
	mu            sync.RWMutex
	f             *os.File
	kvSpec        spec.KVCacheSpec
	totalSegments int64
	segmentSize   int64
	bm            *Bitmap
	index         map[string]int64
	inflight      map[int64]*atomic.Int32
	emit          EmitFunc
	afterSync     func()
	log           *telemetry.Logger
	nodeID        string
	pins          PinView
	watermarkPct  int
	// access 记录块哈希到最后访问的逻辑序号，驱逐候选按它从旧到新排序；
	// 用进程内单调序号而非墙钟，排序确定且不受时钟精度影响。
	access    map[string]int64
	accessGen int64
	// evicting 记录已摘键尚未回位图的段号，补登窗口内拒绝采纳，防止驱逐与对账竞态。
	evicting map[int64]bool
}

// Open 打开或建立数据大文件，扫描段区完成对账与映射重建（D43）。
// 文件头与节点口径不一致时按错误返回，清空重建属启动包编排（D55）。
func Open(dataDir, nodeID string, kvSpec spec.KVCacheSpec, capacityBytes int64, opts Options) (*Store, error) {
	if err := kvSpec.Validate(); err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = telemetry.New()
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	path := filepath.Join(dataDir, dataFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}

	segmentSize := alignUp(FileHeaderSize+HeaderSize+kvSpec.BlockBytes, kvSpec.AlignmentPad)
	if segmentSize <= 0 {
		f.Close()
		return nil, ErrCapacityTooSmall
	}
	totalSegments := capacityBytes / segmentSize
	if totalSegments <= 0 {
		f.Close()
		return nil, ErrCapacityTooSmall
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat data file: %w", err)
	}

	if st.Size() == 0 {
		buf, err := encodeFileHeader(kvSpec, totalSegments)
		if err != nil {
			f.Close()
			return nil, err
		}
		if _, err := f.WriteAt(buf, 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("write file header: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("sync file header: %w", err)
		}
	} else {
		buf := make([]byte, FileHeaderSize)
		if _, err := f.ReadAt(buf, 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("%w: %v", ErrFileHeader, err)
		}
		fhSpec, fhTotal, err := decodeFileHeader(buf)
		if err != nil {
			f.Close()
			return nil, err
		}
		if !fhSpec.Equal(kvSpec) || fhTotal != totalSegments {
			f.Close()
			return nil, ErrFileHeaderMismatch
		}
	}

	s := &Store{
		f:             f,
		kvSpec:        kvSpec,
		totalSegments: totalSegments,
		segmentSize:   segmentSize,
		bm:            NewBitmap(totalSegments),
		index:         make(map[string]int64),
		inflight:      make(map[int64]*atomic.Int32),
		emit:          opts.Emit,
		afterSync:     opts.AfterSync,
		log:           opts.Log,
		nodeID:        nodeID,
		pins:          opts.Pins,
		watermarkPct:  opts.WatermarkPct,
		access:        make(map[string]int64),
		evicting:      make(map[int64]bool),
	}
	s.reconcile()
	return s, nil
}

// Close 关闭数据文件。
func (s *Store) Close() error {
	return s.f.Close()
}

// SegmentSize 返回段长。
func (s *Store) SegmentSize() int64 {
	return s.segmentSize
}

// TotalSegments 返回段总数。
func (s *Store) TotalSegments() int64 {
	return s.totalSegments
}

// UsedSegments 返回当前占用段数，水位口径与位图占用一致（D42）。
func (s *Store) UsedSegments() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bm.UsedCount()
}

// HighWater 返回位图高水位序列号（D49）。
func (s *Store) HighWater() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bm.HighWater()
}

func (s *Store) offset(seq int64) int64 {
	return FileHeaderSize + seq*s.segmentSize
}

func alignUp(v, align int64) int64 {
	if align <= 1 {
		return v
	}
	return (v + align - 1) / align * align
}

func encodeFileHeader(kvSpec spec.KVCacheSpec, totalSegments int64) ([]byte, error) {
	if len(kvSpec.KVPrecision) > precisionSlotSize {
		return nil, fmt.Errorf("kv precision exceeds file header slot")
	}
	buf := make([]byte, FileHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], fileHeaderMagic)
	buf[4] = fileHeaderVersion
	binary.LittleEndian.PutUint64(buf[5:13], uint64(kvSpec.BlockBytes))
	binary.LittleEndian.PutUint16(buf[13:15], uint16(len(kvSpec.KVPrecision)))
	copy(buf[15:15+precisionSlotSize], kvSpec.KVPrecision)
	binary.LittleEndian.PutUint64(buf[47:55], uint64(kvSpec.BytesPerToken))
	binary.LittleEndian.PutUint64(buf[55:63], uint64(kvSpec.AlignmentPad))
	binary.LittleEndian.PutUint64(buf[63:71], uint64(totalSegments))
	return buf, nil
}

func decodeFileHeader(buf []byte) (spec.KVCacheSpec, int64, error) {
	if len(buf) != FileHeaderSize {
		return spec.KVCacheSpec{}, 0, fmt.Errorf("%w: buffer %d bytes", ErrFileHeader, len(buf))
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != fileHeaderMagic {
		return spec.KVCacheSpec{}, 0, fmt.Errorf("%w: magic mismatch", ErrFileHeader)
	}
	if buf[4] != fileHeaderVersion {
		return spec.KVCacheSpec{}, 0, fmt.Errorf("%w: version mismatch", ErrFileHeader)
	}
	precisionLen := int(binary.LittleEndian.Uint16(buf[13:15]))
	if precisionLen > precisionSlotSize {
		return spec.KVCacheSpec{}, 0, fmt.Errorf("%w: precision length invalid", ErrFileHeader)
	}
	return spec.KVCacheSpec{
		BlockBytes:    int64(binary.LittleEndian.Uint64(buf[5:13])),
		KVPrecision:   string(buf[15 : 15+precisionLen]),
		BytesPerToken: int64(binary.LittleEndian.Uint64(buf[47:55])),
		AlignmentPad:  int64(binary.LittleEndian.Uint64(buf[55:63])),
	}, int64(binary.LittleEndian.Uint64(buf[63:71])), nil
}

// ReconcileBitmap 以段头为准修正位图并重建映射（D43）：
// 位图占用而段头空或损坏的位直接释放，段头已写而位图未标占用的位补标；
// 状态停在写入中、CRC 未写全或段头解码失败的段按损坏处理，段号回位图。
func ReconcileBitmap(bm *Bitmap, heads []Header, bad []bool) map[string]int64 {
	index := make(map[string]int64)
	for seq := range heads {
		valid := seq < len(bad) && !bad[seq] && heads[seq].State == StateWritten
		occupied := bm.IsOccupied(int64(seq))
		switch {
		case occupied && !valid:
			bm.Release(int64(seq))
		case !occupied && valid:
			bm.Mark(int64(seq))
			index[heads[seq].Hash] = int64(seq)
		case valid:
			index[heads[seq].Hash] = int64(seq)
		}
	}
	return index
}

// reconcile 扫描全部段头，与从零建立的位图对账（无清单快照时位图以段头为准重建）。
func (s *Store) reconcile() {
	heads := make([]Header, s.totalSegments)
	bad := make([]bool, s.totalSegments)
	buf := make([]byte, HeaderSize)
	for seq := int64(0); seq < s.totalSegments; seq++ {
		if _, err := s.f.ReadAt(buf, s.offset(seq)); err != nil {
			bad[seq] = true
			continue
		}
		h, err := Decode(buf)
		if err != nil {
			bad[seq] = true
			continue
		}
		if h.State == StateWritten && h.BlockBytes != s.kvSpec.BlockBytes {
			bad[seq] = true
			continue
		}
		heads[seq] = h
	}
	s.index = ReconcileBitmap(s.bm, heads, bad)
}

// Put 写入一个满块，返回段序号与是否新写（D43）。
// 写入顺序硬约束：块字节进写缓冲、位图取段、写块字节、写段头（写入中）、算 CRC 置已写、
// fsync 数据文件、更新内存映射、发 register 事件；fsync 必须发生在事件发射之前。
// 同键覆写复用原段号整段重写，位图不变；无空闲段时按 ErrNoFreeSegment 拒绝（D46）。
// 写入完成后读水位，超过阈值触发驱逐，占用回落到水位以下（D42）。
func (s *Store) Put(blockHash string, data []byte) (int64, bool, error) {
	s.mu.Lock()
	seq, isNew, err := s.putLocked(blockHash, data)
	need := err == nil && s.watermarkExceededLocked()
	s.mu.Unlock()
	if need {
		s.evictToWatermark()
	}
	return seq, isNew, err
}

func (s *Store) putLocked(blockHash string, data []byte) (int64, bool, error) {
	if blockHash == "" {
		return 0, false, fmt.Errorf("%w: empty block hash", ErrBadPutLength)
	}
	if int64(len(data)) != s.kvSpec.BlockBytes {
		return 0, false, fmt.Errorf("%w: want %d bytes, got %d", ErrBadPutLength, s.kvSpec.BlockBytes, len(data))
	}

	var seq int64
	isNew := true
	if old, ok := s.index[blockHash]; ok {
		seq = old
		isNew = false
	} else {
		var ok bool
		seq, ok = s.bm.Allocate()
		if !ok {
			return 0, false, fmt.Errorf("%w: %d segments all occupied", ErrNoFreeSegment, s.totalSegments)
		}
	}

	off := s.offset(seq)
	// 块字节先进写缓冲再整段顺序写下（D43）。
	buf := make([]byte, len(data))
	copy(buf, data)

	hdr := Header{
		Hash:       blockHash,
		SegmentSeq: seq,
		BlockBytes: int64(len(buf)),
		State:      StateWriting,
	}
	hbuf := make([]byte, HeaderSize)

	rollback := func(reason error) (int64, bool, error) {
		if isNew {
			s.bm.Release(seq)
		}
		return 0, false, fmt.Errorf("segment put failed: %w", reason)
	}

	if _, err := s.f.WriteAt(buf, off+HeaderSize); err != nil {
		return rollback(err)
	}
	if err := hdr.Encode(hbuf); err != nil {
		return rollback(err)
	}
	if _, err := s.f.WriteAt(hbuf, off); err != nil {
		return rollback(err)
	}
	hdr.State = StateWritten
	hdr.DataCRC = block.CRC32C(buf)
	if err := hdr.Encode(hbuf); err != nil {
		return rollback(err)
	}
	if _, err := s.f.WriteAt(hbuf, off); err != nil {
		return rollback(err)
	}
	if err := s.f.Sync(); err != nil {
		return rollback(err)
	}
	if s.afterSync != nil {
		s.afterSync()
	}

	s.index[blockHash] = seq
	s.touchLocked(blockHash)

	if s.emit != nil {
		s.emit(Event{
			Kind:       "register",
			NodeID:     s.nodeID,
			BlockHash:  blockHash,
			SegmentSeq: seq,
			Layer:      "local",
			TsMs:       time.Now().UnixMilli(),
		})
	}
	return seq, isNew, nil
}

// Get 按块哈希读回块字节，返回数据与段序号（D43）。
// 比段头块字节与节点口径，再验 CRC32C；长度不符按丢弃处理并记日志，CRC 不符回 DataLoss，
// 段头哈希与请求键不符回 NotFound。读开始加一、读完减一，计数保护期内段号不被复用。
// 命中算一次访问，刷新驱逐候选排序（D42）。
func (s *Store) Get(blockHash string) ([]byte, int64, error) {
	s.mu.Lock()
	seq, ok := s.index[blockHash]
	if !ok {
		s.mu.Unlock()
		return nil, 0, fmt.Errorf("%w: %s", ErrNotFound, blockHash)
	}
	s.touchLocked(blockHash)
	cnt := s.inflight[seq]
	if cnt == nil {
		cnt = &atomic.Int32{}
		s.inflight[seq] = cnt
	}
	s.mu.Unlock()

	cnt.Add(1)
	defer cnt.Add(-1)

	s.mu.RLock()
	defer s.mu.RUnlock()

	off := s.offset(seq)
	hbuf := make([]byte, HeaderSize)
	if _, err := s.f.ReadAt(hbuf, off); err != nil {
		s.log.Error("segment header read failed", Fields(err, s.nodeID, blockHash, seq, "header_read_failed"))
		return nil, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	h, err := Decode(hbuf)
	if err != nil {
		s.log.Error("segment header decode failed", Fields(err, s.nodeID, blockHash, seq, "header_decode_failed"))
		return nil, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if h.State != StateWritten {
		err := fmt.Errorf("%w: state %s", ErrCorrupt, h.State)
		s.log.Error("segment state invalid", Fields(err, s.nodeID, blockHash, seq, "state_"+h.State.String()))
		return nil, 0, err
	}
	if h.Hash != blockHash {
		return nil, 0, fmt.Errorf("%w: want %s got %s", ErrHashMismatch, blockHash, h.Hash)
	}
	if h.BlockBytes != s.kvSpec.BlockBytes {
		err := fmt.Errorf("%w: want %d got %d", ErrBadBlockLength, s.kvSpec.BlockBytes, h.BlockBytes)
		s.log.Error("segment block size mismatch, drop", Fields(err, s.nodeID, blockHash, seq, "length_mismatch"))
		return nil, 0, err
	}
	data := make([]byte, h.BlockBytes)
	if _, err := s.f.ReadAt(data, off+HeaderSize); err != nil {
		s.log.Error("segment data read failed", Fields(err, s.nodeID, blockHash, seq, "data_read_failed"))
		return nil, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if !block.VerifyCRC32C(data, h.DataCRC) {
		err := fmt.Errorf("%w: segment %d crc mismatch", ErrCRCMismatch, seq)
		s.log.Error("segment crc verify failed", Fields(err, s.nodeID, blockHash, seq, "crc_mismatch"))
		return nil, 0, err
	}
	return data, seq, nil
}

// touchLocked 刷新块的最后访问逻辑序号，调用方须持写锁。
func (s *Store) touchLocked(blockHash string) {
	s.accessGen++
	s.access[blockHash] = s.accessGen
}

// watermarkExceededLocked 返回占用是否超过水位百分比（D42），调用方须持锁。
func (s *Store) watermarkExceededLocked() bool {
	if s.watermarkPct <= 0 || s.totalSegments <= 0 {
		return false
	}
	return int64(s.bm.UsedCount())*100 > int64(s.watermarkPct)*s.totalSegments
}

// watermarkPctLocked 返回当前占用百分比（D42），调用方须持锁。
func (s *Store) watermarkPctLocked() int {
	if s.totalSegments <= 0 {
		return 0
	}
	return int(int64(s.bm.UsedCount()) * 100 / s.totalSegments)
}

// pickVictimLocked 选最后访问最早且未 pin 的候选（D42、R34），调用方须持写锁。
// 全部候选带 pin 标记时返回空。
func (s *Store) pickVictimLocked() (string, int64, *atomic.Int32) {
	bestHash := ""
	bestSeq := int64(-1)
	bestGen := int64(math.MaxInt64)
	for h, seq := range s.index {
		if s.pins != nil && s.pins.Pinned(h) {
			continue
		}
		if g := s.access[h]; g < bestGen {
			bestHash, bestSeq, bestGen = h, seq, g
		}
	}
	if bestHash == "" {
		return "", 0, nil
	}
	return bestHash, bestSeq, s.inflight[bestSeq]
}

// evictToWatermark 逐段驱逐直到占用回落到水位以下或无可用候选（D42）。
func (s *Store) evictToWatermark() {
	for s.watermarkExceeded() {
		if !s.evictOldest(ReasonWatermark) {
			return
		}
	}
}

// watermarkExceeded 在锁外读水位是否超限（D42）。
func (s *Store) watermarkExceeded() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watermarkExceededLocked()
}

// evictOldest 驱逐一个最旧候选，一段的驱逐顺序固定（D43）：
// 先从内存映射摘键，等在途读计数归零，发 evict 事件，最后把段号写回位图；
// 事件先于回位图，索引在事件到达前不会命中到已被复用的段。
func (s *Store) evictOldest(reason string) bool {
	s.mu.Lock()
	hash, seq, cnt := s.pickVictimLocked()
	if hash == "" {
		s.mu.Unlock()
		return false
	}
	delete(s.index, hash)
	delete(s.access, hash)
	s.evicting[seq] = true
	s.mu.Unlock()

	s.waitInflight(cnt)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.emit != nil {
		s.emit(Event{
			Kind:       "evict",
			NodeID:     s.nodeID,
			BlockHash:  hash,
			SegmentSeq: seq,
			Layer:      "local",
			Reason:     reason,
			TsMs:       time.Now().UnixMilli(),
		})
	}
	s.log.Info("block evicted", telemetry.Fields{
		NodeID:       s.nodeID,
		BlockHash:    hash,
		SegmentSeq:   seq,
		Result:       "evicted",
		Reason:       reason,
		Layer:        "local",
		WatermarkPct: s.watermarkPctLocked(),
	})
	s.bm.Release(seq)
	if cnt != nil {
		delete(s.inflight, seq)
	}
	delete(s.evicting, seq)
	return true
}

// DropBlock 按指定原因驱逐单个块，顺序与水位驱逐一致（D54、R34）：
// 摘键、等在途读归零、发 evict 事件、段号回位图。映射无该键时返回 false。
// pin 配额回收路径经装配回调进入本方法，按 ReasonPinQuota 打回收日志。
func (s *Store) DropBlock(blockHash, reason string) (int64, bool) {
	s.mu.Lock()
	seq, ok := s.index[blockHash]
	if !ok {
		s.mu.Unlock()
		return 0, false
	}
	delete(s.index, blockHash)
	delete(s.access, blockHash)
	cnt := s.inflight[seq]
	s.evicting[seq] = true
	s.mu.Unlock()

	s.waitInflight(cnt)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.emit != nil {
		s.emit(Event{
			Kind:       "evict",
			NodeID:     s.nodeID,
			BlockHash:  blockHash,
			SegmentSeq: seq,
			Layer:      "local",
			Reason:     reason,
			TsMs:       time.Now().UnixMilli(),
		})
	}
	if reason == ReasonPinQuota {
		s.log.Info("pin reclaimed", telemetry.Fields{
			NodeID:     s.nodeID,
			BlockHash:  blockHash,
			SegmentSeq: seq,
			Result:     "pin_reclaimed",
			Reason:     reason,
			Layer:      "local",
		})
	}
	s.bm.Release(seq)
	if cnt != nil {
		delete(s.inflight, seq)
	}
	delete(s.evicting, seq)
	return seq, true
}

// waitInflight 在锁外等在途读计数归零，避免与读方互相持锁死锁（D43）。
func (s *Store) waitInflight(cnt *atomic.Int32) {
	for cnt != nil && cnt.Load() > 0 {
		time.Sleep(100 * time.Microsecond)
	}
}

// AuditScan 扫全部段头并与内存映射对照，只读不改状态（D54）。
// 返回验过块哈希与 CRC32C 的完好孤儿段，以及损坏段号列表；
// 段头解码失败、状态异常、块字节与口径不符、CRC 不符、同键冗余段（映射指向别的段）
// 一并按损坏返回，由调用方释放。
func (s *Store) AuditScan() ([]Orphan, []int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var orphans []Orphan
	var corrupt []int64
	hbuf := make([]byte, HeaderSize)
	zero := make([]byte, HeaderSize)
	for seq := int64(0); seq < s.totalSegments; seq++ {
		if _, err := s.f.ReadAt(hbuf, s.offset(seq)); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, nil, fmt.Errorf("audit read segment header %d: %w", seq, err)
		}
		// 全零段是从未写入的空段，不算损坏。
		if bytes.Equal(hbuf, zero) {
			continue
		}
		h, err := Decode(hbuf)
		if err != nil || h.State != StateWritten || h.Hash == "" || h.BlockBytes != s.kvSpec.BlockBytes {
			corrupt = append(corrupt, seq)
			continue
		}
		if cur, mapped := s.index[h.Hash]; mapped && cur == seq {
			continue
		}
		data := make([]byte, h.BlockBytes)
		if _, err := s.f.ReadAt(data, s.offset(seq)+HeaderSize); err != nil {
			corrupt = append(corrupt, seq)
			continue
		}
		if !block.VerifyCRC32C(data, h.DataCRC) {
			corrupt = append(corrupt, seq)
			continue
		}
		if _, mapped := s.index[h.Hash]; mapped {
			// 同键冗余段：权威登记指向别的段，本段释放回位图防容量泄漏。
			corrupt = append(corrupt, seq)
			continue
		}
		orphans = append(orphans, Orphan{Hash: h.Hash, SegmentSeq: seq})
	}
	return orphans, corrupt, nil
}

// Adopt 把对账补登的孤儿段写进内存映射并补位图标记（D54）。
// 段号越界、正被驱逐摘键、或已被其他键占用时拒绝，返回是否采纳；
// 位图漏标（盘上有段而位图未标）的孤儿段在本方法补标。
func (s *Store) Adopt(blockHash string, seq int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq < 0 || seq >= s.totalSegments || s.evicting[seq] {
		return false
	}
	if cur, ok := s.index[blockHash]; ok {
		return cur == seq
	}
	for _, sq := range s.index {
		if sq == seq {
			return false
		}
	}
	s.bm.Mark(seq)
	s.index[blockHash] = seq
	s.touchLocked(blockHash)
	return true
}

// ReleaseCorrupt 释放损坏段：摘除映射到该段的键并把段号写回位图（D54），
// 返回被摘除的块哈希，无键时返回空串。
func (s *Store) ReleaseCorrupt(seq int64) string {
	s.mu.Lock()
	var removed string
	for h, sq := range s.index {
		if sq == seq {
			removed = h
			break
		}
	}
	if removed != "" {
		delete(s.index, removed)
		delete(s.access, removed)
	}
	s.bm.Release(seq)
	s.mu.Unlock()
	return removed
}

// RegisteredHashes 返回内存映射全部块哈希快照，抽读候选与清单如实上报共用（D54）。
func (s *Store) RegisteredHashes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.index))
	for h := range s.index {
		out = append(out, h)
	}
	return out
}

func Fields(err error, nodeID, blockHash string, seq int64, result string) telemetry.Fields {
	f := telemetry.Fields{
		NodeID:     nodeID,
		BlockHash:  blockHash,
		SegmentSeq: seq,
		Result:     result,
	}
	if err != nil {
		f.Reason = err.Error()
	}
	return f
}
