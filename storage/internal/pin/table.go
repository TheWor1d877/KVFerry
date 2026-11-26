package pin

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/block"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// ErrQuotaExceeded 表示单块字节超过 pin 配额，配额内无法容纳（D46）。
var ErrQuotaExceeded = errors.New("pin quota exceeds pin_quota_bytes")

// ErrHashTooLong 表示块哈希超过清单定长槽（R34）。
var ErrHashTooLong = errors.New("block hash exceeds pin manifest slot")

// Clock 提供 TTL 判定与清单恢复的时刻（R34），测试注入假时钟。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

const (
	manifestFile            = "pin.manifest"
	manifestTempFile        = "pin.manifest.tmp"
	hashSlotSize            = 64
	entrySize               = 2 + hashSlotSize + 8 // 长度前缀 2 + 哈希槽 64 + 到期时刻 8
	manifestFixed           = 13                   // magic 4 + version 1 + count 8
	manifestMagic    uint32 = 0x50494E4D
	manifestVersion  uint8  = 1
)

// Options 是 pin 表构造选项（D42、R34）。
type Options struct {
	// DataDir 是数据目录，pin 清单单独落此目录，与段数据分文件（R34）。
	DataDir string
	// BlockBytes 是单块字节，配额核算按节点口径（D42）。
	BlockBytes int64
	// QuotaBytes 是 pin 总量配额，取 0 不限（D42）。
	QuotaBytes int64
	// DefaultTTL 是请求未带 pin_ttl_s 时的默认 TTL，取 0 表示 pin 不限期（D42）。
	DefaultTTL time.Duration
	// Clock 为 TTL 判定提供时刻，nil 用系统时钟（R34）。
	Clock Clock
	// Log 为结构化日志器，nil 时用默认 stdout 日志器。
	Log *telemetry.Logger
	// OnReclaim 在配额回收解除一个 pin 标记并落盘后调用，
	// 装配方接段层驱逐释放对应段（R34）；回调不得回调回本表。
	OnReclaim func(blockHash string)
}

type tableEntry struct {
	hash      string
	expiresAt time.Time
	// gen 是 LRU 逻辑序号，Add 与刷新递增，回收取最小值（R34）。
	gen uint64
}

// Table 是 pin 标记表：块哈希到 TTL 到期时刻（R34）。
// 配额非零且总量超限时按 LRU 回收最旧标记；带 TTL 的标记到期后由 Sweep 释放，
// 块回到普通候选。每次增删把全量清单落数据目录并 fsync，重启先于段头扫描 Load 恢复。
type Table struct {
	mu        sync.Mutex
	opts      Options
	clock     Clock
	log       *telemetry.Logger
	entries   map[string]*tableEntry
	gen       uint64
	usedBytes int64
}

// New 建立 pin 表，标记集为空；存量标记经 Load 恢复（R34）。
func New(opts Options) *Table {
	if opts.Clock == nil {
		opts.Clock = systemClock{}
	}
	if opts.Log == nil {
		opts.Log = telemetry.New()
	}
	return &Table{
		opts:    opts,
		clock:   opts.Clock,
		log:     opts.Log,
		entries: make(map[string]*tableEntry),
	}
}

// Add 写入一个 pin 标记并落盘（R34）。
// ttl 取零或负数用默认 TTL，默认 TTL 也为零表示不限期；已存在的键刷新 TTL 与 LRU 位次。
// 配额非零且加入后总量超限时，先按 LRU 解除最旧标记并逐个回调 OnReclaim；
// 单块字节本身超过配额时按 ErrQuotaExceeded 拒绝，标记集不变（D46）。
func (t *Table) Add(blockHash string, ttl time.Duration) error {
	t.mu.Lock()
	if t.opts.QuotaBytes > 0 && t.opts.BlockBytes > t.opts.QuotaBytes {
		t.mu.Unlock()
		return fmt.Errorf("%w: block %d bytes, quota %d", ErrQuotaExceeded, t.opts.BlockBytes, t.opts.QuotaBytes)
	}
	expiresAt := t.expiryLocked(ttl)
	var reclaimed []string
	for t.opts.QuotaBytes > 0 && t.usedBytes+t.opts.BlockBytes > t.opts.QuotaBytes {
		if _, ok := t.entries[blockHash]; ok {
			break
		}
		h := t.reclaimOldestLocked()
		if h == "" {
			break
		}
		reclaimed = append(reclaimed, h)
	}
	t.putLocked(blockHash, expiresAt)
	err := t.persistLocked()
	t.mu.Unlock()
	if err != nil {
		return err
	}
	for _, h := range reclaimed {
		if t.opts.OnReclaim != nil {
			t.opts.OnReclaim(h)
		}
	}
	return nil
}

// Remove 解除一个 pin 标记并落盘；键不存在时为无害空操作（R34）。
func (t *Table) Remove(blockHash string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.entries[blockHash]; !ok {
		return nil
	}
	t.removeLocked(blockHash)
	return t.persistLocked()
}

// Pinned 返回标记状态：在册且未过 TTL（R34）。
// 到期条目的剔除统一由 Sweep 完成，本方法只读，供驱逐候选生成时高频查询。
func (t *Table) Pinned(blockHash string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[blockHash]
	if !ok {
		return false
	}
	return !t.expiredLocked(e)
}

// Sweep 释放全部 TTL 到期的标记并落盘，返回释放的块哈希；标记回普通候选（R34）。
// 被释放块的段是否驱逐由装配方决定，本表不动段层。
func (t *Table) Sweep() []string {
	t.mu.Lock()
	var released []string
	for h, e := range t.entries {
		if t.expiredLocked(e) {
			t.removeLocked(h)
			released = append(released, h)
		}
	}
	if len(released) > 0 {
		if err := t.persistLocked(); err != nil {
			t.log.Warn("pin manifest persist failed", telemetry.Fields{Reason: err.Error(), Result: "pin_persist_failed"})
		}
	}
	t.mu.Unlock()
	return released
}

// Hashes 返回在册标记的块哈希快照，含尚未 Sweep 的到期条目（R34）。
func (t *Table) Hashes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.entries))
	for h := range t.entries {
		out = append(out, h)
	}
	return out
}

// UsedBytes 返回当前 pin 标记占用的总字节（R34）。
func (t *Table) UsedBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.usedBytes
}

// Load 读 pin 清单恢复标记，须先于段头扫描重建调用（R34、D49）。
// 过期条目不恢复；清单文件缺失按空标记集处理，损坏按错误返回。
func (t *Table) Load() error {
	buf, err := os.ReadFile(filepath.Join(t.opts.DataDir, manifestFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read pin manifest: %w", err)
	}
	list, err := decodeManifest(buf)
	if err != nil {
		return err
	}
	now := t.clock.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = make(map[string]*tableEntry, len(list))
	t.usedBytes = 0
	t.gen = 0
	for _, en := range list {
		if !en.ExpiresAt.IsZero() && now.After(en.ExpiresAt) {
			continue
		}
		t.gen++
		t.entries[en.BlockHash] = &tableEntry{hash: en.BlockHash, expiresAt: en.ExpiresAt, gen: t.gen}
		t.usedBytes += t.opts.BlockBytes
	}
	return nil
}

func (t *Table) expiryLocked(ttl time.Duration) time.Time {
	if ttl <= 0 {
		ttl = t.opts.DefaultTTL
	}
	if ttl <= 0 {
		return time.Time{}
	}
	return t.clock.Now().Add(ttl)
}

func (t *Table) expiredLocked(e *tableEntry) bool {
	return !e.expiresAt.IsZero() && t.clock.Now().After(e.expiresAt)
}

func (t *Table) putLocked(blockHash string, expiresAt time.Time) {
	if e, ok := t.entries[blockHash]; ok {
		e.expiresAt = expiresAt
		t.gen++
		e.gen = t.gen
		return
	}
	t.gen++
	t.entries[blockHash] = &tableEntry{hash: blockHash, expiresAt: expiresAt, gen: t.gen}
	t.usedBytes += t.opts.BlockBytes
}

func (t *Table) removeLocked(blockHash string) {
	delete(t.entries, blockHash)
	t.usedBytes -= t.opts.BlockBytes
}

// reclaimOldestLocked 按 LRU 解除最旧标记并返回其哈希，标记集为空时返回空串（R34）。
func (t *Table) reclaimOldestLocked() string {
	oldest := ""
	var bestGen uint64 = math.MaxUint64
	for h, e := range t.entries {
		if e.gen < bestGen {
			oldest, bestGen = h, e.gen
		}
	}
	if oldest == "" {
		return ""
	}
	t.removeLocked(oldest)
	return oldest
}

// persistLocked 把全量清单按 LRU 旧到新编码落盘并 fsync（R34）。
// 恢复按文件顺序重建访问位次，顺序颠倒会让最旧标记在重启后被优先回收；
// 先写临时文件再原子改名，读方不会遇到写半截的清单。
func (t *Table) persistLocked() error {
	type kv struct {
		h string
		e *tableEntry
	}
	all := make([]kv, 0, len(t.entries))
	for h, e := range t.entries {
		all = append(all, kv{h: h, e: e})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].e.gen < all[j].e.gen })

	buf := make([]byte, manifestFixed+entrySize*len(all)+4)
	binary.LittleEndian.PutUint32(buf[0:4], manifestMagic)
	buf[4] = manifestVersion
	binary.LittleEndian.PutUint64(buf[5:13], uint64(len(all)))
	for i, it := range all {
		off := manifestFixed + entrySize*i
		if len(it.h) > hashSlotSize {
			return fmt.Errorf("%w: %d bytes, slot %d", ErrHashTooLong, len(it.h), hashSlotSize)
		}
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(it.h)))
		copy(buf[off+2:off+2+hashSlotSize], it.h)
		var nano int64
		if !it.e.expiresAt.IsZero() {
			nano = it.e.expiresAt.UnixNano()
		}
		binary.LittleEndian.PutUint64(buf[off+2+hashSlotSize:off+entrySize], uint64(nano))
	}
	body := len(buf) - 4
	binary.LittleEndian.PutUint32(buf[body:], block.CRC32C(buf[:body]))

	tmp := filepath.Join(t.opts.DataDir, manifestTempFile)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open pin manifest temp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write pin manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync pin manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close pin manifest: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(t.opts.DataDir, manifestFile)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename pin manifest: %w", err)
	}
	return nil
}

type manifestEntry struct {
	BlockHash string
	ExpiresAt time.Time
}

func decodeManifest(buf []byte) ([]manifestEntry, error) {
	if len(buf) < manifestFixed+4 {
		return nil, fmt.Errorf("pin manifest invalid: buffer %d bytes too short", len(buf))
	}
	body := len(buf) - 4
	if binary.LittleEndian.Uint32(buf[0:4]) != manifestMagic {
		return nil, errors.New("pin manifest invalid: magic mismatch")
	}
	if buf[4] != manifestVersion {
		return nil, errors.New("pin manifest invalid: version mismatch")
	}
	if !block.VerifyCRC32C(buf[:body], binary.LittleEndian.Uint32(buf[body:])) {
		return nil, errors.New("pin manifest invalid: crc mismatch")
	}
	count := int64(binary.LittleEndian.Uint64(buf[5:13]))
	if manifestFixed+entrySize*count+4 != int64(len(buf)) {
		return nil, fmt.Errorf("pin manifest invalid: entry count %d mismatches buffer %d bytes", count, len(buf))
	}
	list := make([]manifestEntry, 0, count)
	for i := int64(0); i < count; i++ {
		off := manifestFixed + entrySize*int(i)
		hashLen := int(binary.LittleEndian.Uint16(buf[off : off+2]))
		if hashLen > hashSlotSize {
			return nil, fmt.Errorf("%w: manifest entry %d hash length %d", ErrHashTooLong, i, hashLen)
		}
		en := manifestEntry{BlockHash: string(buf[off+2 : off+2+hashLen])}
		nano := int64(binary.LittleEndian.Uint64(buf[off+2+hashSlotSize : off+entrySize]))
		if nano != 0 {
			en.ExpiresAt = time.Unix(0, nano)
		}
		list = append(list, en)
	}
	return list, nil
}
