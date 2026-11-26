package audit

import (
	"context"
	"math/rand"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// minSampleBlocks 是每轮抽读样本数下限，登记数达到下限前抽满下限（D54）。
const minSampleBlocks = 100

// Clock 是可注入时钟：限速等待按它推进，测试注入假时钟（D54）。
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

type systemClock struct{}

func (systemClock) Now() time.Time        { return time.Now() }
func (systemClock) Sleep(d time.Duration) { time.Sleep(d) }

// Options 是对账器构造选项（D54）。
type Options struct {
	NodeID string
	// Store 是段层实例，元数据对照与数据读回都经它（D54）。
	Store *segment.Store
	// Emit 是事件发射回调：对账补登发 register，抽读不符注销发 evict（D54）。
	Emit segment.EmitFunc
	Log  *telemetry.Logger
	// IntervalS 是对账周期秒，取 0 时不做周期调度（D54）。
	IntervalS int
	// SamplePct 是每轮抽样的登记块百分比（D54）。
	SamplePct int
	// MaxBytesPerS 是抽读限速的字节每秒预算，取 0 不限速（D54）。
	MaxBytesPerS int64
	// Clock 为限速提供时钟，nil 用系统时钟（D54）。
	Clock Clock
	// RandSeed 固定抽样随机源，取 0 用时间种子（D54）。
	RandSeed int64
}

// Result 是一轮对账的统计（D54）。
type Result struct {
	OrphansRegistered int
	CorruptReleased   int
	Sampled           int
	Deregistered      int
}

// Auditor 做元数据全量对账与数据抽读（D54）。
// 「索引有登记而盘上无段」的注销由决策侧心跳对账负责，本节点只按盘上内容
// 补登孤儿段、释放损坏段并如实上报清单。
type Auditor struct {
	opts  Options
	clock Clock
	rng   *rand.Rand
	lim   *limiter
}

// New 建立对账器（D54）。
func New(opts Options) *Auditor {
	if opts.Log == nil {
		opts.Log = telemetry.New()
	}
	clk := opts.Clock
	if clk == nil {
		clk = systemClock{}
	}
	seed := opts.RandSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Auditor{
		opts:  opts,
		clock: clk,
		rng:   rand.New(rand.NewSource(seed)),
		lim:   newLimiter(clk, opts.MaxBytesPerS),
	}
}

// RunOnce 跑一轮对账：先元数据全量对账，再数据抽读（D54）。
func (a *Auditor) RunOnce() (Result, error) {
	var res Result
	orphans, corrupt, err := a.opts.Store.AuditScan()
	if err != nil {
		return res, err
	}
	for _, seq := range corrupt {
		removed := a.opts.Store.ReleaseCorrupt(seq)
		a.opts.Log.Warn("audit corrupt segment released", telemetry.Fields{
			NodeID:     a.opts.NodeID,
			BlockHash:  removed,
			SegmentSeq: seq,
			Result:     "corrupt_released",
			Reason:     "audit_corrupt",
			Layer:      "local",
		})
		res.CorruptReleased++
	}
	for _, o := range orphans {
		if !a.opts.Store.Adopt(o.Hash, o.SegmentSeq) {
			continue
		}
		res.OrphansRegistered++
		if a.opts.Emit != nil {
			a.opts.Emit(segment.Event{
				Kind:       "register",
				NodeID:     a.opts.NodeID,
				BlockHash:  o.Hash,
				SegmentSeq: o.SegmentSeq,
				Layer:      "local",
				Reason:     "audit_orphan",
				TsMs:       time.Now().UnixMilli(),
			})
		}
		a.opts.Log.Info("audit orphan registered", telemetry.Fields{
			NodeID:     a.opts.NodeID,
			BlockHash:  o.Hash,
			SegmentSeq: o.SegmentSeq,
			Result:     "orphan_registered",
			Reason:     "audit_orphan",
			Layer:      "local",
		})
	}
	res.Sampled, res.Deregistered = a.sample()
	return res, nil
}

// sample 随机抽登记块读回重算校验，不符注销该块并记事件（D54）。
// 读回复用段层 Get 的长度、哈希与 CRC32C 校验；校验不符即数据不符，注销由
// DropBlock 走完整驱逐顺序，索引经 evict 事件注销登记。
func (a *Auditor) sample() (sampled, deregistered int) {
	hashes := a.opts.Store.RegisteredHashes()
	n := sampleCount(len(hashes), a.opts.SamplePct)
	if n == 0 {
		return 0, 0
	}
	a.rng.Shuffle(len(hashes), func(i, j int) { hashes[i], hashes[j] = hashes[j], hashes[i] })
	for _, h := range hashes[:n] {
		data, _, err := a.opts.Store.Get(h)
		sampled++
		if err != nil {
			seq, ok := a.opts.Store.DropBlock(h, segment.ReasonAuditHash)
			if ok {
				deregistered++
				if a.opts.Emit != nil {
					a.opts.Emit(segment.Event{
						Kind:       "evict",
						NodeID:     a.opts.NodeID,
						BlockHash:  h,
						SegmentSeq: seq,
						Layer:      "local",
						Reason:     segment.ReasonAuditHash,
						TsMs:       time.Now().UnixMilli(),
					})
				}
				a.opts.Log.Warn("audit block deregistered", telemetry.Fields{
					NodeID:     a.opts.NodeID,
					BlockHash:  h,
					SegmentSeq: seq,
					Result:     "deregistered",
					Reason:     segment.ReasonAuditHash,
					Layer:      "local",
				})
			}
			continue
		}
		if a.lim != nil {
			a.lim.take(int64(len(data)))
		}
	}
	return sampled, deregistered
}

// sampleCount 计算本轮抽样数：登记数乘百分比，不足下限时取下限，封顶登记数（D54）。
func sampleCount(total, pct int) int {
	if total <= 0 {
		return 0
	}
	n := total * pct / 100
	if n < minSampleBlocks {
		n = minSampleBlocks
	}
	if n > total {
		n = total
	}
	return n
}

// Run 按 audit_interval_s 周期跑对账，ctx 取消即退出（D54）。
func (a *Auditor) Run(ctx context.Context) {
	if a.opts.IntervalS <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(a.opts.IntervalS) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := a.RunOnce(); err != nil {
				a.opts.Log.Warn("audit round failed", telemetry.Fields{
					NodeID: a.opts.NodeID,
					Result: "audit_failed",
					Reason: err.Error(),
				})
			}
		}
	}
}

// limiter 按秒窗口做字节预算限速，窗口边界由注入时钟推进（D54）。
type limiter struct {
	clock     Clock
	perWindow int64
	windowEnd time.Time
	used      int64
}

func newLimiter(clock Clock, bytesPerSecond int64) *limiter {
	if bytesPerSecond <= 0 {
		return nil
	}
	return &limiter{clock: clock, perWindow: bytesPerSecond, windowEnd: clock.Now().Add(time.Second)}
}

// take 记账 n 字节；当前窗口预算不足时推进到下一窗口再记账（D54）。
func (l *limiter) take(n int64) {
	for {
		now := l.clock.Now()
		if !now.Before(l.windowEnd) {
			l.used = 0
			l.windowEnd = now.Add(time.Second)
		}
		if l.used+n <= l.perWindow {
			l.used += n
			return
		}
		l.clock.Sleep(l.windowEnd.Sub(now))
	}
}
