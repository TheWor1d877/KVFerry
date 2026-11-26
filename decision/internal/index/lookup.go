package index

import (
	"time"

	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
)

// NodeState 查询节点是否跳过（D51）：疑似与判死的节点跳过命中查询，登记保留。
// 状态机本身属失联侧，本包只定义查询接口，由外部注入。
type NodeState interface {
	Skip(node string) bool
}

// Querier 是命中查询（D53、R32）：进程内调用，不走网络。
type Querier struct {
	ix     *Index
	form   *Form
	window func() bool
	state  NodeState
	log    *telemetry.Logger
}

// NewQuerier 建立命中查询（D53）：window 报告重建窗口是否开着，state 为 nil 时全部节点可用。
// 日志器由构造方注入，未注入时用默认 stdout 日志器。
func NewQuerier(ix *Index, form *Form, window func() bool, state NodeState, logs ...*telemetry.Logger) *Querier {
	log := telemetry.New()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	return &Querier{ix: ix, form: form, window: window, state: state, log: log}
}

// Hit 按块哈希查询登记（D53、R32）：重建窗口内一律未命中并打点记 result 与耗时，
// 跨口径块按未命中，失联节点跳过；命中返回层位、节点、偏移与长度。
func (q *Querier) Hit(hash string, spec Spec) (Record, bool) {
	start := time.Now()
	if q.window != nil && q.window() {
		q.log.Info("lookup missed in rebuild window", telemetry.Fields{
			TraceID:    telemetry.NewTraceID(hash, time.Now().String()),
			NodeID:     "decision",
			DurationMs: time.Since(start).Milliseconds(),
			Result:     "window_miss",
		})
		return Record{}, false
	}
	if !q.form.SameSpec(spec) {
		return Record{}, false
	}
	for _, node := range q.ix.Nodes() {
		if q.state != nil && q.state.Skip(node) {
			continue
		}
		if r, ok := q.ix.Lookup(node, hash); ok {
			return r, true
		}
	}
	return Record{}, false
}
