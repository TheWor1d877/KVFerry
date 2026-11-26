package health

import (
	"sync"
	"time"

	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
)

// State 是节点失联四态（D51）：正常、疑似、判死、滞回。
type State int

const (
	// StateNormal 正常：节点在调度池内参与命中。
	StateNormal State = iota
	// StateSuspect 疑似：连接断开即进入，摘出调度池，数据保留。
	StateSuspect
	// StateDead 判死：疑似持续 down_after_s 未重连，决策绕行该节点。
	StateDead
	// StateHysteresis 滞回：重连后滞回一个心跳周期，期满回正常。
	StateHysteresis
)

// String 返回状态名，用于日志的状态前后值字段（D51）。
func (s State) String() string {
	switch s {
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	case StateHysteresis:
		return "hysteresis"
	default:
		return "normal"
	}
}

type nodeHealth struct {
	state     State
	since     time.Time // 当前状态起点
	downSince time.Time // 最近一次断开时刻，判死时长由此起算
}

// Tracker 是失联状态机（D51）：判定与调度摘挂由决策服务执行，连接状态的来源是
// StorageReporter 服务端对流断开与重建的观测。时钟经 now 注入，测试推时钟验证阈值边界。
type Tracker struct {
	mu         sync.Mutex
	downAfter  time.Duration
	hysteresis time.Duration
	now        func() time.Time
	nodes      map[string]*nodeHealth
	log        *telemetry.Logger
}

// NewTracker 建立状态机，用真实时钟（D51）。
func NewTracker(downAfter, hysteresis time.Duration, logs ...*telemetry.Logger) *Tracker {
	return NewTrackerWithClock(downAfter, hysteresis, time.Now, logs...)
}

// NewTrackerWithClock 建立带注入时钟的状态机（D51）。
func NewTrackerWithClock(downAfter, hysteresis time.Duration, now func() time.Time, logs ...*telemetry.Logger) *Tracker {
	log := telemetry.New()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	if now == nil {
		now = time.Now
	}
	return &Tracker{
		downAfter:  downAfter,
		hysteresis: hysteresis,
		now:        now,
		nodes:      make(map[string]*nodeHealth),
		log:        log,
	}
}

// Reconnected 记录节点连接建立（D51）：疑似与判死进滞回，正常与滞回保持；
// 连接建立打 info，字段含 node_id、状态前后值与持续时长。
func (t *Tracker) Reconnected(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	nh := t.nodes[node]
	if nh == nil {
		nh = &nodeHealth{state: StateNormal, since: now}
		t.nodes[node] = nh
	} else if nh.state == StateSuspect || nh.state == StateDead {
		prev := nh.state
		nh.state = StateHysteresis
		nh.since = now
		t.log.Info("node connected", telemetry.Fields{
			NodeID:     node,
			FromState:  prev.String(),
			ToState:    nh.state.String(),
			DurationMs: now.Sub(nh.downSince).Milliseconds(),
			Result:     "connected",
		})
		return
	}
	t.log.Info("node connected", telemetry.Fields{
		NodeID:     node,
		FromState:  nh.state.String(),
		ToState:    nh.state.String(),
		DurationMs: 0,
		Result:     "connected",
	})
}

// Disconnected 记录节点连接断开（D51）：正常与滞回进疑似并记断开时刻，
// 疑似与判死保持；断线打 warn，字段含 node_id、状态前后值与持续时长。
func (t *Tracker) Disconnected(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	nh := t.nodes[node]
	if nh == nil {
		nh = &nodeHealth{state: StateNormal, since: now}
		t.nodes[node] = nh
	}
	if nh.state == StateSuspect || nh.state == StateDead {
		return
	}
	prev := nh.state
	duration := now.Sub(nh.since)
	nh.state = StateSuspect
	nh.downSince = now
	nh.since = now
	t.log.Warn("node disconnected", telemetry.Fields{
		NodeID:     node,
		FromState:  prev.String(),
		ToState:    nh.state.String(),
		DurationMs: duration.Milliseconds(),
		Result:     "disconnected",
	})
}

// State 返回惰性评估后的节点状态（D51）：疑似超 down_after_s 迁判死，滞回超周期迁正常。
func (t *Tracker) State(node string) State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.evaluate(node).state
}

// Skip 实现索引的失联跳过（D51）：疑似与判死摘出调度池，滞回期内块仍在池外，
// 滞回期满回正常后块重新参与命中；登记全程保留。
func (t *Tracker) Skip(node string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.evaluate(node).state != StateNormal
}

// evaluate 在锁内做惰性迁移与打点（D51）：判死打 warn 记疑似持续时长，
// 复活打 info 记滞回持续时长。
func (t *Tracker) evaluate(node string) *nodeHealth {
	nh := t.nodes[node]
	if nh == nil {
		nh = &nodeHealth{state: StateNormal, since: t.now()}
		t.nodes[node] = nh
		return nh
	}
	now := t.now()
	switch nh.state {
	case StateSuspect:
		if now.Sub(nh.downSince) >= t.downAfter {
			duration := now.Sub(nh.downSince)
			prev := nh.state
			nh.state = StateDead
			nh.since = now
			t.log.Warn("node dead", telemetry.Fields{
				NodeID:     node,
				FromState:  prev.String(),
				ToState:    nh.state.String(),
				DurationMs: duration.Milliseconds(),
				Result:     "dead",
			})
		}
	case StateHysteresis:
		if now.Sub(nh.since) >= t.hysteresis {
			duration := now.Sub(nh.since)
			prev := nh.state
			nh.state = StateNormal
			nh.since = now
			t.log.Info("node recovered", telemetry.Fields{
				NodeID:     node,
				FromState:  prev.String(),
				ToState:    nh.state.String(),
				DurationMs: duration.Milliseconds(),
				Result:     "recovered",
			})
		}
	}
	return nh
}
