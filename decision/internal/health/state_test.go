package health

import (
	"strings"
	"testing"
	"time"

	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
)

// fakeClock 是可推进的注入时钟（D51）：测试推时钟验证四态迁移落在阈值边界上。
type fakeClock struct {
	cur time.Time
}

func (c *fakeClock) Now() time.Time { return c.cur }

func (c *fakeClock) Advance(d time.Duration) { c.cur = c.cur.Add(d) }

type capturedLog struct {
	level string
	msg   string
	line  string
}

func newTracker(downAfter, hysteresis time.Duration) (*fakeClock, *Tracker, *[]capturedLog) {
	clock := &fakeClock{cur: time.Unix(0, 0)}
	logs := &[]capturedLog{}
	tracker := NewTrackerWithClock(downAfter, hysteresis, clock.Now, logCapture(logs))
	return clock, tracker, logs
}

func logCapture(logs *[]capturedLog) *telemetry.Logger {
	return telemetry.NewWithHook(func(level, msg string, f telemetry.Fields, line []byte) {
		*logs = append(*logs, capturedLog{level: level, msg: msg, line: string(line)})
	})
}

// TestDisconnectEntersSuspect 覆盖连接断开进疑似并摘出调度池（D51）。
func TestDisconnectEntersSuspect(t *testing.T) {
	clock, tracker, _ := newTracker(30*time.Second, 30*time.Second)
	_ = clock
	if tracker.Skip("node-1") {
		t.Fatal("node must not be skipped while normal")
	}
	tracker.Disconnected("node-1")
	if got := tracker.State("node-1"); got != StateSuspect {
		t.Fatalf("state after disconnect = %s, want suspect", got)
	}
	if !tracker.Skip("node-1") {
		t.Fatal("suspect node must be skipped from scheduling")
	}
}

// TestDeadAtDownAfterBoundary 覆盖疑似持续 down_after_s 判死，迁移落在阈值边界（D51）。
func TestDeadAtDownAfterBoundary(t *testing.T) {
	clock, tracker, logs := newTracker(30*time.Second, 30*time.Second)
	tracker.Disconnected("node-1")
	clock.Advance(29 * time.Second)
	if got := tracker.State("node-1"); got != StateSuspect {
		t.Fatalf("state at down_after-1s = %s, want suspect", got)
	}
	clock.Advance(time.Second)
	if got := tracker.State("node-1"); got != StateDead {
		t.Fatalf("state at down_after = %s, want dead", got)
	}
	if !tracker.Skip("node-1") {
		t.Fatal("dead node must be skipped")
	}
	found := false
	for _, l := range *logs {
		if l.msg == "node dead" {
			found = true
			if !strings.Contains(l.line, `"duration_ms":30000`) {
				t.Errorf("dead log duration = %s, want 30000", l.line)
			}
			if !strings.Contains(l.line, `"state_from":"suspect"`) || !strings.Contains(l.line, `"state_to":"dead"`) {
				t.Errorf("dead log states = %s", l.line)
			}
		}
	}
	if !found {
		t.Fatal("dead transition log missing")
	}
}

// TestRecoverAtHysteresisBoundary 覆盖重连后滞回一个周期恢复正常，迁移落在阈值边界（D51）。
func TestRecoverAtHysteresisBoundary(t *testing.T) {
	clock, tracker, logs := newTracker(30*time.Second, 30*time.Second)
	tracker.Disconnected("node-1")
	tracker.Reconnected("node-1")
	if got := tracker.State("node-1"); got != StateHysteresis {
		t.Fatalf("state after reconnect = %s, want hysteresis", got)
	}
	if !tracker.Skip("node-1") {
		t.Fatal("hysteresis node must stay out of scheduling until the period ends")
	}
	clock.Advance(29 * time.Second)
	if got := tracker.State("node-1"); got != StateHysteresis {
		t.Fatalf("state at hysteresis-1s = %s, want hysteresis", got)
	}
	clock.Advance(time.Second)
	if got := tracker.State("node-1"); got != StateNormal {
		t.Fatalf("state at hysteresis = %s, want normal", got)
	}
	if tracker.Skip("node-1") {
		t.Fatal("recovered node must rejoin scheduling")
	}
	found := false
	for _, l := range *logs {
		if l.msg == "node recovered" {
			found = true
			if !strings.Contains(l.line, `"duration_ms":30000`) {
				t.Errorf("recovered log duration = %s, want 30000", l.line)
			}
			if !strings.Contains(l.line, `"state_from":"hysteresis"`) || !strings.Contains(l.line, `"state_to":"normal"`) {
				t.Errorf("recovered log states = %s", l.line)
			}
		}
	}
	if !found {
		t.Fatal("recovered transition log missing")
	}
}

// TestDeadReconnectRecovers 覆盖判死后重连复活的完整链路（D51）。
func TestDeadReconnectRecovers(t *testing.T) {
	clock, tracker, _ := newTracker(10*time.Second, 10*time.Second)
	tracker.Disconnected("node-1")
	clock.Advance(10 * time.Second)
	if got := tracker.State("node-1"); got != StateDead {
		t.Fatalf("state = %s, want dead", got)
	}
	tracker.Reconnected("node-1")
	if got := tracker.State("node-1"); got != StateHysteresis {
		t.Fatalf("state after reconnect = %s, want hysteresis", got)
	}
	clock.Advance(10 * time.Second)
	if got := tracker.State("node-1"); got != StateNormal {
		t.Fatalf("state after hysteresis = %s, want normal", got)
	}
}

// TestHysteresisDisconnectBackToSuspect 覆盖滞回期内断开回疑似（D51）。
func TestHysteresisDisconnectBackToSuspect(t *testing.T) {
	clock, tracker, _ := newTracker(30*time.Second, 30*time.Second)
	tracker.Disconnected("node-1")
	tracker.Reconnected("node-1")
	clock.Advance(5 * time.Second)
	tracker.Disconnected("node-1")
	if got := tracker.State("node-1"); got != StateSuspect {
		t.Fatalf("state after hysteresis disconnect = %s, want suspect", got)
	}
}

// TestFourTransitionLogs 覆盖连接建立、断线、判死、复活四处日志钩子（D51）：
// 每处各捕获一条，字段含 node_id、状态前后值与持续时长。
func TestFourTransitionLogs(t *testing.T) {
	clock, tracker, logs := newTracker(5*time.Second, 5*time.Second)
	tracker.Reconnected("node-1") // 连接建立
	tracker.Disconnected("node-1")
	clock.Advance(5 * time.Second)
	tracker.State("node-1") // 判死
	tracker.Reconnected("node-1")
	clock.Advance(5 * time.Second)
	tracker.State("node-1") // 复活

	want := map[string]bool{"node connected": false, "node disconnected": false, "node dead": false, "node recovered": false}
	counts := map[string]int{}
	for _, l := range *logs {
		if _, ok := want[l.msg]; ok {
			counts[l.msg]++
			if !strings.Contains(l.line, `"node_id":"node-1"`) {
				t.Errorf("log %s missing node_id: %s", l.msg, l.line)
			}
			if !strings.Contains(l.line, `"state_from"`) || !strings.Contains(l.line, `"state_to"`) {
				t.Errorf("log %s missing state transition fields: %s", l.msg, l.line)
			}
			if !strings.Contains(l.line, `"duration_ms"`) {
				t.Errorf("log %s missing duration: %s", l.msg, l.line)
			}
		}
	}
	for msg, seen := range want {
		if !seen && counts[msg] == 0 {
			t.Errorf("transition log %q missing", msg)
		}
	}
	if counts["node dead"] != 1 {
		t.Errorf("dead logs = %d, want 1", counts["node dead"])
	}
	if counts["node recovered"] != 1 {
		t.Errorf("recovered logs = %d, want 1", counts["node recovered"])
	}
	if counts["node disconnected"] != 1 {
		t.Errorf("disconnected logs = %d, want 1", counts["node disconnected"])
	}
}
