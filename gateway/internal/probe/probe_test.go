package probe

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TheWor1d877/KVFerry/internal/config"
)

// fakeUpstream 是可切换状态码、响应延迟与按命中序编排状态的模拟副本。
type fakeUpstream struct {
	status atomic.Int32
	delay  atomic.Int64
	hits   atomic.Int64

	script []int

	mu        sync.Mutex
	lastPath  string
	lastVerb  string
	gates     map[int64]bool
	gateHits  chan int64
	releaseCh chan struct{}
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.hits.Add(1)

	status := int(f.status.Load())
	if len(f.script) > 0 {
		idx := int(n) - 1
		if idx >= len(f.script) {
			idx = len(f.script) - 1
		}
		status = f.script[idx]
	}

	f.mu.Lock()
	f.lastPath = r.URL.Path
	f.lastVerb = r.Method
	gated := f.gates != nil && f.gates[n]
	f.mu.Unlock()

	if gated {
		f.gateHits <- n
		select {
		case <-f.releaseCh:
		case <-r.Context().Done():
			return
		}
	}

	if d := time.Duration(f.delay.Load()); d > 0 {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}
	w.WriteHeader(status)
}

// enableGates 让第 at 次命中停在响应之前，供测试同步检查中间状态。
func (f *fakeUpstream) enableGates(at ...int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gates = make(map[int64]bool, len(at))
	for _, n := range at {
		f.gates[n] = true
	}
	f.gateHits = make(chan int64, len(at)+1)
	f.releaseCh = make(chan struct{}, len(at))
}

func (f *fakeUpstream) release() {
	f.releaseCh <- struct{}{}
}

func (f *fakeUpstream) requestSnapshot() (string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastVerb, f.lastPath
}

func newFakeUpstream(t *testing.T, status int) (*fakeUpstream, string) {
	t.Helper()
	f := &fakeUpstream{}
	f.status.Store(int32(status))
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.Listener.Addr().String()
}

func newTestConfig(upstreams []string, interval time.Duration, threshold int, timeout time.Duration) config.Config {
	return config.Config{
		Upstreams:      upstreams,
		ProbeInterval:  interval,
		ProbeThreshold: threshold,
		ProbeTimeout:   timeout,
		ProbePath:      "/health",
	}
}

func runProbeManager(t *testing.T, cfg config.Config, cb Callbacks) (*Manager, context.CancelFunc) {
	t.Helper()
	m := NewManager(cfg, cb)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.Start(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("probe Start did not return after context cancel")
		}
	})
	return m, cancel
}

// startManager 在返回前记下启动时刻，让时间下界以 Start 的调用点为基准。
func startManager(t *testing.T, m *Manager) time.Time {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("probe Start did not return after context cancel")
		}
	})
	start := time.Now()
	go func() {
		m.Start(ctx)
		close(done)
	}()
	return start
}

func waitEvent(t *testing.T, events <-chan string) string {
	t.Helper()
	select {
	case node := <-events:
		return node
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for probe callback")
		return ""
	}
}

func waitGate(t *testing.T, hits <-chan int64) int64 {
	t.Helper()
	select {
	case n := <-hits:
		return n
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for gated probe hit")
		return 0
	}
}

func drainEvent(events <-chan string) string {
	select {
	case node := <-events:
		return node
	default:
		return ""
	}
}

func assertHealthy(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("healthy = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("healthy = %v, want %v", got, want)
		}
	}
}

func TestProbeRemovesAfterConsecutiveFailures(t *testing.T) {
	up, addr := newFakeUpstream(t, http.StatusInternalServerError)
	removed := make(chan string, 4)
	var removeCount atomic.Int32

	m, _ := runProbeManager(t, newTestConfig([]string{addr}, 10*time.Millisecond, 3, 200*time.Millisecond), Callbacks{
		OnRemove: func(node string) {
			removeCount.Add(1)
			removed <- node
		},
		OnRecover: func(string) {},
	})

	if node := waitEvent(t, removed); node != addr {
		t.Fatalf("OnRemove node = %q, want %q", node, addr)
	}
	if got := removeCount.Load(); got != 1 {
		t.Fatalf("OnRemove calls = %d, want 1", got)
	}
	assertHealthy(t, m.Healthy())

	select {
	case extra := <-removed:
		t.Fatalf("duplicate OnRemove for %q", extra)
	case <-time.After(80 * time.Millisecond):
	}
	if got := up.hits.Load(); got < 3 {
		t.Fatalf("probe hits = %d, want at least 3", got)
	}
	verb, path := up.requestSnapshot()
	if verb != http.MethodGet || path != "/health" {
		t.Fatalf("probe request = %s %s, want GET /health", verb, path)
	}
}

func TestProbeRecoversAfterConsecutiveSuccesses(t *testing.T) {
	failing, failAddr := newFakeUpstream(t, http.StatusInternalServerError)
	_, okAddr := newFakeUpstream(t, http.StatusOK)

	removed := make(chan string, 4)
	recovered := make(chan string, 4)
	var recoverCount atomic.Int32

	m, _ := runProbeManager(t, newTestConfig([]string{failAddr, okAddr}, 10*time.Millisecond, 2, 200*time.Millisecond), Callbacks{
		OnRemove: func(node string) { removed <- node },
		OnRecover: func(node string) {
			recoverCount.Add(1)
			recovered <- node
		},
	})

	if node := waitEvent(t, removed); node != failAddr {
		t.Fatalf("OnRemove node = %q, want %q", node, failAddr)
	}
	assertHealthy(t, m.Healthy(), okAddr)

	failing.status.Store(int32(http.StatusOK))

	if node := waitEvent(t, recovered); node != failAddr {
		t.Fatalf("OnRecover node = %q, want %q", node, failAddr)
	}
	if got := recoverCount.Load(); got != 1 {
		t.Fatalf("OnRecover calls = %d, want 1", got)
	}
	assertHealthy(t, m.Healthy(), failAddr, okAddr)

	select {
	case extra := <-recovered:
		t.Fatalf("duplicate OnRecover for %q", extra)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestProbeTimeoutCountsAsFailure(t *testing.T) {
	up, addr := newFakeUpstream(t, http.StatusOK)
	up.delay.Store(int64(60 * time.Millisecond))

	removed := make(chan string, 4)
	m, _ := runProbeManager(t, newTestConfig([]string{addr}, 10*time.Millisecond, 2, 10*time.Millisecond), Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(string) {},
	})

	if node := waitEvent(t, removed); node != addr {
		t.Fatalf("OnRemove node = %q, want %q", node, addr)
	}
	assertHealthy(t, m.Healthy())
}

func TestProbeDetectionLatencyMatchesThreshold(t *testing.T) {
	const interval = 20 * time.Millisecond
	const threshold = 3
	const slack = 3*interval + 100*time.Millisecond

	up, addr := newFakeUpstream(t, http.StatusInternalServerError)
	up.enableGates(threshold - 1)

	removed := make(chan string, 4)
	var removeCount atomic.Int32
	var capturedHits atomic.Int64
	m := NewManager(newTestConfig([]string{addr}, interval, threshold, time.Second), Callbacks{
		OnRemove: func(node string) {
			removeCount.Add(1)
			capturedHits.Store(up.hits.Load())
			removed <- node
		},
		OnRecover: func(string) {},
	})

	start := startManager(t, m)

	if n := waitGate(t, up.gateHits); n != threshold-1 {
		t.Fatalf("gated hit = %d, want %d", n, threshold-1)
	}
	if got := up.hits.Load(); got != int64(threshold-1) {
		t.Fatalf("hits at gate = %d, want %d", got, threshold-1)
	}
	if got := removeCount.Load(); got != 0 {
		t.Fatalf("OnRemove calls after %d hits = %d, want 0 before threshold", threshold-1, got)
	}
	assertHealthy(t, m.Healthy(), addr)

	up.release()

	if node := waitEvent(t, removed); node != addr {
		t.Fatalf("OnRemove node = %q, want %q", node, addr)
	}
	elapsed := time.Since(start)

	minDelay := time.Duration(threshold) * interval
	if elapsed < minDelay {
		t.Fatalf("removed after %v, want no earlier than %v", elapsed, minDelay)
	}
	if elapsed > minDelay+slack {
		t.Fatalf("removed after %v, want within %v", elapsed, minDelay+slack)
	}
	if got := capturedHits.Load(); got != int64(threshold) {
		t.Fatalf("hits captured in OnRemove = %d, want exactly %d", got, threshold)
	}
	assertHealthy(t, m.Healthy())
}

func TestProbeSuccessShortOfThresholdDoesNotRecover(t *testing.T) {
	const interval = 20 * time.Millisecond
	const threshold = 3

	up, addr := newFakeUpstream(t, http.StatusInternalServerError)
	up.script = []int{
		http.StatusInternalServerError, // 1：连续失败 1
		http.StatusInternalServerError, // 2：连续失败 2
		http.StatusInternalServerError, // 3：达阈值摘除
		http.StatusOK,                  // 4：摘除后连续成功 1
		http.StatusOK,                  // 5：连续成功 2，未达阈值
		http.StatusInternalServerError, // 6：成功中断，successRun 清零
		http.StatusOK,                  // 7：重新从 1 计数
		http.StatusOK,                  // 8：连续成功 2
		http.StatusOK,                  // 9：连续成功 3 恢复
	}
	up.enableGates(6, 8)

	removed := make(chan string, 4)
	recovered := make(chan string, 4)
	m := NewManager(newTestConfig([]string{addr}, interval, threshold, time.Second), Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(node string) { recovered <- node },
	})
	startManager(t, m)

	if node := waitEvent(t, removed); node != addr {
		t.Fatalf("OnRemove node = %q, want %q", node, addr)
	}

	if n := waitGate(t, up.gateHits); n != 6 {
		t.Fatalf("gated hit = %d, want 6", n)
	}
	assertHealthy(t, m.Healthy())
	if node := drainEvent(recovered); node != "" {
		t.Fatalf("OnRecover fired after %d successes, want none before threshold", threshold-1)
	}
	up.release()

	if n := waitGate(t, up.gateHits); n != 8 {
		t.Fatalf("gated hit = %d, want 8", n)
	}
	assertHealthy(t, m.Healthy())
	if node := drainEvent(recovered); node != "" {
		t.Fatalf("OnRecover fired after successRun reset, node %q", node)
	}
	up.release()

	if node := waitEvent(t, recovered); node != addr {
		t.Fatalf("OnRecover node = %q, want %q", node, addr)
	}
	assertHealthy(t, m.Healthy(), addr)
}

func TestProbeTransientFailureDoesNotRemoveHealthyNode(t *testing.T) {
	const interval = 20 * time.Millisecond
	const threshold = 3

	up, addr := newFakeUpstream(t, http.StatusOK)
	up.script = []int{
		http.StatusInternalServerError, // 1：failRun 1
		http.StatusOK,                  // 2：failRun 清零
		http.StatusInternalServerError, // 3：failRun 1
		http.StatusInternalServerError, // 4：failRun 2
		http.StatusInternalServerError, // 5：未清零时会在此达阈值
	}
	up.enableGates(5)

	removed := make(chan string, 4)
	m := NewManager(newTestConfig([]string{addr}, interval, threshold, time.Second), Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(string) {},
	})
	startManager(t, m)

	if n := waitGate(t, up.gateHits); n != 5 {
		t.Fatalf("gated hit = %d, want 5", n)
	}
	assertHealthy(t, m.Healthy(), addr)
	if node := drainEvent(removed); node != "" {
		t.Fatalf("OnRemove fired after transient failure, node %q", node)
	}
}

func TestProbeFirstTickWaitsForInterval(t *testing.T) {
	const interval = 150 * time.Millisecond

	up, addr := newFakeUpstream(t, http.StatusOK)
	m := NewManager(newTestConfig([]string{addr}, interval, 3, 500*time.Millisecond), Callbacks{})
	startManager(t, m)

	time.Sleep(interval / 3)
	if got := up.hits.Load(); got != 0 {
		t.Fatalf("probe hits within %v of start = %d, want 0 before first tick", interval/3, got)
	}
}

func TestProbeHealthyShrinksWhenOnRemoveReturns(t *testing.T) {
	const interval = 10 * time.Millisecond

	_, addr := newFakeUpstream(t, http.StatusInternalServerError)

	type observation struct {
		node      string
		stillSeen bool
	}
	observed := make(chan observation, 1)

	var m *Manager
	m = NewManager(newTestConfig([]string{addr}, interval, 2, 500*time.Millisecond), Callbacks{
		OnRemove: func(node string) {
			still := false
			for _, n := range m.Healthy() {
				if n == node {
					still = true
				}
			}
			observed <- observation{node: node, stillSeen: still}
		},
		OnRecover: func(string) {},
	})
	startManager(t, m)

	select {
	case got := <-observed:
		if got.node != addr {
			t.Fatalf("OnRemove node = %q, want %q", got.node, addr)
		}
		if got.stillSeen {
			t.Fatalf("Healthy still contains %q when OnRemove returns", got.node)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnRemove")
	}
	assertHealthy(t, m.Healthy())
}

func TestProbePerNodeIndependentCounts(t *testing.T) {
	_, failAddr := newFakeUpstream(t, http.StatusInternalServerError)
	_, okAddr := newFakeUpstream(t, http.StatusOK)

	removed := make(chan string, 4)
	m, _ := runProbeManager(t, newTestConfig([]string{failAddr, okAddr}, 10*time.Millisecond, 2, 200*time.Millisecond), Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(string) {},
	})

	if node := waitEvent(t, removed); node != failAddr {
		t.Fatalf("OnRemove node = %q, want %q", node, failAddr)
	}
	assertHealthy(t, m.Healthy(), okAddr)

	select {
	case extra := <-removed:
		t.Fatalf("unexpected OnRemove for %q", extra)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestProbeStartReturnsOnContextCancel(t *testing.T) {
	_, addr := newFakeUpstream(t, http.StatusOK)

	m := NewManager(newTestConfig([]string{addr}, 5*time.Millisecond, 3, 200*time.Millisecond), Callbacks{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.Start(ctx)
		close(done)
	}()

	time.Sleep(40 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

func TestProbeHealthyConcurrentWithRemoval(t *testing.T) {
	_, addr := newFakeUpstream(t, http.StatusInternalServerError)

	removed := make(chan string, 4)
	m, _ := runProbeManager(t, newTestConfig([]string{addr}, 5*time.Millisecond, 3, 200*time.Millisecond), Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(string) {},
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, node := range m.Healthy() {
					if node != addr {
						t.Errorf("healthy contains unexpected node %q", node)
					}
				}
				time.Sleep(200 * time.Microsecond)
			}
		}()
	}

	if node := waitEvent(t, removed); node != addr {
		t.Fatalf("OnRemove node = %q, want %q", node, addr)
	}
	close(stop)
	wg.Wait()
	assertHealthy(t, m.Healthy())
}

func TestProbeURL(t *testing.T) {
	cases := []struct {
		name string
		addr string
		path string
		want string
	}{
		{"no scheme", "127.0.0.1:9001", "/health", "http://127.0.0.1:9001/health"},
		{"scheme and trailing slash", "http://127.0.0.1:9001/", "/health", "http://127.0.0.1:9001/health"},
		{"https without path slash", "https://127.0.0.1:9001", "health", "https://127.0.0.1:9001/health"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := probeURL(tc.addr, tc.path); got != tc.want {
				t.Fatalf("probeURL(%q, %q) = %q, want %q", tc.addr, tc.path, got, tc.want)
			}
		})
	}
}

// pathUpstream 只对 okPath 返回 200，其余路径返回 404，并记录每次探测命中的路径。
type pathUpstream struct {
	okPath string

	mu    sync.Mutex
	paths []string
}

func (p *pathUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	p.paths = append(p.paths, r.URL.Path)
	p.mu.Unlock()

	if r.URL.Path == p.okPath {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (p *pathUpstream) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.paths...)
}

// TestProbeUsesConfiguredPath 验证探针按 cfg.ProbePath 判定：只认该路径的副本保持健康，其余被摘除（D16、D21）。
func TestProbeUsesConfiguredPath(t *testing.T) {
	ready := &pathUpstream{okPath: "/ready"}
	readySrv := httptest.NewServer(ready)
	t.Cleanup(readySrv.Close)

	onlyDefault := &pathUpstream{okPath: "/health"}
	onlyDefaultSrv := httptest.NewServer(onlyDefault)
	t.Cleanup(onlyDefaultSrv.Close)
	onlyDefaultAddr := onlyDefaultSrv.Listener.Addr().String()

	cfg := newTestConfig([]string{readySrv.Listener.Addr().String(), onlyDefaultAddr}, 10*time.Millisecond, 2, 200*time.Millisecond)
	cfg.ProbePath = "/ready"

	removed := make(chan string, 4)
	m, _ := runProbeManager(t, cfg, Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(string) {},
	})

	if node := waitEvent(t, removed); node != onlyDefaultAddr {
		t.Fatalf("OnRemove node = %q, want %q", node, onlyDefaultAddr)
	}
	assertHealthy(t, m.Healthy(), readySrv.Listener.Addr().String())

	for _, path := range onlyDefault.seen() {
		if path != "/ready" {
			t.Fatalf("probe path = %q, want configured /ready", path)
		}
	}
	if got := ready.seen(); len(got) == 0 {
		t.Fatal("ready upstream received no probe")
	}
	for _, path := range ready.seen() {
		if path != "/ready" {
			t.Fatalf("probe path = %q, want configured /ready", path)
		}
	}
}

// TestProbeRoundFanOutConcurrent 验证一轮探测对副本并发发出：两副本各延迟约单次预算，总耗时接近单次而非两倍（D21）。
func TestProbeRoundFanOutConcurrent(t *testing.T) {
	const delay = 120 * time.Millisecond

	first, firstAddr := newFakeUpstream(t, http.StatusOK)
	second, secondAddr := newFakeUpstream(t, http.StatusOK)
	first.delay.Store(int64(delay))
	second.delay.Store(int64(delay))

	m := NewManager(newTestConfig([]string{firstAddr, secondAddr}, time.Second, 1, 500*time.Millisecond), Callbacks{})

	start := time.Now()
	m.probeAll(context.Background())
	elapsed := time.Since(start)

	if got := first.hits.Load(); got != 1 {
		t.Errorf("first upstream hits in one round = %d, want 1", got)
	}
	if got := second.hits.Load(); got != 1 {
		t.Errorf("second upstream hits in one round = %d, want 1", got)
	}
	if elapsed < delay {
		t.Fatalf("round elapsed %v, want at least one probe delay %v", elapsed, delay)
	}
	if elapsed >= delay*3/2 {
		t.Fatalf("round elapsed %v, want under 1.5x single probe %v (fan-out)", elapsed, delay*3/2)
	}
}

// syncBuffer 是互斥保护的日志缓冲，probe goroutine 写与测试读并发安全。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureProbeLogs 把默认 logger 换成写进 buffer 的 JSON handler，测试结束恢复。
func captureProbeLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

// waitLogLine 轮询 buffer，返回第一条同时包含全部 want 子串的日志行。
func waitLogLine(t *testing.T, buf *syncBuffer, want ...string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, line := range strings.Split(buf.String(), "\n") {
			if line == "" {
				continue
			}
			matched := true
			for _, sub := range want {
				if !strings.Contains(line, sub) {
					matched = false
					break
				}
			}
			if matched {
				return line
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("log line containing %v not found; logs:\n%s", want, buf.String())
			return ""
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestProbeLogsAfterCallbacks 验证摘除与恢复日志在回调返回后由 probe 打出（D21）。
func TestProbeLogsAfterCallbacks(t *testing.T) {
	logs := captureProbeLogs(t)

	const threshold = 2
	up, addr := newFakeUpstream(t, http.StatusInternalServerError)
	removed := make(chan string, 4)
	recovered := make(chan string, 4)

	runProbeManager(t, newTestConfig([]string{addr}, 10*time.Millisecond, threshold, 200*time.Millisecond), Callbacks{
		OnRemove:  func(node string) { removed <- node },
		OnRecover: func(node string) { recovered <- node },
	})

	if node := waitEvent(t, removed); node != addr {
		t.Fatalf("OnRemove node = %q, want %q", node, addr)
	}
	waitLogLine(t, logs, `"level":"WARN"`, `"node":"`+addr+`"`, fmt.Sprintf(`"failures":%d`, threshold))

	up.status.Store(int32(http.StatusOK))
	if node := waitEvent(t, recovered); node != addr {
		t.Fatalf("OnRecover node = %q, want %q", node, addr)
	}
	waitLogLine(t, logs, `"level":"INFO"`, `"node":"`+addr+`"`)
}
