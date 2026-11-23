package probe

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/TheWor1d877/KVFerry/internal/config"
)

// Callbacks 由 main 装配，摘除与恢复触发时同步调用。
type Callbacks struct {
	OnRemove  func(node string)
	OnRecover func(node string)
}

// nodeState 是单个副本的探针状态，healthy 初始为 true（D21）。
type nodeState struct {
	addr       string
	healthy    bool
	failRun    int
	successRun int
}

// Manager 维护副本健康状态（D21）。
type Manager struct {
	cfg    config.Config
	cb     Callbacks
	client *http.Client

	mu    sync.Mutex
	nodes []nodeState
}

func NewManager(cfg config.Config, cb Callbacks) *Manager {
	nodes := make([]nodeState, len(cfg.Upstreams))
	for i, addr := range cfg.Upstreams {
		nodes[i] = nodeState{addr: addr, healthy: true}
	}
	return &Manager{
		cfg:    cfg,
		cb:     cb,
		client: &http.Client{},
		nodes:  nodes,
	}
}

// Start 按 cfg.ProbeInterval 周期探测全部副本，阻塞到 ctx 取消（D21）。
func (m *Manager) Start(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.probeAll(ctx)
		}
	}
}

// probeAll 对全部副本并发探测，结果回到本 goroutine 后按配置顺序串行应用（D21）。
func (m *Manager) probeAll(ctx context.Context) {
	m.mu.Lock()
	addrs := make([]string, len(m.nodes))
	for i := range m.nodes {
		addrs[i] = m.nodes[i].addr
	}
	m.mu.Unlock()

	results := make([]bool, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			results[i] = m.probe(ctx, addr)
		}(i, addr)
	}
	wg.Wait()

	select {
	case <-ctx.Done():
		return
	default:
	}
	m.apply(results)
}

// probe 发一次 GET：2xx 计成功，其余状态码、网络错误与超时计失败，响应体必须关闭（D21）。
func (m *Manager) probe(ctx context.Context, addr string) bool {
	reqCtx, cancel := context.WithTimeout(ctx, m.cfg.ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, probeURL(addr, m.cfg.ProbePath), nil)
	if err != nil {
		return false
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
}

// probeURL 给不带 scheme 的副本补 http，去掉末尾多余的 /，再拼 cfg.ProbePath（D21）。
func probeURL(addr, path string) string {
	base := strings.TrimRight(addr, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path
}

// apply 按配置顺序更新状态并串行触发回调，回调在锁外调用（D21）。
func (m *Manager) apply(results []bool) {
	type trigger struct {
		node   string
		remove bool
	}
	var fired []trigger

	m.mu.Lock()
	for i, ok := range results {
		n := &m.nodes[i]
		if !ok {
			n.successRun = 0
			if !n.healthy {
				continue
			}
			n.failRun++
			if n.failRun < m.cfg.ProbeThreshold {
				continue
			}
			n.failRun = 0
			n.healthy = false
			fired = append(fired, trigger{node: n.addr, remove: true})
			continue
		}

		n.failRun = 0
		if n.healthy {
			continue
		}
		n.successRun++
		if n.successRun < m.cfg.ProbeThreshold {
			continue
		}
		n.successRun = 0
		n.healthy = true
		fired = append(fired, trigger{node: n.addr, remove: false})
	}
	m.mu.Unlock()

	for _, t := range fired {
		if t.remove {
			if m.cb.OnRemove != nil {
				m.cb.OnRemove(t.node)
			}
			slog.Warn("upstream removed",
				"component", "gateway",
				"node", t.node,
				"failures", m.cfg.ProbeThreshold,
			)
			continue
		}
		if m.cb.OnRecover != nil {
			m.cb.OnRecover(t.node)
		}
		slog.Info("upstream recovered",
			"component", "gateway",
			"node", t.node,
		)
	}
}

// Healthy 返回健康副本地址的拷贝，保持配置顺序（D21）。
func (m *Manager) Healthy() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]string, 0, len(m.nodes))
	for i := range m.nodes {
		if m.nodes[i].healthy {
			out = append(out, m.nodes[i].addr)
		}
	}
	return out
}
