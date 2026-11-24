package proxy

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/TheWor1d877/KVFerry/internal/config"
	"github.com/TheWor1d877/KVFerry/internal/drill"
	"github.com/TheWor1d877/KVFerry/internal/probe"
	"github.com/TheWor1d877/KVFerry/internal/sticky"
)

// upstreamRecorder 记录到达模拟上游的请求，并按配置回响应。
type upstreamRecorder struct {
	mu         sync.Mutex
	hits       int
	method     string
	path       string
	host       string
	header     http.Header
	body       []byte
	status     int
	respBody   string
	respHeader http.Header
}

func (u *upstreamRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	u.mu.Lock()
	u.hits++
	u.method = r.Method
	u.path = r.URL.Path
	u.host = r.Host
	u.header = r.Header.Clone()
	u.body = body
	u.mu.Unlock()

	for name, values := range u.respHeader {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	status := u.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, u.respBody)
}

func (u *upstreamRecorder) snapshot() (int, string, string, string, http.Header, []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits, u.method, u.path, u.host, u.header.Clone(), append([]byte(nil), u.body...)
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", raw, err)
	}
	return u
}

func newGateway(t *testing.T, upstreams []string) (http.Handler, *sticky.Table) {
	t.Helper()
	cfg := config.Config{Upstreams: upstreams}
	table := sticky.NewTable()
	probes := probe.NewManager(cfg, probe.Callbacks{})
	events := drill.NewBuffer(16)
	return New(cfg, table, probes, events), table
}

func newGatewayServer(t *testing.T, upstreams []string) (*httptest.Server, *sticky.Table) {
	t.Helper()
	handler, table := newGateway(t, upstreams)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, table
}

func doRequest(t *testing.T, gwURL, method, path, body string, header http.Header) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, gwURL+path, reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for name, values := range header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

func TestProxyForwardsRequestVerbatim(t *testing.T) {
	up := &upstreamRecorder{respBody: `{"ok":true}`, respHeader: http.Header{"X-Upstream": {"yes"}}}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	upAddr := upSrv.Listener.Addr().String()

	gw, _ := newGatewayServer(t, []string{upAddr})

	const body = `{"session_id":"s1","model":"m"}`
	header := http.Header{
		"Content-Type": {"application/json"},
		"X-Session-ID": {"s1"},
		"X-Request-Id": {"rid-1"},
	}
	resp := doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", body, header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	hits, method, path, host, gotHeader, gotBody := up.snapshot()
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits)
	}
	if method != http.MethodPost {
		t.Errorf("upstream method = %q, want POST", method)
	}
	if path != "/v1/chat/completions" {
		t.Errorf("upstream path = %q, want /v1/chat/completions", path)
	}
	if host != upAddr {
		t.Errorf("upstream host = %q, want %q", host, upAddr)
	}
	if got := gotHeader.Get("X-Session-ID"); got != "s1" {
		t.Errorf("upstream X-Session-ID = %q, want s1", got)
	}
	if got := gotHeader.Get("X-Request-Id"); got != "rid-1" {
		t.Errorf("upstream X-Request-Id = %q, want rid-1", got)
	}
	if got := gotHeader.Get("Content-Type"); got != "application/json" {
		t.Errorf("upstream Content-Type = %q, want application/json", got)
	}
	if string(gotBody) != body {
		t.Errorf("upstream body = %q, want %q", gotBody, body)
	}
	if values := gotHeader.Values("X-Forwarded-For"); len(values) != 0 {
		t.Errorf("upstream received X-Forwarded-For = %v, want none", values)
	}
}

func TestProxyForwardsForwardedHeaders(t *testing.T) {
	up := &upstreamRecorder{}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)

	gw, _ := newGatewayServer(t, []string{upSrv.Listener.Addr().String()})

	want := http.Header{
		"Forwarded":         {"for=203.0.113.7;proto=https;by=203.0.113.1"},
		"X-Forwarded-For":   {"198.51.100.9", "198.51.100.10"},
		"X-Forwarded-Host":  {"edge.example.test"},
		"X-Forwarded-Proto": {"https"},
	}
	resp := doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", want)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	hits, _, _, _, gotHeader, _ := up.snapshot()
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits)
	}
	for name, values := range want {
		got := gotHeader.Values(name)
		if len(got) != len(values) {
			t.Errorf("upstream %s = %v, want %v", name, got, values)
			continue
		}
		for i := range values {
			if got[i] != values[i] {
				t.Errorf("upstream %s[%d] = %q, want %q", name, i, got[i], values[i])
			}
		}
	}
}

func TestProxyPassthroughResponse(t *testing.T) {
	up := &upstreamRecorder{
		status:     http.StatusCreated,
		respBody:   `{"created":true}`,
		respHeader: http.Header{"X-Upstream": {"yes"}, "Content-Type": {"application/json"}},
	}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)

	gw, _ := newGatewayServer(t, []string{upSrv.Listener.Addr().String()})

	resp := doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", nil)
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want 201", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Upstream"); got != "yes" {
		t.Errorf("X-Upstream = %q, want yes", got)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(raw) != `{"created":true}` {
		t.Errorf("response body = %q, want {\"created\":true}", raw)
	}
}

func TestProxyRoundRobinWithoutSession(t *testing.T) {
	first := &upstreamRecorder{}
	firstSrv := httptest.NewServer(first)
	t.Cleanup(firstSrv.Close)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)

	gw, table := newGatewayServer(t, []string{
		firstSrv.Listener.Addr().String(),
		secondSrv.Listener.Addr().String(),
	})

	doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", nil)
	doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", nil)

	firstHits, _, _, _, _, _ := first.snapshot()
	secondHits, _, _, _, _, _ := second.snapshot()
	if firstHits != 1 || secondHits != 1 {
		t.Fatalf("upstream hits = %d, %d; want 1, 1", firstHits, secondHits)
	}
	if node, ok := table.Lookup(""); ok {
		t.Errorf("Lookup(\"\") = %q, true; want absent for sessionless request", node)
	}
}

func TestProxySessionSticky(t *testing.T) {
	first := &upstreamRecorder{}
	firstSrv := httptest.NewServer(first)
	t.Cleanup(firstSrv.Close)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)
	firstAddr := firstSrv.Listener.Addr().String()

	gw, table := newGatewayServer(t, []string{
		firstAddr,
		secondSrv.Listener.Addr().String(),
	})

	header := http.Header{"X-Session-ID": {"s1"}, "Content-Type": {"application/json"}}
	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)
	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)

	firstHits, _, _, _, _, _ := first.snapshot()
	secondHits, _, _, _, _, _ := second.snapshot()
	if firstHits != 2 || secondHits != 0 {
		t.Fatalf("upstream hits = %d, %d; want 2, 0", firstHits, secondHits)
	}
	if node, ok := table.Lookup("s1"); !ok || node != firstAddr {
		t.Errorf("Lookup(s1) = %q, %v; want %q, true", node, ok, firstAddr)
	}
}

func TestProxyStickyNodeDropReroutes(t *testing.T) {
	first := &upstreamRecorder{}
	firstSrv := httptest.NewServer(first)
	t.Cleanup(firstSrv.Close)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)
	firstAddr := firstSrv.Listener.Addr().String()
	secondAddr := secondSrv.Listener.Addr().String()

	gw, table := newGatewayServer(t, []string{firstAddr, secondAddr})

	header := http.Header{"X-Session-ID": {"s1"}}
	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)
	if node, ok := table.Lookup("s1"); !ok || node != firstAddr {
		t.Fatalf("Lookup(s1) before drop = %q, %v; want %q, true", node, ok, firstAddr)
	}

	table.DropNode(firstAddr)

	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)

	firstHits, _, _, _, _, _ := first.snapshot()
	secondHits, _, _, _, _, _ := second.snapshot()
	if firstHits != 1 || secondHits != 1 {
		t.Fatalf("upstream hits = %d, %d; want 1, 1", firstHits, secondHits)
	}
	if node, ok := table.Lookup("s1"); !ok || node != secondAddr {
		t.Errorf("Lookup(s1) after drop = %q, %v; want %q, true", node, ok, secondAddr)
	}
}

// TestProxyRestartReselectsAndRegisters 验证新实例的空映射下同会话按轮询重选并重新登记，不沿用旧实例映射。
func TestProxyRestartReselectsAndRegisters(t *testing.T) {
	first := &upstreamRecorder{}
	firstSrv := httptest.NewServer(first)
	t.Cleanup(firstSrv.Close)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)
	firstAddr := firstSrv.Listener.Addr().String()
	secondAddr := secondSrv.Listener.Addr().String()
	upstreams := []string{firstAddr, secondAddr}

	gw1, table1 := newGatewayServer(t, upstreams)
	// 先发一次无会话请求把轮询计数器推到 1，让 s1 落在第二个副本，与重启后的首个选择区分开。
	doRequest(t, gw1.URL, http.MethodGet, "/v1/models", "", nil)
	header := http.Header{"X-Session-ID": {"s1"}}
	doRequest(t, gw1.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)
	if node, ok := table1.Lookup("s1"); !ok || node != secondAddr {
		t.Fatalf("Lookup(s1) before restart = %q, %v; want %q, true", node, ok, secondAddr)
	}

	// 丢弃旧实例，新建映射表与 handler 模拟进程重启。
	gw2, table2 := newGatewayServer(t, upstreams)
	doRequest(t, gw2.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)

	if node, ok := table2.Lookup("s1"); !ok || node != firstAddr {
		t.Errorf("Lookup(s1) after restart = %q, %v; want %q, true", node, ok, firstAddr)
	}
	firstHits, _, _, _, _, _ := first.snapshot()
	secondHits, _, _, _, _, _ := second.snapshot()
	if firstHits != 2 || secondHits != 1 {
		t.Errorf("upstream hits after restart = %d, %d; want 2, 1", firstHits, secondHits)
	}
}

func TestProxyNoHealthyUpstream(t *testing.T) {
	gw, _ := newGatewayServer(t, nil)

	resp := doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("x-kvferry-gateway"); got != "unavailable" {
		t.Errorf("x-kvferry-gateway = %q, want unavailable", got)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(raw) != unavailableBody {
		t.Errorf("response body = %q, want %q", raw, unavailableBody)
	}
}

func TestProxyUpstreamConnectionRefused(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closedAddr := closed.Listener.Addr().String()
	closed.Close()

	logs := captureLogs(t)
	gw, _ := newGatewayServer(t, []string{closedAddr})

	header := http.Header{"X-Request-Id": {"rid-502"}}
	resp := doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", header)
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
	if got := resp.Header.Get("x-kvferry-gateway"); got != "" {
		t.Errorf("x-kvferry-gateway = %q, want empty", got)
	}
	if entry := logs.String(); !strings.Contains(entry, `"level":"ERROR"`) ||
		!strings.Contains(entry, `"request_id":"rid-502"`) ||
		!strings.Contains(entry, `"error":"`) {
		t.Errorf("request failure log missing fields: %s", entry)
	}
}

func TestProxyBodyReadErrorReturns400(t *testing.T) {
	up := &upstreamRecorder{}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)

	logs := captureLogs(t)
	handler, _ := newGateway(t, []string{upSrv.Listener.Addr().String()})

	req := &http.Request{
		Method:        http.MethodPost,
		URL:           mustParseURL(t, "http://gateway.test/v1/chat/completions"),
		Header:        http.Header{},
		Body:          io.NopCloser(failingBody{}),
		ContentLength: 16,
	}
	req.Header.Set("X-Request-Id", "rid-400")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	res := rec.Result()

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", res.StatusCode)
	}
	if got := res.Header.Get("x-kvferry-gateway"); got != "" {
		t.Errorf("x-kvferry-gateway = %q, want empty", got)
	}
	hits, _, _, _, _, _ := up.snapshot()
	if hits != 0 {
		t.Errorf("upstream hits = %d, want 0 for rejected request", hits)
	}
	if entry := logs.String(); !strings.Contains(entry, `"level":"ERROR"`) ||
		!strings.Contains(entry, `"request_id":"rid-400"`) ||
		!strings.Contains(entry, `"error":"`) {
		t.Errorf("400 log missing fields: %s", entry)
	}
}
