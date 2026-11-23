package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/TheWor1d877/KVFerry/internal/config"
	"github.com/TheWor1d877/KVFerry/internal/drill"
	"github.com/TheWor1d877/KVFerry/internal/probe"
	"github.com/TheWor1d877/KVFerry/internal/sticky"
)

// rerouteFixture 暴露改路断言需要的组件，事件缓冲与映射表可直接读取。
type rerouteFixture struct {
	handler http.Handler
	table   *sticky.Table
	events  *drill.Buffer
	probes  *probe.Manager
}

func newRerouteFixture(t *testing.T, upstreams []string) *rerouteFixture {
	t.Helper()
	cfg := config.Config{Upstreams: upstreams}
	table := sticky.NewTable()
	probes := probe.NewManager(cfg, probe.Callbacks{})
	events := drill.NewBuffer(16)
	handler := New(cfg, table, probes, events)
	return &rerouteFixture{handler: handler, table: table, events: events, probes: probes}
}

func (f *rerouteFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(f.handler)
	t.Cleanup(srv.Close)
	return srv
}

// closedUpstreamAddr 返回一个已关闭的副本地址，连接被拒绝即响应头前失败。
func closedUpstreamAddr(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.Listener.Addr().String()
	srv.Close()
	return addr
}

func TestRerouteOnPreHeaderFailure(t *testing.T) {
	firstAddr := closedUpstreamAddr(t)
	second := &upstreamRecorder{respBody: `{"ok":true}`}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)
	secondAddr := secondSrv.Listener.Addr().String()

	logs := captureLogs(t)
	fix := newRerouteFixture(t, []string{firstAddr, secondAddr})
	gw := fix.server(t)

	header := http.Header{
		"X-Session-ID": {"s1"},
		"X-Request-Id": {"rid-reroute"},
		"Content-Type": {"application/json"},
	}
	resp := doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(raw) != `{"ok":true}` {
		t.Errorf("response body = %q, want second upstream body", raw)
	}

	hits, _, _, _, _, _ := second.snapshot()
	if hits != 1 {
		t.Errorf("second upstream hits = %d, want 1", hits)
	}

	events := fix.events.Snapshot()
	if len(events) != 1 {
		t.Fatalf("reroute events = %d, want 1: %+v", len(events), events)
	}
	e := events[0]
	if e.Node != secondAddr {
		t.Errorf("event node = %q, want %q", e.Node, secondAddr)
	}
	if e.Action != drill.ActionReroute {
		t.Errorf("event action = %q, want %q", e.Action, drill.ActionReroute)
	}
	if e.RequestID != "rid-reroute" {
		t.Errorf("event request_id = %q, want rid-reroute", e.RequestID)
	}
	if e.TsMs <= 0 {
		t.Errorf("event ts_ms = %d, want positive", e.TsMs)
	}

	if node, ok := fix.table.Lookup("s1"); !ok || node != secondAddr {
		t.Errorf("Lookup(s1) = %q, %v; want %q, true", node, ok, secondAddr)
	}

	entry := logs.String()
	for _, want := range []string{
		`"msg":"reroute"`,
		`"request_id":"rid-reroute"`,
		`"session":"s1"`,
		`"from":"` + firstAddr + `"`,
		`"to":"` + secondAddr + `"`,
		`"elapsed_ms":`,
	} {
		if !strings.Contains(entry, want) {
			t.Errorf("reroute log missing %s: %s", want, entry)
		}
	}
}

func TestRerouteStickyNextRequest(t *testing.T) {
	firstAddr := closedUpstreamAddr(t)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)
	secondAddr := secondSrv.Listener.Addr().String()

	fix := newRerouteFixture(t, []string{firstAddr, secondAddr})
	gw := fix.server(t)

	header := http.Header{"X-Session-ID": {"s1"}}
	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)
	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)

	hits, _, _, _, _, _ := second.snapshot()
	if hits != 2 {
		t.Errorf("second upstream hits = %d, want 2", hits)
	}
	if events := fix.events.Snapshot(); len(events) != 1 {
		t.Errorf("reroute events = %d, want 1 after sticky follow-up", len(events))
	}
	if node, ok := fix.table.Lookup("s1"); !ok || node != secondAddr {
		t.Errorf("Lookup(s1) = %q, %v; want %q, true", node, ok, secondAddr)
	}
}

func TestRerouteStreamingAbortNoReroute(t *testing.T) {
	streaming := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "partial")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(streaming.Close)
	streamAddr := streaming.Listener.Addr().String()

	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)

	fix := newRerouteFixture(t, []string{streamAddr, secondSrv.Listener.Addr().String()})
	gw := fix.server(t)

	header := http.Header{"X-Session-ID": {"s1"}, "X-Request-Id": {"rid-stream"}}
	resp := doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 before truncation", resp.StatusCode)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("response body read succeeded, want truncated stream error")
	}

	hits, _, _, _, _, _ := second.snapshot()
	if hits != 0 {
		t.Errorf("second upstream hits = %d, want 0 after streaming abort", hits)
	}
	if events := fix.events.Snapshot(); len(events) != 0 {
		t.Errorf("reroute events = %d, want 0 after streaming abort: %+v", len(events), events)
	}
}

func TestRerouteEventWithoutRequestID(t *testing.T) {
	firstAddr := closedUpstreamAddr(t)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)

	fix := newRerouteFixture(t, []string{firstAddr, secondSrv.Listener.Addr().String()})
	gw := fix.server(t)

	header := http.Header{"X-Session-ID": {"s1"}}
	doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, header)

	events := fix.events.Snapshot()
	if len(events) != 1 {
		t.Fatalf("reroute events = %d, want 1", len(events))
	}
	if events[0].RequestID != "" {
		t.Errorf("event request_id = %q, want empty", events[0].RequestID)
	}
}

func TestRerouteAtMostOncePerRequest(t *testing.T) {
	firstAddr := closedUpstreamAddr(t)
	secondAddr := closedUpstreamAddr(t)
	third := &upstreamRecorder{}
	thirdSrv := httptest.NewServer(third)
	t.Cleanup(thirdSrv.Close)

	fix := newRerouteFixture(t, []string{firstAddr, secondAddr, thirdSrv.Listener.Addr().String()})
	gw := fix.server(t)

	header := http.Header{"X-Request-Id": {"rid-once"}}
	resp := doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", header)
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
	if got := resp.Header.Get("x-kvferry-gateway"); got != "" {
		t.Errorf("x-kvferry-gateway = %q, want empty for 502", got)
	}

	hits, _, _, _, _, _ := third.snapshot()
	if hits != 0 {
		t.Errorf("third upstream hits = %d, want 0; reroute must happen at most once", hits)
	}
	if events := fix.events.Snapshot(); len(events) != 0 {
		t.Errorf("reroute events = %d, want 0 when reroute retry fails", len(events))
	}
}

// countingFailingTransport 记录调用次数并固定失败，用于直接驱动改路传输层的失败分支。
type countingFailingTransport struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *countingFailingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil, f.err
}

func (f *countingFailingTransport) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newDirectRerouteRequest 直接构造带网关 context 的请求，绕过主流程的会话解析。
func newDirectRerouteRequest(t *testing.T, from, session, body string, marked bool) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://"+from+"/v1/chat/completions", nil)
	req.Header.Set("X-Request-Id", "rid-direct")
	if body != "" {
		req.Body = io.NopCloser(strings.NewReader(body))
		req.GetBody = nil
	}
	ctx := context.WithValue(req.Context(), ctxTargetKey, from)
	if session != "" {
		ctx = context.WithValue(ctx, ctxSessionKey, session)
	}
	if marked {
		ctx = context.WithValue(ctx, ctxReroutedKey, true)
	}
	return req.WithContext(ctx)
}

func TestRerouteNoCandidateKeepsFirstError(t *testing.T) {
	sentinel := errors.New("first transport failure")
	base := &countingFailingTransport{err: sentinel}
	events := drill.NewBuffer(16)
	probes := probe.NewManager(config.Config{Upstreams: []string{"up-a:9000"}}, probe.Callbacks{})
	tr := &rerouteTransport{base: base, events: events, probes: probes}

	req := newDirectRerouteRequest(t, "up-a:9000", "s1", "", false)
	if _, err := tr.RoundTrip(req); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want first failure", err)
	}
	if calls := base.callCount(); calls != 1 {
		t.Errorf("base calls = %d, want 1 without candidate", calls)
	}
	if got := events.Snapshot(); len(got) != 0 {
		t.Errorf("reroute events = %d, want 0: %+v", len(got), got)
	}
}

func TestRerouteNilGetBodyKeepsFirstError(t *testing.T) {
	sentinel := errors.New("first transport failure")
	base := &countingFailingTransport{err: sentinel}
	events := drill.NewBuffer(16)
	probes := probe.NewManager(config.Config{Upstreams: []string{"up-a:9000", "up-b:9000"}}, probe.Callbacks{})
	tr := &rerouteTransport{base: base, events: events, probes: probes}

	req := newDirectRerouteRequest(t, "up-a:9000", "s1", "payload", false)
	if req.GetBody != nil {
		t.Fatal("GetBody set, want nil to exercise the non-replayable body branch")
	}
	if _, err := tr.RoundTrip(req); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want first failure", err)
	}
	if calls := base.callCount(); calls != 1 {
		t.Errorf("base calls = %d, want 1 for non-replayable body", calls)
	}
	if got := events.Snapshot(); len(got) != 0 {
		t.Errorf("reroute events = %d, want 0: %+v", len(got), got)
	}
}

func TestRerouteMarkedRequestDoesNotRetry(t *testing.T) {
	sentinel := errors.New("first transport failure")
	base := &countingFailingTransport{err: sentinel}
	events := drill.NewBuffer(16)
	probes := probe.NewManager(config.Config{Upstreams: []string{"up-a:9000", "up-b:9000"}}, probe.Callbacks{})
	tr := &rerouteTransport{base: base, events: events, probes: probes}

	req := newDirectRerouteRequest(t, "up-a:9000", "s1", "", true)
	if _, err := tr.RoundTrip(req); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want first failure", err)
	}
	if calls := base.callCount(); calls != 1 {
		t.Errorf("base calls = %d, want 1 on already rerouted request", calls)
	}
	if got := events.Snapshot(); len(got) != 0 {
		t.Errorf("reroute events = %d, want 0: %+v", len(got), got)
	}
}

func TestRerouteReplaysRequestBody(t *testing.T) {
	firstAddr := closedUpstreamAddr(t)
	second := &upstreamRecorder{}
	secondSrv := httptest.NewServer(second)
	t.Cleanup(secondSrv.Close)

	fix := newRerouteFixture(t, []string{firstAddr, secondSrv.Listener.Addr().String()})
	gw := fix.server(t)

	const body = `{"session_id":"s1","model":"m","input":"replay-me-replay-me-replay-me"}`
	header := http.Header{"X-Session-ID": {"s1"}, "Content-Type": {"application/json"}}
	resp := doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", body, header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	hits, _, _, _, _, gotBody := second.snapshot()
	if hits != 1 {
		t.Fatalf("second upstream hits = %d, want 1", hits)
	}
	if string(gotBody) != body {
		t.Errorf("replayed body = %q, want %q", gotBody, body)
	}
}
