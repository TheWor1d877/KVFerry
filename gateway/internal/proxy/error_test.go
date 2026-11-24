package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"testing"

	"github.com/TheWor1d877/KVFerry/internal/config"
	"github.com/TheWor1d877/KVFerry/internal/drill"
	"github.com/TheWor1d877/KVFerry/internal/probe"
	"github.com/TheWor1d877/KVFerry/internal/sticky"
)

func TestErrorHandlerNoHealthyWrites503(t *testing.T) {
	logs := captureLogs(t)
	probes := probe.NewManager(config.Config{Upstreams: nil}, probe.Callbacks{})

	req := httptest.NewRequest(http.MethodGet, "http://gateway.test/v1/models", nil)
	req.Header.Set("X-Request-Id", "rid-err-503")
	rec := httptest.NewRecorder()

	writeTransportError(rec, req, errors.New("dial tcp: connection refused"), probes)

	res := rec.Result()
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	if got := res.Header.Get("x-kvferry-gateway"); got != "unavailable" {
		t.Errorf("x-kvferry-gateway = %q, want unavailable", got)
	}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(raw) != unavailableBody {
		t.Errorf("response body = %q, want %q", raw, unavailableBody)
	}
	if entry := logs.String(); !strings.Contains(entry, `"level":"ERROR"`) ||
		!strings.Contains(entry, `"request_id":"rid-err-503"`) ||
		!strings.Contains(entry, `"error":"dial tcp`) {
		t.Errorf("503 log missing fields: %s", entry)
	}
}

func TestErrorHandlerHealthyWrites502WithoutHeader(t *testing.T) {
	logs := captureLogs(t)
	probes := probe.NewManager(config.Config{Upstreams: []string{"127.0.0.1:9001"}}, probe.Callbacks{})

	req := httptest.NewRequest(http.MethodGet, "http://gateway.test/v1/models", nil)
	req.Header.Set("X-Request-Id", "rid-err-502")
	rec := httptest.NewRecorder()

	writeTransportError(rec, req, errors.New("upstream dial failed"), probes)

	res := rec.Result()
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", res.StatusCode)
	}
	if got := res.Header.Get("x-kvferry-gateway"); got != "" {
		t.Errorf("x-kvferry-gateway = %q, want empty when healthy set non-empty", got)
	}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if strings.TrimSpace(string(raw)) != "bad gateway" {
		t.Errorf("response body = %q, want bad gateway", raw)
	}
	if entry := logs.String(); !strings.Contains(entry, `"level":"ERROR"`) ||
		!strings.Contains(entry, `"request_id":"rid-err-502"`) {
		t.Errorf("502 log missing fields: %s", entry)
	}
}

func TestErrorNoHealthyBeforeForward(t *testing.T) {
	gw, _ := newGatewayServer(t, nil)

	resp := doRequest(t, gw.URL, http.MethodPost, "/v1/chat/completions", `{"model":"m"}`, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
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

func TestErrorUpstreamHTTPPassthrough(t *testing.T) {
	const body = `{"error":"upstream exploded"}`
	up := &upstreamRecorder{
		status:     http.StatusInternalServerError,
		respBody:   body,
		respHeader: http.Header{"X-Upstream-Error": {"yes"}, "Content-Type": {"application/json"}},
	}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)

	gw, _ := newGatewayServer(t, []string{upSrv.Listener.Addr().String()})

	resp := doRequest(t, gw.URL, http.MethodGet, "/v1/models", "", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Upstream-Error"); got != "yes" {
		t.Errorf("X-Upstream-Error = %q, want yes", got)
	}
	if got := resp.Header.Get("x-kvferry-gateway"); got != "" {
		t.Errorf("x-kvferry-gateway = %q, want empty on upstream passthrough", got)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(raw) != body {
		t.Errorf("response body = %q, want %q", raw, body)
	}
}

// TestErrorHandlerRerouteFailedNoHealthy503EndToEnd 走 transport 失败到 ErrorHandler 的完整链路：
// 目标不可达、健康集为空使改路无候选，ErrorHandler 按 D24 写 503 且不向其他地址重发。
func TestErrorHandlerRerouteFailedNoHealthy503EndToEnd(t *testing.T) {
	logs := captureLogs(t)

	probes := probe.NewManager(config.Config{Upstreams: nil}, probe.Callbacks{})
	if healthy := probes.Healthy(); len(healthy) != 0 {
		t.Fatalf("healthy = %v, want empty set", healthy)
	}
	events := drill.NewBuffer(16)
	table := sticky.NewTable()

	deadAddr := closedUpstreamAddr(t)

	rp := &httputil.ReverseProxy{
		Transport: newTransport(events, probes),
		Rewrite:   func(*httputil.ProxyRequest) {},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			writeTransportError(w, r, err, probes)
		},
	}

	req := httptest.NewRequest(http.MethodGet, "http://"+deadAddr+"/v1/models", nil)
	req.RequestURI = ""
	req.Header.Set("X-Request-Id", "rid-e2e-503")
	ctx := context.WithValue(req.Context(), ctxTargetKey, deadAddr)
	ctx = context.WithValue(ctx, ctxTableKey, table)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	rp.ServeHTTP(rec, req)

	res := rec.Result()
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	if got := res.Header.Get("x-kvferry-gateway"); got != "unavailable" {
		t.Errorf("x-kvferry-gateway = %q, want unavailable", got)
	}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(raw) != unavailableBody {
		t.Errorf("response body = %q, want %q", raw, unavailableBody)
	}
	if got := events.Snapshot(); len(got) != 0 {
		t.Errorf("reroute events = %d, want 0 without candidate: %+v", len(got), got)
	}
	if entry := logs.String(); !strings.Contains(entry, `"level":"ERROR"`) ||
		!strings.Contains(entry, `"request_id":"rid-e2e-503"`) {
		t.Errorf("503 log missing fields: %s", entry)
	}
}
