package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/TheWor1d877/KVFerry/internal/config"
	"github.com/TheWor1d877/KVFerry/internal/drill"
	"github.com/TheWor1d877/KVFerry/internal/probe"
	"github.com/TheWor1d877/KVFerry/internal/sticky"
)

// ctxKey 标识网关写进请求 context 的值，改路传输层按同一组键取值（D20）。
type ctxKey int

const (
	ctxTargetKey ctxKey = iota
	ctxSessionKey
	ctxTableKey
	ctxReroutedKey
)

const unavailableBody = `{"error":{"message":"no healthy upstream"}}`

// New 返回网关主处理器：会话解析、粘性路由、标准库反代与错误响应（D17、D18、D19、D24、R10、R11）。
func New(cfg config.Config, table *sticky.Table, probes *probe.Manager, events *drill.Buffer) http.Handler {
	var counter atomic.Uint64

	rp := &httputil.ReverseProxy{
		Transport: newTransport(events, probes),
		Rewrite: func(pr *httputil.ProxyRequest) {
			target, _ := pr.In.Context().Value(ctxTargetKey).(string)
			scheme, host := splitTarget(target)
			pr.Out.URL.Scheme = scheme
			pr.Out.URL.Host = host
			pr.Out.Host = host
			restoreForwardedHeaders(pr.Out.Header, pr.In.Header)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			writeTransportError(w, r, err, probes)
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, err := SessionID(r)
		if err != nil {
			if errors.Is(err, errBodyRead) {
				slog.Error("request failed",
					"component", "gateway",
					"request_id", r.Header.Get("X-Request-Id"),
					"error", err.Error(),
				)
				http.Error(w, "read request body", http.StatusBadRequest)
				return
			}
			session = ""
		}

		healthy := probes.Healthy()
		if len(healthy) == 0 {
			writeUnavailable(w)
			return
		}

		var target string
		if session != "" {
			if node, ok := table.Lookup(session); ok {
				target = node
			}
		}
		if target == "" {
			idx := (counter.Add(1) - 1) % uint64(len(healthy))
			target = healthy[idx]
			if session != "" {
				table.Assign(session, target)
			}
		}

		ctx := context.WithValue(r.Context(), ctxTargetKey, target)
		ctx = context.WithValue(ctx, ctxSessionKey, session)
		ctx = context.WithValue(ctx, ctxTableKey, table)
		rp.ServeHTTP(w, r.WithContext(ctx))
	})
}

// forwardedHeaderNames 是标准库 Rewrite 模式剥离的转发头，网关按入站原值恢复（D18）。
var forwardedHeaderNames = []string{
	"Forwarded",
	"X-Forwarded-For",
	"X-Forwarded-Host",
	"X-Forwarded-Proto",
}

// restoreForwardedHeaders 把入站转发头逐字拷回出站请求，值切片整体拷贝，不拼接、不追加客户端地址（D18）。
func restoreForwardedHeaders(out, in http.Header) {
	for _, name := range forwardedHeaderNames {
		values := in.Values(name)
		if len(values) == 0 {
			continue
		}
		out[name] = append([]string(nil), values...)
	}
}

// splitTarget 把副本地址拆成 scheme 与 host；配置缺省不带 scheme 时按 http（D16）。
func splitTarget(target string) (string, string) {
	if strings.Contains(target, "://") {
		if u, err := url.Parse(target); err == nil && u.Host != "" {
			return u.Scheme, u.Host
		}
	}
	return "http", target
}

// writeTransportError 是 ReverseProxy 的 ErrorHandler 主体：先记请求失败日志，再按健康集是否为空写完响应（D24）。
func writeTransportError(w http.ResponseWriter, r *http.Request, err error, probes *probe.Manager) {
	slog.Error("request failed",
		"component", "gateway",
		"request_id", r.Header.Get("X-Request-Id"),
		"error", err.Error(),
	)
	if len(probes.Healthy()) == 0 {
		writeUnavailable(w)
		return
	}
	http.Error(w, "bad gateway", http.StatusBadGateway)
}

// writeUnavailable 写全摘 503 的判据头与响应体（D24）。
func writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-kvferry-gateway", "unavailable")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, unavailableBody)
}
