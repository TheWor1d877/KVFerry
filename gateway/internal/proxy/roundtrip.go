package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/TheWor1d877/KVFerry/internal/drill"
	"github.com/TheWor1d877/KVFerry/internal/probe"
	"github.com/TheWor1d877/KVFerry/internal/sticky"
)

// rerouteTransport 在响应头到达前的传输失败时改路重发一次（D20）。
type rerouteTransport struct {
	base   http.RoundTripper
	events *drill.Buffer
	probes *probe.Manager
}

// newTransport 返回注入改路能力的传输层，底层用 http.DefaultTransport（D20、R10）。
func newTransport(events *drill.Buffer, probes *probe.Manager) http.RoundTripper {
	return &rerouteTransport{base: http.DefaultTransport, events: events, probes: probes}
}

// RoundTrip 首次失败后从健康副本改路重发一次，成功时改写会话映射并记事件与日志（D19、D20、D22）。
func (t *rerouteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	start := time.Now()

	resp, err := t.base.RoundTrip(r)
	if err == nil {
		return resp, nil
	}

	ctx := r.Context()
	if marked, _ := ctx.Value(ctxReroutedKey).(bool); marked {
		return nil, err
	}

	from, _ := ctx.Value(ctxTargetKey).(string)
	if from == "" {
		return nil, err
	}

	to := firstOther(t.probes.Healthy(), from)
	if to == "" {
		return nil, err
	}

	var body io.ReadCloser
	hasBody := r.Body != nil && r.Body != http.NoBody
	if hasBody {
		if r.GetBody == nil {
			return nil, err
		}
		body, err = r.GetBody()
		if err != nil {
			return nil, err
		}
	}

	clone := r.Clone(context.WithValue(ctx, ctxReroutedKey, true))
	scheme, host := splitTarget(to)
	clone.URL.Scheme = scheme
	clone.URL.Host = host
	clone.Host = host
	if hasBody {
		clone.Body = body
	}

	resp2, err2 := t.base.RoundTrip(clone)
	if err2 != nil {
		return nil, err2
	}

	session, _ := ctx.Value(ctxSessionKey).(string)
	if session != "" {
		if table, ok := ctx.Value(ctxTableKey).(*sticky.Table); ok && table != nil {
			table.Assign(session, to)
		}
	}

	requestID := r.Header.Get("X-Request-Id")
	t.events.Record(drill.Event{
		TsMs:      time.Now().UnixMilli(),
		Node:      to,
		Action:    drill.ActionReroute,
		RequestID: requestID,
	})
	slog.Info("reroute",
		"component", "gateway",
		"request_id", requestID,
		"session", session,
		"from", from,
		"to", to,
		"elapsed_ms", time.Since(start).Milliseconds(),
	)
	return resp2, nil
}

// firstOther 按配置顺序返回第一个不是 failed 的健康副本，没有候选时返回空串（D20）。
func firstOther(healthy []string, failed string) string {
	for _, node := range healthy {
		if node != failed {
			return node
		}
	}
	return ""
}
