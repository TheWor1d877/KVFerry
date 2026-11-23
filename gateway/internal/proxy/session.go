package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// errBodyRead 标记请求体读取失败，主流程据此回 400（D24）。
var errBodyRead = errors.New("read request body")

// SessionID 先统一缓存请求体，再按请求头、请求体顺序解析会话标识（D17、R13）。
func SessionID(r *http.Request) (string, error) {
	if err := cacheBody(r); err != nil {
		return "", err
	}
	if v := r.Header.Get("X-Session-ID"); v != "" {
		return v, nil
	}
	if v := r.Header.Get("X-KVFerry-Session"); v != "" {
		return v, nil
	}
	return sessionFromBody(r)
}

// cacheBody 把非空请求体读进内存并回填 r.Body 与 r.GetBody，改路重发依赖这份可重放副本（D18）。
func cacheBody(r *http.Request) error {
	if r.Method == http.MethodGet {
		return nil
	}
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return nil
	}
	buf, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return errBodyRead
	}
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf)), nil
	}
	return nil
}

// sessionFromBody 读缓存副本里的 session_id 字段；解析失败返回 error，缓存保持完整（D17）。
func sessionFromBody(r *http.Request) (string, error) {
	if r.GetBody == nil {
		return "", nil
	}
	rc, err := r.GetBody()
	if err != nil {
		return "", err
	}
	defer rc.Close()

	raw, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	if len(raw) == 0 {
		return "", nil
	}

	var payload struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", err
	}
	return payload.SessionID, nil
}
