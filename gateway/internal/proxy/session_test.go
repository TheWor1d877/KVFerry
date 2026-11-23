package proxy

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func newSessionRequest(t *testing.T, method, body string) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	r, err := http.NewRequest(method, "http://gateway.test/v1/chat/completions", reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return r
}

func TestSessionFromHeaderXSessionID(t *testing.T) {
	r := newSessionRequest(t, http.MethodPost, `{"session_id":"body-id"}`)
	r.Header.Set("X-Session-ID", "header-id")

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID: %v", err)
	}
	if session != "header-id" {
		t.Errorf("session = %q, want header-id", session)
	}
}

func TestSessionFromHeaderKVFerrySession(t *testing.T) {
	r := newSessionRequest(t, http.MethodPost, `{"session_id":"body-id"}`)
	r.Header.Set("X-KVFerry-Session", "lab-id")

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID: %v", err)
	}
	if session != "lab-id" {
		t.Errorf("session = %q, want lab-id", session)
	}
}

func TestSessionFromBody(t *testing.T) {
	r := newSessionRequest(t, http.MethodPost, `{"model":"m","session_id":"json-id"}`)

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID: %v", err)
	}
	if session != "json-id" {
		t.Errorf("session = %q, want json-id", session)
	}
}

func TestSessionHeaderSkipsBodyParse(t *testing.T) {
	r := newSessionRequest(t, http.MethodPost, "{not json")
	r.Header.Set("X-Session-ID", "header-id")

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID with header hit: %v", err)
	}
	if session != "header-id" {
		t.Errorf("session = %q, want header-id", session)
	}
}

func TestSessionBodyReplayAfterHeaderHit(t *testing.T) {
	const body = `{"session_id":"s1","payload":"` + "xxxxxxxx" + `"}`
	r := newSessionRequest(t, http.MethodPost, body)
	r.Header.Set("X-Session-ID", "s1")

	if _, err := SessionID(r); err != nil {
		t.Fatalf("SessionID: %v", err)
	}
	if r.GetBody == nil {
		t.Fatal("GetBody is nil after caching request body")
	}

	for i := 0; i < 2; i++ {
		rc, err := r.GetBody()
		if err != nil {
			t.Fatalf("GetBody #%d: %v", i, err)
		}
		raw, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read GetBody #%d: %v", i, err)
		}
		if string(raw) != body {
			t.Errorf("GetBody #%d = %q, want %q", i, raw, body)
		}
	}

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read r.Body: %v", err)
	}
	if string(raw) != body {
		t.Errorf("r.Body = %q, want %q", raw, body)
	}
}

func TestSessionBodyParseErrorKeepsCache(t *testing.T) {
	const body = "{not json"
	r := newSessionRequest(t, http.MethodPost, body)

	session, err := SessionID(r)
	if err == nil {
		t.Fatal("SessionID succeeded on invalid JSON, want error")
	}
	if errors.Is(err, errBodyRead) {
		t.Errorf("err = %v, want plain parse error", err)
	}
	if session != "" {
		t.Errorf("session = %q, want empty", session)
	}
	if r.GetBody == nil {
		t.Fatal("GetBody is nil after failed JSON parse")
	}
	rc, err := r.GetBody()
	if err != nil {
		t.Fatalf("GetBody: %v", err)
	}
	raw, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read GetBody: %v", err)
	}
	if string(raw) != body {
		t.Errorf("cached body = %q, want %q", raw, body)
	}
}

func TestSessionMissingEverywhere(t *testing.T) {
	r := newSessionRequest(t, http.MethodPost, `{"model":"m"}`)

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID: %v", err)
	}
	if session != "" {
		t.Errorf("session = %q, want empty", session)
	}
}

func TestSessionGETWithoutBody(t *testing.T) {
	r := newSessionRequest(t, http.MethodGet, "")

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID on bodyless GET: %v", err)
	}
	if session != "" {
		t.Errorf("session = %q, want empty", session)
	}
	if r.GetBody != nil {
		t.Error("GetBody set for bodyless GET, want nil")
	}
}

func TestSessionGETWithBodySkipsCache(t *testing.T) {
	const body = `{"session_id":"s1"}`
	r := newSessionRequest(t, http.MethodGet, body)
	// 服务端接到的请求不带 GetBody；网关跳过缓存后应保持 nil。
	r.GetBody = nil

	session, err := SessionID(r)
	if err != nil {
		t.Fatalf("SessionID on GET with body: %v", err)
	}
	if session != "" {
		t.Errorf("session = %q, want empty", session)
	}
	if r.GetBody != nil {
		t.Error("GetBody set for GET with body, want nil")
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read r.Body: %v", err)
	}
	if string(raw) != body {
		t.Errorf("r.Body = %q, want untouched %q", raw, body)
	}
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestSessionBodyReadError(t *testing.T) {
	r := &http.Request{
		Method:        http.MethodPost,
		URL:           mustParseURL(t, "http://gateway.test/v1/chat/completions"),
		Header:        http.Header{},
		Body:          io.NopCloser(failingBody{}),
		ContentLength: 16,
	}
	r.Header.Set("X-Session-ID", "header-id")

	session, err := SessionID(r)
	if !errors.Is(err, errBodyRead) {
		t.Fatalf("err = %v, want errBodyRead", err)
	}
	if session != "" {
		t.Errorf("session = %q, want empty", session)
	}
}
