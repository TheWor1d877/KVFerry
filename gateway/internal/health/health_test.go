package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthGetOK(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/plain; charset=utf-8", ct)
	}
	if body := rec.Body.String(); body != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}
}
