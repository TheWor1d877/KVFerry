package drill

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDrillHTTPGetDrainsBuffer(t *testing.T) {
	b := NewBuffer(1024)
	b.Record(Event{TsMs: 111, Node: "10.0.0.1:6379", Action: ActionRemove})
	b.Record(Event{TsMs: 222, Node: "10.0.0.2:6379", Action: ActionReroute, RequestID: "req-222"})

	handler := Handler(b)

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/drill-events", nil))

	if first.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want 200", first.Code)
	}
	if ct := first.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("first GET Content-Type = %q, want application/json", ct)
	}

	var events []Event
	if err := json.Unmarshal(first.Body.Bytes(), &events); err != nil {
		t.Fatalf("decode first GET body: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("first GET event count = %d, want 2", len(events))
	}
	if events[0].TsMs != 111 || events[0].Node != "10.0.0.1:6379" || events[0].Action != ActionRemove || events[0].RequestID != "" {
		t.Fatalf("first event mismatch: %+v", events[0])
	}
	if events[1].TsMs != 222 || events[1].Node != "10.0.0.2:6379" || events[1].Action != ActionReroute || events[1].RequestID != "req-222" {
		t.Fatalf("second event mismatch: %+v", events[1])
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/drill-events", nil))

	if second.Code != http.StatusOK {
		t.Fatalf("second GET status = %d, want 200", second.Code)
	}
	if body := strings.TrimSpace(second.Body.String()); body != "[]" {
		t.Fatalf("second GET body = %q, want []", body)
	}
}

func TestDrillHTTPPostRejectedWithoutDrain(t *testing.T) {
	b := NewBuffer(1024)
	b.Record(Event{TsMs: 1, Node: "10.0.0.1:6379", Action: ActionRecover})

	handler := Handler(b)

	post := httptest.NewRecorder()
	handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/drill-events", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", post.Code)
	}

	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/drill-events", nil))

	var events []Event
	if err := json.Unmarshal(get.Body.Bytes(), &events); err != nil {
		t.Fatalf("decode GET body after POST: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("GET event count after POST = %d, want 1", len(events))
	}
	if events[0].TsMs != 1 || events[0].Node != "10.0.0.1:6379" || events[0].Action != ActionRecover {
		t.Fatalf("event after POST mismatch: %+v", events[0])
	}
}
