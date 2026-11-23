package drill

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDrillEventJSONFields(t *testing.T) {
	cases := []struct {
		name   string
		event  Event
		action string
	}{
		{
			name:   "remove",
			event:  Event{TsMs: 1000, Node: "10.0.0.1:6379", Action: ActionRemove},
			action: "remove",
		},
		{
			name:   "recover",
			event:  Event{TsMs: 2000, Node: "10.0.0.2:6379", Action: ActionRecover},
			action: "recover",
		},
		{
			name:   "reroute",
			event:  Event{TsMs: 3000, Node: "10.0.0.3:6379", Action: ActionReroute, RequestID: "req-3"},
			action: "reroute",
		},
	}

	wantKeys := map[string]bool{
		"ts_ms":      true,
		"node":       true,
		"action":     true,
		"request_id": true,
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.event)
			if err != nil {
				t.Fatalf("marshal event: %v", err)
			}

			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatalf("unmarshal event: %v", err)
			}

			if len(fields) != len(wantKeys) {
				t.Fatalf("key count = %d, want %d: %s", len(fields), len(wantKeys), raw)
			}
			for key := range wantKeys {
				if _, ok := fields[key]; !ok {
					t.Fatalf("missing key %q in %s", key, raw)
				}
			}
			for key := range fields {
				if !wantKeys[key] {
					t.Fatalf("unexpected key %q in %s", key, raw)
				}
			}

			var action string
			if err := json.Unmarshal(fields["action"], &action); err != nil {
				t.Fatalf("unmarshal action: %v", err)
			}
			if action != tc.action {
				t.Fatalf("action = %q, want %q", action, tc.action)
			}

			var tsMs int64
			if err := json.Unmarshal(fields["ts_ms"], &tsMs); err != nil {
				t.Fatalf("unmarshal ts_ms: %v", err)
			}
			if tsMs != tc.event.TsMs {
				t.Fatalf("ts_ms = %d, want %d", tsMs, tc.event.TsMs)
			}
		})
	}
}

func TestDrillBufferSnapshotDrains(t *testing.T) {
	b := NewBuffer(1024)
	b.Record(Event{TsMs: 1, Node: "n1", Action: ActionRemove})
	b.Record(Event{TsMs: 2, Node: "n2", Action: ActionRecover})

	first := b.Snapshot()
	if len(first) != 2 {
		t.Fatalf("first snapshot len = %d, want 2", len(first))
	}
	if first[0].TsMs != 1 || first[1].TsMs != 2 {
		t.Fatalf("first snapshot order wrong: %+v", first)
	}

	second := b.Snapshot()
	if second == nil {
		t.Fatal("second snapshot is nil, want non-nil empty slice")
	}
	if len(second) != 0 {
		t.Fatalf("second snapshot len = %d, want 0", len(second))
	}
}

func TestDrillBufferOverflowDropsOldest(t *testing.T) {
	b := NewBuffer(1024)
	for i := 1; i <= 1025; i++ {
		b.Record(Event{TsMs: int64(i), Node: fmt.Sprintf("node-%d", i), Action: ActionRemove})
	}

	got := b.Snapshot()
	if len(got) != 1024 {
		t.Fatalf("snapshot len = %d, want 1024", len(got))
	}
	if got[0].TsMs != 2 {
		t.Fatalf("first event ts_ms = %d, want 2", got[0].TsMs)
	}
	if got[len(got)-1].TsMs != 1025 {
		t.Fatalf("last event ts_ms = %d, want 1025", got[len(got)-1].TsMs)
	}
}

func TestDrillBufferEmptyMarshalIsArray(t *testing.T) {
	raw, err := json.Marshal(NewBuffer(1024).Snapshot())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if string(raw) != "[]" {
		t.Fatalf("empty snapshot marshal = %s, want []", raw)
	}
}
