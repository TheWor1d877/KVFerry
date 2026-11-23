package sticky

import (
	"fmt"
	"sync"
	"testing"
)

func TestStickyNewTableEmpty(t *testing.T) {
	table := NewTable()
	if node, ok := table.Lookup("any"); ok {
		t.Fatalf("Lookup on empty table = %q, true; want false", node)
	}
}

func TestStickyNewTableIsEmptyAfterAssign(t *testing.T) {
	first := NewTable()
	first.Assign("s1", "node-a")
	if node, ok := first.Lookup("s1"); !ok || node != "node-a" {
		t.Fatalf("Lookup(s1) on first table = %q, %v; want node-a, true", node, ok)
	}

	second := NewTable()
	if node, ok := second.Lookup("s1"); ok {
		t.Errorf("Lookup(s1) on new table = %q, true; want absent after restart", node)
	}
}

func TestStickyDropNodeOnEmptyTable(t *testing.T) {
	table := NewTable()
	table.DropNode("node-a")

	if node, ok := table.Lookup("any"); ok {
		t.Fatalf("Lookup after DropNode on empty table = %q, true; want false", node)
	}
}

func TestStickyAssignOverwrites(t *testing.T) {
	table := NewTable()
	table.Assign("s1", "node-a")
	table.Assign("s1", "node-b")

	node, ok := table.Lookup("s1")
	if !ok || node != "node-b" {
		t.Fatalf("Lookup(s1) = %q, %v; want node-b, true", node, ok)
	}
}

func TestStickyDropNode(t *testing.T) {
	table := NewTable()
	table.Assign("a", "node-a")
	table.Assign("b", "node-a")
	table.Assign("c", "node-b")

	table.DropNode("node-a")

	for _, session := range []string{"a", "b"} {
		if node, ok := table.Lookup(session); ok {
			t.Errorf("Lookup(%q) = %q, true; want absent after DropNode", session, node)
		}
	}
	if node, ok := table.Lookup("c"); !ok || node != "node-b" {
		t.Errorf("Lookup(c) = %q, %v; want node-b, true", node, ok)
	}
}

func TestStickyConcurrentAssignWithUnrelatedDrop(t *testing.T) {
	table := NewTable()
	const sessions = 200

	stop := make(chan struct{})
	var dropper sync.WaitGroup
	dropper.Add(1)
	go func() {
		defer dropper.Done()
		for {
			select {
			case <-stop:
				return
			default:
				table.DropNode("node-a")
			}
		}
	}()

	var writers sync.WaitGroup
	for i := 0; i < sessions; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			table.Assign(fmt.Sprintf("session-%d", i), "node-b")
		}(i)
	}
	writers.Wait()
	close(stop)
	dropper.Wait()

	for i := 0; i < sessions; i++ {
		session := fmt.Sprintf("session-%d", i)
		node, ok := table.Lookup(session)
		if !ok {
			t.Fatalf("Lookup(%q) missing after concurrent Assign", session)
		}
		if node != "node-b" {
			t.Fatalf("Lookup(%q) = %q, want node-b", session, node)
		}
	}
}

func TestStickyConcurrentAssignAndDropSameNode(t *testing.T) {
	table := NewTable()
	const rounds = 200

	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		session := fmt.Sprintf("session-%d", i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			table.Assign(session, "node-a")
		}()
		go func() {
			defer wg.Done()
			table.DropNode("node-a")
		}()
	}
	wg.Wait()

	for i := 0; i < rounds; i++ {
		session := fmt.Sprintf("session-%d", i)
		node, ok := table.Lookup(session)
		if ok && node != "node-a" {
			t.Fatalf("Lookup(%q) = %q, want node-a or absent", session, node)
		}
	}
}
