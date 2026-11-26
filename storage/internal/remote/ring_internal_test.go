package remote

import (
	"fmt"
	"reflect"
	"testing"
)

// TestRingConvergenceStable 覆盖同键收敛同节点（D50）：同一键反复取节点结果一致，
// 重启重建后的新环上同键仍映射同一节点。本文件为内部测试包：rebuild 未导出，
// 重启重建场景只能在此验证。
func TestRingConvergenceStable(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	ring := NewRing(nodes, nil)
	keys := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		keys = append(keys, fmt.Sprintf("hash-%02d", i))
	}
	first := make(map[string]string, len(keys))
	for _, k := range keys {
		node := ring.Pick(k)
		if node == "" {
			t.Fatalf("key %s picked no node", k)
		}
		first[k] = node
		for i := 0; i < 5; i++ {
			if again := ring.Pick(k); again != node {
				t.Fatalf("key %s drifted across picks: %s -> %s", k, node, again)
			}
		}
	}
	rebuilt := NewRing(nodes, nil)
	for _, k := range keys {
		if got := rebuilt.Pick(k); got != first[k] {
			t.Fatalf("key %s mapped to %s on rebuilt ring, want %s", k, got, first[k])
		}
	}
	if got := ring.Nodes(); !reflect.DeepEqual(got, nodes) {
		t.Errorf("ring nodes = %v, want %v", got, nodes)
	}
}

// TestRingRebuildKeepsMappingAfterRestart 覆盖成员变化后重启重建映射（D50、R27）：
// 三个节点全部重启后整环重建，同键仍映射到同一节点。
func TestRingRebuildKeepsMappingAfterRestart(t *testing.T) {
	nodes := []string{"remote-a", "remote-b", "remote-c"}
	ring := NewRing(nodes, nil)
	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6"}
	before := make(map[string]string, len(keys))
	for _, k := range keys {
		before[k] = ring.Pick(k)
	}
	// rebuild 未导出：语义限定为测试模拟全部远端节点重启后的整环一致重建（R27）。
	ring.rebuild(nodes)
	for _, k := range keys {
		if got := ring.Pick(k); got != before[k] {
			t.Fatalf("key %s remapped after restart rebuild: %s -> %s", k, before[k], got)
		}
	}
}
