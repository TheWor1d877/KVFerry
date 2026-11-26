package segment

import "testing"

func TestSequentialAllocate(t *testing.T) {
	bm := NewBitmap(8)
	for i := int64(0); i < 8; i++ {
		seq, ok := bm.Allocate()
		if !ok {
			t.Fatalf("allocate %d should succeed", i)
		}
		if seq != i {
			t.Errorf("allocate = %d, want %d", seq, i)
		}
		if bm.HighWater() != i+1 {
			t.Errorf("high water = %d, want %d", bm.HighWater(), i+1)
		}
	}
	if bm.UsedCount() != 8 {
		t.Errorf("used = %d, want 8", bm.UsedCount())
	}
}

func TestReleaseReturnsToBitmap(t *testing.T) {
	bm := NewBitmap(4)
	for i := 0; i < 4; i++ {
		bm.Allocate()
	}
	bm.Release(2)
	if bm.IsOccupied(2) {
		t.Error("released segment should be free")
	}
	if bm.UsedCount() != 3 {
		t.Errorf("used = %d, want 3", bm.UsedCount())
	}
	if bm.HighWater() != 4 {
		t.Errorf("high water should not shrink, got %d", bm.HighWater())
	}
}

func TestHoleReusedFirst(t *testing.T) {
	bm := NewBitmap(8)
	for i := 0; i < 5; i++ {
		bm.Allocate()
	}
	bm.Release(1)
	bm.Release(3)
	seq, ok := bm.Allocate()
	if !ok || seq != 1 {
		t.Errorf("first allocate after release = %d ok=%v, want 1 (lowest free bit)", seq, ok)
	}
	seq, _ = bm.Allocate()
	if seq != 3 {
		t.Errorf("second allocate = %d, want 3", seq)
	}
}

func TestExhausted(t *testing.T) {
	bm := NewBitmap(3)
	for i := 0; i < 3; i++ {
		if _, ok := bm.Allocate(); !ok {
			t.Fatalf("allocate %d should succeed", i)
		}
	}
	if _, ok := bm.Allocate(); ok {
		t.Error("allocate on exhausted bitmap should return false")
	}
	bm.Release(2)
	if seq, ok := bm.Allocate(); !ok || seq != 2 {
		t.Errorf("allocate after release = %d ok=%v, want 2", seq, ok)
	}
}

func TestMarkAndBoundary(t *testing.T) {
	bm := NewBitmap(64)
	bm.Mark(63)
	if !bm.IsOccupied(63) {
		t.Error("marked segment should be occupied")
	}
	if bm.HighWater() != 64 {
		t.Errorf("high water = %d, want 64", bm.HighWater())
	}
	bm.Mark(100)
	if bm.IsOccupied(100) {
		t.Error("out of range mark should be ignored")
	}
	bm.Release(-1)
	bm.Release(100)

	if bm.Total() != 64 {
		t.Errorf("total = %d, want 64", bm.Total())
	}
}
