package segment

import "math/bits"

// Bitmap 是空闲段位图，含高水位序列号，空闲段从最低位分配、释放回位图优先重用（D43、D49）。
type Bitmap struct {
	words     []uint64
	total     int64
	highWater int64
}

// NewBitmap 建立全空闲位图，段总数为 total。
func NewBitmap(total int64) *Bitmap {
	if total < 0 {
		total = 0
	}
	return &Bitmap{
		words: make([]uint64, (total+63)/64),
		total: total,
	}
}

// Allocate 从最低空闲位取段号并标记占用；耗尽返回 ok=false（D43）。
func (b *Bitmap) Allocate() (int64, bool) {
	for i, w := range b.words {
		if ^w == 0 {
			continue
		}
		bit := bits.TrailingZeros64(^w)
		seq := int64(i)*64 + int64(bit)
		if seq >= b.total {
			continue
		}
		b.words[i] |= 1 << uint(bit)
		if seq+1 > b.highWater {
			b.highWater = seq + 1
		}
		return seq, true
	}
	return 0, false
}

// Mark 标记段占用，用于对账补标（D43）；高水位随之抬升。
func (b *Bitmap) Mark(seq int64) {
	if seq < 0 || seq >= b.total {
		return
	}
	b.words[seq/64] |= 1 << uint(seq%64)
	if seq+1 > b.highWater {
		b.highWater = seq + 1
	}
}

// Release 释放段回位图，越界序号忽略（D43）。
func (b *Bitmap) Release(seq int64) {
	if seq < 0 || seq >= b.total {
		return
	}
	b.words[seq/64] &^= 1 << uint(seq%64)
}

// IsOccupied 返回段占用状态，越界序号视为未占用。
func (b *Bitmap) IsOccupied(seq int64) bool {
	if seq < 0 || seq >= b.total {
		return false
	}
	return b.words[seq/64]&(1<<uint(seq%64)) != 0
}

// HighWater 返回高水位序列号，即历史分配达到过的最大段序号加一（D49）。
func (b *Bitmap) HighWater() int64 {
	return b.highWater
}

// Total 返回位图段总数。
func (b *Bitmap) Total() int64 {
	return b.total
}

// UsedCount 返回当前占用段数。
func (b *Bitmap) UsedCount() int {
	n := 0
	for _, w := range b.words {
		n += bits.OnesCount64(w)
	}
	return n
}
