package utils_test

import (
	"container/heap"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/rawdb/utils"
)

func TestPush(t *testing.T) {
	h := utils.NewBlockMaxHeap(-1)
	heap.Push(h, newB(1, []byte{}))
	if h.Len() != 1 {
		t.Fatal("expected 1")
	}

	heap.Push(h, newB(2, []byte{}))
	if h.Len() != 2 {
		t.Fatal("expected 2")
	}
}

func TestPop(t *testing.T) {
	h := utils.NewBlockMaxHeap(-1)
	heap.Push(h, newB(4, []byte{0x01}))
	heap.Push(h, newB(99, []byte{0x02}))
	heap.Push(h, newB(3, []byte{0x03}))

	x := heap.Pop(h).(*utils.BlockId)
	if x.Number != 3 || lastByte(x) != 0x03 {
		t.Fatal("unexpected")
	}

	x = heap.Pop(h).(*utils.BlockId)
	if x.Number != 4 || lastByte(x) != 0x01 {
		t.Fatal("unexpected")
	}

	x = h.Pop().(*utils.BlockId)
	if x.Number != 99 || lastByte(x) != 0x02 {
		t.Fatal("unexpected")
	}
}

func TestPopWithLimits(t *testing.T) {
	h := utils.NewBlockMaxHeap(2)
	heap.Push(h, newB(4, []byte{0x01}))
	heap.Push(h, newB(2, []byte{0x04}))
	heap.Push(h, newB(99, []byte{0x02}))
	heap.Push(h, newB(3, []byte{0x03}))

	if h.Len() != 2 {
		t.Fatal("expected 2 got ", h.Len())
	}
	x := heap.Pop(h).(*utils.BlockId)
	if x.Number != 4 || lastByte(x) != 0x01 {
		t.Fatal("unexpected")
	}

	x = heap.Pop(h).(*utils.BlockId)
	if x.Number != 99 || lastByte(x) != 0x02 {
		t.Fatal("unexpected")
	}
}

func TestPopWithEmptyHeap(t *testing.T) {
	h := utils.NewBlockMaxHeap(0)
	heap.Push(h, newB(4, []byte{0x01}))
	if h.Len() != 0 {
		t.Fatal("expected 0")
	}
}

func newB(id uint64, hash []byte) *utils.BlockId {
	return &utils.BlockId{Number: id, Hash: common.BytesToHash(hash)}
}

func lastByte(b *utils.BlockId) byte {
	by := b.Hash.Bytes()
	return by[len(by)-1]
}
