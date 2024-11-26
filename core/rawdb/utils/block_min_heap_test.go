package utils_test

import (
	"container/heap"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/rawdb/utils"
)

func NewB(id uint64, hash []byte) *utils.BlockId {
	return &utils.BlockId{Number: id, Hash: common.BytesToHash(hash)}
}

func LastByte(b *utils.BlockId) byte {
	by := b.Hash.Bytes()
	return by[len(by)-1]
}

func TestPush(t *testing.T) {
	h := utils.NewBlockMinHeap(-1)
	heap.Push(h, NewB(1, []byte{}))
	if h.Len() != 1 {
		t.Fatal("expected 1")
	}

	heap.Push(h, NewB(2, []byte{}))
	if h.Len() != 2 {
		t.Fatal("expected 2")
	}
}

func TestPop(t *testing.T) {
	h := utils.NewBlockMinHeap(-1)
	heap.Push(h, NewB(4, []byte{0x01}))
	heap.Push(h, NewB(99, []byte{0x02}))
	heap.Push(h, NewB(3, []byte{0x03}))

	x := heap.Pop(h).(*utils.BlockId)
	if x.Number != 3 || LastByte(x) != 0x03 {
		t.Fatal("unexpected")
	}

	x = heap.Pop(h).(*utils.BlockId)
	if x.Number != 4 || LastByte(x) != 0x01 {
		t.Fatal("unexpected")
	}

	x = h.Pop().(*utils.BlockId)
	if x.Number != 99 || LastByte(x) != 0x02 {
		t.Fatal("unexpected")
	}
}

func TestPopWithLimits(t *testing.T) {
	h := utils.NewBlockMinHeap(2)
	heap.Push(h, NewB(4, []byte{0x01}))
	heap.Push(h, NewB(2, []byte{0x04}))
	heap.Push(h, NewB(99, []byte{0x02}))
	heap.Push(h, NewB(3, []byte{0x03}))

	if h.Len() != 2 {
		t.Fatal("expected 2 got ", h.Len())
	}
	x := heap.Pop(h).(*utils.BlockId)
	if x.Number != 4 || LastByte(x) != 0x01 {
		t.Fatal("unexpected")
	}

	x = heap.Pop(h).(*utils.BlockId)
	if x.Number != 99 || LastByte(x) != 0x02 {
		t.Fatal("unexpected")
	}
}

func TestPopWithEmptyHeap(t *testing.T) {
	h := utils.NewBlockMinHeap(0)
	heap.Push(h, NewB(4, []byte{0x01}))
	if h.Len() != 0 {
		t.Fatal("expected 0")
	}
}
