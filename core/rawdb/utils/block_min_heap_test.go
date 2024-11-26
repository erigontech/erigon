package utils_test

import (
	"container/heap"
	"testing"

	"github.com/erigontech/erigon/core/rawdb/utils"
)

func TestPush(t *testing.T) {
	h := utils.NewBlockMinHeap(-1)
	heap.Push(h, &utils.BlockId{Number: 1, Hash: []byte{}})
	if h.Len() != 1 {
		t.Fatal("expected 1")
	}

	heap.Push(h, &utils.BlockId{Number: 2, Hash: []byte{}})
	if h.Len() != 2 {
		t.Fatal("expected 2")
	}
}

func TestPop(t *testing.T) {
	h := utils.NewBlockMinHeap(-1)
	heap.Push(h, &utils.BlockId{Number: 4, Hash: []byte{0x01}})
	heap.Push(h, &utils.BlockId{Number: 99, Hash: []byte{0x02}})
	heap.Push(h, &utils.BlockId{Number: 3, Hash: []byte{0x03}})

	x := heap.Pop(h).(*utils.BlockId)
	if x.Number != 3 || x.Hash[0] != 0x03 {
		t.Fatal("unexpected")
	}

	x = heap.Pop(h).(*utils.BlockId)
	if x.Number != 4 || x.Hash[0] != 0x01 {
		t.Fatal("unexpected")
	}

	x = h.Pop().(*utils.BlockId)
	if x.Number != 99 || x.Hash[0] != 0x02 {
		t.Fatal("unexpected")
	}
}

func TestPopWithLimits(t *testing.T) {
	h := utils.NewBlockMinHeap(2)
	heap.Push(h, &utils.BlockId{Number: 4, Hash: []byte{0x01}})
	heap.Push(h, &utils.BlockId{Number: 2, Hash: []byte{0x04}})
	heap.Push(h, &utils.BlockId{Number: 99, Hash: []byte{0x02}})
	heap.Push(h, &utils.BlockId{Number: 3, Hash: []byte{0x03}})

	if h.Len() != 2 {
		t.Fatal("expected 2 got ", h.Len())
	}
	x := heap.Pop(h).(*utils.BlockId)
	if x.Number != 4 || x.Hash[0] != 0x01 {
		t.Fatal("unexpected")
	}

	x = heap.Pop(h).(*utils.BlockId)
	if x.Number != 99 || x.Hash[0] != 0x02 {
		t.Fatal("unexpected")
	}
}

func TestPopWithEmptyHeap(t *testing.T) {
	h := utils.NewBlockMinHeap(0)
	heap.Push(h, &utils.BlockId{Number: 4, Hash: []byte{0x01}})
	if h.Len() != 0 {
		t.Fatal("expected 0")
	}
}
