package utils

import (
	"container/heap"

	"github.com/erigontech/erigon-lib/common"
)

// blockMaxHeap stores a (limited-length) priority queue of blocks.
// the queue will maintain a fixed length of blocks, discarding the lowest block num when limit is exceeded.
// Effectively the heap stores the maximum `limit` blocks (by block_num) among all blocks pushed.
// -1 means unlimited length
type blockMaxHeap struct {
	heap  []*BlockId
	limit int
}

type BlockId struct {
	Number uint64
	Hash   common.Hash
}

type ExtendedHeap interface {
	heap.Interface
	SortedValues() []*BlockId
}

func NewBlockMaxHeap(limit int) ExtendedHeap {
	return &blockMaxHeap{limit: limit}
}

func (h *blockMaxHeap) Len() int {
	return len(h.heap)
}

func (h *blockMaxHeap) Less(i, j int) bool {
	return h.heap[i].Number < h.heap[j].Number
}

func (h *blockMaxHeap) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

func (h *blockMaxHeap) Push(x any) {
	if h.limit == 0 {
		return
	}
	len := len(h.heap)
	if h.limit != -1 && len >= h.limit && len > 0 {
		if h.heap[0].Number < x.(*BlockId).Number {
			_ = heap.Pop(h)
		} else {
			// discard
			return
		}
	}
	h.heap = append(h.heap, x.(*BlockId))
}

func (h *blockMaxHeap) Pop() any {
	n := len(h.heap)
	x := h.heap[n-1]
	h.heap = h.heap[0 : n-1]
	return x
}

func (h *blockMaxHeap) copy() *blockMaxHeap {
	newHeap := NewBlockMaxHeap(h.limit)
	for _, b := range h.heap {
		heap.Push(newHeap, b)
	}
	return newHeap.(*blockMaxHeap)
}

func (h *blockMaxHeap) SortedValues() []*BlockId {
	// copy
	copyHeap := h.copy()
	res := make([]*BlockId, len(copyHeap.heap))
	for copyHeap.Len() > 0 {
		res[copyHeap.Len()-1] = heap.Pop(copyHeap).(*BlockId)
	}

	return res
}
