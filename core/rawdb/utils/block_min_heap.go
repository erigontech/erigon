package utils

import "container/heap"

// blockMinHeap stores a (limited-length) priority queue of blocks, wherein the "min" block is the one with lowest block num
// the queue will maintain a fixed length of blocks, discarding the lowest block num when limit is exceeded.
// In this sense, the min heap is not a priority queue but a jettisoning queue.
// -1 means unlimited length
type blockMinHeap struct {
	heap  []*BlockId
	limit int
}

type BlockId struct {
	Number uint64
	Hash   []byte
}

func NewBlockMinHeap(limit int) heap.Interface {
	return &blockMinHeap{limit: limit}
}

func (h *blockMinHeap) Len() int {
	return len(h.heap)
}

func (h *blockMinHeap) Less(i, j int) bool {
	return h.heap[i].Number < h.heap[j].Number
}

func (h *blockMinHeap) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

func (h *blockMinHeap) Push(x any) {
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

func (h *blockMinHeap) Pop() any {
	n := len(h.heap)
	x := h.heap[n-1]
	h.heap = h.heap[0 : n-1]
	return x
}
