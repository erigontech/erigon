/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distwributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package etl

import (
	"bytes"
)

type HeapElem struct {
	Key     []byte
	Value   []byte
	TimeIdx int
}

type Heap struct {
	elems []*HeapElem
}

func (h *Heap) Len() int {
	return len(h.elems)
}

func (h *Heap) Less(i, j int) bool {
	if c := bytes.Compare(h.elems[i].Key, h.elems[j].Key); c != 0 {
		return c < 0
	}
	return h.elems[i].TimeIdx < h.elems[j].TimeIdx
}

func (h *Heap) Swap(i, j int) {
	h.elems[i], h.elems[j] = h.elems[j], h.elems[i]
}

func (h *Heap) Push(x *HeapElem) {
	h.elems = append(h.elems, x)
}

func (h *Heap) Pop() *HeapElem {
	old := h.elems
	n := len(old) - 1
	x := old[n]
	//old[n].Key, old[n].Value, old[n].TimeIdx = nil, nil, 0
	old[n] = nil
	h.elems = old[0:n]
	return x
}

// ------ Copy-Paste of `container/heap/heap.go` without interface conversion

// Init establishes the heap invariants required by the other routines in this package.
// Init is idempotent with respect to the heap invariants
// and may be called whenever the heap invariants may have been invalidated.
// The complexity is O(n) where n = h.Len().
func heapInit(h *Heap) {
	// heapify
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func heapPush(h *Heap, x *HeapElem) {
	h.Push(x)
	up(h, h.Len()-1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func heapPop(h *Heap) *HeapElem {
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	return h.Pop()
}

func up(h *Heap, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down(h *Heap, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
