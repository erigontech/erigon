package etl

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

type HeapElem struct {
	Key     []byte
	TimeIdx int
	Value   []byte
}

type Heap struct {
	comparator dbutils.CmpFunc
	elems      []HeapElem
}

func (h Heap) SetComparator(cmp dbutils.CmpFunc) {
	h.comparator = cmp
}

func (h Heap) Len() int {
	return len(h.elems)
}

func (h Heap) Less(i, j int) bool {
	if h.comparator != nil {
		if c := h.comparator(h.elems[i].Key, h.elems[j].Key, h.elems[i].Value, h.elems[j].Value); c != 0 {
			return c < 0
		}
		return h.elems[i].TimeIdx < h.elems[j].TimeIdx
	}

	if c := bytes.Compare(h.elems[i].Key, h.elems[j].Key); c != 0 {
		return c < 0
	}
	return h.elems[i].TimeIdx < h.elems[j].TimeIdx
}

func (h Heap) Swap(i, j int) {
	h.elems[i], h.elems[j] = h.elems[j], h.elems[i]
}

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	h.elems = append(h.elems, x.(HeapElem))
}

func (h *Heap) Pop() interface{} {
	old := h.elems
	n := len(old)
	x := old[n-1]
	h.elems = old[0 : n-1]
	return x
}
