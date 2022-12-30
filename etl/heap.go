/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
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
	elems []HeapElem
}

func (h Heap) Len() int {
	return len(h.elems)
}

func (h Heap) Less(i, j int) bool {
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
	old[n-1] = HeapElem{}
	h.elems = old[0 : n-1]
	return x
}
