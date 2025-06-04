// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"

	"github.com/erigontech/erigon-lib/seg"
)

// Algorithms for reconstituting the state from state history

type ReconItem struct {
	g           *seg.Reader
	key         []byte
	txNum       uint64
	startTxNum  uint64
	endTxNum    uint64
	startOffset uint64
	lastOffset  uint64
}

type ReconHeap []*ReconItem

func (rh ReconHeap) Len() int {
	return len(rh)
}

// Less (part of heap.Interface) compares two links. For persisted links, those with the lower block heights getBeforeTxNum evicted first. This means that more recently persisted links are preferred.
// For non-persisted links, those with the highest block heights getBeforeTxNum evicted first. This is to prevent "holes" in the block heights that may cause inability to
// insert headers in the ascending order of their block heights.
func (rh ReconHeap) Less(i, j int) bool {
	c := bytes.Compare(rh[i].key, rh[j].key)
	if c == 0 {
		return rh[i].txNum < rh[j].txNum
	}
	return c < 0
}

// Swap (part of heap.Interface) moves two links in the queue into each other's places. Note that each link has idx attribute that is getting adjusted during
// the swap. The idx attribute allows the removal of links from the middle of the queue (in case if links are getting invalidated due to
// failed verification of unavailability of parent headers)
func (rh ReconHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

// Push (part of heap.Interface) places a new link onto the end of queue. Note that idx attribute is set to the correct position of the new link
func (rh *ReconHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	l := x.(*ReconItem)
	*rh = append(*rh, l)
}

// Pop (part of heap.Interface) removes the first link from the queue
func (rh *ReconHeap) Pop() interface{} {
	old := *rh
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*rh = old[0 : n-1]
	return x
}

type ReconHeapOlderFirst struct {
	ReconHeap
}

func (rh ReconHeapOlderFirst) Less(i, j int) bool {
	c := bytes.Compare(rh.ReconHeap[i].key, rh.ReconHeap[j].key)
	if c == 0 {
		return rh.ReconHeap[i].txNum >= rh.ReconHeap[j].txNum
	}
	return c < 0
}
