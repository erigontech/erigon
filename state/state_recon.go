/*
   Copyright 2022 Erigon contributors

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

package state

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

// Algorithms for reconstituting the state from state history

func (hc *HistoryContext) IsMaxTxNum(key []byte, txNum uint64) bool {
	var found bool
	var foundTxNum uint64
	hc.indexFiles.AscendGreaterOrEqual(ctxItem{startTxNum: txNum, endTxNum: txNum}, func(item ctxItem) bool {
		if item.endTxNum <= txNum {
			return true
		}
		if item.startTxNum > txNum {
			if !found || foundTxNum != txNum {
				return false
			}
		}
		if item.reader.Empty() {
			return true
		}
		offset := item.reader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
			eliasVal, _ := g.NextUncompressed()
			found = true
			foundTxNum = eliasfano32.Max(eliasVal)
			// if there is still chance to find higher ef.Max() than txNum, we continue
			return foundTxNum == txNum
		}
		return true
	})
	return found && txNum == foundTxNum
}

type ReconItem struct {
	g           *compress.Getter
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

// Less (part of heap.Interface) compares two links. For persisted links, those with the lower block heights get evicted first. This means that more recently persisted links are preferred.
// For non-persisted links, those with the highest block heights get evicted first. This is to prevent "holes" in the block heights that may cause inability to
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

type ScanIterator struct {
	hc        *HistoryContext
	h         ReconHeap
	nextKey   []byte
	fromKey   []byte
	toKey     []byte
	key       []byte
	uptoTxNum uint64
	nextTxNum uint64
	progress  uint64
	total     uint64
	hasNext   bool
}

func (si *ScanIterator) advance() {
	for si.h.Len() > 0 {
		top := heap.Pop(&si.h).(*ReconItem)
		key := top.key
		val, offset := top.g.NextUncompressed()
		si.progress += offset - top.lastOffset
		top.lastOffset = offset
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			if si.toKey == nil || bytes.Compare(top.key, si.toKey) <= 0 {
				heap.Push(&si.h, top)
			}
		}
		if !bytes.Equal(key, si.key) {
			si.key = key
			max := eliasfano32.Max(val)
			if max < si.uptoTxNum {
				si.nextTxNum = max
				si.nextKey = key
				si.hasNext = true
				return
			}
		}
	}
	si.hasNext = false
}

func (si *ScanIterator) HasNext() bool {
	return si.hasNext
}

func (si *ScanIterator) Next() ([]byte, uint64, uint64) {
	k, n, p := si.nextKey, si.nextTxNum, si.progress
	si.advance()
	return k, n, p
}

func (si *ScanIterator) Total() uint64 {
	return si.total
}

func (hc *HistoryContext) iterateReconTxs(fromKey, toKey []byte, uptoTxNum uint64) *ScanIterator {
	var si ScanIterator
	hc.indexFiles.Ascend(func(item ctxItem) bool {
		g := item.getter
		for g.HasNext() {
			key, offset := g.NextUncompressed()
			if fromKey == nil || bytes.Compare(key, fromKey) > 0 {
				heap.Push(&si.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key, startOffset: offset, lastOffset: offset})
				break
			} else {
				g.SkipUncompressed()
			}
		}
		si.total += uint64(item.getter.Size())
		return true
	})
	si.hc = hc
	si.fromKey = fromKey
	si.toKey = toKey
	si.uptoTxNum = uptoTxNum
	si.advance()
	return &si
}

type HistoryIterator struct {
	hc           *HistoryContext
	h            ReconHeap
	key          []byte
	val          []byte
	fromKey      []byte
	toKey        []byte
	txNum        uint64
	progress     uint64
	total        uint64
	hasNext      bool
	compressVals bool
}

func (hi *HistoryIterator) advance() {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		val, offset := top.g.NextUncompressed()
		hi.progress += offset - top.lastOffset
		top.lastOffset = offset
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			if hi.toKey == nil || bytes.Compare(top.key, hi.toKey) <= 0 {
				heap.Push(&hi.h, top)
			}
		}
		if bytes.Equal(hi.key, key) {
			continue
		}
		ef, _ := eliasfano32.ReadEliasFano(val)
		n, ok := ef.Search(hi.txNum)
		if !ok {
			continue
		}
		hi.key = key
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], n)
		historyItem, ok := hi.hc.historyFiles.Get(ctxItem{startTxNum: top.startTxNum, endTxNum: top.endTxNum})
		if !ok {
			panic(fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.key))
		}
		offset = historyItem.reader.Lookup2(txKey[:], hi.key)
		g := historyItem.getter
		g.Reset(offset)
		if hi.compressVals {
			hi.val, _ = g.Next(nil)
		} else {
			hi.val, _ = g.NextUncompressed()
		}
		hi.hasNext = true
		return
	}
	hi.hasNext = false
}

func (hi *HistoryIterator) HasNext() bool {
	return hi.hasNext
}

func (hi *HistoryIterator) Next() ([]byte, []byte, uint64) {
	k, v, p := hi.key, hi.val, hi.progress
	hi.advance()
	return k, v, p
}

func (hi *HistoryIterator) Total() uint64 {
	return hi.total
}

// Creates iterator that provides history values for the state just before transaction txNum
func (hc *HistoryContext) iterateHistoryBeforeTxNum(fromKey, toKey []byte, txNum uint64) *HistoryIterator {
	var hi HistoryIterator
	heap.Init(&hi.h)
	hc.indexFiles.Ascend(func(item ctxItem) bool {
		g := item.getter
		g.Reset(0)
		for g.HasNext() {
			key, offset := g.NextUncompressed()
			if fromKey == nil || bytes.Compare(key, fromKey) > 0 {
				heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
				break
			} else {
				g.SkipUncompressed()
			}
		}
		hi.total += uint64(item.getter.Size())
		return true
	})
	hi.hc = hc
	hi.compressVals = hc.h.compressVals
	hi.txNum = txNum
	hi.fromKey = fromKey
	hi.toKey = toKey
	hi.advance()
	return &hi
}
