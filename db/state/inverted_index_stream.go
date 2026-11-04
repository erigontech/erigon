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

	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// InvertedIdxStreamFiles allows iteration over range of txn numbers
// Iteration is not implemented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
// InvertedIdxStreamFiles must be closed after use to prevent leaking of resources like cursor
type InvertedIdxStreamFiles struct {
	key                  []byte
	startTxNum, endTxNum int
	limit                int
	orderAscend          order.By

	seqIt      stream.Uno[uint64]
	indexTable string
	stack      []visibleFile

	nextN   uint64
	hasNext bool
	err     error

	seq       multiencseq.SequenceReader
	accessors statecfg.Accessors
	ii        *InvertedIndexRoTx
}

func (it *InvertedIdxStreamFiles) Close() {
	for _, item := range it.stack {
		item.reader.Close()
	}
}

func (it *InvertedIdxStreamFiles) advance() {
	if it.hasNext {
		it.advanceInFiles()
	}
}

func (it *InvertedIdxStreamFiles) HasNext() bool {
	if it.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if it.limit == 0 { // limit reached
		return false
	}
	return it.hasNext
}

func (it *InvertedIdxStreamFiles) Next() (uint64, error) { return it.next(), nil }

func (it *InvertedIdxStreamFiles) next() uint64 {
	it.limit--
	n := it.nextN
	it.advance()
	return n
}

func (it *InvertedIdxStreamFiles) CanSeek() bool { return it.limit < 0 }

// seekLinearSteps is how many elements Seek steps over instead of searching: a search in an Elias-Fano
// sequence costs more than a few steps, and targets that close are common when both inputs are dense.
const seekLinearSteps = 16

func (it *InvertedIdxStreamFiles) before(v uint64) bool {
	asc := bool(it.orderAscend)
	return it.err == nil && it.hasNext && (asc && it.nextN < v || !asc && it.nextN > v)
}

// near estimates from the density of the current file whether v is at most seekLinearSteps elements ahead.
func (it *InvertedIdxStreamFiles) near(v uint64) bool {
	if it.seqIt == nil {
		return false
	}
	dist := max(v, it.nextN) - min(v, it.nextN)
	span := it.seq.Max() - it.seq.Min() + 1
	return dist <= seekLinearSteps*span/it.seq.Count()
}

// Seek drops the files that end before v without looking the key up in them, then seeks inside the current file.
func (it *InvertedIdxStreamFiles) Seek(v uint64) error {
	if it.near(v) {
		for i := 0; i < seekLinearSteps && it.before(v); i++ {
			it.advanceInFiles()
		}
	}
	if !it.before(v) {
		return it.err
	}
	it.startTxNum = int(v)
	asc := bool(it.orderAscend)
	for len(it.stack) > 0 {
		top := it.stack[len(it.stack)-1]
		if asc && top.endTxNum > v || !asc && top.startTxNum <= v {
			break
		}
		top.reader.Close()
		it.stack = it.stack[:len(it.stack)-1]
	}
	if it.seqIt != nil {
		it.seqIt.(interface{ Seek(uint64) }).Seek(v)
	}
	it.advanceInFiles()
	return it.err
}

func (it *InvertedIdxStreamFiles) advanceInFiles() {
	for {
		for it.seqIt == nil {
			if len(it.stack) == 0 {
				it.hasNext = false
				return
			}
			item := it.stack[len(it.stack)-1]
			it.stack = it.stack[:len(it.stack)-1]
			offset, ok := item.reader.TwoLayerLookup(it.key)
			if !ok {
				continue
			}

			g := item.getter
			g.Reset(offset)
			k, _ := g.NextUncompressed()
			if !bytes.Equal(k, it.key) { // handle MPH false-positives
				continue
			}
			numSeqVal, _ := g.NextUncompressed()
			it.seq.Reset(item.startTxNum, numSeqVal)
			var seqIt stream.Uno[uint64]
			if it.orderAscend {
				seqIt = it.seq.Iterator(it.startTxNum)
			} else {
				seqIt = it.seq.ReverseIterator(it.startTxNum)
			}
			it.seqIt = seqIt
		}

		// Asc:  [from, to) AND from < to
		// Desc: [from, to) AND from > to
		if it.orderAscend {
			for it.seqIt.HasNext() {
				n, err := it.seqIt.Next()
				if err != nil {
					it.err = err
					return
				}
				isBeforeRange := int(n) < it.startTxNum
				if isBeforeRange { // skip
					continue
				}
				isAfterRange := it.endTxNum >= 0 && int(n) >= it.endTxNum
				if isAfterRange { // terminate
					it.hasNext = false
					return
				}
				it.hasNext = true
				it.nextN = n
				return
			}
		} else {
			for it.seqIt.HasNext() {
				n, err := it.seqIt.Next()
				if err != nil {
					it.err = err
					return
				}
				isAfterRange := it.startTxNum >= 0 && int(n) > it.startTxNum
				if isAfterRange { // skip
					continue
				}
				isBeforeRange := it.endTxNum >= 0 && int(n) <= it.endTxNum
				if isBeforeRange { // terminate
					it.hasNext = false
					return
				}
				it.hasNext = true
				it.nextN = n
				return
			}
		}
		it.seqIt = nil // Exhausted this iterator
	}
}
