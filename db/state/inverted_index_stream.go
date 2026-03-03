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

	seq       *multiencseq.SequenceReader
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

		//Asc:  [from, to) AND from < to
		//Desc: [from, to) AND from > to
		if it.orderAscend {
			for it.seqIt.HasNext() {
				n, err := it.seqIt.Next()
				if err != nil {
					it.err = err
					return
				}
				isBeforeRange := int(n) < it.startTxNum
				if isBeforeRange { //skip
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
				if isAfterRange { //skip
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
