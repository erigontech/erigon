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
	"container/heap"
	"encoding/binary"

	"github.com/RoaringBitmap/roaring/roaring64"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/bitmapdb"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
)

// InvertedIdxStreamFiles allows iteration over range of txn numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
// InvertedIdxStreamFiles must be closed after use to prevent leaking of resources like cursor
type InvertedIdxStreamFiles struct {
	key                  []byte
	startTxNum, endTxNum int
	limit                int
	orderAscend          order.By

	efIt       stream.Uno[uint64]
	indexTable string
	stack      []visibleFile

	nextN   uint64
	hasNext bool
	err     error

	ef *eliasfano32.EliasFano
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
		for it.efIt == nil {
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
			if bytes.Equal(k, it.key) {
				eliasVal, _ := g.NextUncompressed()
				it.ef.Reset(eliasVal)
				var efiter *eliasfano32.EliasFanoIter
				if it.orderAscend {
					efiter = it.ef.Iterator()
				} else {
					efiter = it.ef.ReverseIterator()
				}
				if it.startTxNum > 0 {
					efiter.Seek(uint64(it.startTxNum))
				}
				it.efIt = efiter
			}
		}

		//Asc:  [from, to) AND from < to
		//Desc: [from, to) AND from > to
		if it.orderAscend {
			for it.efIt.HasNext() {
				n, err := it.efIt.Next()
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
			for it.efIt.HasNext() {
				n, err := it.efIt.Next()
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
		it.efIt = nil // Exhausted this iterator
	}
}

// RecentInvertedIdxIter allows iteration over range of txn numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
type RecentInvertedIdxIter struct {
	key                  []byte
	startTxNum, endTxNum int
	limit                int
	orderAscend          order.By

	roTx       kv.Tx
	cursor     kv.CursorDupSort
	indexTable string

	nextN   uint64
	hasNext bool
	err     error

	bm *roaring64.Bitmap
}

func (it *RecentInvertedIdxIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
	bitmapdb.ReturnToPool64(it.bm)
}

func (it *RecentInvertedIdxIter) advanceInDB() {
	var v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		var k []byte
		if k, _, err = it.cursor.SeekExact(it.key); err != nil {
			panic(err)
		}
		if k == nil {
			it.hasNext = false
			return
		}
		//Asc:  [from, to) AND from < to
		//Desc: [from, to) AND from > to
		var keyBytes [8]byte
		if it.startTxNum > 0 {
			binary.BigEndian.PutUint64(keyBytes[:], uint64(it.startTxNum))
		}
		if v, err = it.cursor.SeekBothRange(it.key, keyBytes[:]); err != nil {
			panic(err)
		}
		if v == nil {
			if !it.orderAscend {
				_, v, err = it.cursor.PrevDup()
				if err != nil {
					panic(err)
				}
			}
			if v == nil {
				it.hasNext = false
				return
			}
		}
	} else {
		if it.orderAscend {
			_, v, err = it.cursor.NextDup()
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
		} else {
			_, v, err = it.cursor.PrevDup()
			if err != nil {
				panic(err)
			}
		}
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	if it.orderAscend {
		for ; v != nil; _, v, err = it.cursor.NextDup() {
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
			n := binary.BigEndian.Uint64(v)
			if it.endTxNum >= 0 && int(n) >= it.endTxNum {
				it.hasNext = false
				return
			}
			if int(n) >= it.startTxNum {
				it.hasNext = true
				it.nextN = n
				return
			}
		}
	} else {
		for ; v != nil; _, v, err = it.cursor.PrevDup() {
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
			n := binary.BigEndian.Uint64(v)
			if int(n) <= it.endTxNum {
				it.hasNext = false
				return
			}
			if it.startTxNum >= 0 && int(n) <= it.startTxNum {
				it.hasNext = true
				it.nextN = n
				return
			}
		}
	}

	it.hasNext = false
}

func (it *RecentInvertedIdxIter) advance() {
	if it.hasNext {
		it.advanceInDB()
	}
}

func (it *RecentInvertedIdxIter) HasNext() bool {
	if it.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if it.limit == 0 { // limit reached
		return false
	}
	return it.hasNext
}

func (it *RecentInvertedIdxIter) Next() (uint64, error) {
	if it.err != nil {
		return 0, it.err
	}
	it.limit--
	n := it.nextN
	it.advance()
	return n, nil
}

type InvertedIterator1 struct {
	roTx           kv.Tx
	cursor         kv.CursorDupSort
	indexTable     string
	key            []byte
	h              ReconHeap
	nextKey        []byte
	nextFileKey    []byte
	nextDbKey      []byte
	endTxNum       uint64
	startTxNum     uint64
	startTxKey     [8]byte
	hasNextInDb    bool
	hasNextInFiles bool
}

func (it *InvertedIterator1) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}

func (it *InvertedIterator1) advanceInFiles() {
	for it.h.Len() > 0 {
		top := heap.Pop(&it.h).(*ReconItem)
		key := top.key
		val, _ := top.g.Next(nil)
		if top.g.HasNext() {
			top.key, _ = top.g.Next(nil)
			heap.Push(&it.h, top)
		}
		if !bytes.Equal(key, it.key) {
			ef, _ := eliasfano32.ReadEliasFano(val)
			_min := ef.Get(0)
			_max := ef.Max()
			if _min < it.endTxNum && _max >= it.startTxNum { // Intersection of [min; max) and [it.startTxNum; it.endTxNum)
				it.key = key
				it.nextFileKey = key
				return
			}
		}
	}
	it.hasNextInFiles = false
}

func (it *InvertedIterator1) advanceInDb() {
	var k, v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if k, _, err = it.cursor.First(); err != nil {
			// TODO pass error properly around
			panic(err)
		}
	} else {
		if k, _, err = it.cursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	for k != nil {
		if v, err = it.cursor.SeekBothRange(k, it.startTxKey[:]); err != nil {
			panic(err)
		}
		if v != nil {
			txNum := binary.BigEndian.Uint64(v)
			if txNum < it.endTxNum {
				it.nextDbKey = append(it.nextDbKey[:0], k...)
				return
			}
		}
		if k, _, err = it.cursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	it.cursor.Close()
	it.cursor = nil
	it.hasNextInDb = false
}

func (it *InvertedIterator1) advance() {
	if it.hasNextInFiles {
		if it.hasNextInDb {
			c := bytes.Compare(it.nextFileKey, it.nextDbKey)
			if c < 0 {
				it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
				it.advanceInFiles()
			} else if c > 0 {
				it.nextKey = append(it.nextKey[:0], it.nextDbKey...)
				it.advanceInDb()
			} else {
				it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
				it.advanceInDb()
				it.advanceInFiles()
			}
		} else {
			it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
			it.advanceInFiles()
		}
	} else if it.hasNextInDb {
		it.nextKey = append(it.nextKey[:0], it.nextDbKey...)
		it.advanceInDb()
	} else {
		it.nextKey = nil
	}
}

func (it *InvertedIterator1) HasNext() bool {
	return it.hasNextInFiles || it.hasNextInDb || it.nextKey != nil
}

func (it *InvertedIterator1) Next(keyBuf []byte) []byte {
	result := append(keyBuf, it.nextKey...)
	it.advance()
	return result
}
