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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	btree2 "github.com/tidwall/btree"
)

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	DB_CURSOR
	RAM_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	cDup    kv.CursorDupSort
	cNonDup kv.Cursor

	iter         btree2.MapIter[string, dataWithPrevStep]
	dg           *seg.Reader
	dg2          *seg.Reader
	btCursor     *Cursor
	key          []byte
	val          []byte
	step         uint64
	startTxNum   uint64
	endTxNum     uint64
	latestOffset uint64     // offset of the latest value in the file
	t            CursorType // Whether this item represents state file or DB record, or tree
	reverse      bool
}

type CursorHeap []*CursorItem

func (ch CursorHeap) Len() int {
	return len(ch)
}

func (ch CursorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(ch[i].key, ch[j].key)
	if cmp == 0 {
		// when keys match, the items with later blocks are preferred
		if ch[i].reverse {
			return ch[i].endTxNum > ch[j].endTxNum
		}
		return ch[i].endTxNum < ch[j].endTxNum
	}
	return cmp < 0
}

func (ch *CursorHeap) Swap(i, j int) {
	(*ch)[i], (*ch)[j] = (*ch)[j], (*ch)[i]
}

func (ch *CursorHeap) Push(x interface{}) {
	*ch = append(*ch, x.(*CursorItem))
}

func (ch *CursorHeap) Pop() interface{} {
	old := *ch
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*ch = old[0 : n-1]
	return x
}

type DomainLatestIterFile struct {
	aggStep   uint64
	roTx      kv.Tx
	valsTable string

	limit       int
	largeVals   bool
	from, to    []byte
	orderAscend order.By

	h *CursorHeap

	nextKey, nextVal       []byte
	k, v, kBackup, vBackup []byte

	logger log.Logger
}

func (hi *DomainLatestIterFile) Close() {
}
func (hi *DomainLatestIterFile) Trace(prefix string) *stream.TracedDuo[[]byte, []byte] {
	return stream.TraceDuo(hi, hi.logger, "[dbg] DomainLatestIterFile.Next "+prefix)
}
func (hi *DomainLatestIterFile) init(dc *DomainRoTx) error {
	// Implementation:
	//     File endTxNum  = last txNum of file step
	//     DB endTxNum    = first txNum of step in db
	//     RAM endTxNum   = current txnum
	//  Example: stepSize=8, file=0-2.kv, db has key of step 2, current txn num is 17
	//     File endTxNum  = 15, because `0-2.kv` has steps 0 and 1, last txNum of step 1 is 15
	//     DB endTxNum    = 16, because db has step 2, and first txNum of step 2 is 16.
	//     RAM endTxNum   = 17, because current tcurrent txNum is 17
	hi.largeVals = dc.d.largeVals
	heap.Init(hi.h)
	var key, value []byte

	if dc.d.largeVals {
		valsCursor, err := hi.roTx.Cursor(dc.d.valsTable)
		if err != nil {
			return err
		}
		if key, value, err = valsCursor.Seek(hi.from); err != nil {
			return err
		}
		if key != nil && (hi.to == nil || bytes.Compare(key[:len(key)-8], hi.to) < 0) {
			k := key[:len(key)-8]
			stepBytes := key[len(key)-8:]
			step := ^binary.BigEndian.Uint64(stepBytes)
			endTxNum := step * dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files

			heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(value), cNonDup: valsCursor, endTxNum: endTxNum, reverse: true})
		}
	} else {
		valsCursor, err := hi.roTx.CursorDupSort(dc.d.valsTable)
		if err != nil {
			return err
		}

		if key, value, err = valsCursor.Seek(hi.from); err != nil {
			return err
		}
		if key != nil && (hi.to == nil || bytes.Compare(key, hi.to) < 0) {
			stepBytes := value[:8]
			value = value[8:]
			step := ^binary.BigEndian.Uint64(stepBytes)
			endTxNum := step * dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files

			heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(key), val: common.Copy(value), cDup: valsCursor, endTxNum: endTxNum, reverse: true})
		}
	}

	for i, item := range dc.files {
		// todo release btcursor when iter over/make it truly stateless
		btCursor, err := dc.statelessBtree(i).Seek(dc.statelessGetter(i), hi.from)
		if err != nil {
			return err
		}
		if btCursor == nil {
			continue
		}

		key := btCursor.Key()
		if key != nil && (hi.to == nil || bytes.Compare(key, hi.to) < 0) {
			val := btCursor.Value()
			txNum := item.endTxNum - 1 // !important: .kv files have semantic [from, t)
			heap.Push(hi.h, &CursorItem{t: FILE_CURSOR, key: key, val: val, btCursor: btCursor, endTxNum: txNum, reverse: true})
		}
	}
	return hi.advanceInFiles()
}

func (hi *DomainLatestIterFile) advanceInFiles() error {
	for hi.h.Len() > 0 {
		lastKey := (*hi.h)[0].key
		lastVal := (*hi.h)[0].val

		// Advance all the items that have this key (including the top)
		for hi.h.Len() > 0 && bytes.Equal((*hi.h)[0].key, lastKey) {
			ci1 := heap.Pop(hi.h).(*CursorItem)
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.btCursor.Next() {
					ci1.key = ci1.btCursor.Key()
					ci1.val = ci1.btCursor.Value()
					if ci1.key != nil && (hi.to == nil || bytes.Compare(ci1.key, hi.to) < 0) {
						heap.Push(hi.h, ci1)
					}
				}
			case DB_CURSOR:
				if hi.largeVals {
					// start from current go to next
					initial, v, err := ci1.cNonDup.Current()
					if err != nil {
						return err
					}
					var k []byte
					for initial != nil && (k == nil || bytes.Equal(initial[:len(initial)-8], k[:len(k)-8])) {
						k, v, err = ci1.cNonDup.Next()
						if err != nil {
							return err
						}
						if k == nil {
							break
						}
					}

					if len(k) > 0 && (hi.to == nil || bytes.Compare(k[:len(k)-8], hi.to) < 0) {
						stepBytes := k[len(k)-8:]
						k = k[:len(k)-8]
						ci1.key = common.Copy(k)
						step := ^binary.BigEndian.Uint64(stepBytes)
						endTxNum := step * hi.aggStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
						ci1.endTxNum = endTxNum

						ci1.val = common.Copy(v)
						heap.Push(hi.h, ci1)
					}
				} else {
					// start from current go to next
					k, stepBytesWithValue, err := ci1.cDup.NextNoDup()
					if err != nil {
						return err
					}

					if len(k) > 0 && (hi.to == nil || bytes.Compare(k, hi.to) < 0) {
						stepBytes := stepBytesWithValue[:8]
						v := stepBytesWithValue[8:]
						ci1.key = common.Copy(k)
						step := ^binary.BigEndian.Uint64(stepBytes)
						endTxNum := step * hi.aggStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
						ci1.endTxNum = endTxNum

						ci1.val = common.Copy(v)
						heap.Push(hi.h, ci1)
					}
				}

			}
		}
		if len(lastVal) > 0 {
			hi.nextKey, hi.nextVal = lastKey, lastVal
			return nil // founc
		}
	}
	hi.nextKey = nil
	return nil
}

func (hi *DomainLatestIterFile) HasNext() bool {
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	if hi.to == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	cmp := bytes.Compare(hi.nextKey, hi.to)
	return (bool(hi.orderAscend) && cmp < 0) || (!bool(hi.orderAscend) && cmp > 0)
}

func (hi *DomainLatestIterFile) Next() ([]byte, []byte, error) {
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advanceInFiles(); err != nil {
		return nil, nil, err
	}
	order.Asc.Assert(hi.kBackup, hi.nextKey)
	// TODO: remove `common.Copy`. it protecting from some existing bug. https://github.com/erigontech/erigon/issues/12672
	return common.Copy(hi.kBackup), common.Copy(hi.vBackup), nil
}
