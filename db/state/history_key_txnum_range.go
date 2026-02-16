// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
)

func (ht *HistoryRoTx) iterateKeyTxNumFrozen(fromTxNum, toTxNum int, asc order.By, limit int) (stream.KU64, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	if len(ht.iit.files) == 0 {
		return stream.EmptyKU64, nil
	}

	if fromTxNum >= 0 && ht.iit.files.EndTxNum() <= uint64(fromTxNum) {
		return stream.EmptyKU64, nil
	}

	s := &HistoryKeyTxNumIterFiles{
		startTxNum: max(0, uint64(fromTxNum)),
		endTxNum:   toTxNum,
		limit:      limit,
	}
	for _, item := range ht.iit.files {
		if fromTxNum >= 0 && item.endTxNum <= uint64(fromTxNum) {
			continue
		}
		if toTxNum >= 0 && item.startTxNum >= uint64(toTxNum) {
			break
		}
		g := ht.iit.dataReader(item.src.decompressor)
		g.Reset(0)
		wrapper := NewSegReaderWrapper(g)
		if wrapper.HasNext() {
			key, val, err := wrapper.Next()
			if err != nil {
				s.Close()
				return nil, err
			}
			heap.Push(&s.h, &ReconItem{g: wrapper, key: key, val: val, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum})
		}
	}
	if err := s.advance(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (ht *HistoryRoTx) iterateKeyTxNumRecent(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.KU64, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	rangeIsInFiles := toTxNum >= 0 && len(ht.iit.files) > 0 && ht.iit.files.EndTxNum() >= uint64(toTxNum)
	if rangeIsInFiles {
		return stream.EmptyKU64, nil
	}
	s := &HistoryKeyTxNumIterDB{
		endTxNum:    toTxNum,
		roTx:        roTx,
		largeValues: ht.h.HistoryLargeValues,
		valsTable:   ht.h.ValuesTable,
		limit:       limit,
	}
	if fromTxNum >= 0 {
		s.startTxNum = uint64(fromTxNum)
		binary.BigEndian.PutUint64(s.startTxKey[:], uint64(fromTxNum))
	}
	if err := s.advance(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// HistoryKeyTxNumIterFiles emits (key, txNum) for every txNum at which a key changed.
// Unlike HistoryChangesIterFiles, it does not dedup across files and emits all txNums per key.
type HistoryKeyTxNumIterFiles struct {
	nextKey    []byte
	nextTxNum  uint64
	h          ReconHeap
	startTxNum uint64
	endTxNum   int

	k, kBackup []byte
	txNum      uint64
	err        error
	limit      int

	curKey    []byte
	curIdxVal []byte
	curSeq    multiencseq.SequenceReader
	curTxIter stream.U64
}

func (hi *HistoryKeyTxNumIterFiles) Close() {
	if hi.curTxIter != nil {
		hi.curTxIter.Close()
		hi.curTxIter = nil
	}
}

func (hi *HistoryKeyTxNumIterFiles) advance() error {
	for {
		if hi.curTxIter != nil && hi.curTxIter.HasNext() {
			txNum, err := hi.curTxIter.Next()
			if err != nil {
				return err
			}
			if hi.endTxNum < 0 || int(txNum) < hi.endTxNum {
				hi.nextKey = hi.curKey
				hi.nextTxNum = txNum
				return nil
			}
			// txNums are ascending — all remaining will also be >= endTxNum
			hi.curTxIter = nil
		}

		if hi.h.Len() == 0 {
			hi.nextKey = nil
			return nil
		}

		top := heap.Pop(&hi.h).(*ReconItem)
		key, idxVal := top.key, top.val

		if top.g.HasNext() {
			var err error
			top.key, top.val, err = top.g.Next()
			if err != nil {
				return err
			}
			heap.Push(&hi.h, top)
		}

		// Clone: segment reader reuses buffers
		hi.curKey = append(hi.curKey[:0], key...)
		hi.curIdxVal = append(hi.curIdxVal[:0], idxVal...)

		hi.curSeq.Reset(top.startTxNum, hi.curIdxVal)
		hi.curTxIter = hi.curSeq.Iterator(int(hi.startTxNum))
	}
}

func (hi *HistoryKeyTxNumIterFiles) HasNext() bool {
	return hi.err != nil || (hi.limit != 0 && hi.nextKey != nil)
}

func (hi *HistoryKeyTxNumIterFiles) Next() ([]byte, uint64, error) {
	if hi.err != nil {
		return nil, 0, hi.err
	}
	hi.limit--
	hi.k = append(hi.k[:0], hi.nextKey...)
	hi.txNum = hi.nextTxNum

	// Satisfy iter.Duo Invariant 2 for key buffer
	hi.k, hi.kBackup = hi.kBackup, hi.k
	if err := hi.advance(); err != nil {
		return nil, 0, err
	}
	return hi.kBackup, hi.txNum, nil
}

// HistoryKeyTxNumIterDB emits (key, txNum) for every txNum at which a key changed in the DB.
// Unlike HistoryChangesIterDB, it iterates ALL dups per key (not just the first one).
type HistoryKeyTxNumIterDB struct {
	largeValues     bool
	roTx            kv.Tx
	valsC           kv.Cursor
	valsCDup        kv.CursorDupSort
	valsTable       string
	limit, endTxNum int
	startTxNum      uint64
	startTxKey      [8]byte // startTxNum encoded as big-endian for cursor seeks

	nextKey   []byte
	nextTxNum uint64
	k         []byte
	txNum     uint64
	err       error
}

func (hi *HistoryKeyTxNumIterDB) Close() {
	if hi.valsC != nil {
		hi.valsC.Close()
	}
	if hi.valsCDup != nil {
		hi.valsCDup.Close()
	}
}

// setNext copies k (cursor ops invalidate previous return values).
func (hi *HistoryKeyTxNumIterDB) setNext(k []byte, txNum uint64) {
	hi.nextKey = common.Copy(k)
	hi.nextTxNum = txNum
}

func (hi *HistoryKeyTxNumIterDB) advance() error {
	if hi.largeValues {
		return hi.advanceLargeVals()
	}
	return hi.advanceSmallVals()
}

func (hi *HistoryKeyTxNumIterDB) seekNextSmallKey(k []byte) error {
	for k != nil {
		v, err := hi.valsCDup.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			return err
		}
		if v != nil {
			txNum := binary.BigEndian.Uint64(v[:8])
			if hi.endTxNum < 0 || int(txNum) < hi.endTxNum {
				hi.setNext(k, txNum)
				return nil
			}
		}
		// After SeekBothRange returns nil, cursor position is indeterminate;
		// use NextSubtree+Seek (not NextNoDup) to safely advance.
		next, ok := kv.NextSubtree(k)
		if !ok {
			break
		}
		k, _, err = hi.valsCDup.Seek(next)
		if err != nil {
			return err
		}
	}
	hi.nextKey = nil
	return nil
}

func (hi *HistoryKeyTxNumIterDB) advanceSmallVals() error {
	var err error
	if hi.valsCDup == nil {
		if hi.valsCDup, err = hi.roTx.CursorDupSort(hi.valsTable); err != nil {
			return err
		}
		k, _, err := hi.valsCDup.First()
		if err != nil {
			return err
		}
		return hi.seekNextSmallKey(k)
	}

	k, v, err := hi.valsCDup.NextDup()
	if err != nil {
		return err
	}
	if v != nil {
		txNum := binary.BigEndian.Uint64(v[:8])
		if hi.endTxNum < 0 || int(txNum) < hi.endTxNum {
			hi.setNext(k, txNum)
			return nil
		}
	}
	k, _, err = hi.valsCDup.NextNoDup()
	if err != nil {
		return err
	}
	return hi.seekNextSmallKey(k)
}

func (hi *HistoryKeyTxNumIterDB) advanceLargeVals() error {
	var err error
	if hi.valsC == nil {
		if hi.valsC, err = hi.roTx.Cursor(hi.valsTable); err != nil {
			return err
		}
		k, _, err := hi.valsC.First()
		if err != nil {
			return err
		}
		if k == nil {
			hi.nextKey = nil
			return nil
		}
		seek := append(common.Copy(k[:len(k)-8]), hi.startTxKey[:]...)
		k, _, err = hi.valsC.Seek(seek)
		if err != nil {
			return err
		}
		return hi.scanLargeVals(k)
	}

	k, _, err := hi.valsC.Next()
	if err != nil {
		return err
	}
	if k == nil {
		hi.nextKey = nil
		return nil
	}
	if hi.nextKey != nil && !bytes.Equal(k[:len(k)-8], hi.nextKey) {
		seek := append(common.Copy(k[:len(k)-8]), hi.startTxKey[:]...)
		k, _, err = hi.valsC.Seek(seek)
		if err != nil {
			return err
		}
	}
	return hi.scanLargeVals(k)
}

func (hi *HistoryKeyTxNumIterDB) scanLargeVals(k []byte) error {
	for k != nil {
		txNum := binary.BigEndian.Uint64(k[len(k)-8:])
		if hi.endTxNum >= 0 && int(txNum) >= hi.endTxNum {
			next, ok := kv.NextSubtree(k[:len(k)-8])
			if !ok {
				hi.nextKey = nil
				return nil
			}
			seek := append(next, hi.startTxKey[:]...)
			var err error
			k, _, err = hi.valsC.Seek(seek)
			if err != nil {
				return err
			}
			continue
		}
		if txNum < binary.BigEndian.Uint64(hi.startTxKey[:]) {
			seek := append(common.Copy(k[:len(k)-8]), hi.startTxKey[:]...)
			var err error
			k, _, err = hi.valsC.Seek(seek)
			if err != nil {
				return err
			}
			continue
		}
		hi.setNext(k[:len(k)-8], txNum)
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *HistoryKeyTxNumIterDB) HasNext() bool {
	return hi.err != nil || (hi.limit != 0 && hi.nextKey != nil)
}

func (hi *HistoryKeyTxNumIterDB) Next() ([]byte, uint64, error) {
	if hi.err != nil {
		return nil, 0, hi.err
	}
	hi.limit--
	hi.k, hi.txNum = hi.nextKey, hi.nextTxNum
	if err := hi.advance(); err != nil {
		return nil, 0, err
	}
	return hi.k, hi.txNum, nil
}
