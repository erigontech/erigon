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
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

// StateAsOfIter - returns state range at given time in history
type StateAsOfIterF struct {
	hc    *HistoryRoTx
	limit int

	from, toPrefix []byte
	nextVal        []byte
	nextKey        []byte

	h          ReconHeap
	startTxNum uint64
	startTxKey [8]byte
	txnKey     [8]byte

	k, v, kBackup, vBackup []byte
	orderAscend            order.By

	logger log.Logger
	ctx    context.Context
}

func (hi *StateAsOfIterF) Close() {
}

func (hi *StateAsOfIterF) init(files visibleFiles) error {
	for i, item := range files {
		if item.endTxNum <= hi.startTxNum {
			continue
		}
		// TODO: seek(from)
		g := seg.NewReader(item.src.decompressor.MakeGetter(), hi.hc.h.compression)

		idx := hi.hc.iit.statelessIdxReader(i)
		var offset uint64
		if len(hi.from) > 0 {
			n := item.src.decompressor.Count() / 2
			var ok bool
			offset, ok = g.BinarySearch(hi.from, n, idx.OrdinalLookup)
			if !ok {
				offset = 0
			}
		}
		g.Reset(offset)
		if g.HasNext() {
			key, offset := g.Next(nil)
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	binary.BigEndian.PutUint64(hi.startTxKey[:], hi.startTxNum)
	return hi.advanceInFiles()
}

func (hi *StateAsOfIterF) Trace(prefix string) *stream.TracedDuo[[]byte, []byte] {
	return stream.TraceDuo(hi, hi.logger, "[dbg] StateAsOfIterF.Next "+prefix)
}

func (hi *StateAsOfIterF) advanceInFiles() error {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		//if hi.compressVals {
		idxVal, _ = top.g.Next(nil)
		//} else {
		//	idxVal, _ = top.g.NextUncompressed()
		//}
		if top.g.HasNext() {
			//if hi.compressVals {
			top.key, _ = top.g.Next(nil)
			//} else {
			//	top.key, _ = top.g.NextUncompressed()
			//}
			if hi.toPrefix == nil || bytes.Compare(top.key, hi.toPrefix) < 0 {
				heap.Push(&hi.h, top)
			}
		}

		if hi.from != nil && bytes.Compare(key, hi.from) < 0 { //TODO: replace by seekInFiles()
			continue
		}

		if bytes.Equal(key, hi.nextKey) {
			continue
		}
		n, ok := eliasfano32.Seek(idxVal, hi.startTxNum)
		if !ok {
			continue
		}

		hi.nextKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFileDeprecated(top.startTxNum, top.endTxNum)
		if !ok {
			return fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextKey)
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset, ok := reader.Lookup2(hi.txnKey[:], hi.nextKey)
		if !ok {
			continue
		}
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		hi.nextVal, _ = g.Next(nil)
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *StateAsOfIterF) HasNext() bool {
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	if hi.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	cmp := bytes.Compare(hi.nextKey, hi.toPrefix)
	return (bool(hi.orderAscend) && cmp < 0) || (!bool(hi.orderAscend) && cmp > 0)
}

func (hi *StateAsOfIterF) Next() ([]byte, []byte, error) {
	select {
	case <-hi.ctx.Done():
		return nil, nil, hi.ctx.Err()
	default:
	}

	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy stream.Duo Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advanceInFiles(); err != nil {
		return nil, nil, err
	}
	hi.orderAscend.Assert(hi.kBackup, hi.nextKey)
	// TODO: remove `common.Copy`. it protecting from some existing bug. https://github.com/erigontech/erigon/issues/12672
	return common.Copy(hi.kBackup), common.Copy(hi.vBackup), nil
}

// StateAsOfIterDB - returns state range at given time in history
type StateAsOfIterDB struct {
	largeValues bool
	roTx        kv.Tx
	valsC       kv.Cursor
	valsCDup    kv.CursorDupSort
	valsTable   string

	from, toPrefix []byte
	orderAscend    order.By
	limit          int

	nextKey, nextVal []byte

	startTxNum uint64
	startTxKey [8]byte

	k, v, kBackup, vBackup []byte
	err                    error

	logger log.Logger
	ctx    context.Context
}

func (hi *StateAsOfIterDB) Close() {
	if hi.valsC != nil {
		hi.valsC.Close()
	}
}

func (hi *StateAsOfIterDB) Trace(prefix string) *stream.TracedDuo[[]byte, []byte] {
	return stream.TraceDuo(hi, hi.logger, "[dbg] StateAsOfIterDB.Next "+prefix)
}

func (hi *StateAsOfIterDB) advance() (err error) {
	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	if hi.largeValues {
		return hi.advanceLargeVals()
	}
	return hi.advanceSmallVals()
}
func (hi *StateAsOfIterDB) advanceLargeVals() error {
	var seek []byte
	var err error
	if hi.valsC == nil {
		if hi.valsC, err = hi.roTx.Cursor(hi.valsTable); err != nil {
			return err
		}
		firstKey, _, err := hi.valsC.Seek(hi.from)
		if err != nil {
			return err
		}
		if firstKey == nil {
			hi.nextKey = nil
			return nil
		}
		seek = append(common.Copy(firstKey[:len(firstKey)-8]), hi.startTxKey[:]...)
	} else {
		next, ok := kv.NextSubtree(hi.nextKey)
		if !ok {
			hi.nextKey = nil
			return nil
		}

		seek = append(next, hi.startTxKey[:]...)
	}
	for k, v, err := hi.valsC.Seek(seek); k != nil; k, v, err = hi.valsC.Seek(seek) {
		if err != nil {
			return err
		}
		if hi.toPrefix != nil && bytes.Compare(k[:len(k)-8], hi.toPrefix) >= 0 {
			break
		}
		if !bytes.Equal(seek[:len(k)-8], k[:len(k)-8]) {
			copy(seek[:len(k)-8], k[:len(k)-8])
			continue
		}
		hi.nextKey = k[:len(k)-8]
		hi.nextVal = v
		return nil
	}
	hi.nextKey = nil
	return nil
}
func (hi *StateAsOfIterDB) advanceSmallVals() error {
	var seek []byte
	var err error
	if hi.valsCDup == nil {
		if hi.valsCDup, err = hi.roTx.CursorDupSort(hi.valsTable); err != nil {
			return err
		}
		seek = hi.from
	} else {
		next, ok := kv.NextSubtree(hi.nextKey)
		if !ok {
			hi.nextKey = nil
			return nil
		}
		seek = next
	}
	for k, _, err := hi.valsCDup.Seek(seek); k != nil; k, _, err = hi.valsCDup.NextNoDup() {
		if err != nil {
			return err
		}
		if hi.toPrefix != nil && bytes.Compare(k, hi.toPrefix) >= 0 {
			break
		}
		v, err := hi.valsCDup.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			return err
		}
		if v == nil {
			continue
		}
		hi.nextKey = k
		hi.nextVal = v[8:]
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *StateAsOfIterDB) HasNext() bool {
	if hi.err != nil {
		return true
	}
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	if hi.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	cmp := bytes.Compare(hi.nextKey, hi.toPrefix)
	return (bool(hi.orderAscend) && cmp < 0) || (!bool(hi.orderAscend) && cmp > 0)
}

func (hi *StateAsOfIterDB) Next() ([]byte, []byte, error) {
	select {
	case <-hi.ctx.Done():
		return nil, nil, hi.ctx.Err()
	default:
	}

	if hi.err != nil {
		return nil, nil, hi.err
	}
	hi.limit--
	hi.k, hi.v = hi.nextKey, hi.nextVal

	// Satisfy stream.Duo Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advance(); err != nil {
		return nil, nil, err
	}
	hi.orderAscend.Assert(hi.kBackup, hi.nextKey)
	// TODO: remove `common.Copy`. it protecting from some existing bug. https://github.com/erigontech/erigon/issues/12672
	return common.Copy(hi.kBackup), common.Copy(hi.vBackup), nil
}

type HistoryChangesIterFiles struct {
	hc         *HistoryRoTx
	nextVal    []byte
	nextKey    []byte
	h          ReconHeap
	startTxNum uint64
	endTxNum   int
	startTxKey [8]byte
	txnKey     [8]byte

	k, v, kBackup, vBackup []byte
	err                    error
	limit                  int
}

func (hi *HistoryChangesIterFiles) Close() {
}

func (hi *HistoryChangesIterFiles) advance() error {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		//if hi.compressVals {
		idxVal, _ = top.g.Next(nil)
		//} else {
		//	idxVal, _ = top.g.NextUncompressed()
		//}
		if top.g.HasNext() {
			//if hi.compressVals {
			top.key, _ = top.g.Next(nil)
			//} else {
			//	top.key, _ = top.g.NextUncompressed()
			//}
			heap.Push(&hi.h, top)
		}

		if bytes.Equal(key, hi.nextKey) {
			continue
		}
		n, ok := eliasfano32.Seek(idxVal, hi.startTxNum)
		if !ok {
			continue
		}
		if int(n) >= hi.endTxNum {
			continue
		}

		hi.nextKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFileDeprecated(top.startTxNum, top.endTxNum)
		if !ok {
			return fmt.Errorf("HistoryChangesIterFiles: no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextKey)
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset, ok := reader.Lookup2(hi.txnKey[:], hi.nextKey)
		if !ok {
			continue
		}
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		hi.nextVal, _ = g.Next(nil)
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *HistoryChangesIterFiles) HasNext() bool {
	if hi.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	return true
}

func (hi *HistoryChangesIterFiles) Next() ([]byte, []byte, error) {
	if hi.err != nil {
		return nil, nil, hi.err
	}
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Duo Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advance(); err != nil {
		return nil, nil, err
	}
	fmt.Printf("[dbg] hist.Next: %x\n", hi.kBackup)
	return hi.kBackup, hi.vBackup, nil
}

type HistoryChangesIterDB struct {
	largeValues     bool
	roTx            kv.Tx
	valsC           kv.Cursor
	valsCDup        kv.CursorDupSort
	valsTable       string
	limit, endTxNum int
	startTxKey      [8]byte

	nextKey, nextVal []byte
	nextStep         uint64
	k, v             []byte
	step             uint64
	err              error
}

func (hi *HistoryChangesIterDB) Close() {
	if hi.valsC != nil {
		hi.valsC.Close()
	}
	if hi.valsCDup != nil {
		hi.valsCDup.Close()
	}
}
func (hi *HistoryChangesIterDB) advance() (err error) {
	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	if hi.largeValues {
		return hi.advanceLargeVals()
	}
	return hi.advanceSmallVals()
}

func (hi *HistoryChangesIterDB) advanceLargeVals() error {
	var seek []byte
	var err error
	if hi.valsC == nil {
		if hi.valsC, err = hi.roTx.Cursor(hi.valsTable); err != nil {
			return err
		}
		firstKey, _, err := hi.valsC.First()
		if err != nil {
			return err
		}
		if firstKey == nil {
			hi.nextKey = nil
			return nil
		}
		seek = append(common.Copy(firstKey[:len(firstKey)-8]), hi.startTxKey[:]...)
	} else {
		next, ok := kv.NextSubtree(hi.nextKey)
		if !ok {
			hi.nextKey = nil
			return nil
		}

		seek = append(next, hi.startTxKey[:]...)
	}
	for k, v, err := hi.valsC.Seek(seek); k != nil; k, v, err = hi.valsC.Seek(seek) {
		if err != nil {
			return err
		}
		if hi.endTxNum >= 0 && int(binary.BigEndian.Uint64(k[len(k)-8:])) >= hi.endTxNum {
			next, ok := kv.NextSubtree(k[:len(k)-8])
			if !ok {
				hi.nextKey = nil
				return nil
			}
			seek = append(next, hi.startTxKey[:]...)
			continue
		}
		if hi.nextKey != nil && bytes.Equal(k[:len(k)-8], hi.nextKey) && bytes.Equal(v, hi.nextVal) {
			// stuck on the same key, move to first key larger than seek
			for {
				k, v, err = hi.valsC.Next()
				if err != nil {
					return err
				}
				if k == nil {
					hi.nextKey = nil
					return nil
				}
				if bytes.Compare(seek[:len(seek)-8], k[:len(k)-8]) < 0 {
					break
				}
			}
		}
		//fmt.Printf("[seek=%x][RET=%t] '%x' '%x'\n", seek, bytes.Equal(seek[:len(seek)-8], k[:len(k)-8]), k, v)
		if !bytes.Equal(seek[:len(seek)-8], k[:len(k)-8]) /*|| int(binary.BigEndian.Uint64(k[len(k)-8:])) > hi.endTxNum */ {
			if len(seek) != len(k) {
				seek = append(append(seek[:0], k[:len(k)-8]...), hi.startTxKey[:]...)
				continue
			}
			copy(seek[:len(k)-8], k[:len(k)-8])
			continue
		}
		hi.nextKey = k[:len(k)-8]
		hi.nextVal = v
		return nil
	}
	hi.nextKey = nil
	return nil
}
func (hi *HistoryChangesIterDB) advanceSmallVals() (err error) {
	var k []byte
	if hi.valsCDup == nil {
		if hi.valsCDup, err = hi.roTx.CursorDupSort(hi.valsTable); err != nil {
			return err
		}

		if k, _, err = hi.valsCDup.First(); err != nil {
			return err
		}
	} else {
		if k, _, err = hi.valsCDup.NextNoDup(); err != nil {
			return err
		}
	}
	for ; k != nil; k, _, err = hi.valsCDup.NextNoDup() {
		if err != nil {
			return err
		}
		v, err := hi.valsCDup.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			return err
		}
		if v == nil {
			continue
		}
		foundTxNumVal := v[:8]
		if hi.endTxNum >= 0 && int(binary.BigEndian.Uint64(foundTxNumVal)) >= hi.endTxNum {
			continue
		}
		hi.nextKey = k
		hi.nextVal = v[8:]
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *HistoryChangesIterDB) HasNext() bool {
	if hi.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	return true
}

func (hi *HistoryChangesIterDB) Next() ([]byte, []byte, uint64, error) {
	if hi.err != nil {
		return nil, nil, 0, hi.err
	}
	hi.limit--
	hi.k, hi.v, hi.step = hi.nextKey, hi.nextVal, hi.nextStep
	if err := hi.advance(); err != nil {
		return nil, nil, 0, err
	}
	order.Asc.Assert(hi.k, hi.nextKey)
	return hi.k, hi.v, hi.step, nil
}
