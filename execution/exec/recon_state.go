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

package exec

import (
	//"fmt"

	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	btree2 "github.com/anacrolix/btree"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
)

type reconPair struct {
	txNum      uint64 // txNum where the item has been created
	key1, key2 []byte
	val        []byte
}

func ReconnLess(i, thanItem reconPair) bool {
	if i.txNum == thanItem.txNum {
		c1 := bytes.Compare(i.key1, thanItem.key1)
		if c1 == 0 {
			c2 := bytes.Compare(i.key2, thanItem.key2)
			return c2 < 0
		}
		return c1 < 0
	}
	return i.txNum < thanItem.txNum
}

func reconnCmp(i, j reconPair) int {
	if ReconnLess(i, j) {
		return -1
	}
	if ReconnLess(j, i) {
		return 1
	}
	return 0
}

type ReconnWork struct {
	lock          sync.RWMutex
	doneBitmap    roaring64.Bitmap
	triggers      map[uint64][]*TxTask
	workCh        chan *TxTask
	queue         Queue[Task]
	rollbackCount uint64
	maxTxNum      uint64
}

// ReconState is the accumulator of changes to the state
type ReconState struct {
	*ReconnWork //has it's own mutex. allow avoid lock-contention between state.Get() and work.Done() methods

	lock         sync.RWMutex
	changes      map[string]*btree2.Map[reconPair, reconPair] // table => [] (txNum; key1; key2; val)
	sizeEstimate int
}

func NewReconState(workCh chan *TxTask) *ReconState {
	rs := &ReconState{
		ReconnWork: &ReconnWork{
			workCh:   workCh,
			triggers: map[uint64][]*TxTask{},
		},
		changes: map[string]*btree2.Map[reconPair, reconPair]{},
	}
	return rs
}

func (rs *ReconState) Reset(workCh chan *TxTask) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.workCh = workCh
	rs.triggers = map[uint64][]*TxTask{}
	rs.rollbackCount = 0
	rs.queue = rs.queue[:cap(rs.queue)]
	for i := 0; i < len(rs.queue); i++ {
		rs.queue[i] = nil
	}
	rs.queue = rs.queue[:0]
}

func (rs *ReconState) Put(table string, key1, key2, val []byte, txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		m := btree2.MakeMap[reconPair, reconPair](reconnCmp)
		t = &m
		rs.changes[table] = t
	}
	item := reconPair{key1: key1, key2: key2, val: val, txNum: txNum}
	_, old, replaced := t.Upsert(item, item)
	rs.sizeEstimate += len(key1) + len(key2) + len(val)
	if replaced {
		rs.sizeEstimate -= len(old.key1) + len(old.key2) + len(old.val)
	}
}

func (rs *ReconState) Delete(table string, key1, key2 []byte, txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		m := btree2.MakeMap[reconPair, reconPair](reconnCmp)
		t = &m
		rs.changes[table] = t
	}
	item := reconPair{key1: key1, key2: key2, val: nil, txNum: txNum}
	_, old, replaced := t.Upsert(item, item)
	rs.sizeEstimate += len(key1) + len(key2)
	if replaced {
		rs.sizeEstimate -= len(old.key1) + len(old.key2) + len(old.val)
	}
}

func (rs *ReconState) RemoveAll(table string, key1 []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		return
	}
	var toDelete []reconPair
	iter := t.Iterator()
	iter.SeekGE(reconPair{key1: key1, key2: nil})
	for ; iter.Valid(); iter.Next() {
		item := iter.Cur()
		if !bytes.Equal(item.key1, key1) {
			break
		}
		if item.key2 == nil {
			continue
		}
		toDelete = append(toDelete, item)
	}
	for _, item := range toDelete {
		t.Delete(item)
	}
}

func (rs *ReconState) Get(table string, key1, key2 []byte, txNum uint64) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	i, ok := t.Get(reconPair{txNum: txNum, key1: key1, key2: key2})
	if !ok {
		return nil
	}
	return i.val
}

func (rs *ReconState) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		iter := t.Iterator()
		for iter.First(); iter.Valid(); iter.Next() {
			item := iter.Cur()
			var composite []byte
			if item.key2 == nil {
				composite = make([]byte, 8+len(item.key1))
			} else {
				composite = make([]byte, 8+len(item.key1)+8+len(item.key2))
				binary.BigEndian.PutUint64(composite[8+len(item.key1):], state.FirstContractIncarnation)
				copy(composite[8+len(item.key1)+8:], item.key2)
			}
			binary.BigEndian.PutUint64(composite, item.txNum)
			copy(composite[8:], item.key1)
			if len(item.val) == 0 {
				if err := rwTx.Put(table, composite[:8], composite[8:]); err != nil {
					return err
				}
			} else {
				if err := rwTx.Put(table, composite, item.val); err != nil {
					return err
				}
			}
		}
		t.Reset()
	}
	rs.sizeEstimate = 0
	return nil
}

func (rs *ReconnWork) Schedule(ctx context.Context) (*TxTask, bool, error) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
Loop:
	for rs.queue.Len() < 16 {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case txTask, ok := <-rs.workCh:
			if !ok {
				// No more work, channel is closed
				break Loop
			}
			heap.Push(&rs.queue, txTask)
		}
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(*TxTask), true, nil
	}
	return nil, false, nil
}

func (rs *ReconnWork) CommitTxNum(txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if tt, ok := rs.triggers[txNum]; ok {
		for _, t := range tt {
			heap.Push(&rs.queue, t)
		}
		delete(rs.triggers, txNum)
	}
	rs.doneBitmap.Add(txNum)
	if txNum > rs.maxTxNum {
		rs.maxTxNum = txNum
	}
}

func (rs *ReconnWork) RollbackTx(txTask *TxTask, dependency uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if rs.doneBitmap.Contains(dependency) {
		heap.Push(&rs.queue, txTask)
	} else {
		tt := rs.triggers[dependency]
		tt = append(tt, txTask)
		rs.triggers[dependency] = tt
	}
	rs.rollbackCount++
}

func (rs *ReconnWork) Done(txNum uint64) bool {
	rs.lock.RLock()
	c := rs.doneBitmap.Contains(txNum)
	rs.lock.RUnlock()
	return c
}

func (rs *ReconnWork) DoneCount() uint64 {
	rs.lock.RLock()
	c := rs.doneBitmap.GetCardinality()
	rs.lock.RUnlock()
	return c
}

func (rs *ReconnWork) MaxTxNum() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.maxTxNum
}

func (rs *ReconnWork) RollbackCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.rollbackCount
}

func (rs *ReconnWork) QueueLen() int {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.queue.Len()
}

func (rs *ReconState) SizeEstimate() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return uint64(rs.sizeEstimate)
}
