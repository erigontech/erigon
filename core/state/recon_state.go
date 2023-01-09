package state

import (
	//"fmt"

	"bytes"
	"container/heap"
	"encoding/binary"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	btree2 "github.com/tidwall/btree"
)

type reconPair struct {
	txNum      uint64 // txNum where the item has been created
	key1, key2 []byte
	val        []byte
}

func (i reconPair) Less(than btree.Item) bool { return ReconnLess(i, than.(reconPair)) }

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

type ReconnWork struct {
	lock          sync.RWMutex
	doneBitmap    roaring64.Bitmap
	triggers      map[uint64][]*exec22.TxTask
	workCh        chan *exec22.TxTask
	queue         exec22.TxTaskQueue
	rollbackCount uint64
	maxTxNum      uint64
}

// ReconState is the accumulator of changes to the state
type ReconState struct {
	*ReconnWork //has it's own mutex. allow avoid lock-contention between state.Get() and work.Done() methods

	lock         sync.RWMutex
	changes      map[string]*btree2.BTreeG[reconPair] // table => [] (txNum; key1; key2; val)
	hints        map[string]*btree2.PathHint
	sizeEstimate uint64
}

func NewReconState(workCh chan *exec22.TxTask) *ReconState {
	rs := &ReconState{
		ReconnWork: &ReconnWork{
			workCh:   workCh,
			triggers: map[uint64][]*exec22.TxTask{},
		},
		changes: map[string]*btree2.BTreeG[reconPair]{},
		hints:   map[string]*btree2.PathHint{},
	}
	return rs
}

func (rs *ReconState) Reset(workCh chan *exec22.TxTask) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.workCh = workCh
	rs.triggers = map[uint64][]*exec22.TxTask{}
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
		t = btree2.NewBTreeGOptions[reconPair](ReconnLess, btree2.Options{Degree: 128, NoLocks: true})
		rs.changes[table] = t
		rs.hints[table] = &btree2.PathHint{}
	}
	item := reconPair{key1: key1, key2: key2, val: val, txNum: txNum}
	old, ok := t.SetHint(item, rs.hints[table])
	rs.sizeEstimate += btreeOverhead + uint64(len(key1)) + uint64(len(key2)) + uint64(len(val))
	if ok {
		rs.sizeEstimate -= btreeOverhead + uint64(len(old.key1)) + uint64(len(old.key2)) + uint64(len(old.val))
	}
}

func (rs *ReconState) Delete(table string, key1, key2 []byte, txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = btree2.NewBTreeGOptions[reconPair](ReconnLess, btree2.Options{Degree: 128, NoLocks: true})
		rs.changes[table] = t
		rs.hints[table] = &btree2.PathHint{}
	}
	item := reconPair{key1: key1, key2: key2, val: nil, txNum: txNum}
	old, ok := t.SetHint(item, rs.hints[table])
	rs.sizeEstimate += btreeOverhead + uint64(len(key1)) + uint64(len(key2))
	if ok {
		rs.sizeEstimate -= btreeOverhead + uint64(len(old.key1)) + uint64(len(old.key2)) + uint64(len(old.val))
	}
}

func (rs *ReconState) RemoveAll(table string, key1 []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		return
	}
	t.Ascend(reconPair{key1: key1, key2: nil}, func(item reconPair) bool {
		if !bytes.Equal(item.key1, key1) {
			return false
		}
		if item.key2 == nil {
			return true
		}
		t.Delete(item)
		return true
	})
}

func (rs *ReconState) Get(table string, key1, key2 []byte, txNum uint64) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	i, ok := t.GetHint(reconPair{txNum: txNum, key1: key1, key2: key2}, rs.hints[table])
	if !ok {
		return nil
	}
	return i.val
}

func (rs *ReconState) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		var err error
		t.Walk(func(items []reconPair) bool {
			for _, item := range items {
				var composite []byte
				if item.key2 == nil {
					composite = make([]byte, 8+len(item.key1))
				} else {
					composite = make([]byte, 8+len(item.key1)+8+len(item.key2))
					binary.BigEndian.PutUint64(composite[8+len(item.key1):], FirstContractIncarnation)
					copy(composite[8+len(item.key1)+8:], item.key2)
				}
				binary.BigEndian.PutUint64(composite, item.txNum)
				copy(composite[8:], item.key1)
				if len(item.val) == 0 {
					if err = rwTx.Put(table, composite[:8], composite[8:]); err != nil {
						return false
					}
				} else {
					if err = rwTx.Put(table, composite, item.val); err != nil {
						return false
					}
				}
			}
			return true
		})
		if err != nil {
			return err
		}
		t.Clear()
	}
	rs.sizeEstimate = 0
	return nil
}

func (rs *ReconnWork) Schedule() (*exec22.TxTask, bool) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for rs.queue.Len() < 16 {
		txTask, ok := <-rs.workCh
		if !ok {
			// No more work, channel is closed
			break
		}
		heap.Push(&rs.queue, txTask)
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(*exec22.TxTask), true
	}
	return nil, false
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

func (rs *ReconnWork) RollbackTx(txTask *exec22.TxTask, dependency uint64) {
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
	return rs.sizeEstimate
}
