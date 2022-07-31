package state

import (
	//"fmt"

	"bytes"
	"container/heap"
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type ReconStateItem struct {
	txNum      uint64 // txNum where the item has been created
	key1, key2 []byte
	val        []byte
}

func (i ReconStateItem) Less(than btree.Item) bool {
	thanItem := than.(ReconStateItem)
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

// ReconState is the accumulator of changes to the state
type ReconState struct {
	lock          sync.RWMutex
	doneBitmap    roaring64.Bitmap
	triggers      map[uint64][]*TxTask
	workCh        chan *TxTask
	queue         TxTaskQueue
	changes       map[string]*btree.BTree // table => [] (txNum; key1; key2; val)
	sizeEstimate  uint64
	rollbackCount uint64
}

func NewReconState(workCh chan *TxTask) *ReconState {
	rs := &ReconState{
		workCh:   workCh,
		triggers: map[uint64][]*TxTask{},
		changes:  map[string]*btree.BTree{},
	}
	return rs
}

func (rs *ReconState) Put(table string, key1, key2, val []byte, txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = btree.New(32)
		rs.changes[table] = t
	}
	item := ReconStateItem{key1: libcommon.Copy(key1), key2: libcommon.Copy(key2), val: libcommon.Copy(val), txNum: txNum}
	t.ReplaceOrInsert(item)
	rs.sizeEstimate += uint64(unsafe.Sizeof(item)) + uint64(len(key1)) + uint64(len(key2)) + uint64(len(val))
}

func (rs *ReconState) Get(table string, key1, key2 []byte, txNum uint64) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	i := t.Get(ReconStateItem{txNum: txNum, key1: key1, key2: key2})
	if i == nil {
		return nil
	}
	return i.(ReconStateItem).val
}

func (rs *ReconState) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		var err error
		t.Ascend(func(i btree.Item) bool {
			item := i.(ReconStateItem)
			if len(item.val) == 0 {
				return true
			}
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
			if err = rwTx.Put(table, composite, item.val); err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
		t.Clear(true)
	}
	rs.sizeEstimate = 0
	return nil
}

func (rs *ReconState) Schedule() (*TxTask, bool) {
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
		return heap.Pop(&rs.queue).(*TxTask), true
	}
	return nil, false
}

func (rs *ReconState) CommitTxNum(txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if tt, ok := rs.triggers[txNum]; ok {
		for _, t := range tt {
			heap.Push(&rs.queue, t)
		}
		delete(rs.triggers, txNum)
	}
	rs.doneBitmap.Add(txNum)
}

func (rs *ReconState) RollbackTx(txTask *TxTask, dependency uint64) {
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

func (rs *ReconState) Done(txNum uint64) bool {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.doneBitmap.Contains(txNum)
}

func (rs *ReconState) DoneCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.doneBitmap.GetCardinality()
}

func (rs *ReconState) RollbackCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.rollbackCount
}

func (rs *ReconState) SizeEstimate() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.sizeEstimate
}

type StateReconWriter struct {
	ac    *libstate.Aggregator22Context
	rs    *ReconState
	txNum uint64
}

func NewStateReconWriter(ac *libstate.Aggregator22Context, rs *ReconState) *StateReconWriter {
	return &StateReconWriter{
		ac: ac,
		rs: rs,
	}
}

func (w *StateReconWriter) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateReconWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	found, txNum := w.ac.MaxAccountsTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change account [%x] txNum = %d\n", address, txNum)
		return nil
	}
	value := make([]byte, account.EncodingLengthForStorage())
	if account.Incarnation > 0 {
		account.Incarnation = FirstContractIncarnation
	}
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.rs.Put(kv.PlainStateR, address[:], nil, value, w.txNum)
	return nil
}

func (w *StateReconWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	found, txNum := w.ac.MaxCodeTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change code [%x] %d %x txNum = %d\n", address, incarnation, codeHash, txNum)
		return nil
	}
	w.rs.Put(kv.CodeR, codeHash[:], nil, code, w.txNum)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => %d CodeHash: %x, txNum: %d\n", address, len(code), codeHash, w.txNum)
		w.rs.Put(kv.PlainContractR, dbutils.PlainGenerateStoragePrefix(address[:], FirstContractIncarnation), nil, codeHash[:], w.txNum)
	}
	return nil
}

func (w *StateReconWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	return nil
}

func (w *StateReconWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	found, txNum := w.ac.MaxStorageTxNum(address.Bytes(), key.Bytes())
	if !found {
		//fmt.Printf("no found storage [%x] [%x]\n", address, *key)
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change storage [%x] [%x] txNum = %d\n", address, *key, txNum)
		return nil
	}
	if !value.IsZero() {
		//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, value.Bytes(), w.txNum)
		w.rs.Put(kv.PlainStateR, address.Bytes(), key.Bytes(), value.Bytes(), w.txNum)
	}
	return nil
}

func (w *StateReconWriter) CreateContract(address common.Address) error {
	return nil
}
