package state

import (
	//"fmt"

	"container/heap"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"golang.org/x/exp/constraints"
	"sync"
)

type theap[T constraints.Ordered] []T

func (h theap[T]) Len() int {
	return len(h)
}

func (h theap[T]) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h theap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *theap[T]) Push(a interface{}) {
	*h = append(*h, a.(T))
}

func (h *theap[T]) Pop() interface{} {
	c := *h
	*h = c[:len(c)-1]
	return c[len(c)-1]
}

// ReconState is the accumulator of changes to the state
type ReconState struct {
	lock          sync.RWMutex
	workIterator  roaring64.IntPeekable64
	doneBitmap    roaring64.Bitmap
	triggers      map[uint64][]uint64
	queue         theap[uint64]
	changes       map[string]map[string][]byte
	sizeEstimate  uint64
	rollbackCount uint64
}

func NewReconState() *ReconState {
	rs := &ReconState{
		triggers: map[uint64][]uint64{},
		changes:  map[string]map[string][]byte{},
	}
	return rs
}

func (rs *ReconState) SetWorkBitmap(workBitmap *roaring64.Bitmap) {
	rs.workIterator = workBitmap.Iterator()
}

func (rs *ReconState) Put(table string, key, val []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = map[string][]byte{}
		rs.changes[table] = t
	}
	t[string(key)] = val
	rs.sizeEstimate += uint64(len(key)) + uint64(len(val))
}

func (rs *ReconState) Delete(table string, key []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = map[string][]byte{}
		rs.changes[table] = t
	}
	t[string(key)] = nil
	rs.sizeEstimate += uint64(len(key))
}

func (rs *ReconState) Get(table string, key []byte) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	return t[string(key)]
}

func (rs *ReconState) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		for ks, val := range t {
			if len(val) == 0 {
				if err := rwTx.Delete(table, []byte(ks), nil); err != nil {
					return err
				}
			} else {
				if err := rwTx.Put(table, []byte(ks), val); err != nil {
					return err
				}
			}
		}
	}
	rs.changes = map[string]map[string][]byte{}
	rs.sizeEstimate = 0
	return nil
}

func (rs *ReconState) Schedule() (uint64, bool) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for rs.queue.Len() < 16 && rs.workIterator.HasNext() {
		heap.Push(&rs.queue, rs.workIterator.Next())
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(uint64), true
	}
	return 0, false
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

func (rs *ReconState) RollbackTxNum(txNum, dependency uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if rs.doneBitmap.Contains(dependency) {
		heap.Push(&rs.queue, txNum)
	} else {
		tt, _ := rs.triggers[dependency]
		tt = append(tt, txNum)
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
	ac    *libstate.AggregatorContext
	rs    *ReconState
	txNum uint64
}

func NewStateReconWriter(ac *libstate.AggregatorContext, rs *ReconState) *StateReconWriter {
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
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.rs.Put(kv.PlainState, address[:], value)
	return nil
}

func (w *StateReconWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	found, txNum := w.ac.MaxCodeTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change code [%x] txNum = %d\n", address, txNum)
		return nil
	}
	w.rs.Put(kv.Code, codeHash[:], code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.rs.Put(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], FirstContractIncarnation), codeHash[:])
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
	v := value.Bytes()
	if len(v) != 0 {
		//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, v, w.txNum)
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), FirstContractIncarnation, key.Bytes())
		w.rs.Put(kv.PlainState, compositeKey, v)
	}
	return nil
}

func (w *StateReconWriter) CreateContract(address common.Address) error {
	return nil
}
