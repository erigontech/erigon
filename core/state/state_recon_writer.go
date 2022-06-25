package state

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"golang.org/x/exp/constraints"
	"sync"
	"github.com/RoaringBitmap/roaring/roaring64"
	"container/heap"
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
	lock     sync.RWMutex
	workIterator roaring64.IntPeekable64
	doneBitmap roaring64.Bitmap
	triggers map[uint64][]uint64
	queue theap[uint64]
	changes map[string]map[string][]byte
}

func NewReconState(workBitmap *roaring64.Bitmap) *ReconState {
	rs := &ReconState{
		workIterator: workBitmap.Iterator(),
		triggers: map[uint64][]uint64{},
		changes: map[string]map[string][]byte{},
	}
	return rs
}

func (rs *ReconState) Put(table string,	key, val []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = map[string][]byte{}
		rs.changes[table] = t
	}
	t[string(key)] = val
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
			if err := rwTx.Put(table, []byte(ks), val); err != nil {
				return err
			}
		}
	}
	rs.changes = map[string]map[string][]byte{}
	return nil
}

func (rs *ReconState) HasWork() bool {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.queue.Len() > 0 || rs.workIterator.HasNext()
}

func (rs *ReconState) Schedule() uint64 {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	var txNum uint64
	if rs.queue.Len() < 16 && rs.workIterator.HasNext() {
		heap.Push(&rs.queue, rs.workIterator.Next())
	}
	if rs.queue.Len() > 0 {
		txNum = heap.Pop(&rs.queue).(uint64)
	}
	return txNum
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
	tt, _ := rs.triggers[dependency]
	tt = append(tt, txNum)
	rs.triggers[dependency] = tt
}

func (rs *ReconState) Done(txNum uint64) bool {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.doneBitmap.Contains(txNum)
}

type StateReconWriter struct {
	a     *libstate.Aggregator
	rs *ReconState
	txNum uint64
}

func NewStateReconWriter(a *libstate.Aggregator, rs *ReconState) *StateReconWriter {
	return &StateReconWriter{
		a: a,
		rs: rs,
	}
}

func (w *StateReconWriter) SetTxNum(txNum uint64) {
	w.txNum = txNum
	w.a.SetTxNum(txNum)
}

func (w *StateReconWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	found, txNum := w.a.MaxAccountsTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change account [%x] txNum = %d\n", address, txNum)
		return nil
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	w.rs.Put(kv.PlainState, address[:], value)
	return nil
}

func (w *StateReconWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	found, txNum := w.a.MaxCodeTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change code [%x] txNum = %d\n", address, txNum)
		return nil
	}
	w.rs.Put(kv.Code, codeHash[:], code)
	if len(code) > 0 {
		fmt.Printf("code [%x] => [%x] CodeHash: %x\n", address, code, codeHash)
		w.rs.Put(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], FirstContractIncarnation), codeHash[:])
	}
	return nil
}

func (w *StateReconWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	return nil
}

func (w *StateReconWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	found, txNum := w.a.MaxStorageTxNum(address.Bytes(), key.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change code [%x] [%x] txNum = %d\n", address, *key, txNum)
		return nil
	}
	v := value.Bytes()
	if len(v) != 0 {
		fmt.Printf("storage [%x] [%x] => [%x]\n", address, *key, v)
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), FirstContractIncarnation, key.Bytes())
		w.rs.Put(kv.PlainState, compositeKey, v)
	}
	return nil
}

func (w *StateReconWriter) CreateContract(address common.Address) error {
	return nil
}
