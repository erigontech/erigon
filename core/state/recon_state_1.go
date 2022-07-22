package state

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/params"
)

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type TxTask struct {
	TxNum              uint64
	BlockNum           uint64
	Rules              *params.Rules
	Header             *types.Header
	Block              *types.Block
	BlockHash          common.Hash
	Sender             *common.Address
	TxIndex            int // -1 for block initialisation
	Final              bool
	Tx                 types.Transaction
	BalanceIncreaseSet map[common.Address]uint256.Int
	ReadLists          map[string]*KvList
	WriteLists         map[string]*KvList
	ResultsSize        int64
	Error              error
}

type TxTaskQueue []TxTask

func (h TxTaskQueue) Len() int {
	return len(h)
}

func (h TxTaskQueue) Less(i, j int) bool {
	return h[i].TxNum < h[j].TxNum
}

func (h TxTaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TxTaskQueue) Push(a interface{}) {
	*h = append(*h, a.(TxTask))
}

func (h *TxTaskQueue) Pop() interface{} {
	c := *h
	*h = c[:len(c)-1]
	return c[len(c)-1]
}

const CodeSizeTable = "CodeSize"

type ReconState1 struct {
	lock         sync.RWMutex
	receiveWork  *sync.Cond
	triggers     map[uint64]TxTask
	senderTxNums map[common.Address]uint64
	triggerLock  sync.RWMutex
	queue        TxTaskQueue
	queueLock    sync.Mutex
	changes      map[string]*btree.BTreeG[ReconStateItem1]
	sizeEstimate uint64
	txsDone      uint64
	finished     bool
}

type ReconStateItem1 struct {
	key []byte
	val []byte
}

func reconStateItem1Less(i, j ReconStateItem1) bool {
	return bytes.Compare(i.key, j.key) < 0
}

func NewReconState1() *ReconState1 {
	rs := &ReconState1{
		triggers:     map[uint64]TxTask{},
		senderTxNums: map[common.Address]uint64{},
		changes:      map[string]*btree.BTreeG[ReconStateItem1]{},
	}
	rs.receiveWork = sync.NewCond(&rs.queueLock)
	return rs
}

func (rs *ReconState1) put(table string, key, val []byte) {
	t, ok := rs.changes[table]
	if !ok {
		t = btree.NewG[ReconStateItem1](32, reconStateItem1Less)
		rs.changes[table] = t
	}
	item := ReconStateItem1{key: libcommon.Copy(key), val: libcommon.Copy(val)}
	t.ReplaceOrInsert(item)
	rs.sizeEstimate += uint64(unsafe.Sizeof(item)) + uint64(len(key)) + uint64(len(val))
}

func (rs *ReconState1) Get(table string, key []byte) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.get(table, key)
}

func (rs *ReconState1) get(table string, key []byte) []byte {
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	if i, ok := t.Get(ReconStateItem1{key: key}); ok {
		return i.val
	}
	return nil
}

func (rs *ReconState1) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		var err error
		t.Ascend(func(item ReconStateItem1) bool {
			if len(item.val) == 0 {
				if err = rwTx.Delete(table, item.key, nil); err != nil {
					return false
				}
				//fmt.Printf("Flush [%x]=>\n", ks)
			} else {
				if err = rwTx.Put(table, item.key, item.val); err != nil {
					return false
				}
				//fmt.Printf("Flush [%x]=>[%x]\n", ks, val)
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

func (rs *ReconState1) Schedule() (TxTask, bool) {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	for !rs.finished && rs.queue.Len() == 0 {
		rs.receiveWork.Wait()
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(TxTask), true
	}
	return TxTask{}, false
}

func (rs *ReconState1) RegisterSender(txTask TxTask) bool {
	rs.triggerLock.Lock()
	defer rs.triggerLock.Unlock()
	lastTxNum, deferral := rs.senderTxNums[*txTask.Sender]
	if deferral {
		// Transactions with the same sender have obvious data dependency, no point running it before lastTxNum
		// So we add this data dependency as a trigger
		//fmt.Printf("trigger[%d] sender [%x]<=%x\n", lastTxNum, *txTask.Sender, txTask.Tx.Hash())
		rs.triggers[lastTxNum] = txTask
	}
	//fmt.Printf("senderTxNums[%x]=%d\n", *txTask.Sender, txTask.TxNum)
	rs.senderTxNums[*txTask.Sender] = txTask.TxNum
	return !deferral
}

func (rs *ReconState1) CommitTxNum(sender *common.Address, txNum uint64) uint64 {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	rs.triggerLock.Lock()
	defer rs.triggerLock.Unlock()
	count := uint64(0)
	if triggered, ok := rs.triggers[txNum]; ok {
		heap.Push(&rs.queue, triggered)
		rs.receiveWork.Signal()
		count++
		delete(rs.triggers, txNum)
	}
	if sender != nil {
		if lastTxNum, ok := rs.senderTxNums[*sender]; ok && lastTxNum == txNum {
			// This is the last transaction so far with this sender, remove
			delete(rs.senderTxNums, *sender)
		}
	}
	rs.txsDone++
	return count
}

func (rs *ReconState1) AddWork(txTask TxTask) {
	txTask.BalanceIncreaseSet = nil
	txTask.ReadLists = nil
	txTask.WriteLists = nil
	txTask.ResultsSize = 0
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	heap.Push(&rs.queue, txTask)
	rs.receiveWork.Signal()
}

func (rs *ReconState1) Finish() {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	rs.finished = true
	rs.receiveWork.Broadcast()
}

func (rs *ReconState1) Apply(emptyRemoval bool, roTx kv.Tx, txTask TxTask) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if txTask.WriteLists != nil {
		for table, list := range txTask.WriteLists {
			for i, key := range list.Keys {
				val := list.Vals[i]
				rs.put(table, key, val)
			}
		}
	}
	for addr, increase := range txTask.BalanceIncreaseSet {
		//if increase.IsZero() {
		//	continue
		//}
		enc := rs.get(kv.PlainState, addr.Bytes())
		if enc == nil {
			var err error
			enc, err = roTx.GetOne(kv.PlainState, addr.Bytes())
			if err != nil {
				return err
			}
		}
		var a accounts.Account
		if err := a.DecodeForStorage(enc); err != nil {
			return err
		}
		a.Balance.Add(&a.Balance, &increase)
		if emptyRemoval && a.Nonce == 0 && a.Balance.IsZero() && a.IsEmptyCodeHash() {
			enc = []byte{}
		} else {
			l := a.EncodingLengthForStorage()
			enc = make([]byte, l)
			a.EncodeForStorage(enc)
		}
		rs.put(kv.PlainState, addr.Bytes(), enc)
	}
	return nil
}

func (rs *ReconState1) DoneCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.txsDone
}

func (rs *ReconState1) SizeEstimate() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.sizeEstimate
}

func (rs *ReconState1) ReadsValid(readLists map[string]*KvList) bool {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	//fmt.Printf("ValidReads\n")
	for table, list := range readLists {
		//fmt.Printf("Table %s\n", table)
		var t *btree.BTreeG[ReconStateItem1]
		var ok bool
		if table == CodeSizeTable {
			t, ok = rs.changes[kv.Code]
		} else {
			t, ok = rs.changes[table]
		}
		if !ok {
			continue
		}
		for i, key := range list.Keys {
			val := list.Vals[i]
			if item, ok := t.Get(ReconStateItem1{key: key}); ok {
				//fmt.Printf("key [%x] => [%x] vs [%x]\n", key, val, rereadVal)
				if table == CodeSizeTable {
					if binary.BigEndian.Uint64(val) != uint64(len(item.val)) {
						return false
					}
				} else if !bytes.Equal(val, item.val) {
					return false
				}
			} else {
				//fmt.Printf("key [%x] => [%x] not present in changes\n", key, val)
			}
		}
	}
	return true
}

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys, Vals [][]byte
}

func (l KvList) Len() int {
	return len(l.Keys)
}

func (l KvList) Less(i, j int) bool {
	return bytes.Compare(l.Keys[i], l.Keys[j]) < 0
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
}

type StateReconWriter1 struct {
	rs         *ReconState1
	txNum      uint64
	writeLists map[string]*KvList
}

func NewStateReconWriter1(rs *ReconState1) *StateReconWriter1 {
	return &StateReconWriter1{
		rs: rs,
		writeLists: map[string]*KvList{
			kv.PlainState:        &KvList{},
			kv.Code:              &KvList{},
			kv.PlainContractCode: &KvList{},
			kv.IncarnationMap:    &KvList{},
		},
	}
}

func (w *StateReconWriter1) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateReconWriter1) ResetWriteSet() {
	w.writeLists = map[string]*KvList{
		kv.PlainState:        &KvList{},
		kv.Code:              &KvList{},
		kv.PlainContractCode: &KvList{},
		kv.IncarnationMap:    &KvList{},
	}
}

func (w *StateReconWriter1) WriteSet() map[string]*KvList {
	for _, list := range w.writeLists {
		sort.Sort(list)
	}
	return w.writeLists
}

func (w *StateReconWriter1) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, address.Bytes())
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value)
	return nil
}

func (w *StateReconWriter1) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	w.writeLists[kv.Code].Keys = append(w.writeLists[kv.Code].Keys, codeHash.Bytes())
	w.writeLists[kv.Code].Vals = append(w.writeLists[kv.Code].Vals, code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.writeLists[kv.PlainContractCode].Keys = append(w.writeLists[kv.PlainContractCode].Keys, dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
		w.writeLists[kv.PlainContractCode].Vals = append(w.writeLists[kv.PlainContractCode].Vals, codeHash.Bytes())
	}
	return nil
}

func (w *StateReconWriter1) DeleteAccount(address common.Address, original *accounts.Account) error {
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, address.Bytes())
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, []byte{})
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		w.writeLists[kv.IncarnationMap].Keys = append(w.writeLists[kv.IncarnationMap].Keys, address.Bytes())
		w.writeLists[kv.IncarnationMap].Vals = append(w.writeLists[kv.IncarnationMap].Vals, b[:])
	}
	return nil
}

func (w *StateReconWriter1) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes()))
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value.Bytes())
	//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, v, w.txNum)
	return nil
}

func (w *StateReconWriter1) CreateContract(address common.Address) error {
	return nil
}

type StateReconReader1 struct {
	tx         kv.Tx
	txNum      uint64
	trace      bool
	rs         *ReconState1
	readError  bool
	stateTxNum uint64
	composite  []byte
	readLists  map[string]*KvList
}

func NewStateReconReader1(rs *ReconState1) *StateReconReader1 {
	return &StateReconReader1{
		rs: rs,
		readLists: map[string]*KvList{
			kv.PlainState:     &KvList{},
			kv.Code:           &KvList{},
			CodeSizeTable:     &KvList{},
			kv.IncarnationMap: &KvList{},
		},
	}
}

func (r *StateReconReader1) SetTxNum(txNum uint64) {
	r.txNum = txNum
}

func (r *StateReconReader1) SetTx(tx kv.Tx) {
	r.tx = tx
}

func (r *StateReconReader1) ResetReadSet() {
	r.readLists = map[string]*KvList{
		kv.PlainState:     &KvList{},
		kv.Code:           &KvList{},
		CodeSizeTable:     &KvList{},
		kv.IncarnationMap: &KvList{},
	}
}

func (r *StateReconReader1) ReadSet() map[string]*KvList {
	for _, list := range r.readLists {
		sort.Sort(list)
	}
	return r.readLists
}

func (r *StateReconReader1) SetTrace(trace bool) {
	r.trace = trace
}

func (r *StateReconReader1) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc := r.rs.Get(kv.PlainState, address.Bytes())
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, address.Bytes())
		if err != nil {
			return nil, err
		}
	}
	r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, address.Bytes())
	r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, common.CopyBytes(enc))
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err := a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	if r.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", address, a.Nonce, &a.Balance, a.CodeHash, r.txNum)
	}
	return &a, nil
}

func (r *StateReconReader1) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if cap(r.composite) < 20+8+32 {
		r.composite = make([]byte, 20+8+32)
	} else if len(r.composite) != 20+8+32 {
		r.composite = r.composite[:20+8+32]
	}
	copy(r.composite, address.Bytes())
	binary.BigEndian.PutUint64(r.composite[20:], incarnation)
	copy(r.composite[20+8:], key.Bytes())

	enc := r.rs.Get(kv.PlainState, r.composite)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, r.composite)
		if err != nil {
			return nil, err
		}
	}
	r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, common.CopyBytes(r.composite))
	r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, common.CopyBytes(enc))
	if r.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [], txNum: %d\n", address, key.Bytes(), r.txNum)
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x], txNum: %d\n", address, key.Bytes(), enc, r.txNum)
		}
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (r *StateReconReader1) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	enc := r.rs.Get(kv.Code, codeHash.Bytes())
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHash.Bytes())
		if err != nil {
			return nil, err
		}
	}
	r.readLists[kv.Code].Keys = append(r.readLists[kv.Code].Keys, address.Bytes())
	r.readLists[kv.Code].Vals = append(r.readLists[kv.Code].Vals, common.CopyBytes(enc))
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *StateReconReader1) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	enc := r.rs.Get(kv.Code, codeHash.Bytes())
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHash.Bytes())
		if err != nil {
			return 0, err
		}
	}
	var sizebuf [8]byte
	binary.BigEndian.PutUint64(sizebuf[:], uint64(len(enc)))
	r.readLists[CodeSizeTable].Keys = append(r.readLists[CodeSizeTable].Keys, address.Bytes())
	r.readLists[CodeSizeTable].Vals = append(r.readLists[CodeSizeTable].Vals, sizebuf[:])
	size := len(enc)
	if r.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, r.txNum)
	}
	return size, nil
}

func (r *StateReconReader1) ReadAccountIncarnation(address common.Address) (uint64, error) {
	enc := r.rs.Get(kv.IncarnationMap, address.Bytes())
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.IncarnationMap, address.Bytes())
		if err != nil {
			return 0, err
		}
	}
	r.readLists[kv.IncarnationMap].Keys = append(r.readLists[kv.IncarnationMap].Keys, address.Bytes())
	r.readLists[kv.IncarnationMap].Vals = append(r.readLists[kv.IncarnationMap].Vals, common.CopyBytes(enc))
	if len(enc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(enc), nil
}
