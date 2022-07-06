package state

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type TxTask struct {
	TxNum              uint64
	BlockNum           uint64
	Header             *types.Header
	Block              *types.Block
	BlockHash          common.Hash
	Sender             *common.Address
	TxIndex            int // -1 for block initialisation
	Final              bool
	Tx                 types.Transaction
	BalanceIncreaseSet map[common.Address]uint256.Int
	ReadKeys           map[string][][]byte
	ReadVals           map[string][][]byte
	WriteKeys          map[string][][]byte
	WriteVals          map[string][][]byte
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

type ReconState1 struct {
	lock          sync.RWMutex
	triggers      map[uint64]TxTask
	senderTxNums  map[common.Address]uint64
	workCh        chan TxTask
	queue         TxTaskQueue
	changes       map[string]map[string][]byte
	sizeEstimate  uint64
	rollbackCount uint64
	txsDone       uint64
}

func NewReconState1(workCh chan TxTask) *ReconState1 {
	rs := &ReconState1{
		workCh:       workCh,
		triggers:     map[uint64]TxTask{},
		senderTxNums: map[common.Address]uint64{},
		changes:      map[string]map[string][]byte{},
	}
	return rs
}

func (rs *ReconState1) put(table string, key, val []byte) {
	t, ok := rs.changes[table]
	if !ok {
		t = map[string][]byte{}
		rs.changes[table] = t
	}
	t[string(key)] = val
	rs.sizeEstimate += uint64(len(key)) + uint64(len(val))
}

func (rs *ReconState1) Delete(table string, key []byte) {
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
	return t[string(key)]
}

func (rs *ReconState1) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		for ks, val := range t {
			if len(val) > 0 {
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

func (rs *ReconState1) Schedule() (TxTask, bool) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for rs.queue.Len() < 16 {
		txTask, ok := <-rs.workCh
		if !ok {
			// No more work, channel is closed
			break
		}
		if txTask.Sender == nil {
			heap.Push(&rs.queue, txTask)
		} else if rs.registerSender(txTask) {
			heap.Push(&rs.queue, txTask)
		}
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(TxTask), true
	}
	return TxTask{}, false
}

func (rs *ReconState1) registerSender(txTask TxTask) bool {
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

func (rs *ReconState1) CommitTxNum(sender *common.Address, txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if triggered, ok := rs.triggers[txNum]; ok {
		heap.Push(&rs.queue, triggered)
		delete(rs.triggers, txNum)
	}
	if sender != nil {
		if lastTxNum, ok := rs.senderTxNums[*sender]; ok && lastTxNum == txNum {
			// This is the last transaction so far with this sender, remove
			delete(rs.senderTxNums, *sender)
		}
	}
	rs.txsDone++
}

func (rs *ReconState1) RollbackTx(txTask TxTask) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	heap.Push(&rs.queue, txTask)
	rs.rollbackCount++
}

func (rs *ReconState1) Apply(writeKeys, writeVals map[string][][]byte, balanceIncreaseSet map[common.Address]uint256.Int) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, keyList := range writeKeys {
		valList := writeVals[table]
		for i, key := range keyList {
			val := valList[i]
			rs.put(table, key, val)
		}
	}
	for addr, increase := range balanceIncreaseSet {
		enc := rs.get(kv.PlainState, addr.Bytes())
		var a accounts.Account
		if err := a.DecodeForStorage(enc); err != nil {
			panic(err)
		}
		a.Balance.Add(&a.Balance, &increase)
		l := a.EncodingLengthForStorage()
		enc = make([]byte, l)
		a.EncodeForStorage(enc)
		rs.put(kv.PlainState, addr.Bytes(), enc)
	}
}

func (rs *ReconState1) DoneCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.txsDone
}

func (rs *ReconState1) RollbackCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.rollbackCount
}

func (rs *ReconState1) SizeEstimate() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.sizeEstimate
}

func (rs *ReconState1) ReadsValid(readKeys, readVals map[string][][]byte) bool {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	for table, keyList := range readKeys {
		t, ok := rs.changes[table]
		if !ok {
			continue
		}
		valList := readVals[table]
		for i, key := range keyList {
			val := valList[i]
			if rereadVal, ok := t[string(key)]; ok {
				if !bytes.Equal(val, rereadVal) {
					return false
				}
			}
		}
	}
	return true
}

type StateReconWriter1 struct {
	rs        *ReconState1
	txNum     uint64
	writeKeys map[string][][]byte
	writeVals map[string][][]byte
}

func NewStateReconWriter1(rs *ReconState1) *StateReconWriter1 {
	return &StateReconWriter1{
		rs: rs,
	}
}

func (w *StateReconWriter1) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateReconWriter1) ResetWriteSet() {
	w.writeKeys = map[string][][]byte{}
	w.writeVals = map[string][][]byte{}
}

func (w *StateReconWriter1) WriteSet() (map[string][][]byte, map[string][][]byte) {
	return w.writeKeys, w.writeVals
}

func (w *StateReconWriter1) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.writeKeys[kv.PlainState] = append(w.writeKeys[kv.PlainState], address.Bytes())
	w.writeVals[kv.PlainState] = append(w.writeVals[kv.PlainState], value)
	return nil
}

func (w *StateReconWriter1) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	w.writeKeys[kv.Code] = append(w.writeKeys[kv.Code], codeHash.Bytes())
	w.writeVals[kv.Code] = append(w.writeVals[kv.Code], code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.writeKeys[kv.PlainContractCode] = append(w.writeKeys[kv.PlainContractCode], dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
		w.writeVals[kv.PlainContractCode] = append(w.writeVals[kv.PlainContractCode], codeHash.Bytes())
	}
	return nil
}

func (w *StateReconWriter1) DeleteAccount(address common.Address, original *accounts.Account) error {
	w.writeKeys[kv.PlainState] = append(w.writeKeys[kv.PlainState], address.Bytes())
	w.writeVals[kv.PlainState] = append(w.writeVals[kv.PlainState], nil)
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		w.writeKeys[kv.IncarnationMap] = append(w.writeKeys[kv.IncarnationMap], address.Bytes())
		w.writeVals[kv.IncarnationMap] = append(w.writeVals[kv.IncarnationMap], b[:])
	}
	return nil
}

func (w *StateReconWriter1) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	w.writeKeys[kv.PlainState] = append(w.writeKeys[kv.PlainState], dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes()))
	w.writeVals[kv.PlainState] = append(w.writeVals[kv.PlainState], value.Bytes())
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
	readKeys   map[string][][]byte
	readVals   map[string][][]byte
}

func NewStateReconReader1(rs *ReconState1) *StateReconReader1 {
	return &StateReconReader1{rs: rs}
}

func (r *StateReconReader1) SetTxNum(txNum uint64) {
	r.txNum = txNum
}

func (r *StateReconReader1) SetTx(tx kv.Tx) {
	r.tx = tx
}

func (r *StateReconReader1) ResetReadSet() {
	r.readKeys = map[string][][]byte{}
	r.readVals = map[string][][]byte{}
}

func (r *StateReconReader1) ReadSet() (map[string][][]byte, map[string][][]byte) {
	return r.readKeys, r.readVals
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
	r.readKeys[kv.PlainState] = append(r.readKeys[kv.PlainState], address.Bytes())
	r.readVals[kv.PlainState] = append(r.readVals[kv.PlainState], enc)
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
	r.readKeys[kv.PlainState] = append(r.readKeys[kv.PlainState], r.composite)
	r.readVals[kv.PlainState] = append(r.readVals[kv.PlainState], enc)
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
	r.readKeys[kv.Code] = append(r.readKeys[kv.Code], address.Bytes())
	r.readVals[kv.Code] = append(r.readVals[kv.Code], enc)
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
	r.readKeys[kv.Code] = append(r.readKeys[kv.Code], address.Bytes())
	r.readVals[kv.Code] = append(r.readVals[kv.Code], enc)
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
	r.readKeys[kv.IncarnationMap] = append(r.readKeys[kv.IncarnationMap], address.Bytes())
	r.readVals[kv.IncarnationMap] = append(r.readVals[kv.IncarnationMap], enc)
	if len(enc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(enc), nil
}
