package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type TxTask struct {
	TxNum              uint64
	BlockNum           uint64
	Rules              *params.Rules
	Header             *types.Header
	Txs                types.Transactions
	Uncles             []*types.Header
	Coinbase           common.Address
	BlockHash          common.Hash
	Sender             *common.Address
	SkipAnalysis       bool
	TxIndex            int // -1 for block initialisation
	Final              bool
	Tx                 types.Transaction
	TxAsMessage        types.Message
	BalanceIncreaseSet map[common.Address]uint256.Int
	ReadLists          map[string]*KvList
	WriteLists         map[string]*KvList
	AccountPrevs       map[string][]byte
	AccountDels        map[string]*accounts.Account
	StoragePrevs       map[string][]byte
	CodePrevs          map[string]uint64
	ResultsSize        int64
	Error              error
	Logs               []*types.Log
	TraceFroms         map[common.Address]struct{}
	TraceTos           map[common.Address]struct{}
}

type TxTaskQueue []*TxTask

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
	*h = append(*h, a.(*TxTask))
}

func (h *TxTaskQueue) Pop() interface{} {
	c := *h
	*h = c[:len(c)-1]
	return c[len(c)-1]
}

const CodeSizeTable = "CodeSize"

type State22 struct {
	lock         sync.RWMutex
	receiveWork  *sync.Cond
	triggers     map[uint64]*TxTask
	senderTxNums map[common.Address]uint64
	triggerLock  sync.RWMutex
	queue        TxTaskQueue
	queueLock    sync.Mutex
	changes      map[string]*btree.BTreeG[statePair]
	sizeEstimate uint64
	txsDone      uint64
	finished     bool
}

type statePair struct {
	key, val []byte
}

func stateItemLess(i, j statePair) bool {
	return bytes.Compare(i.key, j.key) < 0
}

func NewState22() *State22 {
	rs := &State22{
		triggers:     map[uint64]*TxTask{},
		senderTxNums: map[common.Address]uint64{},
		changes:      map[string]*btree.BTreeG[statePair]{},
	}
	rs.receiveWork = sync.NewCond(&rs.queueLock)
	return rs
}

func (rs *State22) put(table string, key, val []byte) {
	t, ok := rs.changes[table]
	if !ok {
		t = btree.NewG[statePair](64, stateItemLess)
		rs.changes[table] = t
	}
	t.ReplaceOrInsert(statePair{key: key, val: val})
	rs.sizeEstimate += PairSize + uint64(len(key)) + uint64(len(val))
}

func (rs *State22) Get(table string, key []byte) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.get(table, key)
}

func (rs *State22) get(table string, key []byte) []byte {
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	if i, ok := t.Get(statePair{key: key}); ok {
		return i.val
	}
	return nil
}

func (rs *State22) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		var err error
		t.Ascend(func(item statePair) bool {
			if len(item.val) == 0 {
				if err = rwTx.Delete(table, item.key); err != nil {
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

func (rs *State22) Schedule() (*TxTask, bool) {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	for !rs.finished && rs.queue.Len() == 0 {
		rs.receiveWork.Wait()
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(*TxTask), true
	}
	return nil, false
}

func (rs *State22) RegisterSender(txTask *TxTask) bool {
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

func (rs *State22) CommitTxNum(sender *common.Address, txNum uint64) uint64 {
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

func (rs *State22) AddWork(txTask *TxTask) {
	txTask.BalanceIncreaseSet = nil
	txTask.ReadLists = nil
	txTask.WriteLists = nil
	txTask.ResultsSize = 0
	txTask.Logs = nil
	txTask.TraceFroms = nil
	txTask.TraceTos = nil

	/*
		txTask.ReadLists = nil
		txTask.WriteLists = nil
		txTask.AccountPrevs = nil
		txTask.AccountDels = nil
		txTask.StoragePrevs = nil
		txTask.CodePrevs = nil
	*/
	rs.queueLock.Lock()
	heap.Push(&rs.queue, txTask)
	rs.queueLock.Unlock()
	rs.receiveWork.Signal()
}

func (rs *State22) Finish() {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	rs.finished = true
	rs.receiveWork.Broadcast()
}

func (rs *State22) Apply(roTx kv.Tx, txTask *TxTask, agg *libstate.Aggregator22) error {
	emptyRemoval := txTask.Rules.IsSpuriousDragon
	rs.lock.Lock()
	defer rs.lock.Unlock()

	agg.SetTxNum(txTask.TxNum)
	for addr := range txTask.BalanceIncreaseSet {
		addrBytes := addr.Bytes()
		increase := txTask.BalanceIncreaseSet[addr]
		enc0 := rs.get(kv.PlainState, addrBytes)
		if enc0 == nil {
			var err error
			enc0, err = roTx.GetOne(kv.PlainState, addrBytes)
			if err != nil {
				return err
			}
		}
		var a accounts.Account
		if err := a.DecodeForStorage(enc0); err != nil {
			return err
		}
		if len(enc0) > 0 {
			// Need to convert before balance increase
			enc0 = accounts.Serialise2(&a)
		}
		a.Balance.Add(&a.Balance, &increase)
		var enc1 []byte
		if emptyRemoval && a.Nonce == 0 && a.Balance.IsZero() && a.IsEmptyCodeHash() {
			enc1 = []byte{}
		} else {
			enc1 = make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(enc1)
		}
		rs.put(kv.PlainState, addrBytes, enc1)
		if err := agg.AddAccountPrev(addrBytes, enc0); err != nil {
			return err
		}
	}

	if len(txTask.AccountDels) > 0 {
		cursor, err := roTx.Cursor(kv.PlainState)
		if err != nil {
			return err
		}
		defer cursor.Close()
		addr1 := make([]byte, 20+8)
		search := statePair{}
		psChanges := rs.changes[kv.PlainState]
		for addrS, original := range txTask.AccountDels {
			addr := []byte(addrS)
			copy(addr1, addr)
			binary.BigEndian.PutUint64(addr1[len(addr):], original.Incarnation)

			prev := accounts.Serialise2(original)
			if err := agg.AddAccountPrev(addr, prev); err != nil {
				return err
			}
			codeHashBytes := original.CodeHash.Bytes()
			codePrev := rs.get(kv.Code, codeHashBytes)
			if codePrev == nil {
				var err error
				codePrev, err = roTx.GetOne(kv.Code, codeHashBytes)
				if err != nil {
					return err
				}
			}
			if err := agg.AddCodePrev(addr, codePrev); err != nil {
				return err
			}
			// Iterate over storage
			var k, v []byte
			_, _ = k, v
			var e error
			if k, v, e = cursor.Seek(addr1); err != nil {
				return e
			}
			if !bytes.HasPrefix(k, addr1) {
				k = nil
			}
			if psChanges != nil {
				search.key = addr1
				psChanges.AscendGreaterOrEqual(search, func(item statePair) bool {
					if !bytes.HasPrefix(item.key, addr1) {
						return false
					}
					for ; e == nil && k != nil && bytes.HasPrefix(k, addr1) && bytes.Compare(k, item.key) <= 0; k, v, e = cursor.Next() {
						if !bytes.Equal(k, item.key) {
							// Skip the cursor item when the key is equal, i.e. prefer the item from the changes tree
							if e = agg.AddStoragePrev(addr, k[28:], v); e != nil {
								return false
							}
						}
					}
					if e != nil {
						return false
					}
					if e = agg.AddStoragePrev(addr, item.key[28:], item.val); e != nil {
						return false
					}
					return true
				})
			}
			for ; e == nil && k != nil && bytes.HasPrefix(k, addr1); k, v, e = cursor.Next() {
				if e = agg.AddStoragePrev(addr, k[28:], v); e != nil {
					return e
				}
			}
			if e != nil {
				return e
			}
		}
	}

	for addrS, enc0 := range txTask.AccountPrevs {
		if err := agg.AddAccountPrev([]byte(addrS), enc0); err != nil {
			return err
		}
	}
	for compositeS, val := range txTask.StoragePrevs {
		composite := []byte(compositeS)
		if err := agg.AddStoragePrev(composite[:20], composite[28:], val); err != nil {
			return err
		}
	}

	k := make([]byte, 20+8)
	for addrS, incarnation := range txTask.CodePrevs {
		addr := []byte(addrS)
		copy(k, addr)
		binary.BigEndian.PutUint64(k[20:], incarnation)

		codeHash := rs.get(kv.PlainContractCode, k)
		if codeHash == nil {
			var err error
			codeHash, err = roTx.GetOne(kv.PlainContractCode, k)
			if err != nil {
				return err
			}
		}
		var codePrev []byte
		if codeHash != nil {
			codePrev = rs.get(kv.Code, codeHash)
			if codePrev == nil {
				var err error
				codePrev, err = roTx.GetOne(kv.Code, codeHash)
				if err != nil {
					return err
				}
			}
		}
		if err := agg.AddCodePrev(addr, codePrev); err != nil {
			return err
		}
	}
	if txTask.TraceFroms != nil {
		for addr := range txTask.TraceFroms {
			if err := agg.AddTraceFrom(addr[:]); err != nil {
				return err
			}
		}
	}
	if txTask.TraceTos != nil {
		for addr := range txTask.TraceTos {
			if err := agg.AddTraceTo(addr[:]); err != nil {
				return err
			}
		}
	}
	for _, log := range txTask.Logs {
		if err := agg.AddLogAddr(log.Address[:]); err != nil {
			return fmt.Errorf("adding event log for addr %x: %w", log.Address, err)
		}
		for _, topic := range log.Topics {
			if err := agg.AddLogTopic(topic[:]); err != nil {
				return fmt.Errorf("adding event log for topic %x: %w", topic, err)
			}
		}
	}
	if err := agg.FinishTx(); err != nil {
		return err
	}
	if txTask.WriteLists != nil {
		for table, list := range txTask.WriteLists {
			for i, key := range list.Keys {
				rs.put(table, key, list.Vals[i])
			}
		}
	}
	return nil
}

func recoverCodeHashPlain(acc *accounts.Account, db kv.Tx, key []byte) {
	var address common.Address
	copy(address[:], key)
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func (rs *State22) Unwind(ctx context.Context, tx kv.RwTx, txUnwindTo uint64, agg *libstate.Aggregator22, accumulator *shards.Accumulator) error {
	agg.SetTx(tx)
	var currentInc uint64
	if err := agg.Unwind(ctx, txUnwindTo, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == length.Addr {
			if len(v) > 0 {
				var acc accounts.Account
				if err := accounts.Deserialise2(&acc, v); err != nil {
					return fmt.Errorf("%w, %x", err, v)
				}
				currentInc = acc.Incarnation
				// Fetch the code hash
				recoverCodeHashPlain(&acc, tx, k)
				var address common.Address
				copy(address[:], k)

				// cleanup contract code bucket
				original, err := NewPlainStateReader(tx).ReadAccountData(address)
				if err != nil {
					return fmt.Errorf("read account for %x: %w", address, err)
				}
				if original != nil {
					// clean up all the code incarnations original incarnation and the new one
					for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
						err = tx.Delete(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
						if err != nil {
							return fmt.Errorf("writeAccountPlain for %x: %w", address, err)
						}
					}
				}

				newV := make([]byte, acc.EncodingLengthForStorage())
				acc.EncodeForStorage(newV)
				if accumulator != nil {
					accumulator.ChangeAccount(address, acc.Incarnation, newV)
				}
				if err := next(k, k, newV); err != nil {
					return err
				}
			} else {
				var address common.Address
				copy(address[:], k)
				original, err := NewPlainStateReader(tx).ReadAccountData(address)
				if err != nil {
					return err
				}
				if original != nil {
					currentInc = original.Incarnation
				} else {
					currentInc = 1
				}

				if accumulator != nil {
					accumulator.DeleteAccount(address)
				}
				if err := next(k, k, nil); err != nil {
					return err
				}
			}
			return nil
		}
		if accumulator != nil {
			var address common.Address
			var location common.Hash
			copy(address[:], k[:length.Addr])
			copy(location[:], k[length.Addr:])
			accumulator.ChangeStorage(address, currentInc, location, common2.Copy(v))
		}
		newKeys := dbutils.PlainGenerateCompositeStorageKey(k[:20], currentInc, k[20:])
		if len(v) > 0 {
			if err := next(k, newKeys, v); err != nil {
				return err
			}
		} else {
			if err := next(k, newKeys, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (rs *State22) DoneCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.txsDone
}

func (rs *State22) SizeEstimate() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.sizeEstimate
}

func (rs *State22) ReadsValid(readLists map[string]*KvList) bool {
	search := statePair{}
	var t *btree.BTreeG[statePair]

	rs.lock.RLock()
	defer rs.lock.RUnlock()
	//fmt.Printf("ValidReads\n")
	for table, list := range readLists {
		//fmt.Printf("Table %s\n", table)
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
			search.key = key
			if item, ok := t.Get(search); ok {
				//fmt.Printf("key [%x] => [%x] vs [%x]\n", key, val, rereadVal)
				if table == CodeSizeTable {
					if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(item.val)) {
						return false
					}
				} else if !bytes.Equal(list.Vals[i], item.val) {
					return false
				}
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

type StateWriter22 struct {
	rs           *State22
	txNum        uint64
	writeLists   map[string]*KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
}

func NewStateWriter22(rs *State22) *StateWriter22 {
	return &StateWriter22{
		rs: rs,
		writeLists: map[string]*KvList{
			kv.PlainState:        {},
			kv.Code:              {},
			kv.PlainContractCode: {},
			kv.IncarnationMap:    {},
		},
		accountPrevs: map[string][]byte{},
		accountDels:  map[string]*accounts.Account{},
		storagePrevs: map[string][]byte{},
		codePrevs:    map[string]uint64{},
	}
}

func (w *StateWriter22) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateWriter22) ResetWriteSet() {
	w.writeLists = map[string]*KvList{
		kv.PlainState:        {},
		kv.Code:              {},
		kv.PlainContractCode: {},
		kv.IncarnationMap:    {},
	}
	w.accountPrevs = map[string][]byte{}
	w.accountDels = map[string]*accounts.Account{}
	w.storagePrevs = map[string][]byte{}
	w.codePrevs = map[string]uint64{}
}

func (w *StateWriter22) WriteSet() map[string]*KvList {
	return w.writeLists
}

func (w *StateWriter22) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *StateWriter22) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, address.Bytes())
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value)
	var prev []byte
	if original.Initialised {
		prev = accounts.Serialise2(original)
	}
	w.accountPrevs[string(address.Bytes())] = prev
	return nil
}

func (w *StateWriter22) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	w.writeLists[kv.Code].Keys = append(w.writeLists[kv.Code].Keys, codeHash.Bytes())
	w.writeLists[kv.Code].Vals = append(w.writeLists[kv.Code].Vals, code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.writeLists[kv.PlainContractCode].Keys = append(w.writeLists[kv.PlainContractCode].Keys, dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
		w.writeLists[kv.PlainContractCode].Vals = append(w.writeLists[kv.PlainContractCode].Vals, codeHash.Bytes())
	}
	w.codePrevs[string(address.Bytes())] = incarnation
	return nil
}

func (w *StateWriter22) DeleteAccount(address common.Address, original *accounts.Account) error {
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, address.Bytes())
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, []byte{})
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		w.writeLists[kv.IncarnationMap].Keys = append(w.writeLists[kv.IncarnationMap].Keys, address.Bytes())
		w.writeLists[kv.IncarnationMap].Vals = append(w.writeLists[kv.IncarnationMap].Vals, b[:])
	}
	if original.Initialised {
		w.accountDels[string(address.Bytes())] = original
	}
	return nil
}

func (w *StateWriter22) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	composite := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, composite)
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value.Bytes())
	//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, v, w.txNum)
	w.storagePrevs[string(composite)] = original.Bytes()
	return nil
}

func (w *StateWriter22) CreateContract(address common.Address) error {
	return nil
}

type StateReader22 struct {
	tx        kv.Tx
	txNum     uint64
	trace     bool
	rs        *State22
	composite []byte
	readLists map[string]*KvList
}

func NewStateReader22(rs *State22) *StateReader22 {
	return &StateReader22{
		rs: rs,
		readLists: map[string]*KvList{
			kv.PlainState:     {},
			kv.Code:           {},
			CodeSizeTable:     {},
			kv.IncarnationMap: {},
		},
	}
}

func (r *StateReader22) SetTxNum(txNum uint64) {
	r.txNum = txNum
}

func (r *StateReader22) SetTx(tx kv.Tx) {
	r.tx = tx
}

func (r *StateReader22) ResetReadSet() {
	r.readLists = map[string]*KvList{
		kv.PlainState:     {},
		kv.Code:           {},
		CodeSizeTable:     {},
		kv.IncarnationMap: {},
	}
}

func (r *StateReader22) ReadSet() map[string]*KvList {
	return r.readLists
}

func (r *StateReader22) SetTrace(trace bool) {
	r.trace = trace
}

func (r *StateReader22) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()
	enc := r.rs.Get(kv.PlainState, addr)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, addr)
		if err != nil {
			return nil, err
		}
	}
	r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, addr)
	r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, common2.Copy(enc))
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

func (r *StateReader22) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
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
	r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, common2.Copy(r.composite))
	r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, common2.Copy(enc))
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

func (r *StateReader22) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	addr, codeHashBytes := address.Bytes(), codeHash.Bytes()
	enc := r.rs.Get(kv.Code, codeHashBytes)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHashBytes)
		if err != nil {
			return nil, err
		}
	}
	r.readLists[kv.Code].Keys = append(r.readLists[kv.Code].Keys, addr)
	r.readLists[kv.Code].Vals = append(r.readLists[kv.Code].Vals, common2.Copy(enc))
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *StateReader22) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	codeHashBytes := codeHash.Bytes()
	enc := r.rs.Get(kv.Code, codeHashBytes)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHashBytes)
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

func (r *StateReader22) ReadAccountIncarnation(address common.Address) (uint64, error) {
	enc := r.rs.Get(kv.IncarnationMap, address.Bytes())
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.IncarnationMap, address.Bytes())
		if err != nil {
			return 0, err
		}
	}
	r.readLists[kv.IncarnationMap].Keys = append(r.readLists[kv.IncarnationMap].Keys, address.Bytes())
	r.readLists[kv.IncarnationMap].Vals = append(r.readLists[kv.IncarnationMap].Vals, common2.Copy(enc))
	if len(enc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(enc), nil
}
