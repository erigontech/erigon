package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/shards"
	btree2 "github.com/tidwall/btree"
	atomic2 "go.uber.org/atomic"
)

const CodeSizeTable = "CodeSize"

type State22 struct {
	lock         sync.RWMutex
	receiveWork  *sync.Cond
	triggers     map[uint64]*exec22.TxTask
	senderTxNums map[common.Address]uint64
	triggerLock  sync.RWMutex
	queue        exec22.TxTaskQueue
	queueLock    sync.Mutex
	changes      map[string]*btree2.BTreeG[statePair]
	sizeEstimate uint64
	txsDone      *atomic2.Uint64
	finished     bool

	applyStateHint *btree2.PathHint //only for apply func (it always work in 1-thread), only for kv.PlainState table
}

type statePair struct {
	key, val []byte
}

func stateItemLess(i, j statePair) bool {
	return bytes.Compare(i.key, j.key) < 0
}

func NewState22() *State22 {
	rs := &State22{
		triggers:       map[uint64]*exec22.TxTask{},
		senderTxNums:   map[common.Address]uint64{},
		changes:        map[string]*btree2.BTreeG[statePair]{},
		txsDone:        atomic2.NewUint64(0),
		applyStateHint: &btree2.PathHint{},
	}
	rs.receiveWork = sync.NewCond(&rs.queueLock)
	return rs
}

func (rs *State22) putHint(table string, key, val []byte, hint *btree2.PathHint) {
	t, ok := rs.changes[table]
	if !ok {
		t = btree2.NewBTreeGOptions[statePair](stateItemLess, btree2.Options{Degree: 128, NoLocks: true})
		rs.changes[table] = t
	}
	old, ok := t.SetHint(statePair{key: key, val: val}, hint)
	rs.sizeEstimate += btreeOverhead + uint64(len(key)) + uint64(len(val))
	if ok {
		rs.sizeEstimate -= btreeOverhead + uint64(len(old.key)) + uint64(len(old.val))
	}
}

func (rs *State22) put(table string, key, val []byte) {
	t, ok := rs.changes[table]
	if !ok {
		t = btree2.NewBTreeGOptions[statePair](stateItemLess, btree2.Options{Degree: 128, NoLocks: true})
		rs.changes[table] = t
	}
	old, ok := t.Set(statePair{key: key, val: val})
	rs.sizeEstimate += btreeOverhead + uint64(len(key)) + uint64(len(val))
	if ok {
		rs.sizeEstimate -= btreeOverhead + uint64(len(old.key)) + uint64(len(old.val))
	}
}

const btreeOverhead = 16

func (rs *State22) GetHint(table string, key []byte, hint *btree2.PathHint) []byte {
	rs.lock.RLock()
	v := rs.getHint(table, key, hint)
	rs.lock.RUnlock()
	return v
}
func (rs *State22) Get(table string, key []byte) []byte {
	rs.lock.RLock()
	v := rs.get(table, key)
	rs.lock.RUnlock()
	return v
}

func (rs *State22) getHint(table string, key []byte, hint *btree2.PathHint) []byte {
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	if i, ok := t.GetHint(statePair{key: key}, hint); ok {
		return i.val
	}
	return nil
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
		c, err := rwTx.RwCursor(table)
		if err != nil {
			return err
		}
		t.Walk(func(items []statePair) bool {
			for _, item := range items {
				if len(item.val) == 0 {
					if err = c.Delete(item.key); err != nil {
						return false
					}
					//fmt.Printf("Flush [%x]=>\n", item.key)
				} else {
					if err = c.Put(item.key, item.val); err != nil {
						return false
					}
					//fmt.Printf("Flush [%x]=>[%x]\n", item.key, item.val)
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

func (rs *State22) Schedule() (*exec22.TxTask, bool) {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	for !rs.finished && rs.queue.Len() == 0 {
		rs.receiveWork.Wait()
	}
	if rs.finished {
		return nil, false
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(*exec22.TxTask), true
	}
	return nil, false
}

func (rs *State22) RegisterSender(txTask *exec22.TxTask) bool {
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
	rs.txsDone.Add(1)

	rs.triggerLock.Lock()
	defer rs.triggerLock.Unlock()
	count := uint64(0)
	if triggered, ok := rs.triggers[txNum]; ok {
		rs.queuePush(triggered)
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
	return count
}

func (rs *State22) queuePush(t *exec22.TxTask) {
	rs.queueLock.Lock()
	heap.Push(&rs.queue, t)
	rs.queueLock.Unlock()
}

func (rs *State22) AddWork(txTask *exec22.TxTask) {
	txTask.BalanceIncreaseSet = nil
	returnReadList(txTask.ReadLists)
	txTask.ReadLists = nil
	returnWriteList(txTask.WriteLists)
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
	rs.queuePush(txTask)
	rs.receiveWork.Signal()
}

func (rs *State22) Finish() {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	rs.finished = true
	rs.receiveWork.Broadcast()
}

func (rs *State22) appplyState1(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.Aggregator22) error {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

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
				psChanges.Ascend(search, func(item statePair) bool {
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
	return nil
}

func (rs *State22) appplyState(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.Aggregator22) error {
	emptyRemoval := txTask.Rules.IsSpuriousDragon
	rs.lock.Lock()
	defer rs.lock.Unlock()

	for addr := range txTask.BalanceIncreaseSet {
		addrBytes := addr.Bytes()
		increase := txTask.BalanceIncreaseSet[addr]
		enc0 := rs.getHint(kv.PlainState, addrBytes, rs.applyStateHint)
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
		rs.putHint(kv.PlainState, addrBytes, enc1, rs.applyStateHint)
		if err := agg.AddAccountPrev(addrBytes, enc0); err != nil {
			return err
		}
	}

	if txTask.WriteLists != nil {
		for table, list := range txTask.WriteLists {
			if table == kv.PlainState {
				for i, key := range list.Keys {
					rs.putHint(table, key, list.Vals[i], rs.applyStateHint)
				}
			} else {
				for i, key := range list.Keys {
					rs.put(table, key, list.Vals[i])
				}

			}
		}
	}
	return nil
}

func (rs *State22) ApplyState(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.Aggregator22) error {
	agg.SetTxNum(txTask.TxNum)
	if err := rs.appplyState1(roTx, txTask, agg); err != nil {
		return err
	}
	if err := rs.appplyState(roTx, txTask, agg); err != nil {
		return err
	}

	returnReadList(txTask.ReadLists)
	returnWriteList(txTask.WriteLists)

	txTask.ReadLists, txTask.WriteLists = nil, nil
	return nil
}

func (rs *State22) ApplyHistory(txTask *exec22.TxTask, agg *libstate.Aggregator22) error {
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

func (rs *State22) DoneCount() uint64 { return rs.txsDone.Load() }

func (rs *State22) SizeEstimate() uint64 {
	rs.lock.RLock()
	r := rs.sizeEstimate
	rs.lock.RUnlock()
	return r
}

func (rs *State22) ReadsValid(readLists map[string]*exec22.KvList) bool {
	search := statePair{}
	var t *btree2.BTreeG[statePair]

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

type StateWriter22 struct {
	rs           *State22
	txNum        uint64
	writeLists   map[string]*exec22.KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
}

func NewStateWriter22(rs *State22) *StateWriter22 {
	return &StateWriter22{
		rs:           rs,
		writeLists:   newWriteList(),
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
	w.writeLists = newWriteList()
	w.accountPrevs = map[string][]byte{}
	w.accountDels = map[string]*accounts.Account{}
	w.storagePrevs = map[string][]byte{}
	w.codePrevs = map[string]uint64{}
}

func (w *StateWriter22) WriteSet() map[string]*exec22.KvList {
	return w.writeLists
}

func (w *StateWriter22) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *StateWriter22) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addressBytes := address.Bytes()
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, addressBytes)
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value)
	var prev []byte
	if original.Initialised {
		prev = accounts.Serialise2(original)
	}
	w.accountPrevs[string(addressBytes)] = prev
	return nil
}

func (w *StateWriter22) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	addressBytes, codeHashBytes := address.Bytes(), codeHash.Bytes()
	w.writeLists[kv.Code].Keys = append(w.writeLists[kv.Code].Keys, codeHashBytes)
	w.writeLists[kv.Code].Vals = append(w.writeLists[kv.Code].Vals, code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.writeLists[kv.PlainContractCode].Keys = append(w.writeLists[kv.PlainContractCode].Keys, dbutils.PlainGenerateStoragePrefix(addressBytes, incarnation))
		w.writeLists[kv.PlainContractCode].Vals = append(w.writeLists[kv.PlainContractCode].Vals, codeHashBytes)
	}
	w.codePrevs[string(addressBytes)] = incarnation
	return nil
}

func (w *StateWriter22) DeleteAccount(address common.Address, original *accounts.Account) error {
	addressBytes := address.Bytes()
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, addressBytes)
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, []byte{})
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		w.writeLists[kv.IncarnationMap].Keys = append(w.writeLists[kv.IncarnationMap].Keys, addressBytes)
		w.writeLists[kv.IncarnationMap].Vals = append(w.writeLists[kv.IncarnationMap].Vals, b[:])
	}
	if original.Initialised {
		w.accountDels[string(addressBytes)] = original
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
	readLists map[string]*exec22.KvList

	stateHint *btree2.PathHint
}

func NewStateReader22(rs *State22) *StateReader22 {
	return &StateReader22{
		rs:        rs,
		readLists: newReadList(),
		stateHint: &btree2.PathHint{},
	}
}

func (r *StateReader22) SetTxNum(txNum uint64) {
	r.txNum = txNum
}

func (r *StateReader22) SetTx(tx kv.Tx) {
	r.tx = tx
}

func (r *StateReader22) ResetReadSet() {
	r.readLists = newReadList()
}

func (r *StateReader22) ReadSet() map[string]*exec22.KvList {
	return r.readLists
}

func (r *StateReader22) SetTrace(trace bool) {
	r.trace = trace
}

func (r *StateReader22) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()
	enc := r.rs.GetHint(kv.PlainState, addr, r.stateHint)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, addr)
		if err != nil {
			return nil, err
		}
	}
	// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
	r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, addr)
	r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, enc)
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
	composite := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc := r.rs.GetHint(kv.PlainState, composite, r.stateHint)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, composite)
		if err != nil {
			return nil, err
		}
	}
	r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, composite)
	r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, enc)
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
	r.readLists[kv.Code].Vals = append(r.readLists[kv.Code].Vals, enc)
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
	r.readLists[kv.IncarnationMap].Vals = append(r.readLists[kv.IncarnationMap].Vals, enc)
	if len(enc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(enc), nil
}

var writeListPool = sync.Pool{
	New: func() any {
		return map[string]*exec22.KvList{
			kv.PlainState:        {Keys: make([][]byte, 0, 128), Vals: make([][]byte, 0, 128)},
			kv.Code:              {Keys: make([][]byte, 0, 16), Vals: make([][]byte, 0, 16)},
			kv.PlainContractCode: {Keys: make([][]byte, 0, 16), Vals: make([][]byte, 0, 16)},
			kv.IncarnationMap:    {Keys: make([][]byte, 0, 16), Vals: make([][]byte, 0, 16)},
		}
	},
}

func newWriteList() map[string]*exec22.KvList {
	v := writeListPool.Get().(map[string]*exec22.KvList)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
}
func returnWriteList(v map[string]*exec22.KvList) {
	if v == nil {
		return
	}
	writeListPool.Put(v)
}

var readListPool = sync.Pool{
	New: func() any {
		return map[string]*exec22.KvList{
			kv.PlainState:     {Keys: make([][]byte, 0, 512), Vals: make([][]byte, 0, 512)},
			kv.Code:           {Keys: make([][]byte, 0, 16), Vals: make([][]byte, 0, 16)},
			CodeSizeTable:     {Keys: make([][]byte, 0, 16), Vals: make([][]byte, 0, 16)},
			kv.IncarnationMap: {Keys: make([][]byte, 0, 16), Vals: make([][]byte, 0, 16)},
		}
	},
}

func newReadList() map[string]*exec22.KvList {
	v := readListPool.Get().(map[string]*exec22.KvList)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
}
func returnReadList(v map[string]*exec22.KvList) {
	if v == nil {
		return
	}
	readListPool.Put(v)
}
