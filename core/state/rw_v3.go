package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
	atomic2 "go.uber.org/atomic"
)

const CodeSizeTable = "CodeSize"

type StateV3 struct {
	lock         sync.RWMutex
	receiveWork  *sync.Cond
	triggers     map[uint64]*exec22.TxTask
	senderTxNums map[common.Address]uint64
	triggerLock  sync.Mutex
	queue        exec22.TxTaskQueue
	queueLock    sync.Mutex
	changes      map[string]*btree2.Map[string, []byte]
	sizeEstimate int
	txsDone      *atomic2.Uint64
	finished     atomic2.Bool
}

func NewStateV3() *StateV3 {
	rs := &StateV3{
		triggers:     map[uint64]*exec22.TxTask{},
		senderTxNums: map[common.Address]uint64{},
		changes: map[string]*btree2.Map[string, []byte]{
			kv.PlainState:        btree2.NewMap[string, []byte](128),
			kv.Code:              btree2.NewMap[string, []byte](128),
			kv.IncarnationMap:    btree2.NewMap[string, []byte](128),
			kv.PlainContractCode: btree2.NewMap[string, []byte](128),
		},
		txsDone: atomic2.NewUint64(0),
	}
	rs.receiveWork = sync.NewCond(&rs.queueLock)
	return rs
}

func (rs *StateV3) put(table string, key, val []byte) {
	old, ok := rs.changes[table].Set(string(key), val)
	if ok {
		rs.sizeEstimate += len(val) - len(old)
	} else {
		rs.sizeEstimate += len(key) + len(val)
	}
}

func (rs *StateV3) puts(table string, key string, val []byte) {
	old, ok := rs.changes[table].Set(key, val)
	if ok {
		rs.sizeEstimate += len(val) - len(old)
	} else {
		rs.sizeEstimate += len(key) + len(val)
	}
}

func (rs *StateV3) Get(table string, key []byte) []byte {
	rs.lock.RLock()
	v := rs.get(table, key)
	rs.lock.RUnlock()
	return v
}

func (rs *StateV3) get(table string, key []byte) (v []byte) {
	v, _ = rs.changes[table].Get(*(*string)(unsafe.Pointer(&key)))
	return v
}

func (rs *StateV3) Flush(ctx context.Context, rwTx kv.RwTx, logPrefix string, logEvery *time.Ticker) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		c, err := rwTx.RwCursor(table)
		if err != nil {
			return err
		}

		iter := t.Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			if len(iter.Value()) == 0 {
				if err = c.Delete([]byte(iter.Key())); err != nil {
					return err
				}
				//fmt.Printf("Flush [%x]=>\n", item.key)
			} else {
				if err = c.Put([]byte(iter.Key()), iter.Value()); err != nil {
					return err
				}
				//fmt.Printf("Flush [%x]=>[%x]\n", item.key, item.val)
			}

			select {
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Flush", logPrefix), "table", table, "current_prefix", hex.EncodeToString([]byte(iter.Key())[:4]))
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if err != nil {
			return err
		}
		t.Clear()
	}
	rs.sizeEstimate = 0
	return nil
}

func (rs *StateV3) QueueLen() int {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	return rs.queue.Len()
}

func (rs *StateV3) Schedule() (*exec22.TxTask, bool) {
	rs.queueLock.Lock()
	defer rs.queueLock.Unlock()
	for !rs.finished.Load() && rs.queue.Len() == 0 {
		rs.receiveWork.Wait()
	}
	if rs.finished.Load() {
		return nil, false
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(*exec22.TxTask), true
	}
	return nil, false
}

func (rs *StateV3) RegisterSender(txTask *exec22.TxTask) bool {
	//TODO: it deadlocks on panic, fix it
	defer func() {
		rec := recover()
		if rec != nil {
			fmt.Printf("panic?: %s,%s\n", rec, dbg.Stack())
		}
	}()
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

func (rs *StateV3) CommitTxNum(sender *common.Address, txNum uint64) uint64 {
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

func (rs *StateV3) queuePush(t *exec22.TxTask) int {
	rs.queueLock.Lock()
	heap.Push(&rs.queue, t)
	l := len(rs.queue)
	rs.queueLock.Unlock()
	return l
}

func (rs *StateV3) AddWork(txTask *exec22.TxTask) (queueLen int) {
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
	queueLen = rs.queuePush(txTask)
	rs.receiveWork.Signal()
	return queueLen
}

func (rs *StateV3) Finish() {
	rs.finished.Store(true)
	rs.receiveWork.Broadcast()
}

func (rs *StateV3) appplyState1(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	if len(txTask.AccountDels) > 0 {
		cursor, err := roTx.Cursor(kv.PlainState)
		if err != nil {
			return err
		}
		defer cursor.Close()
		addr1 := make([]byte, 20+8)
		psChanges := rs.changes[kv.PlainState]
		for addrS, original := range txTask.AccountDels {
			addr := []byte(addrS)
			copy(addr1, addr)
			binary.BigEndian.PutUint64(addr1[len(addr):], original.Incarnation)

			prev := accounts.SerialiseV3(original)
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
				//TODO: try full-scan, then can replace btree by map
				iter := psChanges.Iter()
				for ok := iter.Seek(string(addr1)); ok; ok = iter.Next() {
					key := []byte(iter.Key())
					if !bytes.HasPrefix(key, addr1) {
						break
					}
					for ; e == nil && k != nil && bytes.HasPrefix(k, addr1) && bytes.Compare(k, key) <= 0; k, v, e = cursor.Next() {
						if !bytes.Equal(k, key) {
							// Skip the cursor item when the key is equal, i.e. prefer the item from the changes tree
							if e = agg.AddStoragePrev(addr, k[28:], v); e != nil {
								return e
							}
						}
					}
					if e != nil {
						return e
					}
					if e = agg.AddStoragePrev(addr, key[28:], iter.Value()); e != nil {
						break
					}
				}
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

func (rs *StateV3) appplyState(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	emptyRemoval := txTask.Rules.IsSpuriousDragon
	rs.lock.Lock()
	defer rs.lock.Unlock()

	for addr, increase := range txTask.BalanceIncreaseSet {
		increase := increase
		addrBytes := addr.Bytes()
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
			enc0 = accounts.SerialiseV3(&a)
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

	if txTask.WriteLists != nil {
		for table, list := range txTask.WriteLists {
			for i, key := range list.Keys {
				rs.puts(table, key, list.Vals[i])
			}
		}
	}
	return nil
}

func (rs *StateV3) ApplyState(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

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

func (rs *StateV3) ApplyHistory(txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	if dbg.DiscardHistory() {
		return nil
	}
	defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

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

func (rs *StateV3) Unwind(ctx context.Context, tx kv.RwTx, txUnwindTo uint64, agg *libstate.AggregatorV3, accumulator *shards.Accumulator) error {
	agg.SetTx(tx)
	var currentInc uint64
	if err := agg.Unwind(ctx, txUnwindTo, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == length.Addr {
			if len(v) > 0 {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, v); err != nil {
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
			accumulator.ChangeStorage(address, currentInc, location, common.Copy(v))
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

func (rs *StateV3) DoneCount() uint64 { return rs.txsDone.Load() }

func (rs *StateV3) SizeEstimate() uint64 {
	rs.lock.RLock()
	r := rs.sizeEstimate
	rs.lock.RUnlock()
	return uint64(r)
}

func (rs *StateV3) ReadsValid(readLists map[string]*exec22.KvList) bool {
	var t *btree2.Map[string, []byte]

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
			if val, ok := t.Get(key); ok {
				if table == CodeSizeTable {
					if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(val)) {
						return false
					}
				} else if !bytes.Equal(list.Vals[i], val) {
					return false
				}
			}
		}
	}
	return true
}

type StateWriterV3 struct {
	rs           *StateV3
	txNum        uint64
	writeLists   map[string]*exec22.KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
}

func NewStateWriterV3(rs *StateV3) *StateWriterV3 {
	return &StateWriterV3{
		rs:         rs,
		writeLists: newWriteList(),
	}
}

func (w *StateWriterV3) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateWriterV3) ResetWriteSet() {
	w.writeLists = newWriteList()
	w.accountPrevs = nil
	w.accountDels = nil
	w.storagePrevs = nil
	w.codePrevs = nil
}

func (w *StateWriterV3) WriteSet() map[string]*exec22.KvList {
	return w.writeLists
}

func (w *StateWriterV3) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *StateWriterV3) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addressBytes := address.Bytes()
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, string(addressBytes))
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value)
	var prev []byte
	if original.Initialised {
		prev = accounts.SerialiseV3(original)
	}
	if w.accountPrevs == nil {
		w.accountPrevs = map[string][]byte{}
	}
	w.accountPrevs[string(addressBytes)] = prev
	return nil
}

func (w *StateWriterV3) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	addressBytes, codeHashBytes := address.Bytes(), codeHash.Bytes()
	w.writeLists[kv.Code].Keys = append(w.writeLists[kv.Code].Keys, string(codeHashBytes))
	w.writeLists[kv.Code].Vals = append(w.writeLists[kv.Code].Vals, code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.writeLists[kv.PlainContractCode].Keys = append(w.writeLists[kv.PlainContractCode].Keys, string(dbutils.PlainGenerateStoragePrefix(addressBytes, incarnation)))
		w.writeLists[kv.PlainContractCode].Vals = append(w.writeLists[kv.PlainContractCode].Vals, codeHashBytes)
	}

	if w.codePrevs == nil {
		w.codePrevs = map[string]uint64{}
	}
	w.codePrevs[string(addressBytes)] = incarnation
	return nil
}

func (w *StateWriterV3) DeleteAccount(address common.Address, original *accounts.Account) error {
	addressBytes := address.Bytes()
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, string(addressBytes))
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, []byte{})
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		w.writeLists[kv.IncarnationMap].Keys = append(w.writeLists[kv.IncarnationMap].Keys, string(addressBytes))
		w.writeLists[kv.IncarnationMap].Vals = append(w.writeLists[kv.IncarnationMap].Vals, b[:])
	}
	if original.Initialised {
		if w.accountDels == nil {
			w.accountDels = map[string]*accounts.Account{}
		}
		w.accountDels[string(addressBytes)] = original
	}
	return nil
}

func (w *StateWriterV3) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	composite := dbutils.PlainGenerateCompositeStorageKey(address[:], incarnation, key.Bytes())
	cmpositeS := string(composite)
	w.writeLists[kv.PlainState].Keys = append(w.writeLists[kv.PlainState].Keys, cmpositeS)
	w.writeLists[kv.PlainState].Vals = append(w.writeLists[kv.PlainState].Vals, value.Bytes())
	//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, v, w.txNum)
	if w.storagePrevs == nil {
		w.storagePrevs = map[string][]byte{}
	}
	w.storagePrevs[cmpositeS] = original.Bytes()
	return nil
}

func (w *StateWriterV3) CreateContract(address common.Address) error {
	return nil
}

type StateReaderV3 struct {
	tx        kv.Tx
	txNum     uint64
	trace     bool
	rs        *StateV3
	composite []byte

	discardReadList bool
	readLists       map[string]*exec22.KvList
}

func NewStateReaderV3(rs *StateV3) *StateReaderV3 {
	return &StateReaderV3{
		rs:        rs,
		readLists: newReadList(),
	}
}

func (r *StateReaderV3) DiscardReadList()                   { r.discardReadList = true }
func (r *StateReaderV3) SetTxNum(txNum uint64)              { r.txNum = txNum }
func (r *StateReaderV3) SetTx(tx kv.Tx)                     { r.tx = tx }
func (r *StateReaderV3) ReadSet() map[string]*exec22.KvList { return r.readLists }
func (r *StateReaderV3) SetTrace(trace bool)                { r.trace = trace }
func (r *StateReaderV3) ResetReadSet()                      { r.readLists = newReadList() }

func (r *StateReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()
	enc := r.rs.Get(kv.PlainState, addr)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, addr)
		if err != nil {
			return nil, err
		}
	}
	if !r.discardReadList {
		// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
		r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, string(addr))
		r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, enc)
	}
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

func (r *StateReaderV3) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	composite := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc := r.rs.Get(kv.PlainState, composite)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, composite)
		if err != nil {
			return nil, err
		}
	}
	if !r.discardReadList {
		r.readLists[kv.PlainState].Keys = append(r.readLists[kv.PlainState].Keys, string(composite))
		r.readLists[kv.PlainState].Vals = append(r.readLists[kv.PlainState].Vals, enc)
	}
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

func (r *StateReaderV3) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	addr, codeHashBytes := address.Bytes(), codeHash.Bytes()
	enc := r.rs.Get(kv.Code, codeHashBytes)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHashBytes)
		if err != nil {
			return nil, err
		}
	}
	if !r.discardReadList {
		r.readLists[kv.Code].Keys = append(r.readLists[kv.Code].Keys, string(addr))
		r.readLists[kv.Code].Vals = append(r.readLists[kv.Code].Vals, enc)
	}
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *StateReaderV3) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
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
	if !r.discardReadList {
		r.readLists[CodeSizeTable].Keys = append(r.readLists[CodeSizeTable].Keys, string(address[:]))
		r.readLists[CodeSizeTable].Vals = append(r.readLists[CodeSizeTable].Vals, sizebuf[:])
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, r.txNum)
	}
	return size, nil
}

func (r *StateReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	addrBytes := address[:]
	enc := r.rs.Get(kv.IncarnationMap, addrBytes)
	if enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.IncarnationMap, addrBytes)
		if err != nil {
			return 0, err
		}
	}
	if !r.discardReadList {
		r.readLists[kv.IncarnationMap].Keys = append(r.readLists[kv.IncarnationMap].Keys, string(addrBytes))
		r.readLists[kv.IncarnationMap].Vals = append(r.readLists[kv.IncarnationMap].Vals, enc)
	}
	if len(enc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(enc), nil
}

var writeListPool = sync.Pool{
	New: func() any {
		return map[string]*exec22.KvList{
			kv.PlainState:        {},
			kv.Code:              {},
			kv.PlainContractCode: {},
			kv.IncarnationMap:    {},
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
			kv.PlainState:     {},
			kv.Code:           {},
			CodeSizeTable:     {},
			kv.IncarnationMap: {},
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
