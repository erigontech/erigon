package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/metrics"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
)

const CodeSizeTable = "CodeSize"
const StorageTable = "Storage"

var ExecTxsDone = metrics.NewCounter(`exec_txs_done`)

type StateV3 struct {
	lock           sync.RWMutex
	sizeEstimate   int
	chCode         map[string][]byte
	chAccs         map[string][]byte
	chStorage      *btree2.Map[string, []byte]
	chIncs         map[string][]byte
	chContractCode map[string][]byte

	triggers     map[uint64]*exec22.TxTask
	senderTxNums map[common.Address]uint64
	triggerLock  sync.Mutex

	tmpdir              string
	applyPrevAccountBuf []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	addrIncBuf          []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	logger              log.Logger
}

func NewStateV3(tmpdir string, logger log.Logger) *StateV3 {
	rs := &StateV3{
		tmpdir:         tmpdir,
		triggers:       map[uint64]*exec22.TxTask{},
		senderTxNums:   map[common.Address]uint64{},
		chCode:         map[string][]byte{},
		chAccs:         map[string][]byte{},
		chStorage:      btree2.NewMap[string, []byte](128),
		chIncs:         map[string][]byte{},
		chContractCode: map[string][]byte{},

		applyPrevAccountBuf: make([]byte, 256),
		addrIncBuf:          make([]byte, 20+8),
		logger:              logger,
	}
	return rs
}

func (rs *StateV3) put(table string, key, val []byte) {
	rs.puts(table, string(key), val)
}

func (rs *StateV3) puts(table string, key string, val []byte) {
	switch table {
	case StorageTable:
		if old, ok := rs.chStorage.Set(key, val); ok {
			rs.sizeEstimate += len(val) - len(old)
		} else {
			rs.sizeEstimate += len(key) + len(val)
		}
	case kv.PlainState:
		if old, ok := rs.chAccs[key]; ok {
			rs.sizeEstimate += len(val) - len(old)
		} else {
			rs.sizeEstimate += len(key) + len(val)
		}
		rs.chAccs[key] = val
	case kv.Code:
		if old, ok := rs.chCode[key]; ok {
			rs.sizeEstimate += len(val) - len(old)
		} else {
			rs.sizeEstimate += len(key) + len(val)
		}
		rs.chCode[key] = val
	case kv.IncarnationMap:
		if old, ok := rs.chIncs[key]; ok {
			rs.sizeEstimate += len(val) - len(old)
		} else {
			rs.sizeEstimate += len(key) + len(val)
		}
		rs.chIncs[key] = val
	case kv.PlainContractCode:
		if old, ok := rs.chContractCode[key]; ok {
			rs.sizeEstimate += len(val) - len(old)
		} else {
			rs.sizeEstimate += len(key) + len(val)
		}
		rs.chContractCode[key] = val
	default:
		panic(table)
	}
}

func (rs *StateV3) Get(table string, key []byte) (v []byte, ok bool) {
	rs.lock.RLock()
	v, ok = rs.get(table, key)
	rs.lock.RUnlock()
	return v, ok
}

func (rs *StateV3) get(table string, key []byte) (v []byte, ok bool) {
	keyS := *(*string)(unsafe.Pointer(&key))
	switch table {
	case StorageTable:
		v, ok = rs.chStorage.Get(keyS)
	case kv.PlainState:
		v, ok = rs.chAccs[keyS]
	case kv.Code:
		v, ok = rs.chCode[keyS]
	case kv.IncarnationMap:
		v, ok = rs.chIncs[keyS]
	case kv.PlainContractCode:
		v, ok = rs.chContractCode[keyS]
	default:
		panic(table)
	}
	return v, ok
}

func (rs *StateV3) flushMap(ctx context.Context, rwTx kv.RwTx, table string, m map[string][]byte, logPrefix string, logEvery *time.Ticker) error {
	collector := etl.NewCollector(logPrefix, "", etl.NewSortableBuffer(etl.BufferOptimalSize), rs.logger)
	defer collector.Close()

	var count int
	total := len(m)
	for k, v := range m {
		if err := collector.Collect([]byte(k), v); err != nil {
			return err
		}
		count++
		select {
		default:
		case <-logEvery.C:
			progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, float64(total)/1_000_000)
			rs.logger.Info("Write to db", "progress", progress, "current table", table)
			rwTx.CollectMetrics()
		}
	}
	if err := collector.Load(rwTx, table, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	return nil
}
func (rs *StateV3) flushBtree(ctx context.Context, rwTx kv.RwTx, table string, m *btree2.Map[string, []byte], logPrefix string, logEvery *time.Ticker) error {
	c, err := rwTx.RwCursor(table)
	if err != nil {
		return err
	}
	defer c.Close()
	iter := m.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if len(iter.Value()) == 0 {
			if err = c.Delete([]byte(iter.Key())); err != nil {
				return err
			}
		} else {
			if err = c.Put([]byte(iter.Key()), iter.Value()); err != nil {
				return err
			}
		}

		select {
		case <-logEvery.C:
			rs.logger.Info(fmt.Sprintf("[%s] Flush", logPrefix), "table", table, "current_prefix", hex.EncodeToString([]byte(iter.Key())[:4]))
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (rs *StateV3) Flush(ctx context.Context, rwTx kv.RwTx, logPrefix string, logEvery *time.Ticker) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	if err := rs.flushMap(ctx, rwTx, kv.PlainState, rs.chAccs, logPrefix, logEvery); err != nil {
		return err
	}
	rs.chAccs = map[string][]byte{}
	if err := rs.flushBtree(ctx, rwTx, kv.PlainState, rs.chStorage, logPrefix, logEvery); err != nil {
		return err
	}
	rs.chStorage.Clear()
	if err := rs.flushMap(ctx, rwTx, kv.Code, rs.chCode, logPrefix, logEvery); err != nil {
		return err
	}
	rs.chCode = map[string][]byte{}
	if err := rs.flushMap(ctx, rwTx, kv.PlainContractCode, rs.chContractCode, logPrefix, logEvery); err != nil {
		return err
	}
	rs.chContractCode = map[string][]byte{}
	if err := rs.flushMap(ctx, rwTx, kv.IncarnationMap, rs.chIncs, logPrefix, logEvery); err != nil {
		return err
	}
	rs.chIncs = map[string][]byte{}

	rs.sizeEstimate = 0
	return nil
}

func (rs *StateV3) ReTry(txTask *exec22.TxTask, in *exec22.QueueWithRetry) {
	rs.resetTxTask(txTask)
	in.ReTry(txTask)
}
func (rs *StateV3) AddWork(ctx context.Context, txTask *exec22.TxTask, in *exec22.QueueWithRetry) {
	rs.resetTxTask(txTask)
	in.Add(ctx, txTask)
}
func (rs *StateV3) resetTxTask(txTask *exec22.TxTask) {
	txTask.BalanceIncreaseSet = nil
	returnReadList(txTask.ReadLists)
	txTask.ReadLists = nil
	returnWriteList(txTask.WriteLists)
	txTask.WriteLists = nil
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

func (rs *StateV3) CommitTxNum(sender *common.Address, txNum uint64, in *exec22.QueueWithRetry) (count int) {
	ExecTxsDone.Inc()

	rs.triggerLock.Lock()
	defer rs.triggerLock.Unlock()
	if triggered, ok := rs.triggers[txNum]; ok {
		in.ReTry(triggered)
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

func (rs *StateV3) writeStateHistory(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	if len(txTask.AccountDels) > 0 {
		cursor, err := roTx.Cursor(kv.PlainState)
		if err != nil {
			return err
		}
		defer cursor.Close()
		addr1 := rs.addrIncBuf
		for addrS, original := range txTask.AccountDels {
			addr := []byte(addrS)
			copy(addr1, addr)
			binary.BigEndian.PutUint64(addr1[len(addr):], original.Incarnation)

			prev := rs.applyPrevAccountBuf[:accounts.SerialiseV3Len(original)]
			accounts.SerialiseV3To(original, prev)
			if err := agg.AddAccountPrev(addr, prev); err != nil {
				return err
			}
			codeHashBytes := original.CodeHash.Bytes()
			codePrev, ok := rs.get(kv.Code, codeHashBytes)
			if !ok || codePrev == nil {
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
			//TODO: try full-scan, then can replace btree by map
			iter := rs.chStorage.Iter()
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

	k := rs.addrIncBuf
	for addrS, incarnation := range txTask.CodePrevs {
		addr := []byte(addrS)
		copy(k, addr)
		binary.BigEndian.PutUint64(k[20:], incarnation)

		codeHash, ok := rs.get(kv.PlainContractCode, k)
		if !ok || codeHash == nil {
			var err error
			codeHash, err = roTx.GetOne(kv.PlainContractCode, k)
			if err != nil {
				return err
			}
		}
		var codePrev []byte
		if codeHash != nil {
			codePrev, ok = rs.get(kv.Code, codeHash)
			if !ok || codePrev == nil {
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

func (rs *StateV3) applyState(roTx kv.Tx, txTask *exec22.TxTask, agg *libstate.AggregatorV3) error {
	emptyRemoval := txTask.Rules.IsSpuriousDragon
	rs.lock.Lock()
	defer rs.lock.Unlock()

	for addr, increase := range txTask.BalanceIncreaseSet {
		increase := increase
		addrBytes := addr.Bytes()
		enc0, ok := rs.get(kv.PlainState, addrBytes)
		if !ok {
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
			enc1 = nil
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
	if err := rs.writeStateHistory(roTx, txTask, agg); err != nil {
		return err
	}
	if err := rs.applyState(roTx, txTask, agg); err != nil {
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
			if err := agg.PutIdx(kv.TblTracesFromIdx, addr[:]); err != nil {
				return err
			}
		}
	}
	if txTask.TraceTos != nil {
		for addr := range txTask.TraceTos {
			if err := agg.PutIdx(kv.TblTracesToIdx, addr[:]); err != nil {
				return err
			}
		}
	}
	for _, log := range txTask.Logs {
		if err := agg.PutIdx(kv.TblLogAddressIdx, log.Address[:]); err != nil {
			return err
		}
		for _, topic := range log.Topics {
			if err := agg.PutIdx(kv.LogTopicIndex, topic[:]); err != nil {
				return err
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
	handle := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == length.Addr {
			if len(v) > 0 {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, v); err != nil {
					return fmt.Errorf("%w, %x", err, v)
				}
				currentInc = acc.Incarnation
				// Fetch the code hash
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
	}
	stateChanges := etl.NewCollector("", "", etl.NewOldestEntryBuffer(etl.BufferOptimalSize), rs.logger)
	defer stateChanges.Close()
	actx := tx.(*temporal.Tx).AggCtx()
	{
		iter, err := actx.AccountHistoryRange(int(txUnwindTo), -1, order.Asc, -1, tx)
		if err != nil {
			return err
		}
		for iter.HasNext() {
			k, v, err := iter.Next()
			if err != nil {
				return err
			}
			if err := stateChanges.Collect(k, v); err != nil {
				return err
			}
		}
	}
	{
		iter, err := actx.StorageHistoryRange(int(txUnwindTo), -1, order.Asc, -1, tx)
		if err != nil {
			return err
		}
		for iter.HasNext() {
			k, v, err := iter.Next()
			if err != nil {
				return err
			}
			if err := stateChanges.Collect(k, v); err != nil {
				return err
			}
		}
	}

	if err := stateChanges.Load(tx, kv.PlainState, handle, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := agg.Unwind(ctx, txUnwindTo); err != nil {
		return err
	}
	return nil
}

func (rs *StateV3) DoneCount() uint64 { return ExecTxsDone.Get() }

func (rs *StateV3) SizeEstimate() (r uint64) {
	rs.lock.RLock()
	r = uint64(rs.sizeEstimate)
	rs.lock.RUnlock()
	return r * 2 // multiply 2 here, to cover data-structures overhead. more precise accounting - expensive.
}

func (rs *StateV3) ReadsValid(readLists map[string]*libstate.KvList) bool {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	for table, list := range readLists {
		switch table {
		case kv.PlainState:
			if !rs.readsValidMap(table, list, rs.chAccs) {
				return false
			}
		case CodeSizeTable:
			if !rs.readsValidMap(table, list, rs.chCode) {
				return false
			}
		case StorageTable:
			if !rs.readsValidBtree(table, list, rs.chStorage) {
				return false
			}
		case kv.Code:
			if !rs.readsValidMap(table, list, rs.chCode) {
				return false
			}
		case kv.IncarnationMap:
			if !rs.readsValidMap(table, list, rs.chIncs) {
				return false
			}
		}
	}
	return true
}

func (rs *StateV3) readsValidMap(table string, list *libstate.KvList, m map[string][]byte) bool {
	switch table {
	case CodeSizeTable:
		for i, key := range list.Keys {
			if val, ok := m[key]; ok {
				if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(val)) {
					return false
				}
			}
		}
	default:
		for i, key := range list.Keys {
			if val, ok := m[key]; ok {
				if !bytes.Equal(list.Vals[i], val) {
					return false
				}
			}
		}
	}
	return true
}

func (rs *StateV3) readsValidBtree(table string, list *libstate.KvList, m *btree2.Map[string, []byte]) bool {
	for i, key := range list.Keys {
		if val, ok := m.Get(key); ok {
			if !bytes.Equal(list.Vals[i], val) {
				return false
			}
		}
	}
	return true
}

// StateWriterBufferedV3 - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type StateWriterBufferedV3 struct {
	rs           *StateV3
	txNum        uint64
	writeLists   map[string]*libstate.KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
}

func NewStateWriterBufferedV3(rs *StateV3) *StateWriterBufferedV3 {
	return &StateWriterBufferedV3{
		rs:         rs,
		writeLists: newWriteList(),
	}
}

func (w *StateWriterBufferedV3) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateWriterBufferedV3) ResetWriteSet() {
	w.writeLists = newWriteList()
	w.accountPrevs = nil
	w.accountDels = nil
	w.storagePrevs = nil
	w.codePrevs = nil
}

func (w *StateWriterBufferedV3) WriteSet() map[string]*libstate.KvList {
	return w.writeLists
}

func (w *StateWriterBufferedV3) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *StateWriterBufferedV3) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addressBytes := address.Bytes()
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.writeLists[kv.PlainState].Push(string(addressBytes), value)
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

func (w *StateWriterBufferedV3) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	addressBytes, codeHashBytes := address.Bytes(), codeHash.Bytes()
	w.writeLists[kv.Code].Push(string(codeHashBytes), code)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
		w.writeLists[kv.PlainContractCode].Push(string(dbutils.PlainGenerateStoragePrefix(addressBytes, incarnation)), codeHashBytes)
	}

	if w.codePrevs == nil {
		w.codePrevs = map[string]uint64{}
	}
	w.codePrevs[string(addressBytes)] = incarnation
	return nil
}

func (w *StateWriterBufferedV3) DeleteAccount(address common.Address, original *accounts.Account) error {
	addressBytes := address.Bytes()
	w.writeLists[kv.PlainState].Push(string(addressBytes), nil)
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		w.writeLists[kv.IncarnationMap].Push(string(addressBytes), b[:])
	}
	if original.Initialised {
		if w.accountDels == nil {
			w.accountDels = map[string]*accounts.Account{}
		}
		w.accountDels[string(addressBytes)] = original
	}
	return nil
}

func (w *StateWriterBufferedV3) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	composite := dbutils.PlainGenerateCompositeStorageKey(address[:], incarnation, key.Bytes())
	cmpositeS := string(composite)
	w.writeLists[StorageTable].Push(cmpositeS, value.Bytes())
	//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, v, w.txNum)
	if w.storagePrevs == nil {
		w.storagePrevs = map[string][]byte{}
	}
	w.storagePrevs[cmpositeS] = original.Bytes()
	return nil
}

func (w *StateWriterBufferedV3) CreateContract(address common.Address) error {
	return nil
}

type StateReaderV3 struct {
	tx        kv.Tx
	txNum     uint64
	trace     bool
	rs        *StateV3
	composite []byte

	discardReadList bool
	readLists       map[string]*libstate.KvList
}

func NewStateReaderV3(rs *StateV3) *StateReaderV3 {
	return &StateReaderV3{
		rs:        rs,
		readLists: newReadList(),
	}
}

func (r *StateReaderV3) DiscardReadList()                     { r.discardReadList = true }
func (r *StateReaderV3) SetTxNum(txNum uint64)                { r.txNum = txNum }
func (r *StateReaderV3) SetTx(tx kv.Tx)                       { r.tx = tx }
func (r *StateReaderV3) ReadSet() map[string]*libstate.KvList { return r.readLists }
func (r *StateReaderV3) SetTrace(trace bool)                  { r.trace = trace }
func (r *StateReaderV3) ResetReadSet()                        { r.readLists = newReadList() }

func (r *StateReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()
	enc, ok := r.rs.Get(kv.PlainState, addr)
	if !ok {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, addr)
		if err != nil {
			return nil, err
		}
	}
	if !r.discardReadList {
		// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
		r.readLists[kv.PlainState].Push(string(addr), enc)
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
	enc, ok := r.rs.Get(StorageTable, composite)
	if !ok || enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.PlainState, composite)
		if err != nil {
			return nil, err
		}
	}
	if !r.discardReadList {
		r.readLists[StorageTable].Push(string(composite), enc)
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
	enc, ok := r.rs.Get(kv.Code, codeHashBytes)
	if !ok || enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHashBytes)
		if err != nil {
			return nil, err
		}
	}
	if !r.discardReadList {
		r.readLists[kv.Code].Push(string(addr), enc)
	}
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *StateReaderV3) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	codeHashBytes := codeHash.Bytes()
	enc, ok := r.rs.Get(kv.Code, codeHashBytes)
	if !ok || enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.Code, codeHashBytes)
		if err != nil {
			return 0, err
		}
	}
	var sizebuf [8]byte
	binary.BigEndian.PutUint64(sizebuf[:], uint64(len(enc)))
	if !r.discardReadList {
		r.readLists[CodeSizeTable].Push(string(address[:]), sizebuf[:])
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, r.txNum)
	}
	return size, nil
}

func (r *StateReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	addrBytes := address[:]
	enc, ok := r.rs.Get(kv.IncarnationMap, addrBytes)
	if !ok || enc == nil {
		var err error
		enc, err = r.tx.GetOne(kv.IncarnationMap, addrBytes)
		if err != nil {
			return 0, err
		}
	}
	if !r.discardReadList {
		r.readLists[kv.IncarnationMap].Push(string(addrBytes), enc)
	}
	if len(enc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(enc), nil
}

var writeListPool = sync.Pool{
	New: func() any {
		return map[string]*libstate.KvList{
			kv.PlainState:        {},
			StorageTable:         {},
			kv.Code:              {},
			kv.PlainContractCode: {},
			kv.IncarnationMap:    {},
		}
	},
}

func newWriteList() map[string]*libstate.KvList {
	v := writeListPool.Get().(map[string]*libstate.KvList)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
}
func returnWriteList(v map[string]*libstate.KvList) {
	if v == nil {
		return
	}
	writeListPool.Put(v)
}

var readListPool = sync.Pool{
	New: func() any {
		return map[string]*libstate.KvList{
			kv.PlainState:     {},
			kv.Code:           {},
			CodeSizeTable:     {},
			StorageTable:      {},
			kv.IncarnationMap: {},
		}
	},
}

func newReadList() map[string]*libstate.KvList {
	v := readListPool.Get().(map[string]*libstate.KvList)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
}
func returnReadList(v map[string]*libstate.KvList) {
	if v == nil {
		return
	}
	readListPool.Put(v)
}
