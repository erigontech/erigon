package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/metrics"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

var ExecTxsDone = metrics.NewCounter(`exec_txs_done`)

type StateV3 struct {
	domains      *libstate.SharedDomains
	triggerLock  sync.Mutex
	triggers     map[uint64]*TxTask
	senderTxNums map[common.Address]uint64

	applyPrevAccountBuf []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	addrIncBuf          []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	logger              log.Logger
}

func NewStateV3(domains *libstate.SharedDomains, logger log.Logger) *StateV3 {
	return &StateV3{
		domains:             domains,
		triggers:            map[uint64]*TxTask{},
		senderTxNums:        map[common.Address]uint64{},
		applyPrevAccountBuf: make([]byte, 256),
		logger:              logger,
	}
}

func (rs *StateV3) ReTry(txTask *TxTask, in *QueueWithRetry) {
	rs.resetTxTask(txTask)
	in.ReTry(txTask)
}
func (rs *StateV3) AddWork(ctx context.Context, txTask *TxTask, in *QueueWithRetry) {
	rs.resetTxTask(txTask)
	in.Add(ctx, txTask)
}
func (rs *StateV3) resetTxTask(txTask *TxTask) {
	txTask.BalanceIncreaseSet = nil
	returnReadList(txTask.ReadLists)
	txTask.ReadLists = nil
	returnWriteList(txTask.WriteLists)
	txTask.WriteLists = nil
	txTask.Logs = nil
	txTask.TraceFroms = nil
	txTask.TraceTos = nil
}

func (rs *StateV3) RegisterSender(txTask *TxTask) bool {
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

func (rs *StateV3) CommitTxNum(sender *common.Address, txNum uint64, in *QueueWithRetry) (count int) {
	ExecTxsDone.Inc()

	if txNum > 0 && txNum%ethconfig.HistoryV3AggregationStep == 0 {
		if _, err := rs.Commitment(txNum, true); err != nil {
			panic(fmt.Errorf("txnum %d: %w", txNum, err))
		}
	}

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

const Assert = false

func (rs *StateV3) applyState(txTask *TxTask, domains *libstate.SharedDomains) error {
	var acc accounts.Account

	for table, list := range txTask.WriteLists {
		switch kv.Domain(table) {
		case kv.AccountsDomain:
			for i, key := range list.Keys {
				kb := []byte(key)
				prev, err := domains.LatestAccount(kb)
				if err != nil {
					return fmt.Errorf("latest account %x: %w", kb, err)
				}
				if list.Vals[i] == nil {
					if Assert {
						original := txTask.AccountDels[key]
						var originalBytes []byte
						if original != nil {
							originalBytes = accounts.SerialiseV3(original)
						}
						if !bytes.Equal(prev, originalBytes) {
							panic(fmt.Sprintf("different prev value %x, %x, %x, %t, %t\n", kb, prev, originalBytes, prev == nil, originalBytes == nil))
						}
					}

					if err := domains.DeleteAccount(kb, prev); err != nil {
						return err
					}
					//fmt.Printf("applied %x DELETE\n", kb)
				} else {
					if err := domains.UpdateAccountData(kb, list.Vals[i], prev); err != nil {
						return err
					}
					//acc.Reset()
					//accounts.DeserialiseV3(&acc, list.Vals[k])
					//fmt.Printf("applied %x b=%d n=%d c=%x\n", kb, &acc.Balance, acc.Nonce, acc.CodeHash)
				}
			}
		case kv.CodeDomain:
			for i, key := range list.Keys {
				if err := domains.UpdateAccountCode([]byte(key), list.Vals[i]); err != nil {
					return err
				}
			}
		case kv.StorageDomain:
			for k, key := range list.Keys {
				hkey := []byte(key)
				prev, err := domains.LatestStorage(hkey)
				if err != nil {
					return fmt.Errorf("latest account %x: %w", key, err)
				}
				//fmt.Printf("applied %x s=%x\n", hkey, list.Vals[k])
				if err := domains.WriteAccountStorage(hkey, nil, list.Vals[k], prev); err != nil {
					return err
				}
			}
		default:
			continue
		}
	}

	emptyRemoval := txTask.Rules.IsSpuriousDragon
	for addr, increase := range txTask.BalanceIncreaseSet {
		increase := increase
		addrBytes := addr.Bytes()
		enc0, err := domains.LatestAccount(addrBytes)
		if err != nil {
			return err
		}
		acc.Reset()
		if len(enc0) > 0 {
			if err := accounts.DeserialiseV3(&acc, enc0); err != nil {
				return err
			}
		}
		acc.Balance.Add(&acc.Balance, &increase)
		var enc1 []byte
		if emptyRemoval && acc.Nonce == 0 && acc.Balance.IsZero() && acc.IsEmptyCodeHash() {
			enc1 = nil
		} else {
			enc1 = accounts.SerialiseV3(&acc)
		}

		//fmt.Printf("+applied %x b=%d n=%d c=%x\n", []byte(addrBytes), &acc.Balance, acc.Nonce, acc.CodeHash.Bytes())
		if err := domains.UpdateAccountData(addrBytes, enc1, enc0); err != nil {
			return err
		}
	}
	return nil
}

func (rs *StateV3) Commitment(txNum uint64, saveState bool) ([]byte, error) {
	//defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()
	rs.domains.SetTxNum(txNum)

	return rs.domains.Commit(saveState, false)
}

func (rs *StateV3) Domains() *libstate.SharedDomains {
	return rs.domains
}

func (rs *StateV3) ApplyState4(txTask *TxTask, agg *libstate.AggregatorV3) error {
	defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	rs.domains.SetTxNum(txTask.TxNum)

	if err := rs.applyState(txTask, rs.domains); err != nil {
		return fmt.Errorf("StateV3.ApplyState: %w", err)
	}
	returnReadList(txTask.ReadLists)
	returnWriteList(txTask.WriteLists)

	if err := rs.ApplyLogsAndTraces4(txTask, rs.domains); err != nil {
		return fmt.Errorf("StateV3.ApplyLogsAndTraces: %w", err)
	}

	txTask.ReadLists, txTask.WriteLists = nil, nil
	return nil
}

func (rs *StateV3) ApplyLogsAndTraces4(txTask *TxTask, domains *libstate.SharedDomains) error {
	if dbg.DiscardHistory() {
		return nil
	}

	for addr := range txTask.TraceFroms {
		if err := domains.IndexAdd(kv.TblTracesFromIdx, addr[:]); err != nil {
			return err
		}
	}
	for addr := range txTask.TraceTos {
		if err := domains.IndexAdd(kv.TblTracesToIdx, addr[:]); err != nil {
			return err
		}
	}
	for _, lg := range txTask.Logs {
		if err := domains.IndexAdd(kv.TblLogAddressIdx, lg.Address[:]); err != nil {
			return err
		}
		for _, topic := range lg.Topics {
			if err := domains.IndexAdd(kv.TblLogTopicsIdx, topic[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rs *StateV3) ApplyLogsAndTraces(txTask *TxTask, agg *libstate.AggregatorV3) error {
	if dbg.DiscardHistory() {
		return nil
	}
	defer agg.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	for addr := range txTask.TraceFroms {
		if err := agg.PutIdx(kv.TblTracesFromIdx, addr[:]); err != nil {
			return err
		}
	}
	for addr := range txTask.TraceTos {
		if err := agg.PutIdx(kv.TblTracesToIdx, addr[:]); err != nil {
			return err
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

func (rs *StateV3) Unwind(ctx context.Context, tx kv.RwTx, txUnwindTo uint64, ac *libstate.AggregatorV3Context, accumulator *shards.Accumulator) error {
	var currentInc uint64

	handle := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == length.Addr {
			if len(v) > 0 {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, v); err != nil {
					return fmt.Errorf("%w, %x", err, v)
				}
				var address common.Address
				copy(address[:], k)

				newV := make([]byte, acc.EncodingLengthForStorage())
				acc.EncodeForStorage(newV)
				if accumulator != nil {
					accumulator.ChangeAccount(address, acc.Incarnation, newV)
				}
			} else {
				var address common.Address
				copy(address[:], k)
				if accumulator != nil {
					accumulator.DeleteAccount(address)
				}
			}
			return nil
		}

		var address common.Address
		var location common.Hash
		copy(address[:], k[:length.Addr])
		copy(location[:], k[length.Addr:])
		if accumulator != nil {
			accumulator.ChangeStorage(address, currentInc, location, common.Copy(v))
		}
		return nil
	}
	stateChanges := etl.NewCollector("", "", etl.NewOldestEntryBuffer(etl.BufferOptimalSize), rs.logger)
	defer stateChanges.Close()

	ttx := tx.(kv.TemporalTx)

	{
		iter, err := ttx.HistoryRange(kv.AccountsHistory, int(txUnwindTo), -1, order.Asc, -1)
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
		iter, err := ttx.HistoryRange(kv.StorageHistory, int(txUnwindTo), -1, order.Asc, -1)
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

	if err := stateChanges.Load(tx, "", handle, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := ac.Unwind(ctx, txUnwindTo, tx); err != nil {
		return err
	}

	return nil
}

func (rs *StateV3) DoneCount() uint64 { return ExecTxsDone.Get() }

func (rs *StateV3) SizeEstimate() (r uint64) {
	if rs.domains != nil {
		r += rs.domains.SizeEstimate()
	}
	return r
}

func (rs *StateV3) ReadsValid(readLists map[string]*libstate.KvList) bool {
	return rs.domains.ReadsValid(readLists)
}

// StateWriterBufferedV3 - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type StateWriterBufferedV3 struct {
	rs           *StateV3
	trace        bool
	writeLists   map[string]*libstate.KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64

	tx kv.Tx
}

func NewStateWriterBufferedV3(rs *StateV3) *StateWriterBufferedV3 {
	return &StateWriterBufferedV3{
		rs: rs,
		//trace:      true,
		writeLists: newWriteList(),
	}
}

func (w *StateWriterBufferedV3) SetTxNum(txNum uint64) {
	w.rs.domains.SetTxNum(txNum)
}
func (w *StateWriterBufferedV3) SetTx(tx kv.Tx) { w.tx = tx }

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
	value := accounts.SerialiseV3(account)
	w.writeLists[string(kv.AccountsDomain)].Push(string(address[:]), value)

	if w.trace {
		fmt.Printf("V3 account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address.Bytes(), &account.Balance, account.Nonce, account.Root, account.CodeHash)
	}
	return nil
}

func (w *StateWriterBufferedV3) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	w.writeLists[string(kv.CodeDomain)].Push(string(address[:]), code)
	if len(code) > 0 {
		if w.trace {
			fmt.Printf("V3 code [%x] => [%x] value: %x\n", address.Bytes(), codeHash, code)
		}
		//w.writeLists[kv.PlainContractCode].Push(addr, code)
	}
	return nil
}

func (w *StateWriterBufferedV3) DeleteAccount(address common.Address, original *accounts.Account) error {
	w.writeLists[string(kv.AccountsDomain)].Push(string(address.Bytes()), nil)
	if w.trace {
		fmt.Printf("V3 account [%x] deleted\n", address.Bytes())
	}
	return nil
}

func (w *StateWriterBufferedV3) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	compositeS := string(append(address.Bytes(), key.Bytes()...))
	w.writeLists[string(kv.StorageDomain)].Push(compositeS, value.Bytes())
	if w.trace {
		fmt.Printf("V3 storage [%x] [%x] => [%x]\n", address, key.Bytes(), value.Bytes())
	}
	return nil
}

func (w *StateWriterBufferedV3) CreateContract(address common.Address) error {
	err := w.rs.domains.IterateStoragePrefix(w.tx, address[:], func(k, v []byte) {
		w.writeLists[string(kv.StorageDomain)].Push(string(k), nil)
	})
	if err != nil {
		return err
	}
	if w.trace {
		fmt.Printf("V3 contract [%x]\n", address)
	}
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
		trace:     false,
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
	enc, err := r.rs.domains.LatestAccount(addr)
	if err != nil {
		return nil, err
	}
	if !r.discardReadList {
		// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
		r.readLists[string(kv.AccountsDomain)].Push(string(addr), enc)
	}
	if len(enc) == 0 {
		if r.trace {
			fmt.Printf("ReadAccountData [%x] => [empty], txNum: %d\n", address, r.txNum)
		}
		return nil, nil
	}

	var acc accounts.Account
	if err := accounts.DeserialiseV3(&acc, enc); err != nil {
		return nil, err
	}
	if r.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", address, acc.Nonce, &acc.Balance, acc.CodeHash, r.txNum)
	}
	return &acc, nil
}

func (r *StateReaderV3) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	var composite [20 + 32]byte
	copy(composite[:], address[:])
	copy(composite[20:], key.Bytes())
	enc, err := r.rs.domains.LatestStorage(composite[:])
	if err != nil {
		return nil, err
	}
	if !r.discardReadList {
		r.readLists[string(kv.StorageDomain)].Push(string(composite[:]), enc)
	}
	if r.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] => [empty], txNum: %d\n", composite, r.txNum)
		} else {
			fmt.Printf("ReadAccountStorage [%x] => [%x], txNum: %d\n", composite, enc, r.txNum)
		}
	}
	return enc, nil
}

func (r *StateReaderV3) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	addr := address.Bytes()
	enc, err := r.rs.domains.LatestCode(addr)
	if err != nil {
		return nil, err
	}

	if !r.discardReadList {
		r.readLists[string(kv.CodeDomain)].Push(string(addr), enc)
	}
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *StateReaderV3) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	addr := address.Bytes()
	enc, err := r.rs.domains.LatestCode(addr)
	if err != nil {
		return 0, err
	}
	var sizebuf [8]byte
	binary.BigEndian.PutUint64(sizebuf[:], uint64(len(enc)))
	if !r.discardReadList {
		r.readLists[libstate.CodeSizeTableFake].Push(string(addr), sizebuf[:])
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, r.txNum)
	}
	return size, nil
}

func (r *StateReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

var writeListPool = sync.Pool{
	New: func() any {
		return map[string]*libstate.KvList{
			string(kv.AccountsDomain): {},
			string(kv.StorageDomain):  {},
			string(kv.CodeDomain):     {},
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
			string(kv.AccountsDomain):  {},
			string(kv.CodeDomain):      {},
			libstate.CodeSizeTableFake: {},
			string(kv.StorageDomain):   {},
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
