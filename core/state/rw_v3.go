// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/shards"
)

var execTxsDone = metrics.NewCounter(`exec_txs_done`)

type StateV3 struct {
	domains             *state.SharedDomains
	applyPrevAccountBuf []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	addrIncBuf          []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	logger              log.Logger

	trace bool
}

func NewStateV3(domains *state.SharedDomains, logger log.Logger) *StateV3 {
	return &StateV3{
		domains:             domains,
		applyPrevAccountBuf: make([]byte, 256),
		logger:              logger,
		//trace: true,
	}
}

func (rs *StateV3) applyState(roTx kv.Tx, writeLists map[string]*state.KvList, balanceIncreases map[common.Address]uint256.Int, domains *state.SharedDomains, rules *chain.Rules) error {
	var acc accounts.Account

	//maps are unordered in Go! don't iterate over it. SharedDomains.deleteAccount will call GetLatest(Code) and expecting it not been delete yet
	if writeLists != nil {
		for _, domain := range []kv.Domain{kv.AccountsDomain, kv.CodeDomain, kv.StorageDomain} {
			list, ok := writeLists[domain.String()]
			if !ok {
				continue
			}

			for i, key := range list.Keys {
				if list.Vals[i] == nil {
					if err := domains.DomainDel(domain, roTx, []byte(key), nil, nil, 0); err != nil {
						return err
					}
				} else {
					if err := domains.DomainPut(domain, roTx, []byte(key), nil, list.Vals[i], nil, 0); err != nil {
						return err
					}
				}
			}
		}
	}

	emptyRemoval := rules.IsSpuriousDragon
	for addr, increase := range balanceIncreases {
		increase := increase
		addrBytes := addr.Bytes()
		enc0, step0, err := domains.GetLatest(kv.AccountsDomain, roTx, addrBytes)
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
		if emptyRemoval && acc.Nonce == 0 && acc.Balance.IsZero() && acc.IsEmptyCodeHash() {
			if err := domains.DomainDel(kv.AccountsDomain, roTx, addrBytes, nil, enc0, step0); err != nil {
				return err
			}
		} else {
			enc1 := accounts.SerialiseV3(&acc)
			if err := domains.DomainPut(kv.AccountsDomain, roTx, addrBytes, nil, enc1, enc0, step0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rs *StateV3) Domains() *state.SharedDomains {
	return rs.domains
}

func (rs *StateV3) SetTxNum(txNum, blockNum uint64) {
	rs.domains.SetTxNum(txNum)
	rs.domains.SetBlockNum(blockNum)
}

func (rs *StateV3) ApplyState4(ctx context.Context,
	roTx kv.Tx,
	blockNum uint64,
	txNum uint64,
	writeLists WriteLists,
	balanceIncreases map[common.Address]uint256.Int,
	logs []*types.Log,
	traceFroms map[common.Address]struct{},
	traceTos map[common.Address]struct{},
	config *chain.Config,
	rules *chain.Rules,
	historyExecution bool) error {
	if historyExecution {
		return nil
	}
	//defer rs.domains.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	if err := rs.applyState(roTx, writeLists, balanceIncreases, rs.domains, rules); err != nil {
		return fmt.Errorf("StateV3.ApplyState: %w", err)
	}
	writeLists.Return()

	if err := rs.ApplyLogsAndTraces4(logs, traceFroms, traceTos, rs.domains); err != nil {
		return fmt.Errorf("StateV3.ApplyLogsAndTraces: %w", err)
	}

	if (txNum+1)%rs.domains.StepSize() == 0 /*&& txTask.TxNum > 0 */ {
		// We do not update txNum before commitment cuz otherwise committed state will be in the beginning of next file, not in the latest.
		// That's why we need to make txnum++ on SeekCommitment to get exact txNum for the latest committed state.
		//fmt.Printf("[commitment] running due to txNum reached aggregation step %d\n", txNum/rs.domains.StepSize())
		_, err := rs.domains.ComputeCommitment(ctx, roTx, true, blockNum,
			fmt.Sprintf("applying step %d", txNum/rs.domains.StepSize()))
		if err != nil {
			return fmt.Errorf("StateV3.ComputeCommitment: %w", err)
		}
	}

	return nil
}

func (rs *StateV3) ApplyLogsAndTraces4(logs []*types.Log, traceFroms map[common.Address]struct{}, traceTos map[common.Address]struct{}, domains *state.SharedDomains) error {
	for addr := range traceFroms {
		if err := domains.IndexAdd(kv.TracesFromIdx, addr[:]); err != nil {
			return err
		}
	}

	for addr := range traceTos {
		if err := domains.IndexAdd(kv.TracesToIdx, addr[:]); err != nil {
			return err
		}
	}

	for _, lg := range logs {
		if err := domains.IndexAdd(kv.LogAddrIdx, lg.Address[:]); err != nil {
			return err
		}
		for _, topic := range lg.Topics {
			if err := domains.IndexAdd(kv.LogTopicIdx, topic[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	mxState3UnwindRunning = metrics.GetOrCreateGauge("state3_unwind_running")
	mxState3Unwind        = metrics.GetOrCreateSummary("state3_unwind")
)

func (rs *StateV3) Unwind(ctx context.Context, tx kv.RwTx, blockUnwindTo, txUnwindTo uint64, accumulator *shards.Accumulator, changeset *[kv.DomainLen][]state.DomainEntryDiff) error {
	mxState3UnwindRunning.Inc()
	defer mxState3UnwindRunning.Dec()
	st := time.Now()
	defer mxState3Unwind.ObserveDuration(st)
	var currentInc uint64

	//TODO: why we don't call accumulator.ChangeCode???
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
	stateChanges.SortAndFlushInBackground(true)

	accountDiffs := changeset[kv.AccountsDomain]
	for _, kv := range accountDiffs {
		if err := stateChanges.Collect(toBytesZeroCopy(kv.Key)[:length.Addr], kv.Value); err != nil {
			return err
		}
	}
	storageDiffs := changeset[kv.StorageDomain]
	for _, kv := range storageDiffs {
		if err := stateChanges.Collect(toBytesZeroCopy(kv.Key), kv.Value); err != nil {
			return err
		}
	}

	if err := stateChanges.Load(tx, "", handle, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := rs.domains.Unwind(ctx, tx, blockUnwindTo, txUnwindTo, changeset); err != nil {
		return err
	}

	return nil
}

func (rs *StateV3) DoneCount() uint64 {
	return execTxsDone.GetValueUint64()
}

func (rs *StateV3) SizeEstimate() (r uint64) {
	if rs.domains != nil {
		r += rs.domains.SizeEstimate()
	}
	return r
}

func (rs *StateV3) ReadsValid(readLists map[string]*state.KvList) bool {
	return rs.domains.ReadsValid(readLists)
}

type bufferedAccount struct {
	originalIncarnation uint64
	data                *accounts.Account
	code                []byte
	storage             map[libcommon.Hash]uint256.Int
}

type StateV3Buffered struct {
	*StateV3
	accounts      map[common.Address]*bufferedAccount
	accountsMutex *sync.RWMutex
}

func NewStateV3Buffered(state *StateV3) *StateV3Buffered {
	bufferedState := &StateV3Buffered{
		StateV3:       state,
		accounts:      map[common.Address]*bufferedAccount{},
		accountsMutex: &sync.RWMutex{},
	}
	return bufferedState
}

func (s *StateV3Buffered) WithDomains(domains *state.SharedDomains) *StateV3Buffered {
	return &StateV3Buffered{
		StateV3:       NewStateV3(domains, s.logger),
		accounts:      s.accounts,
		accountsMutex: s.accountsMutex,
	}
}

// StateWriterBufferedV3 - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type StateWriterBufferedV3 struct {
	rs           *StateV3Buffered
	trace        bool
	writeLists   map[string]*state.KvList
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
	accumulator  *shards.Accumulator
}

func NewStateWriterBufferedV3(rs *StateV3Buffered, accumulator *shards.Accumulator) *StateWriterBufferedV3 {
	return &StateWriterBufferedV3{
		rs:          rs,
		writeLists:  newWriteList(),
		accumulator: accumulator,
		//trace:       true,
	}
}

func (w *StateWriterBufferedV3) SetTxNum(ctx context.Context, txNum uint64) {
	w.rs.domains.SetTxNum(txNum)
}
func (w *StateWriterBufferedV3) SetTx(tx kv.Tx) {}

func (w *StateWriterBufferedV3) ResetWriteSet() {
	w.writeLists = newWriteList()
	w.accountPrevs = nil
	w.accountDels = nil
	w.storagePrevs = nil
	w.codePrevs = nil
}

func (w *StateWriterBufferedV3) WriteSet() map[string]*state.KvList {
	return w.writeLists
}

func (w *StateWriterBufferedV3) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *StateWriterBufferedV3) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}

	value := accounts.SerialiseV3(account)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address, account.Incarnation, value)
	}

	writeList := w.writeLists[kv.AccountsDomain.String()]
	if original.Incarnation > account.Incarnation {
		//del, before create: to clanup code/storage
		writeList.Push(string(address[:]), nil)
	}
	writeList.Push(string(address[:]), value)

	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{}
	}
	obj.originalIncarnation = original.Incarnation
	obj.data = account
	w.rs.accounts[address] = obj
	w.rs.accountsMutex.Unlock()

	return nil
}

func (w *StateWriterBufferedV3) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address, incarnation, code)
	}
	w.writeLists[kv.CodeDomain.String()].Push(string(address[:]), code)

	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{}
	}
	obj.code = code
	w.rs.accounts[address] = obj
	w.rs.accountsMutex.Unlock()

	return nil
}

func (w *StateWriterBufferedV3) DeleteAccount(address common.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	if w.accumulator != nil {
		w.accumulator.DeleteAccount(address)
	}
	w.writeLists[kv.AccountsDomain.String()].Push(string(address.Bytes()), nil)
	w.rs.accountsMutex.Lock()
	delete(w.rs.accounts, address)
	w.rs.accountsMutex.Unlock()
	return nil
}

func (w *StateWriterBufferedV3) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	compositeS := string(append(address[:], key[:]...))
	vb := value.Bytes32() // using [32]byte instead of []byte to avoid heap escape
	w.writeLists[kv.StorageDomain.String()].Push(compositeS, vb[32-value.ByteLen():])
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, vb[32-value.ByteLen():])
	}
	if w.accumulator != nil {
		w.accumulator.ChangeStorage(address, incarnation, key, vb[32-value.ByteLen():])
	}
	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{}
	}
	if obj.storage == nil {
		obj.storage = map[common.Hash]uint256.Int{
			key: value,
		}
	} else {
		obj.storage[key] = value
	}
	w.rs.accounts[address] = obj
	w.rs.accountsMutex.Unlock()
	return nil
}

func (w *StateWriterBufferedV3) DeleteAccountStorage(address common.Address, incarnation uint64, key common.Hash) error {
	compositeS := string(append(address[:], key[:]...))
	w.writeLists[kv.StorageDomain.String()].Push(compositeS, nil)
	if w.trace {
		fmt.Printf("storage delete: %x,%x\n", address, key)
	}
	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{}
	}
	if obj.storage != nil {
		delete(obj.storage, key)
	}
	w.rs.accountsMutex.Unlock()
	return nil
}

func (w *StateWriterBufferedV3) CreateContract(address common.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}

	//seems don't need delete code here - tests starting fail
	//err := w.rs.domains.IterateStoragePrefix(address[:], func(k, v []byte) error {
	//	w.writeLists[string(kv.StorageDomain)].Push(string(k), nil)
	//	return nil
	//})
	//if err != nil {
	//	return err
	//}
	return nil
}

// StateWriterV3 - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type StateWriterV3 struct {
	rs          *StateV3
	tx          kv.Tx
	trace       bool
	accumulator *shards.Accumulator
}

func NewStateWriterV3(rs *StateV3, tx kv.Tx, accumulator *shards.Accumulator) *StateWriterV3 {
	return &StateWriterV3{
		rs:          rs,
		accumulator: accumulator,
		tx:          tx,
		//trace:       true,
	}
}

func (w *StateWriterV3) SetTx(tx kv.Tx) { w.tx = tx }

func (w *StateWriterV3) ResetWriteSet() {}

func (w *StateWriterV3) WriteSet() map[string]*state.KvList {
	return nil
}

func (w *StateWriterV3) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return nil, nil, nil, nil
}

func (w *StateWriterV3) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	if original.Incarnation > account.Incarnation {
		//del, before create: to clanup code/storage
		if err := w.rs.domains.DomainDel(kv.CodeDomain, w.tx, address[:], nil, nil, 0); err != nil {
			return err
		}
		if err := w.rs.domains.DomainDelPrefix(kv.StorageDomain, w.tx, address[:]); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address, account.Incarnation, value)
	}

	if err := w.rs.domains.DomainPut(kv.AccountsDomain, w.tx, address[:], nil, value, nil, 0); err != nil {
		return err
	}
	return nil
}

func (w *StateWriterV3) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	if err := w.rs.domains.DomainPut(kv.CodeDomain, w.tx, address[:], nil, code, nil, 0); err != nil {
		return err
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address, incarnation, code)
	}
	return nil
}

func (w *StateWriterV3) DeleteAccount(address common.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	if err := w.rs.domains.DomainDel(kv.AccountsDomain, w.tx, address[:], nil, nil, 0); err != nil {
		return err
	}
	// if w.accumulator != nil { TODO: investigate later. basically this will always panic. keeping this out should be fine anyway.
	// 	w.accumulator.DeleteAccount(address)
	// }
	return nil
}

func (w *StateWriterV3) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if original == value {
		return nil
	}

	composite := append(address[:], key[:]...)
	v := value.Bytes()
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, v)
	}
	if w.accumulator != nil {
		w.accumulator.ChangeStorage(address, incarnation, key, v)
	}

	return w.rs.domains.DomainPut(kv.StorageDomain, w.tx, composite, nil, v, nil, 0)
}

func (w *StateWriterV3) DeleteAccountStorage(address libcommon.Address, incarnation uint64, key libcommon.Hash) error {
	if w.trace {
		fmt.Printf("storage delete: %x,%x\n", address, key)
	}
	composite := append(address[:], key[:]...)
	return w.rs.domains.DomainDel(kv.StorageDomain, w.tx, composite, nil, nil, 0)
}

func (w *StateWriterV3) CreateContract(address common.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	//if err := w.rs.domains.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
	//	return err
	//}
	return nil
}

type ReaderV3 struct {
	txNum uint64
	trace bool
	sd    *state.SharedDomains
	tx    kv.Tx
}

func NewReaderV3(domains *state.SharedDomains, tx kv.Tx) *ReaderV3 {
	return &ReaderV3{
		//trace:     true,
		tx: tx,
		sd: domains,
	}
}

func (r *ReaderV3) DiscardReadList()      {}
func (r *ReaderV3) SetTxNum(txNum uint64) { r.txNum = txNum }
func (r *ReaderV3) SetTx(tx kv.Tx) {
	r.tx = tx
}

func (r *ReaderV3) SetTrace(trace bool) { r.trace = trace }

func (r *ReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	_, acc, err := r.readAccountData(address)
	return acc, err
}

func (r *ReaderV3) readAccountData(address common.Address) ([]byte, *accounts.Account, error) {
	enc, _, err := r.sd.GetLatest(kv.AccountsDomain, r.tx, address[:])
	if err != nil {
		return nil, nil, err
	}
	if len(enc) == 0 {
		if r.trace {
			fmt.Printf("ReadAccountData [%x] => [empty], txNum: %d\n", address, r.txNum)
		}
		return nil, nil, nil
	}

	var acc accounts.Account
	if err := accounts.DeserialiseV3(&acc, enc); err != nil {
		return nil, nil, err
	}
	if r.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", address, acc.Nonce, &acc.Balance, acc.CodeHash, r.txNum)
	}
	return enc, &acc, nil
}

func (r *ReaderV3) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *ReaderV3) ReadAccountStorage(address common.Address, incarnation uint64, key common.Hash) (uint256.Int, bool, error) {
	var composite [20 + 32]byte
	copy(composite[0:20], address[0:20])
	copy(composite[20:], key[:])

	enc, _, err := r.sd.GetLatest(kv.StorageDomain, r.tx, composite[:])
	if err != nil {
		return uint256.Int{}, false, err
	}
	if r.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] => [empty], txNum: %d\n", composite[:], r.txNum)
		} else {
			fmt.Printf("ReadAccountStorage [%x] => [%x], txNum: %d\n", composite[:], enc, r.txNum)
		}
	}
	var res uint256.Int
	(&res).SetBytes(enc)
	return res, true, nil
}

func (r *ReaderV3) ReadAccountCode(address common.Address, incarnation uint64) ([]byte, error) {
	enc, _, err := r.sd.GetLatest(kv.CodeDomain, r.tx, address[:])
	if err != nil {
		return nil, err
	}
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *ReaderV3) ReadAccountCodeSize(address common.Address, incarnation uint64) (int, error) {
	enc, _, err := r.sd.GetLatest(kv.CodeDomain, r.tx, address[:])
	if err != nil {
		return 0, err
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, r.txNum)
	}
	return size, nil
}

func (r *ReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

type ReaderParallelV3 struct {
	ReaderV3
	discardReadList bool
	readLists       map[string]*state.KvList
}

func NewReaderParallelV3(sd *state.SharedDomains, tx kv.Tx) *ReaderParallelV3 {
	return &ReaderParallelV3{
		//trace:     true,
		ReaderV3:  ReaderV3{sd: sd, tx: tx},
		readLists: newReadList(),
	}
}

func (r *ReaderParallelV3) DiscardReadList() { r.discardReadList = true }

func (r *ReaderParallelV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, acc, err := r.ReaderV3.readAccountData(address)
	if err != nil {
		return nil, err
	}
	if !r.discardReadList {
		// lifecycle of `r.readList` is less than lifecycle of `r.rs` and `r.tx`, also `r.rs` and `r.tx` do store data immutable way
		r.readLists[kv.AccountsDomain.String()].Push(string(address[:]), enc)
	}

	return acc, nil
}

// ReadAccountDataForDebug - is like ReadAccountData, but without adding key to `readList`.
// Used to get `prev` account balance
func (r *ReaderParallelV3) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	return r.ReaderV3.ReadAccountDataForDebug(address)
}

func (r *ReaderParallelV3) ReadAccountStorage(address common.Address, incarnation uint64, key common.Hash) (uint256.Int, bool, error) {
	enc, ok, err := r.ReaderV3.ReadAccountStorage(address, incarnation, key)
	if err != nil {
		return uint256.Int{}, false, err
	}
	if ok && !r.discardReadList {
		var composite [20 + 32]byte
		copy(composite[0:20], address[0:20])
		copy(composite[20:], key[:])
		r.readLists[kv.StorageDomain.String()].Push(string(composite[:]), enc.Bytes())
	}

	return enc, ok, nil
}

func (r *ReaderParallelV3) ReadAccountCode(address common.Address, incarnation uint64) ([]byte, error) {
	enc, err := r.ReaderV3.ReadAccountCode(address, incarnation)
	if err != nil {
		return nil, err
	}

	if !r.discardReadList {
		r.readLists[kv.CodeDomain.String()].Push(string(address[:]), enc)
	}

	return enc, nil
}

func (r *ReaderParallelV3) ReadAccountCodeSize(address common.Address, incarnation uint64) (int, error) {
	size, err := r.ReaderV3.ReadAccountCodeSize(address, incarnation)
	if err != nil {
		return 0, err
	}
	if !r.discardReadList {
		var sizebuf [8]byte
		binary.BigEndian.PutUint64(sizebuf[:], uint64(size))
		r.readLists[state.CodeSizeTableFake].Push(string(address[:]), sizebuf[:])
	}

	return size, nil
}

type bufferedReader struct {
	reader        ResettableStateReader
	bufferedState *StateV3Buffered
}

func NewBufferedReader(bufferedState *StateV3Buffered, reader ResettableStateReader) ResettableStateReader {
	return &bufferedReader{reader: reader, bufferedState: bufferedState}
}

func (r *bufferedReader) SetTrace(trace bool) {
	r.reader.(*ReaderParallelV3).trace = trace
}

func (r *bufferedReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		if reader, ok := r.reader.(*ReaderParallelV3); ok && reader.trace {
			fmt.Printf("ReadAccountData (buf) [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", address, data.Nonce, &data.Balance, data.CodeHash, reader.txNum)
		}

		result := *data
		return &result, nil
	}

	return r.reader.ReadAccountData(address)
}

func (r *bufferedReader) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		result := *data
		return &result, nil
	}

	return r.reader.ReadAccountDataForDebug(address)
}

func (r *bufferedReader) ReadAccountStorage(address common.Address, incarnation uint64, key common.Hash) (uint256.Int, bool, error) {
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]

	if ok && so.storage != nil {
		value, ok := so.storage[key]

		if ok {
			r.bufferedState.accountsMutex.RUnlock()
			return value, true, nil
		}
	}

	r.bufferedState.accountsMutex.RUnlock()

	return r.reader.ReadAccountStorage(address, incarnation, key)
}

func (r *bufferedReader) ReadAccountCode(address common.Address, incarnation uint64) ([]byte, error) {
	var code []byte
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok && len(so.code) != 0 {
		code = so.code
	}
	r.bufferedState.accountsMutex.RUnlock()

	if len(code) != 0 {
		return so.code, nil
	}

	return r.reader.ReadAccountCode(address, incarnation)
}

func (r *bufferedReader) ReadAccountCodeSize(address common.Address, incarnation uint64) (int, error) {
	var code []byte
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok && len(so.code) != 0 {
		code = so.code
	}
	r.bufferedState.accountsMutex.RUnlock()

	if len(code) != 0 {
		return len(code), nil
	}

	return r.reader.ReadAccountCodeSize(address, incarnation)
}

func (r *bufferedReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	var incarnation uint64

	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok && so.data != nil {
		incarnation = so.data.Incarnation
	}
	r.bufferedState.accountsMutex.RUnlock()

	if ok {
		return incarnation, nil
	}

	return r.reader.ReadAccountIncarnation(address)
}

func (r *bufferedReader) SetTx(tx kv.Tx) {
	r.reader.SetTx(tx)
}

func (r *bufferedReader) DiscardReadList() {
	r.reader.DiscardReadList()
}

type WriteLists map[string]*state.KvList

func (v WriteLists) Return() {
	returnWriteList(v)
}

var writeListPool = sync.Pool{

	New: func() any {
		return WriteLists{
			kv.AccountsDomain.String(): {},
			kv.StorageDomain.String():  {},
			kv.CodeDomain.String():     {},
		}
	},
}

func newWriteList() WriteLists {
	v := writeListPool.Get().(WriteLists)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
	//return writeListPool.Get().(map[string]*state.KvList)
}
func returnWriteList(v WriteLists) {
	if v == nil {
		return
	}
	//for _, tbl := range v {
	//	clear(tbl.Keys)
	//	clear(tbl.Vals)
	//	tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	//}
	writeListPool.Put(v)
}

type ReadLists map[string]*state.KvList

func (v ReadLists) Return() {
	returnReadList(v)
}

var readListPool = sync.Pool{
	New: func() any {
		return ReadLists{
			kv.AccountsDomain.String(): {},
			kv.CodeDomain.String():     {},
			state.CodeSizeTableFake:    {},
			kv.StorageDomain.String():  {},
		}
	},
}

func newReadList() ReadLists {
	v := readListPool.Get().(ReadLists)
	for _, tbl := range v {
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	return v
	//return readListPool.Get().(map[string]*state.KvList)
}
func returnReadList(v ReadLists) {
	if v == nil {
		return
	}
	//for _, tbl := range v {
	//	clear(tbl.Keys)
	//	clear(tbl.Vals)
	//	tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	//}
	readListPool.Put(v)
}

func toStringZeroCopy(v []byte) string { return unsafe.String(&v[0], len(v)) }
func toBytesZeroCopy(s string) []byte  { return unsafe.Slice(unsafe.StringData(s), len(s)) }
