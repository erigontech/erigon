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
	"errors"
	"fmt"
	"sync"

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

type StateV3 struct {
	domains *ExecutionContext
	logger  log.Logger
	syncCfg ethconfig.Sync
	trace   bool
}

func NewStateV3(domains *ExecutionContext, syncCfg ethconfig.Sync, logger log.Logger) *StateV3 {
	return &StateV3{
		domains: domains,
		logger:  logger,
		syncCfg: syncCfg,
		//trace: true,
	}
}

func (rs *StateV3) SetTrace(trace bool) {
	rs.trace = trace
}

func (rs *StateV3) applyUpdates(ctx context.Context, roTx kv.TemporalTx, blockNum, txNum uint64, stateUpdates StateUpdates, rules *chain.Rules) error {
	domains := rs.domains
	if stateUpdates.BTreeG != nil {
		var err error
		stateUpdates.Scan(func(update *stateUpdate) bool {
			if update.deleteAccount || (update.data != nil && update.originalIncarnation > update.data.Incarnation) {
				if dbg.TraceApply && (rs.trace || dbg.TraceAccount(update.address.Handle())) {
					fmt.Printf("%d apply:del code+storage: %x\n", blockNum, update.address)
				}
				//del, before create: to clanup code/storage
				if err = domains.DelCode(ctx, update.address, roTx, txNum); err != nil {
					return false
				}
				if err = domains.DelStorage(ctx, update.address, accounts.NilKey, roTx, txNum); err != nil {
					return false
				}
			}

			if update.bufferedAccount != nil {
				if update.data != nil {
					if dbg.TraceApply && (rs.trace || dbg.TraceAccount(update.address.Handle())) {
						fmt.Printf("%d apply:put account: %x balance:%d,nonce:%d,codehash:%x\n", blockNum, update.address, &update.data.Balance, update.data.Nonce, update.data.CodeHash)
					}
					if err = domains.PutAccount(ctx, update.address, update.data, roTx, txNum); err != nil {
						return false
					}
				}

				if update.code != nil {
					if dbg.TraceApply && (rs.trace || dbg.TraceAccount(update.address.Handle())) {
						code := update.code
						if len(code) > 40 {
							code = code[:40]
						}
						fmt.Printf("%d apply:put code: %x %x\n", blockNum, update.address, code)
					}

					if err = domains.PutCode(ctx, update.address, accounts.NilCodeHash, update.code, roTx, txNum); err != nil {
						return false
					}
				}

				if update.storage != nil {
					update.storage.Scan(func(i storageItem) bool {
						v := i.value.Bytes()
						if len(v) == 0 {
							if dbg.TraceApply && (rs.trace || dbg.TraceAccount(update.address.Handle())) {
								fmt.Printf("%d apply:del storage: %x q%x\n", blockNum, update.address, i.key)
							}
							if err = domains.DelStorage(ctx, update.address, i.key, roTx, txNum); err != nil {
								return false
							}
						} else {
							if dbg.TraceApply && (rs.trace || dbg.TraceAccount(update.address.Handle())) {
								fmt.Printf("%d apply:put storage: %x %x %x\n", blockNum, update.address, i.key, &i.value)
							}
							if err = domains.PutStorage(ctx, update.address, i.key, i.value, roTx, txNum); err != nil {
								return false
							}
						}
						return true
					})

					if err != nil {
						return false
					}
				}
			} else if update.deleteAccount {
				if dbg.TraceApply && (rs.trace || dbg.TraceAccount(update.address.Handle())) {
					fmt.Printf("%d apply:del account: %x\n", blockNum, update.address)
				}
				if err = domains.DelAccount(ctx, update.address, roTx, txNum); err != nil {
					return false
				}
			}
			return true
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (rs *StateV3) Domains() *ExecutionContext {
	return rs.domains
}

func (rs *StateV3) SetTxNum(blockNum, txNum uint64) {
	rs.domains.SetTxNum(txNum)
	rs.domains.SetBlockNum(blockNum)
}

func (rs *StateV3) ApplyTxState(ctx context.Context,
	roTx kv.TemporalTx,
	blockNum uint64,
	txNum uint64,
	accountUpdates StateUpdates,
	receipt *types.Receipt,
	logs []*types.Log,
	traceFroms map[accounts.Address]struct{},
	traceTos map[accounts.Address]struct{},
	config *chain.Config,
	rules *chain.Rules,
	historyExecution bool) error {
	if historyExecution {
		return nil
	}
	//defer rs.domains.BatchHistoryWriteStart().BatchHistoryWriteEnd()

	if err := rs.applyUpdates(ctx, roTx, blockNum, txNum, accountUpdates, rules); err != nil {
		return fmt.Errorf("StateV3.ApplyState: %w", err)
	}

	if err := rs.applyLogsAndTraces4(roTx, txNum, receipt, logs, traceFroms, traceTos); err != nil {
		return fmt.Errorf("StateV3.ApplyLogsAndTraces: %w", err)
	}

	if (txNum+1)%roTx.StepSize() == 0 /*&& txTask.TxNum > 0 */ && !dbg.DiscardCommitment() {
		// We do not update txNum before commitment cuz otherwise committed state will be in the beginning of next file, not in the latest.
		// That's why we need to make txnum++ on SeekCommitment to get exact txNum for the latest committed state.
		//fmt.Printf("[commitment] running due to txNum reached aggregation step %d\n", txNum/rs.domains.StepSize())
		_, err := rs.domains.ComputeCommitment(ctx, roTx, true, blockNum, txNum, fmt.Sprintf("applying step %d", txNum/roTx.StepSize()), nil)
		if err != nil {
			return fmt.Errorf("ParallelExecutionState.ComputeCommitment: %w", err)
		}
	}

	return nil
}

func (rs *StateV3) applyLogsAndTraces4(tx kv.TemporalTx, txNum uint64, receipt *types.Receipt, logs []*types.Log, traceFroms map[accounts.Address]struct{}, traceTos map[accounts.Address]struct{}) error {
	domains := rs.domains
	for addr := range traceFroms {
		addrValue := addr.Value()
		if err := domains.IndexAdd(kv.TracesFromIdx, addrValue[:], txNum); err != nil {
			return err
		}
	}

	for addr := range traceTos {
		addrValue := addr.Value()
		if err := domains.IndexAdd(kv.TracesToIdx, addrValue[:], txNum); err != nil {
			return err
		}
	}

	for _, lg := range logs {
		if err := domains.IndexAdd(kv.LogAddrIdx, lg.Address[:], txNum); err != nil {
			return err
		}
		for _, topic := range lg.Topics {
			if err := domains.IndexAdd(kv.LogTopicIdx, topic[:], txNum); err != nil {
				return err
			}
		}
	}

	if rs.syncCfg.PersistReceiptsCacheV2 {
		if err := rawdb.WriteReceiptCacheV2(rs.domains.AsPutDel(tx), receipt, txNum); err != nil {
			return err
		}
	}

	return nil
}

func (rs *StateV3) SizeEstimate() (r uint64) {
	if rs.domains != nil {
		r += rs.domains.SizeEstimate()
	}
	return r
}

type storageItem struct {
	key   accounts.StorageKey
	value uint256.Int
}

var deleted accounts.Account

type bufferedAccount struct {
	originalIncarnation uint64
	data                *accounts.Account
	code                []byte
	storage             *btree.BTreeG[storageItem]
}

type stateUpdate struct {
	*bufferedAccount
	address       accounts.Address
	deleteAccount bool
}

func newStateUpdates() StateUpdates {
	return StateUpdates{
		btree.NewBTreeGOptions[*stateUpdate](func(a, b *stateUpdate) bool {
			return a.address.Cmp(b.address) < 0
		}, btree.Options{NoLocks: true}),
	}
}

type StateUpdates struct {
	*btree.BTreeG[*stateUpdate]
}

func (v StateUpdates) TraceBlockUpdates(blockNum uint64, traceAll bool) {
	if v.BTreeG == nil {
		return
	}

	v.Scan(func(update *stateUpdate) bool {
		if traceAll || dbg.TraceAccount(update.address.Handle()) {
			if update.deleteAccount || (update.data != nil && update.originalIncarnation > update.data.Incarnation) {
				fmt.Printf("%d del code+storage: %x\n", blockNum, update.address)
			}

			if update.bufferedAccount != nil {
				if update.data != nil {
					fmt.Printf("%d put account: %x Balance:[%d],Nonce:[%d],CodeHash:[%x]\n", blockNum, update.address, &update.data.Balance, update.data.Nonce, update.data.CodeHash)
				}

				if update.code != nil {
					code := update.code
					if len(code) > 40 {
						code = code[:40]
					}
					fmt.Printf("%d put code: %x %x\n", blockNum, update.address, code)
				}

				if update.storage != nil {
					update.storage.Scan(func(i storageItem) bool {
						if i.value.ByteLen() == 0 {
							fmt.Printf("%d del storage: %x %x\n", blockNum, update.address, i.key)
						} else {
							fmt.Printf("%d put storage: %x %x %x\n", blockNum, update.address, i.key, &i.value)
						}
						return true
					})
				}
			} else if update.deleteAccount {
				fmt.Printf("%d del account: %x\n", blockNum, update.address)
			}
		}
		return true
	})
}

func (v StateUpdates) UpdateCount() int {
	updateCount := 0

	if v.BTreeG != nil {
		v.Scan(func(update *stateUpdate) bool {
			if update.deleteAccount {
				updateCount++
			}
			if update.bufferedAccount != nil {
				if update.data != nil {
					updateCount++
				}
				if update.storage != nil {
					updateCount += update.storage.Len()
				}
			}
			return true
		})
	}

	return updateCount
}

type StateV3Buffered struct {
	*StateV3
	accounts      map[accounts.Address]*bufferedAccount
	accountsMutex *sync.RWMutex
}

func NewStateV3Buffered(state *StateV3) *StateV3Buffered {
	bufferedState := &StateV3Buffered{
		StateV3:       state,
		accounts:      map[accounts.Address]*bufferedAccount{},
		accountsMutex: &sync.RWMutex{},
	}
	return bufferedState
}

func (s *StateV3Buffered) WithDomains(domains *ExecutionContext) *StateV3Buffered {
	return &StateV3Buffered{
		StateV3:       NewStateV3(domains, s.syncCfg, s.logger),
		accounts:      s.accounts,
		accountsMutex: s.accountsMutex,
	}
}

// BufferedWriter - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type BufferedWriter struct {
	rs           *StateV3Buffered
	trace        bool
	writeSet     StateUpdates
	accountPrevs map[string][]byte
	accountDels  map[string]*accounts.Account
	storagePrevs map[string][]byte
	codePrevs    map[string]uint64
	accumulator  *shards.Accumulator
	txNum        uint64
}

func NewBufferedWriter(rs *StateV3Buffered, accumulator *shards.Accumulator) *BufferedWriter {
	return &BufferedWriter{
		rs:          rs,
		writeSet:    newStateUpdates(),
		accumulator: accumulator,
		//trace:       true,
	}
}

func (w *BufferedWriter) SetTxNum(ctx context.Context, txNum uint64) {
	w.txNum = txNum
	w.rs.domains.SetTxNum(txNum)
}
func (w *BufferedWriter) SetTx(tx kv.TemporalTx) {}

func (w *BufferedWriter) WriteSet() StateUpdates {
	return w.writeSet
}

func (w *BufferedWriter) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *BufferedWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("BufferedWriter: acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}

	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address.Value(), account.Incarnation, accounts.SerialiseV3(account))
	}

	if update, ok := w.writeSet.Get(&stateUpdate{address: address}); !ok {
		update = &stateUpdate{&bufferedAccount{
			originalIncarnation: original.Incarnation,
			data:                account,
		}, address, false}
		w.writeSet.Set(update)
	} else {
		if original.Incarnation < update.originalIncarnation {
			update.originalIncarnation = original.Incarnation
		}
		update.data = account
	}

	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok || obj.data == &deleted {
		obj = &bufferedAccount{}
	}
	obj.originalIncarnation = original.Incarnation
	obj.data = account
	w.rs.accounts[address] = obj
	w.rs.accountsMutex.Unlock()

	return nil
}

func (w *BufferedWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address, codeHash, len(code))
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address.Value(), incarnation, code)
	}

	if update, ok := w.writeSet.Get(&stateUpdate{address: address}); !ok {
		w.writeSet.Set(&stateUpdate{&bufferedAccount{code: code}, address, false})
	} else {
		update.code = code
	}

	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok || obj.data == &deleted {
		obj = &bufferedAccount{}
		w.rs.accounts[address] = obj
	}
	obj.code = code
	w.rs.accountsMutex.Unlock()

	return nil
}

func (w *BufferedWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	if w.accumulator != nil {
		w.accumulator.DeleteAccount(address.Value())
	}

	if update, ok := w.writeSet.Get(&stateUpdate{address: address}); !ok {
		w.writeSet.Set(&stateUpdate{nil, address, true})
	} else {
		update.bufferedAccount = nil
		update.deleteAccount = true
	}

	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{
			data: &deleted,
		}
		w.rs.accounts[address] = obj
	}
	*obj = bufferedAccount{data: &deleted}
	w.rs.accountsMutex.Unlock()
	return nil
}

func (w *BufferedWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if original == value {
		return nil
	}

	update, ok := w.writeSet.Get(&stateUpdate{address: address})
	if !ok {
		update = &stateUpdate{&bufferedAccount{}, address, false}
		w.writeSet.Set(update)
	}

	if update.storage == nil {
		update.storage = btree.NewBTreeGOptions[storageItem](func(a, b storageItem) bool {
			return a.key.Cmp(b.key) > 0
		}, btree.Options{NoLocks: true})
	}

	update.storage.Set(storageItem{key, value})

	if w.trace {
		fmt.Printf("BufferedWriter: storage: %x,%x,%x\n", address, key, &value)
	}

	if w.accumulator != nil {
		vb := value.Bytes32()
		w.accumulator.ChangeStorage(address.Value(), incarnation, key.Value(), vb[32-value.ByteLen():])
	}

	w.rs.accountsMutex.Lock()
	obj, ok := w.rs.accounts[address]
	if !ok || obj.data == &deleted {
		obj = &bufferedAccount{}
		w.rs.accounts[address] = obj
	}
	if obj.storage == nil {
		obj.storage = btree.NewBTreeGOptions[storageItem](func(a, b storageItem) bool {
			return a.key.Cmp(b.key) > 0
		}, btree.Options{NoLocks: true})
	}

	obj.storage.Set(storageItem{key, value})

	w.rs.accountsMutex.Unlock()
	return nil
}

func (w *BufferedWriter) CreateContract(address accounts.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}

	return nil
}

// Writer - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type Writer struct {
	ec          *ExecutionContext
	tx          kv.TemporalTx
	trace       bool
	accumulator *shards.Accumulator
	txNum       uint64
}

func NewWriter(ec *ExecutionContext, tx kv.TemporalTx, accumulator *shards.Accumulator, txNum uint64) *Writer {
	return &Writer{
		ec:          ec,
		tx:          tx,
		accumulator: accumulator,
		txNum:       txNum,
		//trace: true,
	}
}

func (w *Writer) SetTxNum(v uint64)      { w.txNum = v }
func (w *Writer) SetTx(tx kv.TemporalTx) { w.tx = tx }

func (w *Writer) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return nil, nil, nil, nil
}

func (w *Writer) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("Writer: acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	addressValue := address.Value()
	if original.Incarnation > account.Incarnation {
		//del, before create: to clanup code/storage
		if err := w.ec.DelCode(context.Background(), address, w.tx, w.txNum); err != nil {
			return err
		}
		if err := w.ec.DelStorage(context.Background(), address, accounts.NilKey, w.tx, w.txNum); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(addressValue, account.Incarnation, value)
	}

	if err := w.ec.PutAccount(context.Background(), address, account, w.tx, w.txNum); err != nil {
		return err
	}
	return nil
}

func (w *Writer) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address, codeHash, len(code))
	}
	addressValue := address.Value()
	if err := w.ec.PutCode(context.Background(), address, codeHash, code, w.tx, w.txNum); err != nil {
		return err
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(addressValue, incarnation, code)
	}
	return nil
}

func (w *Writer) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	if err := w.ec.DelAccount(context.Background(), address, w.tx, w.txNum); err != nil {
		return err
	}
	// if w.accumulator != nil { TODO: investigate later. basically this will always panic. keeping this out should be fine anyway.
	// 	w.accumulator.DeleteAccount(address)
	// }
	return nil
}

func (w *Writer) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	if address.IsNil() {
		return errors.New("unexpected nil address")
	}
	if key.IsNil() {
		return errors.New("unexpected nil storage key")
	}
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, &value)
	}
	var prev []ValueWithStep[uint256.Int]
	if original.ByteLen() < 0 {
		prev = []ValueWithStep[uint256.Int]{{Value: original}}
	}
	if value.ByteLen() == 0 {
		return w.ec.DelStorage(context.Background(), address, key, w.tx, w.txNum, prev...)
	}
	if w.accumulator != nil {
		addressValue := address.Value()
		keyValue := key.Value()
		w.accumulator.ChangeStorage(addressValue, incarnation, keyValue, value.Bytes())
	}
	return w.ec.PutStorage(context.Background(), address, key, value, w.tx, w.txNum, prev...)
}

var fastCreate = dbg.EnvBool("FAST_CREATE", false)

func (w *Writer) CreateContract(address accounts.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	if fastCreate {
		return nil
	}
	if err := w.ec.DelStorage(context.Background(), address, accounts.NilKey, w.tx, w.txNum); err != nil {
		return err
	}
	return nil
}

type ReaderV3 struct {
	txNum       uint64
	trace       bool
	tracePrefix string
	ec          *ExecutionContext
	tx          kv.TemporalTx
}

func NewStateReader(ec *ExecutionContext, tx kv.TemporalTx) *ReaderV3 {
	return &ReaderV3{
		//trace:  true,
		ec: ec,
		tx: tx,
	}
}

func (r *ReaderV3) DiscardReadList()       {}
func (r *ReaderV3) SetTxNum(txNum uint64)  { r.txNum = txNum }
func (r *ReaderV3) SetTx(tx kv.TemporalTx) { r.tx = tx }

func (r *ReaderV3) SetTrace(trace bool, tracePrefix string) {
	r.trace = trace
	if tplen := len(tracePrefix); tplen > 0 && tracePrefix[tplen-1] != ' ' {
		tracePrefix += " "
	}
	r.tracePrefix = tracePrefix
}

func (r *ReaderV3) Trace() bool {
	return r.trace
}

func (r *ReaderV3) TracePrefix() string {
	return r.tracePrefix
}

func (r *ReaderV3) HasStorage(address accounts.Address) (bool, error) {
	if r.ec != nil {
		hasStorage, err := r.ec.HasStorage(context.Background(), address, r.tx)
		return hasStorage, err
	}
	av := address.Value()
	hasStorage, err := r.tx.HasPrefix(kv.StorageDomain, av[:])
	return hasStorage, err
}

func (r *ReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	acc, err := r.readAccountData(address)
	return acc, err
}

func (r *ReaderV3) readAccountData(address accounts.Address) (*accounts.Account, error) {
	var acc *accounts.Account
	var ok bool
	var err error
	var addressValue common.Address

	if r.ec != nil {
		acc, _, ok, err = r.ec.GetAccount(context.Background(), address, r.tx)
		if err != nil {
			return nil, err
		}
		if r.trace && !address.IsNil() {
			addressValue = address.Value()
		}
	} else {
		if !address.IsNil() {
			addressValue = address.Value()
		}
		enc, _, err := r.tx.GetLatest(kv.AccountsDomain, addressValue[:])
		if err != nil {
			return nil, err
		}
		if len(enc) > 0 {
			var account accounts.Account
			if err = accounts.DeserialiseV3(&account, enc); err != nil {
				return nil, err
			}
			acc = &account
			ok = true
		}
	}

	if !ok {
		if r.trace {
			fmt.Printf("%sReadAccountData [%x] => [empty], txNum: %d\n", r.tracePrefix, addressValue, r.txNum)
		}
		return nil, nil
	}

	if r.trace {
		fmt.Printf("%sReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", r.tracePrefix, addressValue, acc.Nonce, &acc.Balance, acc.CodeHash, r.txNum)
	}
	return acc, nil
}

func (r *ReaderV3) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *ReaderV3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if address.IsNil() || key.IsNil() {
		return uint256.Int{}, false, nil
	}

	var res uint256.Int
	var ok bool
	var err error
	var addressValue common.Address
	var keyValue common.Hash

	if r.ec != nil {
		res, _, ok, err = r.ec.GetStorage(context.Background(), address, key, r.tx)
		if err != nil {
			return uint256.Int{}, false, err
		}
		if r.trace {
			addressValue = address.Value()
			keyValue = key.Value()
		}
	} else {
		var composite [20 + 32]byte
		addressValue = address.Value()
		keyValue = key.Value()
		copy(composite[0:20], addressValue[0:20])
		copy(composite[20:], keyValue[:])
		enc, _, err := r.tx.GetLatest(kv.StorageDomain, composite[:])
		if err != nil {
			return uint256.Int{}, false, err
		}

		ok := enc != nil
		var res uint256.Int
		if ok {
			(&res).SetBytes(enc)
		}
	}

	if r.trace {
		if !ok {
			fmt.Printf("%sReadAccountStorage [%x %x] => [empty], txNum: %d\n", r.tracePrefix, addressValue, keyValue, r.txNum)
		} else {
			fmt.Printf("%sReadAccountStorage [%x %x] => [%x], txNum: %d\n", r.tracePrefix, addressValue, keyValue, &res, r.txNum)
		}
	}

	return res, ok, err
}

func (r *ReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	var code []byte
	var err error
	var addressValue common.Address
	if r.ec != nil {
		_, code, _, _, err = r.ec.GetCode(context.Background(), address, r.tx)
		if err != nil {
			return nil, err
		}
		if r.trace && !address.IsNil() {
			addressValue = address.Value()
		}
	} else {
		if !address.IsNil() {
			addressValue = address.Value()
		}
		code, _, err = r.tx.GetLatest(kv.CodeDomain, addressValue[:])
	}

	if r.trace {
		lenc, cs := printCode(code)
		fmt.Printf("%sReadAccountCode [%x] =>  [%d:%s], txNum: %d\n", r.tracePrefix, addressValue, lenc, cs, r.txNum)
	}
	return code, nil
}

func (r *ReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	var code []byte
	var err error
	var addressValue common.Address
	if r.ec != nil {
		_, code, _, _, err = r.ec.GetCode(context.Background(), address, r.tx)
		if r.trace {
			addressValue = address.Value()
		}
	} else {
		if !address.IsNil() {
			addressValue = address.Value()
		}
		code, _, err = r.tx.GetLatest(kv.CodeDomain, addressValue[:])
	}

	if err != nil {
		return 0, err
	}

	size := len(code)
	if r.trace {
		fmt.Printf("%sReadAccountCodeSize [%x] => [%d], txNum: %d\n", r.tracePrefix, addressValue, size, r.txNum)
	}
	return size, nil
}

func (r *ReaderV3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}

type bufferedReader struct {
	reader        StateReader
	bufferedState *StateV3Buffered
}

func NewBufferedReader(bufferedState *StateV3Buffered, reader StateReader) StateReader {
	return &bufferedReader{reader: reader, bufferedState: bufferedState}
}

func (r *bufferedReader) SetTx(tx kv.TemporalTx) {
	r.reader.(interface{ SetTx(tx kv.TemporalTx) }).SetTx(tx)
}

func (r *bufferedReader) SetTrace(trace bool, tracePrefix string) {
	r.reader.SetTrace(trace, tracePrefix)
}

func (r *bufferedReader) Trace() bool {
	return r.reader.Trace()
}

func (r *bufferedReader) TracePrefix() string {
	return r.reader.TracePrefix()
}

func (r *bufferedReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		if data == &deleted {
			if r.reader.Trace() {
				fmt.Printf("%sReadAccountData (buf)[%x] => [empty]\n", r.reader.TracePrefix(), address)
			}
			return nil, nil
		}
		if r.reader.Trace() {
			fmt.Printf("%sReadAccountData (buf)[%x] => [nonce: %d, balance: %d, codeHash: %x]\n", r.reader.TracePrefix(), address, data.Nonce, &data.Balance, data.CodeHash)
		}

		result := *data
		return &result, nil
	}

	return r.reader.ReadAccountData(address)
}

func (r *bufferedReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		if data == &deleted {
			return nil, nil
		}
		result := *data
		return &result, nil
	}

	return r.reader.ReadAccountDataForDebug(address)
}

func (r *bufferedReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]

	if ok {
		if so.data == &deleted {
			if r.reader.Trace() {
				fmt.Printf("%sReadAccountStorage (buf)[%x %x] => [empty]\n", r.reader.TracePrefix(), address, key)
			}
			r.bufferedState.accountsMutex.RUnlock()
			return uint256.Int{}, false, nil
		}

		if so.storage != nil {
			item, ok := so.storage.Get(storageItem{key: key})

			if ok {
				if r.reader.Trace() {
					fmt.Printf("%sReadAccountStorage (buf)[%x %x] => [%x]\n", r.reader.TracePrefix(), address, key, &item.value)
				}
				r.bufferedState.accountsMutex.RUnlock()
				return item.value, true, nil
			}
		}
	}

	r.bufferedState.accountsMutex.RUnlock()

	return r.reader.ReadAccountStorage(address, key)
}

func (r *bufferedReader) HasStorage(address accounts.Address) (bool, error) {
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]

	if ok {
		if so.data == &deleted {
			r.bufferedState.accountsMutex.RUnlock()
			return false, nil
		}

		if so.storage != nil && so.storage.Len() > 0 {
			// TODO - we really need to return the first key
			// for this we need to order the list of hashes
			r.bufferedState.accountsMutex.RUnlock()
			return true, nil
		}
	}
	r.bufferedState.accountsMutex.RUnlock()
	return r.reader.HasStorage(address)
}

func (r *bufferedReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	var code []byte
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok {
		if so.data == &deleted {
			r.bufferedState.accountsMutex.RUnlock()
			return nil, nil
		}

		if len(so.code) != 0 {
			code = so.code
		}
	}
	r.bufferedState.accountsMutex.RUnlock()

	if len(code) != 0 {
		return so.code, nil
	}

	return r.reader.ReadAccountCode(address)
}

func (r *bufferedReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	var code []byte
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok {
		if so.data == &deleted {
			r.bufferedState.accountsMutex.RUnlock()
			return 0, nil
		}

		if len(so.code) != 0 {
			code = so.code
		}
	}

	r.bufferedState.accountsMutex.RUnlock()

	if len(code) != 0 {
		return len(code), nil
	}

	return r.reader.ReadAccountCodeSize(address)
}

func (r *bufferedReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
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
