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
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/turbo/shards"
)

var execTxsDone = metrics.NewCounter(`exec_txs_done`)

type StateV3 struct {
	domains             *state.SharedDomains
	applyPrevAccountBuf []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	addrIncBuf          []byte // buffer for ApplyState. Doesn't need mutex because Apply is single-threaded
	logger              log.Logger
	syncCfg             ethconfig.Sync
	trace               bool
}

func NewStateV3(domains *state.SharedDomains, syncCfg ethconfig.Sync, logger log.Logger) *StateV3 {
	return &StateV3{
		domains:             domains,
		applyPrevAccountBuf: make([]byte, 256),
		logger:              logger,
		syncCfg:             syncCfg,
		//trace: true,
	}
}

func (rs *StateV3) applyState(roTx kv.Tx, txNum uint64, writeLists map[string]*state.KvList, balanceIncreases map[common.Address]uint256.Int, domains *state.SharedDomains, rules *chain.Rules) error {
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
					if err := domains.DomainDel(domain, roTx, []byte(key), txNum, nil, 0); err != nil {
						return err
					}
				} else {
					if err := domains.DomainPut(domain, roTx, []byte(key), list.Vals[i], txNum, nil, 0); err != nil {
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
			if err := domains.DomainDel(kv.AccountsDomain, roTx, addrBytes, txNum, enc0, step0); err != nil {
				return err
			}
		} else {
			enc1 := accounts.SerialiseV3(&acc)
			if err := domains.DomainPut(kv.AccountsDomain, roTx, addrBytes, enc1, txNum, enc0, step0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rs *StateV3) Domains() *state.SharedDomains {
	return rs.domains
}

func (rs *StateV3) SetTxNum(blockNum, txNum uint64) {
	rs.domains.SetTxNum(txNum)
	rs.domains.SetBlockNum(blockNum)
}

func (rs *StateV3) ApplyState4(ctx context.Context,
	roTx kv.Tx,
	blockNum uint64,
	txNum uint64,
	writeLists WriteLists,
	balanceIncreases map[common.Address]uint256.Int,
	receipts []*types.Receipt,
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

	if err := rs.applyState(roTx, txNum, writeLists, balanceIncreases, rs.domains, rules); err != nil {
		return fmt.Errorf("StateV3.ApplyState: %w", err)
	}
	writeLists.Return()

	if err := rs.ApplyLogsAndTraces4(roTx, txNum, receipts, logs, traceFroms, traceTos, rs.domains); err != nil {
		return fmt.Errorf("StateV3.ApplyLogsAndTraces: %w", err)
	}

	if (txNum+1)%rs.domains.StepSize() == 0 /*&& txTask.TxNum > 0 */ {
		// We do not update txNum before commitment cuz otherwise committed state will be in the beginning of next file, not in the latest.
		// That's why we need to make txnum++ on SeekCommitment to get exact txNum for the latest committed state.
		//fmt.Printf("[commitment] running due to txNum reached aggregation step %d\n", txNum/rs.domains.StepSize())
		_, err := rs.domains.ComputeCommitment(ctx, true, blockNum, txNum, fmt.Sprintf("applying step %d", txNum/rs.domains.StepSize()))
		if err != nil {
			return fmt.Errorf("ParallelExecutionState.ComputeCommitment: %w", err)
		}
	}

	return nil
}

func (rs *StateV3) ApplyLogsAndTraces4(tx kv.Tx, txNum uint64, receipts []*types.Receipt, logs []*types.Log, traceFroms map[common.Address]struct{}, traceTos map[common.Address]struct{}, domains *state.SharedDomains) error {
	for addr := range traceFroms {
		if err := domains.IndexAdd(kv.TracesFromIdx, addr[:], txNum); err != nil {
			return err
		}
	}

	for addr := range traceTos {
		if err := domains.IndexAdd(kv.TracesToIdx, addr[:], txNum); err != nil {
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
		for _, receipt := range receipts {
			if err := rawdb.WriteReceiptCacheV2(rs.domains.AsPutDel(tx), receipt, txNum); err != nil {
				return err
			}
		}
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
	storage             map[common.Hash]uint256.Int
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
		StateV3:       NewStateV3(domains, s.syncCfg, s.logger),
		accounts:      s.accounts,
		accountsMutex: s.accountsMutex,
	}
}

// BufferedWriter - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type BufferedWriter struct {
	rs           *StateV3Buffered
	trace        bool
	writeLists   map[string]*state.KvList
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
		writeLists:  newWriteList(),
		accumulator: accumulator,
		//trace:       true,
	}
}

func (w *BufferedWriter) SetTxNum(ctx context.Context, txNum uint64) {
	w.txNum = txNum
	w.rs.domains.SetTxNum(txNum)
}
func (w *BufferedWriter) SetTx(tx kv.Tx) {}

func (w *BufferedWriter) ResetWriteSet() {
	w.writeLists = newWriteList()
	w.accountPrevs = nil
	w.accountDels = nil
	w.storagePrevs = nil
	w.codePrevs = nil
}

func (w *BufferedWriter) WriteSet() map[string]*state.KvList {
	return w.writeLists
}

func (w *BufferedWriter) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return w.accountPrevs, w.accountDels, w.storagePrevs, w.codePrevs
}

func (w *BufferedWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("BufferedWriter: acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
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

func (w *BufferedWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
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

func (w *BufferedWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
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

func (w *BufferedWriter) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	compositeS := string(append(address[:], key[:]...))
	vb := value.Bytes32() // using [32]byte instead of []byte to avoid heap escape
	w.writeLists[kv.StorageDomain.String()].Push(compositeS, vb[32-value.ByteLen():])
	//if w.trace {
	fmt.Printf("storage: %x,%x,%x\n", address, key, vb[32-value.ByteLen():])
	//}
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

func (w *BufferedWriter) CreateContract(address common.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}

	return nil
}

// Writer - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type Writer struct {
	tx          kv.TemporalPutDel
	trace       bool
	accumulator *shards.Accumulator
	txNum       uint64
}

func NewWriter(tx kv.TemporalPutDel, accumulator *shards.Accumulator, txNum uint64) *Writer {
	return &Writer{
		tx:          tx,
		accumulator: accumulator,
		txNum:       txNum,
		//trace: true,
	}
}

func (w *Writer) SetTxNum(v uint64) { w.txNum = v }
func (w *Writer) ResetWriteSet()    {}

func (w *Writer) WriteSet() map[string]*state.KvList {
	return nil
}

func (w *Writer) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return nil, nil, nil, nil
}

func (w *Writer) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("Writer: acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	if original.Incarnation > account.Incarnation {
		//del, before create: to clanup code/storage
		if err := w.tx.DomainDel(kv.CodeDomain, address[:], w.txNum, nil, 0); err != nil {
			return err
		}
		if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:], w.txNum); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address, account.Incarnation, value)
	}

	if err := w.tx.DomainPut(kv.AccountsDomain, address[:], value, w.txNum, nil, 0); err != nil {
		return err
	}
	return nil
}

func (w *Writer) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	if err := w.tx.DomainPut(kv.CodeDomain, address[:], code, w.txNum, nil, 0); err != nil {
		return err
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address, incarnation, code)
	}
	return nil
}

func (w *Writer) DeleteAccount(address common.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	//TODO: move logic from SD
	//if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
	//	return err
	//}
	//if err := w.tx.DomainDel(kv.CodeDomain, address[:], nil, 0); err != nil {
	//	return err
	//}
	if err := w.tx.DomainDel(kv.AccountsDomain, address[:], w.txNum, nil, 0); err != nil {
		return err
	}
	// if w.accumulator != nil { TODO: investigate later. basically this will always panic. keeping this out should be fine anyway.
	// 	w.accumulator.DeleteAccount(address)
	// }
	return nil
}

func (w *Writer) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if original == value {
		return nil
	}

	composite := append(address[:], key[:]...)
	v := value.Bytes()
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, v)
	}
	if len(v) == 0 {
		return w.tx.DomainDel(kv.StorageDomain, composite, w.txNum, nil, 0)
	}
	if w.accumulator != nil {
		w.accumulator.ChangeStorage(address, incarnation, key, v)
	}

	return w.tx.DomainPut(kv.StorageDomain, composite, v, w.txNum, nil, 0)
}

var fastCreate = dbg.EnvBool("FAST_CREATE", false)

func (w *Writer) CreateContract(address common.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	if fastCreate {
		return nil
	}
	if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:], w.txNum); err != nil {
		return err
	}
	return nil
}

type ReaderV3 struct {
	txNum  uint64
	trace  bool
	getter kv.TemporalGetter
}

func NewReaderV3(getter kv.TemporalGetter) *ReaderV3 {
	return &ReaderV3{
		//trace:     true,
		getter: getter,
	}
}

func (r *ReaderV3) DiscardReadList()      {}
func (r *ReaderV3) SetTxNum(txNum uint64) { r.txNum = txNum }

func (r *ReaderV3) SetTrace(trace bool) { r.trace = trace }

func (r *ReaderV3) HasStorage(address common.Address) (bool, error) {
	_, _, hasStorage, err := r.getter.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}

func (r *ReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	_, acc, err := r.readAccountData(address)
	return acc, err
}

func (r *ReaderV3) readAccountData(address common.Address) ([]byte, *accounts.Account, error) {
	enc, _, err := r.getter.GetLatest(kv.AccountsDomain, address[:])
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

func (r *ReaderV3) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
	var composite [20 + 32]byte
	copy(composite[0:20], address[0:20])
	copy(composite[20:], key[:])
	enc, _, err := r.getter.GetLatest(kv.StorageDomain, composite[:])
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

	ok := enc != nil
	var res uint256.Int
	if ok {
		(&res).SetBytes(enc)
	}
	return res, ok, err
}

func (r *ReaderV3) ReadAccountCode(address common.Address) ([]byte, error) {
	enc, _, err := r.getter.GetLatest(kv.CodeDomain, address[:])
	if err != nil {
		return nil, err
	}
	if r.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], txNum: %d\n", address, enc, r.txNum)
	}
	return enc, nil
}

func (r *ReaderV3) ReadAccountCodeSize(address common.Address) (int, error) {
	enc, _, err := r.getter.GetLatest(kv.CodeDomain, address[:])
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

type bufferedReader struct {
	reader        *ReaderV3
	bufferedState *StateV3Buffered
}

func NewBufferedReader(bufferedState *StateV3Buffered, reader *ReaderV3) StateReader {
	return &bufferedReader{reader: reader, bufferedState: bufferedState}
}

func (r *bufferedReader) SetTrace(trace bool) {
	r.reader.trace = trace
}

func (r *bufferedReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		if r.reader.trace {
			fmt.Printf("ReadAccountData (buf) [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", address, data.Nonce, &data.Balance, data.CodeHash, r.reader.txNum)
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

func (r *bufferedReader) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
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

	return r.reader.ReadAccountStorage(address, key)
}

func (r *bufferedReader) HasStorage(address common.Address) (bool, error) {
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]

	if ok && len(so.storage) > 0 {
		// TODO - we really need to return the first key
		// for this we need to order the list of hashes
		r.bufferedState.accountsMutex.RUnlock()
		return true, nil
	}
	r.bufferedState.accountsMutex.RUnlock()
	return r.reader.HasStorage(address)
}

func (r *bufferedReader) ReadAccountCode(address common.Address) ([]byte, error) {
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

	return r.reader.ReadAccountCode(address)
}

func (r *bufferedReader) ReadAccountCodeSize(address common.Address) (int, error) {
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

	return r.reader.ReadAccountCodeSize(address)
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

func (r *bufferedReader) SetGetter(getter kv.TemporalGetter) {
	r.reader.getter = getter
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
