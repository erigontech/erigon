// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var _ evmtypes.IntraBlockState = new(IntraBlockState)

type revision struct {
	id           int
	journalIndex int
}

type revisions struct {
	nextId int
	valid  []revision
	buf    [16]revision
}

func (r *revisions) init() {
	r.valid = r.buf[:0]
}

func (r *revisions) snapshot(journal *journal) int {
	id := r.nextId
	r.nextId++
	r.valid = append(r.valid, revision{id, journal.length()})
	return id
}

func (r *revisions) returnSnapshot(id int) {
	if lv := len(r.valid); lv > 0 && r.valid[lv-1].id == id {
		r.valid = r.valid[0 : lv-1]
		if r.nextId == id+1 {
			r.nextId = id
		}
	}
}

func (r *revisions) reset() {
	if cap(r.valid) > maxRetainedRevisionsCap {
		r.valid = r.buf[:0]
	} else {
		r.valid = r.valid[:0]
	}
	r.nextId = 0
}

func (r *revisions) revertToSnapshot(revid int) int {
	idx := sort.Search(len(r.valid), func(i int) bool {
		return r.valid[i].id >= revid
	})
	if idx == len(r.valid) || r.valid[idx].id != revid {
		var id int
		if idx < len(r.valid) {
			id = r.valid[idx].id
		}
		panic(fmt.Errorf("revision id %v cannot be reverted (idx=%v,len=%v,id=%v)", revid, idx, len(r.valid), id))
	}
	snapshot := r.valid[idx]
	r.valid = r.valid[:idx]
	if r.nextId == snapshot.id+1 {
		r.nextId = snapshot.id
	}
	return snapshot.journalIndex
}

// Legal call depth keeps this near 1280; growth past 2048 signals broken push/pop discipline, so Reset drops back to the inline buffer.
const maxRetainedRevisionsCap = 2048

// BalanceIncrease is a deferred balance increase applied without reading the account first.
type BalanceIncrease struct {
	increase    uint256.Int
	transferred bool
	count       int
}

type accessOptions struct {
	revertable bool
}

type AccessSet map[accounts.Address]accessOptions

func (aa AccessSet) Merge(other AccessSet) AccessSet {
	if len(other) == 0 {
		return aa
	}
	dst := make(AccessSet, len(aa)+len(other))
	maps.Copy(dst, aa)
	maps.Copy(dst, other)
	return dst
}

// IntraBlockState caches and manages state changes during block execution; NOT THREAD SAFE.
type IntraBlockState struct {
	stateReader StateReader

	stateObjects      map[accounts.Address]*stateObject // used only if `noMaterialize == false`
	stateObjectsDirty map[accounts.Address]struct{}

	nilAccounts map[accounts.Address]struct{}

	refund uint64

	txIndex  int
	blockNum uint64
	logs     logArena

	accessList accessList

	transientStorage transientStorage

	journal          *journal
	stateObjectArena stateObjectArena // same lifetime with `journal`. used only if `noMaterialize == true`

	trace        bool
	tracingHooks *tracing.Hooks
	balanceInc   map[accounts.Address]*BalanceIncrease
	recordAccess bool // gates MarkAddressAccess — enabled in Prepare

	// Versioned storage for parallel tx processing, reset per block; per-path typed maps avoid an allocation on the read hot path.
	versionMap      *VersionMap
	versionedWrites WriteSet
	versionedReads  ReadSet
	// committedBase memoizes the per-tx fallback account read when versionMap has no cell; safe to share since the committed view is block-immutable.
	committedBase       map[accounts.Address]*accounts.Account
	accountReadDuration time.Duration
	accountReadCount    int64
	storageReadDuration time.Duration
	storageReadCount    int64
	codeReadDuration    time.Duration
	codeReadCount       int64
	version             int
	dep                 int

	// sdProbe memoizes the SelfDestruct probe per attempt; sdProbeEpoch invalidates it on Reset/SetTxContext.
	sdProbe      map[accounts.Address]sdProbeEntry
	sdProbeEpoch uint64

	// noMaterialize suppresses the stateObject cache on the parallel path: writes go to versioned cells only. False for genesis/RPC/serial.
	noMaterialize bool

	// eip8246: EIP-8246 drops the SELFDESTRUCT balance burn; set per-tx from block rules in Prepare.
	eip8246 bool
	// eip161/isAura gate nil-vs-empty account equivalence on reads (AuRa keeps an empty SystemAddress; pre-161 empty is gas-observable).
	eip161 bool
	isAura bool

	revisions revisions
}

type sdProbeEntry struct {
	epoch      uint64
	res        ReadResult
	destructed bool
	ok         bool
}

func New(stateReader StateReader) *IntraBlockState {
	ibs := &IntraBlockState{
		stateReader:       stateReader,
		stateObjects:      map[accounts.Address]*stateObject{},
		stateObjectsDirty: map[accounts.Address]struct{}{},
		nilAccounts:       map[accounts.Address]struct{}{},
		journal:           newJournal(),
		accessList:        accessList{addresses: make(map[accounts.Address]int)},
		transientStorage:  newTransientStorage(),
		balanceInc:        map[accounts.Address]*BalanceIncrease{},
		recordAccess:      false,
		txIndex:           0,
		trace:             false,
		dep:               UnknownDep,
	}
	ibs.revisions.init()
	return ibs
}

func NewWithVersionMap(stateReader StateReader, mvhm *VersionMap) *IntraBlockState {
	ibs := New(stateReader)
	ibs.versionMap = mvhm
	return ibs
}

func (sdb *IntraBlockState) ReadDuration() time.Duration {
	return sdb.accountReadDuration + sdb.storageReadDuration + sdb.codeReadDuration
}

func (sdb *IntraBlockState) ReadCount() int64 {
	return sdb.accountReadCount + sdb.storageReadCount + sdb.codeReadCount
}

func (sdb *IntraBlockState) AccountReadDuration() time.Duration {
	return sdb.accountReadDuration
}

func (sdb *IntraBlockState) AccountReadCount() int64 {
	return sdb.accountReadCount
}

func (sdb *IntraBlockState) StorageReadDuration() time.Duration {
	return sdb.storageReadDuration
}

func (sdb *IntraBlockState) StorageReadCount() int64 {
	return sdb.storageReadCount
}

func (sdb *IntraBlockState) CodeReadDuration() time.Duration {
	return sdb.codeReadDuration
}

func (sdb *IntraBlockState) CodeReadCount() int64 {
	return sdb.codeReadCount
}

func (sdb *IntraBlockState) SetVersionMap(versionMap *VersionMap) {
	sdb.versionMap = versionMap
}

func (sdb *IntraBlockState) VersionMap() *VersionMap {
	return sdb.versionMap
}

func (sdb *IntraBlockState) SetNoMaterialize(v bool) {
	if dbg.AssertEnabled && v != sdb.noMaterialize && !sdb.stateObjectArena.empty() {
		panic("noMaterialize changed with arena slots outstanding")
	}
	sdb.noMaterialize = v
}

func (sdb *IntraBlockState) IsVersioned() bool {
	return sdb.versionMap != nil
}

func (sdb *IntraBlockState) SetHooks(hooks *tracing.Hooks) {
	sdb.tracingHooks = hooks
}

func (sdb *IntraBlockState) SetTrace(trace bool) {
	sdb.trace = trace
}

func (sdb *IntraBlockState) hasWrite(addr accounts.Address, path AccountPath, key accounts.StorageKey) bool {
	return sdb.versionedWrites.Has(WriteHeader{Address: addr, Path: path, Key: key})
}

func (sdb *IntraBlockState) HasStorage(addr accounts.Address) (bool, error) {
	so, err := sdb.getStateObject(addr, false)
	if err != nil {
		return false, err
	}
	if so == nil || so.selfdestructed || so.deleted {
		return false, nil
	}

	// fakeStorage overrides storage for debugging only.
	if len(so.fakeStorage) > 0 {
		for _, v := range so.fakeStorage {
			if !v.IsZero() {
				return true, nil
			}
		}

		return false, nil
	}

	for _, v := range so.originStorage {
		if !v.IsZero() {
			return true, nil
		}
	}

	for _, v := range so.dirtyStorage {
		if !v.IsZero() {
			return true, nil
		}
	}

	// A prior-tx IncarnationPath write (only from CreateAccount/Selfdestruct) means the account was created or destroyed this block, so it has no storage.
	if sdb.versionMap != nil {
		if inc, incRes, ok := sdb.versionMap.ReadIncarnation(addr, sdb.txIndex); ok && incRes.Status() == MVReadResultDone {
			sdb.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{
				ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: incRes.DepIdx(), Incarnation: incRes.Incarnation()}},
				Val:        inc,
			})
			return false, nil
		}
	}

	// EIP-684 CREATE-collision fallback: ask the reader when in-memory checks miss (a HasPrefix walk on snapshot-backed storage).
	result, err := sdb.stateReader.HasStorage(addr)
	return result, err
}

func (sdb *IntraBlockState) Reset() {
	clear(sdb.nilAccounts)
	for _, so := range sdb.stateObjects {
		so.release()
	}
	clear(sdb.stateObjects)
	clear(sdb.stateObjectsDirty)
	sdb.logs.reset()
	clear(sdb.balanceInc)
	sdb.clearJournalAndRefund()
	sdb.txIndex = 0
	sdb.sdProbeEpoch++
	sdb.accessList.Reset()
	clear(sdb.transientStorage)
	sdb.versionMap = nil
	// noMaterialize only matters alongside a versionMap, so clear it here too, or a reused IBS could run unversioned with the cache still suppressed.
	sdb.noMaterialize = false
	clear(sdb.committedBase)
	// Rebinds instead of clearing: a finished tx's result may still hold the previous per-path maps.
	sdb.versionedReads = ReadSet{}
	sdb.versionedWrites.ReleaseAndReset()
	sdb.recordAccess = false
	sdb.accountReadDuration = 0
	sdb.accountReadCount = 0
	sdb.storageReadDuration = 0
	sdb.storageReadCount = 0
	sdb.codeReadDuration = 0
	sdb.codeReadCount = 0
	sdb.dep = UnknownDep
}

// Release Deprecated use Close
func (sdb *IntraBlockState) Release(bool) { sdb.Close() }

// Close is idempotent and not thread-safe; Reset re-uses the IntraBlockState instead of releasing it.
func (sdb *IntraBlockState) Close() {
	if sdb == nil || sdb.stateObjects == nil {
		return
	}

	stateObjects, journal := sdb.stateObjects, sdb.journal
	sdb.stateObjects, sdb.journal = nil, nil
	sdb.stateObjectArena.release()
	sdb.logs.release()
	sdb.revisions.reset()
	// Safe to pool: VersionedWrites/FinalizedWrites hand out deep clones, and the set is unexported.
	sdb.versionedWrites.ReleaseAndReset()

	releaseResources(stateObjects, journal)
}

// The noMaterialize path never releases what it takes, so a pool draw here would drain the materializing paths.
func (sdb *IntraBlockState) allocStateObject() *stateObject {
	if sdb.noMaterialize {
		if so := sdb.stateObjectArena.alloc(); so != nil {
			return so
		}
		return newHeapObject()
	}
	return stateObjectPool.Get().(*stateObject)
}

func releaseResources(stateObjects map[accounts.Address]*stateObject, journal *journal) {
	for _, so := range stateObjects {
		so.release()
	}
	if journal != nil {
		journal.release()
	}
}

// AllocLog reserves the next log slot; the arena reuses it later, so callers must never retain it without copying.
func (sdb *IntraBlockState) AllocLog(addr common.Address, numTopics, dataSize int) *types.Log {
	return sdb.logs.alloc(sdb.journal, addr, sdb.txIndex, numTopics, dataSize)
}

func (sdb *IntraBlockState) NotifyLog(lp *types.Log) {
	if dbg.TraceLogs && (sdb.trace || dbg.TraceAccount(accounts.InternAddress(lp.Address).Handle())) {
		var topics string
		for i := 0; i < len(lp.Topics); i++ {
			topics += "[" + hex.EncodeToString(lp.Topics[i][:]) + "]"
		}
		if topics == "" {
			topics = "[]"
		}
		fmt.Printf("%d (%d.%d) Log: Index:%d Account:%x Topics: %s Data:%x\n", sdb.blockNum, sdb.txIndex, sdb.version, lp.Index, lp.Address, topics, lp.Data)
	}
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnLog != nil {
		// The hook may retain the value; the arena entry is reused by later blocks.
		sdb.tracingHooks.OnLog(lp.Copy())
	}
}

func (sdb *IntraBlockState) AddLog(log *types.Log) {
	lp := sdb.AllocLog(log.Address, len(log.Topics), len(log.Data))
	copy(lp.Topics, log.Topics)
	copy(lp.Data, log.Data)
	lp.Removed = log.Removed
	lp.BlockNumber = log.BlockNumber
	lp.TxHash, lp.BlockHash = log.TxHash, log.BlockHash
	sdb.NotifyLog(lp)
}

// GetLogs deep-copies the tx's logs, so the result is safe to hold after the arena reuses the entry.
func (sdb *IntraBlockState) GetLogs(txIndex int, txnHash common.Hash, blockNumber uint64, blockHash common.Hash) types.Logs {
	logs := sdb.logs.forTx(txIndex).Copy()
	for _, l := range logs {
		l.TxHash = txnHash
		l.BlockHash = blockHash
		l.BlockNumber = hexutil.Uint64(blockNumber)
	}
	return logs
}

func (sdb *IntraBlockState) GetRawLogs(txIndex int) types.Logs {
	return sdb.logs.forTx(txIndex).Copy()
}

func (sdb *IntraBlockState) Logs() types.Logs {
	if len(sdb.logs.entries) == 0 {
		return nil
	}
	return sdb.logs.entries.Copy()
}

func (sdb *IntraBlockState) LogsRlpHash() common.Hash {
	return types.RlpHashLogs(sdb.logs.entries)
}

func (sdb *IntraBlockState) AddRefund(gas uint64) {
	sdb.journal.refundChange(sdb.refund)
	sdb.refund += gas
}

func (sdb *IntraBlockState) SubRefund(gas uint64) error {
	sdb.journal.refundChange(sdb.refund)
	if gas > sdb.refund {
		return errors.New("refund counter below zero")
	}
	sdb.refund -= gas
	return nil
}

// Exist also returns true for self-destructed accounts.
func (sdb *IntraBlockState) Exist(addr accounts.Address) (exists bool, err error) {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			fmt.Printf("%d (%d.%d) Exists %x: %v\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, exists)
		}()
	}
	if sdb.versionMap == nil {
		s, err := sdb.getStateObject(addr, true)
		if err != nil {
			return false, err
		}
		return s != nil && !s.deleted, nil
	}

	// Needs only the base record + self-destruct gate, not the per-field overlay.
	readAccount, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	return readAccount != nil, nil
}

// Empty reports non-existence or EIP-161 emptiness (balance = nonce = code = 0).
func (sdb *IntraBlockState) Empty(addr accounts.Address) (empty bool, err error) {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			fmt.Printf("%d (%d.%d) Empty %x: %v\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, empty)
		}()
	}
	if sdb.versionMap == nil {
		so, err := sdb.getStateObject(addr, true)
		if err != nil {
			return false, err
		}

		return so == nil || so.deleted || so.data.Empty(), nil
	}
	// Existence + self-destruct gate only; per-field emptiness is checked below.
	account, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	if account == nil {
		sdb.touchAccount(addr)
		// Do not call accountRead: it would overwrite the nil AddressPath read already recorded, hiding this address from conflict detection.
		return true, nil
	}

	// EIP-6780: an account self-destructed in this tx stays alive until end of tx, so it must not read as empty.
	if sdb.hasWrite(addr, SelfDestructPath, accounts.NilKey) {
		return false, nil
	}

	return sdb.emptyFromVersionedFields(addr, account)
}

// emptyFromVersionedFields checks EIP-161 emptiness per-field, short-circuiting instead of reconstructing the whole account.
func (sdb *IntraBlockState) emptyFromVersionedFields(addr accounts.Address, account *accounts.Account) (bool, error) {
	balance, _, _, err := refreshBalance(sdb, addr, account.Balance)
	if err != nil {
		return false, err
	}
	if !balance.IsZero() {
		return false, nil
	}
	nonce, _, _, err := refreshNonce(sdb, addr, account.Nonce)
	if err != nil {
		return false, err
	}
	if nonce != 0 {
		return false, nil
	}
	codeHash, _, _, err := refreshCodeHash(sdb, addr, account.CodeHash)
	if err != nil {
		return false, err
	}
	return codeHash == accounts.EmptyCodeHash, nil
}

func (sdb *IntraBlockState) GetBalance(addr accounts.Address) (uint256.Int, error) {
	balance, _, err := sdb.getBalance(addr)
	return balance, err
}

func (sdb *IntraBlockState) getBalance(addr accounts.Address) (uint256.Int, bool, error) {
	if sdb.versionMap == nil {
		stateObject, err := sdb.getStateObject(addr, true)
		if err != nil {
			return u256.Num0, false, err
		}
		if stateObject != nil && !stateObject.deleted {
			if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
				balance := stateObject.Balance()
				fmt.Printf("%d (%d.%d) GetBalance %x: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, balance.String())
			}
			return stateObject.Balance(), true, nil
		}
		return u256.Num0, false, nil
	}

	balance, source, _, err := readBalance(sdb, addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetBalance %x: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, balance.String())
	}
	return balance, source == StorageRead || source == MapRead, err
}

func (sdb *IntraBlockState) GetNonce(addr accounts.Address) (uint64, error) {
	if sdb.versionMap == nil {
		stateObject, err := sdb.getStateObject(addr, true)
		if err != nil {
			return 0, err
		}
		if stateObject != nil && !stateObject.deleted {
			return stateObject.Nonce(), nil
		}
		return 0, nil
	}

	nonce, _, _, err := readNonce(sdb, addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetNonce %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, nonce)
	}

	return nonce, err
}

func (sdb *IntraBlockState) TxnIndex() int {
	return sdb.txIndex
}

func (sdb *IntraBlockState) GetCode(addr accounts.Address) ([]byte, error) {
	return sdb.getCode(addr, false)
}

func (sdb *IntraBlockState) getCode(addr accounts.Address, commited bool) ([]byte, error) {
	if sdb.versionMap == nil {
		stateObject, err := sdb.getStateObject(addr, true)
		if err != nil {
			return nil, err
		}
		if stateObject != nil && !stateObject.deleted {
			code, err := stateObject.Code()
			if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
				if err != nil {
					fmt.Printf("%d (%d.%d) GetCode (%s) %x: err: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, StorageRead, addr, err)
				} else {
					fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, StorageRead, addr, len(code))
				}
			}
			if err == nil {
				sdb.callCodeAccessHook(addr, code)
			}
			return code, err
		}
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, StorageRead, addr, 0)
		}
		return nil, nil
	}
	// commited=true (EIP-7702 ResolveCode) may see a stale ReadSet value; hasWrite confirms this tx's own code write.
	if commited {
		if so, ok := sdb.stateObjects[addr]; ok && so.dirtyCode && sdb.hasWrite(addr, CodePath, accounts.NilKey) {
			sdb.callCodeAccessHook(addr, so.code.Bytes)
			return so.code.Bytes, nil
		}
	}
	code, source, _, err := readCode(sdb, addr, commited)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		if err != nil {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: err: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, err)
		} else {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, len(code))
		}
	}
	if err == nil {
		sdb.callCodeAccessHook(addr, code)
	}

	return code, err
}

func (sdb *IntraBlockState) GetCodeSize(addr accounts.Address) (int, error) {
	if sdb.versionMap == nil {
		stateObject, err := sdb.getStateObject(addr, true)
		if err != nil {
			return 0, err
		}
		if stateObject == nil || stateObject.deleted {
			return 0, nil
		}
		if stateObject.code.Bytes != nil {
			sdb.callCodeAccessHook(addr, stateObject.code.Bytes)
			return stateObject.code.Len(), nil
		}
		if stateObject.data.CodeHash.IsEmpty() {
			return 0, nil
		}
		// Must call ReadAccountCodeSize, not ReadAccountCode: a size-only witness node has no bytes and would desync EXTCODESIZE.
		size, err := sdb.stateReader.ReadAccountCodeSize(addr)
		if err != nil {
			return 0, err
		}
		return size, nil
	}

	size, source, _, err := readCodeSize(sdb, addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetCodeSize (%s) %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, size)
	}

	return size, err
}

// codeAccessTracker lets a stateReader observe code accesses (EIP-7928 BAL / EIP-7702); no-op if unimplemented.
type codeAccessTracker interface {
	OnCodeAccess(accounts.Address, []byte)
}

func (sdb *IntraBlockState) callCodeAccessHook(addr accounts.Address, code []byte) {
	if hook, ok := sdb.stateReader.(codeAccessTracker); ok {
		hook.OnCodeAccess(addr, code)
	}
}

func (sdb *IntraBlockState) GetCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	if sdb.versionMap == nil {
		stateObject, err := sdb.getStateObject(addr, true)
		if err != nil {
			return accounts.NilCodeHash, err
		}
		if stateObject == nil || stateObject.deleted {
			return accounts.NilCodeHash, nil
		}
		return stateObject.data.CodeHash, nil
	}

	hash, _, _, err := readCodeHash(sdb, addr)
	if err != nil {
		return accounts.NilCodeHash, err
	}
	// EIP-6780: a same-tx self-destructed contract must still return its real code hash, recomputed since CodeHashPath is cleared.
	if hash == accounts.EmptyCodeHash && sdb.hasWrite(addr, SelfDestructPath, accounts.NilKey) {
		if cw, ok := sdb.versionedWrites.GetCode(addr); ok && len(cw.Val.Bytes) > 0 {
			return accounts.InternCodeHash(crypto.Keccak256Hash(cw.Val.Bytes)), nil
		}
	}
	if sdb.eip8246 && hash == accounts.NilCodeHash {
		// A prior tx's EIP-8246 SELFDESTRUCT leaves an empty-code account whose CodeHashPath is dropped; recover it from the account instead.
		acc, _, _, err := sdb.getVersionedAccount(addr, false)
		if err != nil {
			return accounts.NilCodeHash, err
		}
		if acc != nil {
			return acc.CodeHash, nil
		}
	}
	return hash, err
}

func (sdb *IntraBlockState) ResolveCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	dd, ok, err := sdb.GetDelegatedDesignation(addr)

	if ok {
		return sdb.GetCodeHash(dd)
	}

	if err != nil {
		return accounts.NilCodeHash, err
	}

	return sdb.GetCodeHash(addr)
}

func (sdb *IntraBlockState) ResolveCode(addr accounts.Address) ([]byte, error) {
	// committed=false so this tx's own EIP-7702 authorization-list writes are visible, not stale delegation code.
	code, err := sdb.getCode(addr, false)
	if delegation, ok := types.ParseDelegation(code); ok {
		return sdb.getCode(delegation, false)
	}
	if err != nil {
		return nil, err
	}
	return code, nil
}

func (sdb *IntraBlockState) GetDelegatedDesignation(addr accounts.Address) (accounts.Address, bool, error) {
	// EIP-7702: this probe isn't recorded as an account read, since it may not be a gas-charged access.
	if sdb.versionMap != nil {
		// Reads via versioned CodePath so validation can catch a race; uses readCode directly to avoid a spurious BAL code access.
		code, _, _, err := readCode(sdb, addr, false)
		if err != nil {
			return accounts.ZeroAddress, false, err
		}
		if delegation, ok := types.ParseDelegation(code); ok {
			sdb.callCodeAccessHook(addr, code)
			return delegation, true, nil
		}
		return accounts.ZeroAddress, false, nil
	}
	stateObject, err := sdb.getStateObject(addr, false)
	if err != nil {
		return accounts.ZeroAddress, false, err
	}
	if stateObject != nil && !stateObject.deleted {
		code, err := stateObject.Code()

		if err != nil {
			return accounts.ZeroAddress, false, err
		}
		if delegation, ok := types.ParseDelegation(code); ok {
			sdb.callCodeAccessHook(addr, code)
			return delegation, true, nil
		}
	}
	return accounts.ZeroAddress, false, nil
}

func (sdb *IntraBlockState) GetState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := readState(sdb, addr, key)

	if dbg.TraceTransactionIO && (sdb.trace || (dbg.TraceAccount(addr.Handle()) && traceKey(key))) {
		fmt.Printf("%d (%d.%d) GetState (%s) %x, %x=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, key, versionedValue.Hex()[2:])
	}

	return versionedValue, err
}

func (sdb *IntraBlockState) GetCommittedState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := readCommittedState(sdb, addr, key)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetCommittedState (%s) %x, %x=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, key, versionedValue.Hex()[2:])
	}

	return versionedValue, err
}

func (sdb *IntraBlockState) HasSelfdestructed(addr accounts.Address) (bool, error) {
	destructed, _, _, err := readSelfDestruct(sdb, addr)
	return destructed, err
}

func (sdb *IntraBlockState) ReadVersion(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) ReadResult {
	return sdb.versionMap.ReadStatus(addr, path, key, txIdx)
}

// writeBalanceVersioned skips materializing on the existing-alive path; absent/destroyed accounts still go through GetOrNewStateObject.
func (sdb *IntraBlockState) writeBalanceVersioned(addr accounts.Address, update uint256.Int, wasCommited bool, reason tracing.BalanceChangeReason) error {
	base, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return err
	}
	if base == nil || sdb.accountLifecycle(addr) {
		stateObject, err := sdb.GetOrNewStateObject(addr)
		if err != nil {
			return err
		}
		// A destroyed-then-revived stateObject's transient balance lags this tx's own write; seed it first or SetBalance's journal captures a stale prev.
		if base != nil {
			cur, _, err := sdb.getBalance(addr)
			if err != nil {
				return err
			}
			stateObject.setBalance(cur)
		}
		stateObject.SetBalance(update, wasCommited, reason)
		sdb.recordWriteBalance(addr, update)
		return nil
	}
	prev, _, err := sdb.getBalance(addr)
	if err != nil {
		return err
	}
	sdb.journal.balanceChange(addr, prev, wasCommited)
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
		sdb.tracingHooks.OnBalanceChange(addr, prev, update, reason)
	}
	sdb.recordWriteBalance(addr, update)
	return nil
}

func (sdb *IntraBlockState) AddBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if sdb.versionMap == nil {
		if _, needAccount := sdb.stateObjects[addr]; !needAccount && addr == ripemd && amount.IsZero() {
			sdb.journal.balanceIncrease(addr, amount)

			bi, ok := sdb.balanceInc[addr]
			if !ok {
				bi = &BalanceIncrease{}
				sdb.balanceInc[addr] = bi
			}

			if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
				prev := new(uint256.Int)
				amount := amount
				if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
					sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
				}
				var readStart time.Time
				if dbg.KVReadLevelledMetrics {
					readStart = time.Now()
				}
				account, _ := sdb.stateReader.ReadAccountDataForDebug(addr)
				if dbg.KVReadLevelledMetrics {
					sdb.accountReadDuration += time.Since(readStart)
					sdb.accountReadCount++
				}
				sdb.stateReader.SetTrace(false, "")
				if account != nil {
					prev.Add(&account.Balance, &bi.increase)
				} else {
					prev.Add(prev, &bi.increase)
				}

				sdb.tracingHooks.OnBalanceChange(addr, *prev, *(new(uint256.Int).Add(prev, &amount)), reason)
			}

			bi.increase = u256.Add(bi.increase, amount)
			bi.count++
			return nil
		}
	}

	// EIP-161: check emptiness so a zero-value transfer can still clear a (0,0,0) account.
	if amount.IsZero() {
		return sdb.TouchAccount(addr)
	}

	prev, wasCommited, _ := sdb.getBalance(addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			prev := prev // avoid capture allocation unless we're tracing
			amount := amount
			expected := (&uint256.Int{}).Add(&prev, &amount)
			if bal.Cmp(expected) != 0 {
				panic(fmt.Sprintf("add failed: expected: %d got: %s", expected, bal.String()))
			}
			fmt.Printf("%d (%d.%d) AddBalance %x, %s+%s=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, prev.String(), amount.String(), bal.String())
		}()
	}

	update := u256.Add(prev, amount)

	if sdb.versionMap != nil {
		return sdb.writeBalanceVersioned(addr, update, wasCommited, reason)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(update, wasCommited, reason)
	sdb.recordWriteBalance(addr, update)
	return nil
}

func (sdb *IntraBlockState) touchAccount(addr accounts.Address) {
	sdb.journal.touchAccount(addr, false, uint256.Int{})
	if addr == ripemd {
		// Marks it dirty directly; normal entries get this from flattened journals.
		sdb.journal.dirty(addr)
	}
}

// TouchAccount materializes an empty account, recording the zero-balance touch EIP-161 state clearing needs.
func (sdb *IntraBlockState) TouchAccount(addr accounts.Address) error {
	markTouched := func() {
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) Touch %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
		}
		if sdb.versionMap != nil {
			// Pairs the BalancePath=0 write with a reverting journal entry so the write-set stays in step through reverts.
			prevWrite, had := sdb.versionedWrites.GetBalance(addr)
			var prev uint256.Int
			if had {
				prev = prevWrite.Val
			}
			sdb.recordWriteBalance(addr, uint256.Int{})
			sdb.journal.touchAccount(addr, !had, prev)
			return
		}
		sdb.recordWriteBalance(addr, uint256.Int{})
		if _, ok := sdb.journal.dirties[addr]; !ok {
			sdb.touchAccount(addr)
		}
	}

	if sdb.versionMap != nil {
		// Touch depends only on emptiness, computed from field reads without reconstructing the stateObject.
		account, _, _, err := sdb.versionedAccountBase(addr, true)
		if err != nil {
			return err
		}
		if account != nil {
			empty, err := sdb.emptyFromVersionedFields(addr, account)
			if err != nil {
				return err
			}
			if empty {
				markTouched()
			}
			return nil
		}
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject.data.Empty() {
		markTouched()
	}

	return nil
}

// synthesizeCreatedAccountBase rebuilds an absent account from its EIP-7928 BAL sub-field cells; declines for an empty, estimated, or destroyed result.
func (sdb *IntraBlockState) synthesizeCreatedAccountBase(addr accounts.Address) (*accounts.Account, bool) {
	if sdb.versionMap == nil {
		return nil, false
	}
	if sdb.consumedAddressAbsence(addr) {
		return nil, false
	}
	if destructed, sdRes, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex); ok && sdRes.Status() == MVReadResultDone && destructed {
		if dbg.TraceReexec {
			fmt.Printf("SYNTH-DECLINE reason=sd blk=%d tx=%d %x sdIdx=%d\n", sdb.blockNum, sdb.txIndex, addr, sdRes.DepIdx())
		}
		return nil, false
	}
	acc := &accounts.Account{CodeHash: accounts.EmptyCodeHash}
	found := false
	if bal, res, ok := sdb.versionMap.ReadBalance(addr, sdb.txIndex); ok {
		if res.Status() != MVReadResultDone {
			if dbg.TraceReexec {
				fmt.Printf("SYNTH-DECLINE reason=est-bal blk=%d tx=%d %x cellIdx=%d\n", sdb.blockNum, sdb.txIndex, addr, res.DepIdx())
			}
			return nil, false
		}
		acc.Balance = bal
		found = true
	}
	if nonce, res, ok := sdb.versionMap.ReadNonce(addr, sdb.txIndex); ok {
		if res.Status() != MVReadResultDone {
			if dbg.TraceReexec {
				fmt.Printf("SYNTH-DECLINE reason=est-nonce blk=%d tx=%d %x cellIdx=%d\n", sdb.blockNum, sdb.txIndex, addr, res.DepIdx())
			}
			return nil, false
		}
		acc.Nonce = nonce
		found = true
	}
	if code, res, ok := sdb.versionMap.ReadCode(addr, sdb.txIndex); ok {
		if res.Status() != MVReadResultDone {
			if dbg.TraceReexec {
				fmt.Printf("SYNTH-DECLINE reason=est-code blk=%d tx=%d %x cellIdx=%d\n", sdb.blockNum, sdb.txIndex, addr, res.DepIdx())
			}
			return nil, false
		}
		if len(code.Bytes) > 0 {
			acc.CodeHash = code.Hash
			if _, delegated := types.ParseDelegation(code.Bytes); !delegated {
				acc.Incarnation = 1
			}
		}
		found = true
	}
	if !found || acc.Empty() {
		if dbg.TraceReexec && found {
			fmt.Printf("SYNTH-DECLINE reason=empty blk=%d tx=%d %x\n", sdb.blockNum, sdb.txIndex, addr)
		}
		return nil, false
	}
	acc.Root.SetBytes(trie.EmptyRoot[:])
	return acc, true
}

// consumedAddressAbsence reports whether this tx already holds a definitive nil AddressPath read; later loads must not adopt cells flushed since.
func (sdb *IntraBlockState) consumedAddressAbsence(addr accounts.Address) bool {
	tr, ok := sdb.versionedReads.GetAddress(addr)
	return ok && tr.Source != ProvisionalRead && (tr.Val == nil || tr.Val.Account() == nil)
}

// finalizeProvisionalAddressRead demotes an in-flight nil probe to a definitive read, so a later flush must conflict with it.
func (sdb *IntraBlockState) finalizeProvisionalAddressRead(addr accounts.Address) {
	if tr, ok := sdb.versionedReads.GetAddress(addr); ok && tr.Source == ProvisionalRead {
		tr.Source = StorageRead
		sdb.versionedReads.SetAddress(addr, tr)
	}
}

// readSelfDestructMemo caches the SelfDestruct probe per attempt, stable since it excludes this tx's own writes.
func (sdb *IntraBlockState) readSelfDestructMemo(addr accounts.Address) (bool, ReadResult, bool) {
	if e, hit := sdb.sdProbe[addr]; hit && e.epoch == sdb.sdProbeEpoch {
		return e.destructed, e.res, e.ok
	}
	destructed, res, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex)
	if sdb.sdProbe == nil {
		sdb.sdProbe = make(map[accounts.Address]sdProbeEntry, 8)
	}
	sdb.sdProbe[addr] = sdProbeEntry{epoch: sdb.sdProbeEpoch, res: res, destructed: destructed, ok: ok}
	return destructed, res, ok
}

// eip8246PreservedAccount rebuilds the account a prior EIP-8246 SELFDESTRUCT left alive; returns nil once the balance is drained.
func (sdb *IntraBlockState) eip8246PreservedAccount(addr accounts.Address) (*accounts.Account, error) {
	bal, _, _, err := readBalance(sdb, addr)
	if err != nil {
		return nil, err
	}
	if bal.IsZero() {
		return nil, nil
	}
	acc := accounts.NewAccount()
	acc.Balance = bal
	nonce, _, _, err := readNonce(sdb, addr)
	if err != nil {
		return nil, err
	}
	acc.Nonce = nonce
	codeHash, _, _, err := readCodeHash(sdb, addr)
	if err != nil {
		return nil, err
	}
	if codeHash != accounts.NilCodeHash && !codeHash.IsZero() {
		acc.CodeHash = codeHash
	}
	return &acc, nil
}

func (sdb *IntraBlockState) getVersionedAccount(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	return sdb.versionedAccountBase(addr, readStorage)
}

// versionedAccountBase resolves existence via the AddressPath read, applying the self-destruct/revival gate but not per-field overlays.
func (sdb *IntraBlockState) versionedAccountBase(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	if sdb.versionMap == nil {
		return nil, UnknownSource, UnknownVersion, nil
	}

	readAccount, source, version, err := readAccount(sdb, addr)

	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}

	// EIP-8246: SELFDESTRUCT preserves the account instead of destroying it; reconstruct it here unless a later tx re-created it.
	if sdb.eip8246 && readAccount == nil {
		if destructed, sdRes, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex); ok && sdRes.Status() == MVReadResultDone && destructed {
			// Exempts only the read this same destruct itself produced; any other definitive nil read aborts here.
			if tr, ok := sdb.versionedReads.GetAddress(addr); ok && tr.Source != ProvisionalRead && (tr.Val == nil || tr.Val.Account() == nil) &&
				!(tr.Source == MapRead && tr.Version.TxIndex == sdRes.DepIdx() && tr.Version.Incarnation == sdRes.Incarnation()) {
				if sdRes.DepIdx() > sdb.dep {
					sdb.dep = sdRes.DepIdx()
				}
				panic(ErrDependency)
			}
			destructTxIndex := sdRes.DepIdx()
			// Only a later CreateAccount counts as revival; later Balance/Nonce/CodeHash writes just update the still-preserved account.
			revived := false
			if hi, ok := sdb.versionMap.LatestTxIndex(addr, AddressPath, accounts.NilKey, sdb.txIndex-1); ok && hi > destructTxIndex {
				revived = true
			}
			if !revived {
				preserved, err := sdb.eip8246PreservedAccount(addr)
				if err != nil {
					return nil, StorageRead, UnknownVersion, err
				}
				if preserved == nil {
					sdb.finalizeProvisionalAddressRead(addr)
					return nil, StorageRead, UnknownVersion, nil
				}
				sdb.accountRead(addr, preserved, MapRead, Version{TxIndex: destructTxIndex})
				return preserved, MapRead, Version{TxIndex: destructTxIndex}, nil
			}
		}
	}

	if readAccount == nil {
		if readStorage {
			if cached, ok := sdb.committedBase[addr]; ok {
				readAccount = cached
			} else {
				if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
					sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
				}
				var readStart time.Time
				if dbg.KVReadLevelledMetrics {
					readStart = time.Now()
				}
				readAccount, err = sdb.stateReader.ReadAccountData(addr)
				if dbg.KVReadLevelledMetrics {
					sdb.accountReadDuration += time.Since(readStart)
					sdb.accountReadCount++
				}
				sdb.stateReader.SetTrace(false, "")
				if err == nil {
					if sdb.committedBase == nil {
						sdb.committedBase = make(map[accounts.Address]*accounts.Account)
					}
					sdb.committedBase[addr] = readAccount
				}
			}
			source = StorageRead
		}

		if readAccount == nil || err != nil {
			if err == nil && readStorage {
				if synth, ok := sdb.synthesizeCreatedAccountBase(addr); ok {
					sdb.accountRead(addr, synth, MapRead, UnknownVersion)
					return synth, StorageRead, UnknownVersion, nil
				}
			}
			if readStorage {
				sdb.finalizeProvisionalAddressRead(addr)
			}
			return nil, StorageRead, UnknownVersion, err
		}

		// CachedReaderV3 bypasses versionMap and returns a pre-SD record; without this gate stale fields survive and Empty() misses CallNewAccountGas.
		if destroyed, _, revived := sdb.versionMap.AccountLifecycle(addr, sdb.txIndex); destroyed && !revived {
			sdb.finalizeProvisionalAddressRead(addr)
			return nil, StorageRead, UnknownVersion, nil
		}
		// readAccount recorded a nil map-read; reconcile it now the DB resolved the account, or a later cell would spuriously invalidate it.
		sdb.accountRead(addr, readAccount, source, version)
	}

	return readAccount, source, version, nil
}

func (sdb *IntraBlockState) SubBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if amount.IsZero() {
		if addr == params.SystemAddress {
			// Gnosis/AuRa keeps an empty system account alive post-Spurious-Dragon, as defense-in-depth, not the primary path.
			return sdb.TouchAccount(addr)
		}
		return nil
	}

	prev, wasCommited, _ := sdb.getBalance(addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			prev := prev
			amount := amount
			fmt.Printf("%d (%d.%d) SubBalance %x, %s-%s=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, prev.String(), amount.String(), bal.String())
		}()
	}

	update := u256.Sub(prev, amount)

	if sdb.versionMap != nil {
		return sdb.writeBalanceVersioned(addr, update, wasCommited, reason)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(update, wasCommited, reason)
	return nil
}

func (sdb *IntraBlockState) SetBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		amount := amount
		fmt.Printf("%d (%d.%d) SetBalance %x, %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, amount.String())
	}
	if sdb.versionMap != nil {
		return sdb.writeBalanceVersioned(addr, amount, !sdb.hasWrite(addr, BalancePath, accounts.NilKey), reason)
	}
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(amount, !sdb.hasWrite(addr, BalancePath, accounts.NilKey), reason)
	sdb.recordWriteBalance(addr, stateObject.Balance())
	return nil
}

func (sdb *IntraBlockState) SetNonce(addr accounts.Address, nonce uint64, reason tracing.NonceChangeReason) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetNonce %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, nonce)
	}

	wasCommited := !sdb.hasWrite(addr, NoncePath, accounts.NilKey)
	if sdb.versionMap != nil {
		return sdb.writeNonceVersioned(addr, nonce, wasCommited, reason)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}

	stateObject.SetNonce(nonce, wasCommited, reason)
	sdb.recordWriteNonce(addr, stateObject.Nonce(), reason)
	return nil
}

// writeNonceVersioned skips materializing on the existing-alive path; prev is read without recording an OCC read.
func (sdb *IntraBlockState) writeNonceVersioned(addr accounts.Address, nonce uint64, wasCommited bool, reason tracing.NonceChangeReason) error {
	base, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return err
	}
	if base == nil || sdb.accountLifecycle(addr) {
		stateObject, err := sdb.GetOrNewStateObject(addr)
		if err != nil {
			return err
		}
		stateObject.SetNonce(nonce, wasCommited, reason)
		sdb.recordWriteNonce(addr, nonce, reason)
		return nil
	}
	prev := base.Nonce
	// Keeps an already-materialized stateObject's so.data in step so genesis/RPC's so.data-based commit paths stay correct.
	if so, ok := sdb.stateObjects[addr]; ok {
		prev = so.data.Nonce
		so.setNonce(nonce)
	}
	// The tx's own nonce cell is authoritative for journal prev, since so.data can lag if materialized after those writes.
	if vw, ok := sdb.versionedWrites.GetNonce(addr); ok {
		prev = vw.Val
	}
	sdb.journal.nonceChange(addr, prev, wasCommited)
	if sdb.tracingHooks != nil {
		if sdb.tracingHooks.OnNonceChangeV2 != nil {
			sdb.tracingHooks.OnNonceChangeV2(addr, prev, nonce, reason)
		} else if sdb.tracingHooks.OnNonceChange != nil {
			sdb.tracingHooks.OnNonceChange(addr, prev, nonce)
		}
	}
	sdb.recordWriteNonce(addr, nonce, reason)
	return nil
}

func printCode(c []byte) (int, string) {
	lenc := len(c)

	if lenc == 0 {
		return 0, ""
	}

	if lenc > 41 {
		return lenc, fmt.Sprintf("%x...", c[0:40])
	}

	return lenc, fmt.Sprintf("%x...", c)
}

func (sdb *IntraBlockState) SetCode(addr accounts.Address, code []byte, reason tracing.CodeChangeReason) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		lenc, cs := printCode(code)
		fmt.Printf("%d (%d.%d) SetCode %x, %d: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, lenc, cs)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	canonical := accounts.NewCode(code)
	codeHash := canonical.Hash
	baseCodeHash := stateObject.data.CodeHash
	origHash := stateObject.original.CodeHash
	if sdb.versionMap != nil {
		// baseCodeHash is the current cell (incl. this tx's writes); origHash is the versionMap floor at tx-start.
		if ch, chErr := sdb.GetCodeHash(addr); chErr == nil {
			baseCodeHash = ch
		}
		if ch, res, ok := sdb.versionMap.ReadCodeHash(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone {
			origHash = ch
		} else if sdb.noMaterialize {
			// The rebuilt transient's original reflects this tx's own code cell; with no floor entry it falls back to the committed hash.
			origHash, err = sdb.committedCodeHash(addr)
			if err != nil {
				return err
			}
		}
	}
	if sdb.noMaterialize {
		seed, err := sdb.codeSeed(addr, baseCodeHash)
		if err != nil {
			return err
		}
		stateObject.setCode(seed)
	}
	written, err := stateObject.SetCode(canonical, !sdb.hasWrite(addr, CodePath, accounts.NilKey), reason)
	if err != nil {
		return err
	}
	if written {
		// Skips when code matches the base this call saw, or (unless newly created) the pre-tx original.
		matchesOriginal := !stateObject.newlyCreated && codeHash == origHash
		if codeHash == baseCodeHash || matchesOriginal {
			if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) SetCode SKIP (matches base) %x codeHash=%x baseHash=%x originalHash=%x codeLen=%d\n",
					sdb.blockNum, sdb.txIndex, sdb.version, addr, codeHash, baseCodeHash, stateObject.original.CodeHash, len(code))
			}
			sdb.versionedWrites.DelCode(addr)
			sdb.versionedWrites.DelCodeHash(addr)
			sdb.versionedWrites.DelCodeSize(addr)
		} else {
			if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) SetCode WRITE %x codeHash=%x baseHash=%x codeLen=%d\n",
					sdb.blockNum, sdb.txIndex, sdb.version, addr, codeHash, baseCodeHash, len(code))
			}
			sdb.recordWriteCode(addr, canonical)
			sdb.recordWriteCodeHash(addr, codeHash)
			sdb.recordWriteCodeSize(addr, canonical.Len())
		}
	}
	return nil
}

var tracedKeys map[accounts.StorageKey]struct{}

func traceKey(key accounts.StorageKey) bool {
	if tracedKeys == nil {
		tracedKeys = map[accounts.StorageKey]struct{}{}
		for _, key := range dbg.TraceStateKeys {
			key, _ = strings.CutPrefix(strings.ToLower(key), "Ox")
			tracedKeys[accounts.InternKey(common.HexToHash(key))] = struct{}{}
		}
	}
	_, ok := tracedKeys[key]
	return len(tracedKeys) == 0 || ok
}

func (sdb *IntraBlockState) Trace() bool {
	return sdb.trace || dbg.Trace
}

func (sdb *IntraBlockState) BlockNumber() uint64 {
	return sdb.blockNum
}

func (sdb *IntraBlockState) TxIndex() int {
	return sdb.txIndex
}

func (sdb *IntraBlockState) Incarnation() int {
	return sdb.version
}

func (sdb *IntraBlockState) SetState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	return sdb.setState(addr, key, value, false)
}

func (sdb *IntraBlockState) setState(addr accounts.Address, key accounts.StorageKey, value uint256.Int, force bool) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetState %x, %x=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, key, value.Hex())
	}

	// The EVM SSTORE path (force==false) writes through cells without materializing; force==true or a fakeStorage override still need the object.
	if sdb.versionMap != nil && !force {
		if so, ok := sdb.stateObjects[addr]; !ok || so.fakeStorage == nil {
			return sdb.setStateVersioned(addr, key, value)
		}
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	set, err := stateObject.SetState(key, value, force)
	if err != nil {
		return err
	}
	if set {
		// Always records the write, even value==origin: skipping it once broke revert semantics on a nested-call revert.
		sdb.recordWriteStorage(addr, key, value)
	}
	return nil
}

func (sdb *IntraBlockState) setStateVersioned(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	prev, source, _, commited, err := readStateForSet(sdb, addr, key)
	if err != nil {
		return err
	}
	// Mirrors stateObject.SetState: no versioned write yet means this is the first write, so commited must be true for revert to delete the cell.
	if source != WriteSetRead && source != UnknownSource && source != StorageRead {
		commited = true
	}
	if source != UnknownSource && prev == value {
		return nil
	}
	sdb.journal.storageChange(addr, key, prev, commited)
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnStorageChange != nil {
		sdb.tracingHooks.OnStorageChange(addr, key, prev, value)
	}
	if so, ok := sdb.stateObjects[addr]; ok {
		so.setState(key, value)
	}
	sdb.recordWriteStorage(addr, key, value)
	return nil
}

// SetStorage replaces all storage for the account; debugging use only.
func (sdb *IntraBlockState) SetStorage(addr accounts.Address, storage Storage) error {
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.SetStorage(storage)
	}
	return nil
}

func (sdb *IntraBlockState) SetIncarnation(addr accounts.Address, incarnation uint64) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetIncarnation %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, incarnation)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.setIncarnation(incarnation)
		sdb.recordWriteIncarnation(addr, stateObject.data.Incarnation)
	}
	return nil
}

func (sdb *IntraBlockState) GetIncarnation(addr accounts.Address) (uint64, error) {
	if sdb.versionMap == nil {
		stateObject, err := sdb.getStateObject(addr, true)
		if err != nil {
			return 0, err
		}
		if stateObject != nil {
			return stateObject.data.Incarnation, nil
		}
		return 0, nil
	}

	incarnation, _, _, err := readIncarnation(sdb, addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetIncarnation %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, incarnation)
	}

	return incarnation, err
}

// Selfdestruct marks the account suicided; preserveBalance leaves the balance untouched (EIP-8246) instead of burning it.
func (sdb *IntraBlockState) Selfdestruct(addr accounts.Address, preserveBalance bool) (bool, error) {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SelfDestruct %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
	}
	if sdb.versionMap != nil {
		return sdb.selfdestructVersioned(addr, preserveBalance)
	}
	stateObject, err := sdb.getStateObject(addr, true)
	if err != nil {
		return false, err
	}
	if stateObject == nil || stateObject.deleted {
		return false, nil
	}
	prevBalance := stateObject.Balance()
	sdb.journal.selfdestructChange(addr, stateObject.selfdestructed, prevBalance, !sdb.hasWrite(addr, SelfDestructPath, accounts.NilKey))

	if !preserveBalance && sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		sdb.tracingHooks.OnBalanceChange(addr, prevBalance, zeroBalance, tracing.BalanceDecreaseSelfdestruct)
	}

	stateObject.markSelfdestructed()
	stateObject.createdContract = false

	sdb.recordWriteIncarnation(addr, stateObject.data.Incarnation)
	sdb.recordWriteSelfDestruct(addr, stateObject.selfdestructed)
	if !preserveBalance {
		stateObject.data.Balance.Clear()
		sdb.recordWriteBalance(addr, uint256.Int{})
	}

	// Deliberately skips StoragePath=0 writes for dirty slots: the account stays alive until end-of-tx, so GetState must still see them.

	return true, nil
}

// selfdestructVersioned records self-destruct on the versionMap path without materializing a stateObject.
func (sdb *IntraBlockState) selfdestructVersioned(addr accounts.Address, preserveBalance bool) (bool, error) {
	base, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	// base is nil when absent or destroyed-without-revival; a same-tx repeat SELFDESTRUCT still proceeds since deleted stays false until finalize.
	if base == nil {
		return false, nil
	}

	prev := false
	if vw, ok := sdb.versionedWrites.GetSelfDestruct(addr); ok {
		prev = vw.Val
	}
	prevBalance := base.Balance
	if vw, ok := sdb.versionedWrites.GetBalance(addr); ok {
		prevBalance = vw.Val
	}
	inc := base.Incarnation
	if vw, ok := sdb.versionedWrites.GetIncarnation(addr); ok {
		inc = vw.Val
	}

	// Captures the pre-destruct versioned incarnation write so a revert restores it rather than the cleared value.
	hadIncarnation, prevIncarnation := false, uint64(0)
	if vw, ok := sdb.versionedWrites.GetIncarnation(addr); ok {
		hadIncarnation, prevIncarnation = true, vw.Val
	}
	// Same for the balance write: a revert must restore the pre-destruct write rather than delete the cell.
	hadBalance := false
	var prevBalanceVersioned uint256.Int
	if vw, ok := sdb.versionedWrites.GetBalance(addr); ok {
		hadBalance, prevBalanceVersioned = true, vw.Val
	}
	sdb.journal.selfdestructChangeVersioned(addr, prev, prevBalance,
		!sdb.hasWrite(addr, SelfDestructPath, accounts.NilKey),
		hadIncarnation, prevIncarnation, hadBalance, prevBalanceVersioned)

	if !preserveBalance && sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		sdb.tracingHooks.OnBalanceChange(addr, prevBalance, zeroBalance, tracing.BalanceDecreaseSelfdestruct)
	}

	if so, ok := sdb.stateObjects[addr]; ok {
		so.markSelfdestructed()
		so.createdContract = false
		if !preserveBalance {
			so.data.Balance.Clear()
		}
	}

	sdb.recordWriteSelfDestruct(addr, true)
	if !preserveBalance {
		// Pre-EIP-8246: SELFDESTRUCT burns the balance; keep the pre-destruct incarnation for the storage-delete cascade.
		sdb.recordWriteIncarnation(addr, inc)
		sdb.recordWriteBalance(addr, uint256.Int{})
		return true, nil
	}
	// EIP-8246: balance is preserved, so re-creation bumps incarnation from 0; explicit zero cells caused a phantom collision instead.
	sdb.recordWriteIncarnation(addr, 0)

	return true, nil
}

var zeroBalance uint256.Int

func (sdb *IntraBlockState) IsNewContract(addr accounts.Address) (bool, error) {
	stateObject, err := sdb.getStateObject(addr, true)
	if err != nil {
		return false, err
	}
	if stateObject == nil {
		return false, nil
	}
	if !stateObject.newlyCreated {
		return false, nil
	}
	code, err := sdb.GetCode(addr)
	if err != nil {
		return false, err
	}
	_, delegated := types.ParseDelegation(code)
	return !delegated, nil
}

func (sdb *IntraBlockState) SetTransientState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	prev := sdb.GetTransientState(addr, key)
	if prev == value {
		return
	}

	sdb.journal.transientStorageChange(addr, key, prev)

	sdb.setTransientState(addr, key, value)
}

func (sdb *IntraBlockState) setTransientState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	sdb.transientStorage.Set(addr, key, value)
}

func (sdb *IntraBlockState) GetTransientState(addr accounts.Address, key accounts.StorageKey) uint256.Int {
	return sdb.transientStorage.Get(addr, key)
}

func (sdb *IntraBlockState) stateObjectForAccount(addr accounts.Address, account *accounts.Account) *stateObject {
	obj := newObject(sdb, addr, account, account)
	if sdb.noMaterialize {
		sdb.reconstructCellFlags(obj, addr)
		return obj
	}
	sdb.setStateObject(addr, obj)
	return obj
}

func (sdb *IntraBlockState) getStateObject(addr accounts.Address, recordRead bool) (*stateObject, error) {
	// A cached object skips re-reading the versionMap: materializing flows keep so.data in step with every write.
	if so, ok := sdb.stateObjects[addr]; ok {
		return so, nil
	}

	if _, ok := sdb.nilAccounts[addr]; ok {
		if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred && sdb.versionMap == nil {
			return sdb.createObject(addr, nil), nil
		}
		return nil, nil
	}

	account, _, _, err := sdb.getVersionedAccount(addr, false)
	if err != nil {
		return nil, err
	}

	if account != nil {
		return sdb.stateObjectForAccount(addr, account), nil
	}

	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
		sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	readAccount, err := sdb.stateReader.ReadAccountData(addr)
	if dbg.KVReadLevelledMetrics {
		sdb.accountReadDuration += time.Since(readStart)
		sdb.accountReadCount++
	}
	sdb.stateReader.SetTrace(false, "")

	accountSource := StorageRead
	// A DB-loaded record is pre-block state; UnknownVersion keeps every in-block cell overlay ahead of it.
	accountVersion := UnknownVersion

	if err != nil {
		return nil, err
	}

	if readAccount == nil {
		if sdb.versionMap != nil {
			readAccount, accountSource, accountVersion, err = refreshAccount(sdb, addr)

			if readAccount == nil || err != nil {
				if err == nil {
					if synth, ok := sdb.synthesizeCreatedAccountBase(addr); ok {
						sdb.accountRead(addr, synth, MapRead, UnknownVersion)
						readAccount = synth
						accountSource = StorageRead
						accountVersion = UnknownVersion
					}
				}
				if readAccount == nil {
					sdb.finalizeProvisionalAddressRead(addr)
					return nil, err
				}
			} else {
				// The synthesized path skips this: it already bails on a destructed floor.
				destructed, _, _, err := refreshSelfDestruct(sdb, addr)
				if destructed || err != nil {
					sdb.finalizeProvisionalAddressRead(addr)
					if !sdb.noMaterialize {
						so := sdb.allocStateObject()
						so.db = sdb
						so.address = addr
						so.selfdestructed = destructed
						so.deleted = destructed
						sdb.setStateObject(addr, so)
					}
					return nil, err
				}
			}
		} else {
			sdb.nilAccounts[addr] = struct{}{}
			if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred {
				return sdb.createObject(addr, nil), nil
			}
			return nil, nil
		}
	}

	var code refreshedCode
	var codeSource ReadSource

	if sdb.versionMap != nil {
		account = readAccount

		// A prior tx's SelfDestructPath early-exits versionedReadCore above; read it directly here instead.
		if sdVer, ok := sdb.versionMap.FindDoneSelfDestructInRange(addr, 0, sdb.txIndex, true); ok && !sdb.versionMap.selfDestructRevived(addr, sdVer.TxIndex, sdb.txIndex) {
			// Revival needs cells written after the destruct index; skip if this tx already resurrected it.
			localResurrected := false
			if sdVal, ok := sdb.versionedWriteSelfDestruct(addr); ok {
				if !sdVal {
					localResurrected = true
				}
			}
			if !localResurrected {
				if !sdb.noMaterialize {
					so := sdb.allocStateObject()
					so.db = sdb
					so.address = addr
					so.selfdestructed = true
					so.deleted = true
					sdb.setStateObject(addr, so)
				}
				return nil, nil
			}
		}

		code, codeSource, _, err = refreshCode(sdb, addr)
		if err != nil {
			return nil, err
		}
	} else {
		account = readAccount
	}

	// recordRead=false still must reconcile on the versioned path, since the map-miss above already recorded a nil marker.
	if recordRead || sdb.versionMap != nil {
		sdb.accountRead(addr, account, accountSource, accountVersion)
	}
	obj := newObject(sdb, addr, account, account)
	if code.Bytes != nil {
		// The account record can lag a prior tx's code write, so the resolved hash wins over SetCode's revert-to-original check.
		codeHash := code.codeHash(codeSource, obj.data.CodeHash)
		obj.code = accounts.Code{Hash: codeHash, Bytes: code.Bytes}
		if codeHash != obj.data.CodeHash {
			obj.data.CodeHash = codeHash
			obj.original.CodeHash = codeHash
		}
	}
	if sdb.noMaterialize {
		sdb.reconstructCellFlags(obj, addr)
		return obj, nil
	}
	sdb.setStateObject(addr, obj)
	return obj, nil
}

func (sdb *IntraBlockState) setStateObject(addr accounts.Address, object *stateObject) {
	if dbg.AssertEnabled && object.arena {
		// stateObjects lives for the block, an arena slot only for the transaction.
		panic(fmt.Sprintf("arena slot cached in stateObjects: %x", addr))
	}
	if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred && sdb.versionMap == nil {
		object.data.Balance = u256.Add(object.data.Balance, bi.increase)
		bi.transferred = true
		sdb.journal.balanceIncreaseTransfer(bi)
	}
	sdb.stateObjects[addr] = object
}

func (sdb *IntraBlockState) GetOrNewStateObject(addr accounts.Address) (*stateObject, error) {
	stateObject, err := sdb.getStateObject(addr, true)
	if err != nil {
		return nil, err
	}
	if stateObject == nil || stateObject.deleted {
		stateObject = sdb.createObject(addr, stateObject)
	}
	return stateObject, nil
}

// createObject creates a new state object, overwriting any existing one at addr.
func (sdb *IntraBlockState) createObject(addr accounts.Address, previous *stateObject) (newobj *stateObject) {
	account := &accounts.Account{}
	var original *accounts.Account
	if previous == nil {
		original = &accounts.Account{}
	} else {
		original = &previous.original
	}

	account.Root.SetBytes(trie.EmptyRoot[:]) // old storage should be ignored
	newobj = newObject(sdb, addr, account, original)
	newobj.setNonce(0)
	if previous == nil {
		sdb.journal.createObjectChange(addr)
	} else {
		var prevWrites *createWriteSnapshot
		if sdb.versionMap != nil {
			prevWrites = sdb.versionedWrites.snapshotCreateFields(addr)
		}
		sdb.journal.resetObjectChange(addr, previous, prevWrites)
	}
	newobj.newlyCreated = true
	if !sdb.noMaterialize {
		sdb.setStateObject(addr, newobj)
	}
	data := newobj.data
	sdb.recordWriteAddress(addr, &data)
	// Writes CodeHashPath so a stale versionedReads cache entry is invalidated, recording keccak256("") for a fresh account.
	sdb.recordWriteCodeHash(addr, newobj.data.CodeHash)
	return newobj
}

// CreateAccount creates a state object, carrying over any existing balance so a pre-funded address doesn't lose ether.
func (sdb *IntraBlockState) CreateAccount(addr accounts.Address, contractCreation bool) (err error) {
	var prevInc uint64
	var previous *stateObject

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			var creatingContract string
			if contractCreation {
				creatingContract = " (contract)"
			}
			if err != nil {
				fmt.Printf("%d (%d.%d) Create Account%s: %x, err=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, creatingContract, addr, err)
			} else {
				var bal uint256.Int
				if previous != nil {
					bal = previous.data.Balance
				}
				fmt.Printf("%d (%d.%d) Create Account%s: %x, balance=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, creatingContract, addr, bal.String())
			}
		}()
	}

	if sdb.versionMap == nil {
		previous, err = sdb.getStateObject(addr, true)
		if err != nil {
			return err
		}
	} else {
		readAccount, _, _, err := sdb.getVersionedAccount(addr, true)

		if err != nil {
			return err
		}

		if readAccount != nil {
			account := readAccount

			// Derives destructed without recording a SelfDestructPath read, since that would race a CREATE2 re-creation.
			destructed := false
			if sd, ok := sdb.versionedWriteSelfDestruct(addr); ok {
				destructed = sd
			} else if d, res, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone && d {
				destructed = true
			}

			// Reuses the cached stateObject as previous so selfdestructed/incarnation history survives a later revert.
			if !destructed {
				if so, ok := sdb.stateObjects[addr]; ok && so.selfdestructed {
					previous = so
				}
			}

			// A later tx's non-empty transfer revives a prior self-destruct (EIP-161 emptiness is the authoritative test).
			if destructed && sdb.versionMap != nil && !account.Empty() {
				destructed = false
			}

			if previous == nil {
				previous = newObject(sdb, addr, account, account)
				previous.selfdestructed = destructed
			}
		} else if so, ok := sdb.stateObjects[addr]; ok && so.deleted {
			previous = so
		} else if so, ok := sdb.stateObjects[addr]; ok {
			previous = so
		} else if sd, ok := sdb.versionedWriteSelfDestruct(addr); ok && sd {
			// Cache-free path: rebuilds previous from this tx's own cells since a create-then-destroy leaves no base record or cached object.
			prev := newObject(sdb, addr, &accounts.Account{}, &accounts.Account{})
			prev.selfdestructed = true
			if vw, ok := sdb.versionedWrites.GetIncarnation(addr); ok {
				prev.data.Incarnation = vw.Val
			}
			previous = prev
		}
	}

	if err != nil {
		return err
	}
	if previous != nil && previous.selfdestructed {
		prevInc = previous.data.Incarnation
	} else {
		prevInc = 0
	}
	if previous != nil && prevInc < previous.data.PrevIncarnation {
		prevInc = previous.data.PrevIncarnation
	}
	// Captures each path's own (source, version) for the synthetic reads below, since inheriting the account-record version would trip the validator.
	incSource, incVersion := StorageRead, UnknownVersion
	if sdb.versionMap != nil {
		if inc, res, ok := sdb.versionMap.ReadIncarnation(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone {
			incSource = MapRead
			incVersion = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
			if inc > prevInc {
				prevInc = inc
			}
		}
	}
	balSource, balVersion := StorageRead, UnknownVersion
	if sdb.versionMap != nil {
		if _, res, ok := sdb.versionMap.ReadBalance(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone {
			balSource = MapRead
			balVersion = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
		}
	}
	if sdb.versionMap == nil && previous == nil {
		type deletedIncReader interface {
			ReadDeletedIncarnation(accounts.Address) (uint64, bool)
		}
		if r, ok := sdb.stateReader.(deletedIncReader); ok {
			if inc, ok2 := r.ReadDeletedIncarnation(addr); ok2 && inc > prevInc {
				prevInc = inc
			}
		}
	}

	var carryBalance uint256.Int
	carryBalanceValid := previous != nil && !previous.selfdestructed
	if carryBalanceValid {
		b, _, err := sdb.getBalance(addr)
		if err != nil {
			return err
		}
		carryBalance = b
	}
	newObj := sdb.createObject(addr, previous)
	if previous != nil && previous.selfdestructed {
		// The reset-object journal entry's dirty mark drops on revert; this un-journalled increment keeps it dirty, confined to CreateAccount.
		sdb.journal.dirty(addr)
	}
	if carryBalanceValid {
		newObj.data.Balance.Set(&carryBalance)
	}
	newObj.data.PrevIncarnation = prevInc

	if contractCreation {
		newObj.createdContract = true
		newObj.data.Incarnation = prevInc + 1
		// Records contract creation so Normalize knows this address was created, preventing empty-account deletion.
		sdb.recordWriteCreateContract(addr, true)
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) New Incarnation %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, newObj.data.Incarnation)
		}
	} else {
		newObj.selfdestructed = false
	}

	// Only the first creation's BalancePath read is kept as the BAL baseline; a same-tx re-creation must not overwrite it.
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap != nil {
		if vr, seen := sdb.versionedReads.GetBalance(addr); !seen {
			sdb.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader{Source: balSource, Version: balVersion}, newObj.Balance()})
		} else if vr.internal {
			vr.internal = false
			sdb.versionedReads.SetBalance(addr, vr)
		}
		sdb.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{ReadHeader{Source: incSource, Version: incVersion}, prevInc})
	}
	sdb.recordWriteBalance(addr, newObj.Balance())
	sdb.recordWriteIncarnation(addr, newObj.data.Incarnation)
	if previous == nil || previous.selfdestructed && !newObj.selfdestructed {
		sdb.recordWriteSelfDestruct(addr, false)
	}

	return nil
}

func (sdb *IntraBlockState) PushSnapshot() int {
	return sdb.revisions.snapshot(sdb.journal)
}

func (sdb *IntraBlockState) PopSnapshot(snapshot int) {
	sdb.revisions.returnSnapshot(snapshot)
}

func (sdb *IntraBlockState) RevertToSnapshot(revid int, err error) {
	var traced bool
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TracingAccounts()) {
		for addr := range sdb.journal.dirties {
			if sdb.trace || dbg.TraceAccount(addr.Handle()) {
				traced = true
				if err == nil {
					fmt.Printf("%d (%d.%d) Reverting %x, revid: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, revid)
				} else {
					fmt.Printf("%d (%d.%d) Reverting %x, revid: %d: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, revid, err)
				}
			}
		}
	}

	snapshot := sdb.revisions.revertToSnapshot(revid)
	sdb.journal.revert(sdb, snapshot)

	if traced {
		fmt.Printf("%d (%d.%d) Reverted: %d:%d\n", sdb.blockNum, sdb.txIndex, sdb.version, revid, snapshot)
	}
}

func (sdb *IntraBlockState) GetRefund() uint64 {
	return sdb.refund
}

// EIP161EmptyRemoval reports EIP-161 removal, except AuRa keeps its SystemAddress even when empty.
func EIP161EmptyRemoval(eip161Enabled, isAura bool, addr accounts.Address) bool {
	return eip161Enabled && (!isAura || addr != params.SystemAddress)
}

func updateAccount(eip161Enabled bool, isAura bool, stateWriter StateWriter, addr accounts.Address, stateObject *stateObject, isDirty bool, trace bool, tracingHooks *tracing.Hooks, useBlockOrigin bool, eip8246 bool) error {
	emptyRemoval := EIP161EmptyRemoval(eip161Enabled, isAura, addr) && stateObject.data.Empty()
	// EIP-8246: a self-destructed account still holding a balance resets to a balance-only account, not deleted.
	sdPreserveBalance := eip8246 && stateObject.selfdestructed && !stateObject.data.Balance.IsZero()
	if (stateObject.selfdestructed && !sdPreserveBalance) || (isDirty && emptyRemoval) {
		balance := stateObject.Balance()
		if tracingHooks != nil && tracingHooks.OnBalanceChange != nil && !(&balance).IsZero() && stateObject.selfdestructed {
			tracingHooks.OnBalanceChange(stateObject.address, balance, uint256.Int{}, tracing.BalanceDecreaseSelfdestructBurn)
		}
		if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (trace || dbg.TraceAccount(addr.Handle()))) {
			if _, ok := stateWriter.(*NoopWriter); !ok || dbg.TraceNoopIO {
				fmt.Printf("%d (%d.%d) Delete Account: %x selfdestructed=%v stack=%s\n", stateObject.db.blockNum, stateObject.db.txIndex, stateObject.db.version, addr, stateObject.selfdestructed, dbg.Stack())
			}
		}
		if err := stateWriter.DeleteAccount(addr, &stateObject.original); err != nil {
			return err
		}
		stateObject.deleted = true
	}
	if sdPreserveBalance {
		stateObject.data.Nonce = 0
		stateObject.data.CodeHash = accounts.EmptyCodeHash
		stateObject.data.Incarnation = 0
		stateObject.code = accounts.Code{}
		stateObject.deleted = false
		// Supersedes the pre-destruct IncarnationPath so a later CREATE2 sees this record's 0 explicitly.
		stateObject.db.recordWriteIncarnation(addr, 0)
		if err := stateWriter.CreateContract(addr); err != nil {
			return err
		}
		if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
			return err
		}
	} else if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		stateObject.deleted = false
		// dirtyCode is set only when code actually changed; a clear-to-empty must still write through for a consistent CodeDomain.
		if stateObject.dirtyCode {
			if err := stateWriter.UpdateAccountCode(addr, stateObject.data.Incarnation, stateObject.data.CodeHash, stateObject.code.Bytes); err != nil {
				return err
			}
		}
		if stateObject.createdContract {
			if err := stateWriter.CreateContract(addr); err != nil {
				return err
			}
		}
		if err := stateObject.updateStorage(stateWriter, useBlockOrigin); err != nil {
			return err
		}
		if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (trace || dbg.TraceAccount(addr.Handle()))) {
			if _, ok := stateWriter.(*NoopWriter); !ok || dbg.TraceNoopIO {
				fmt.Printf("%d (%d.%d) Update Account Data (%T): %x balance:%d,nonce:%d,codehash:%x\n",
					stateObject.db.blockNum, stateObject.db.txIndex, stateObject.db.version, stateWriter, addr, &stateObject.data.Balance, stateObject.data.Nonce, stateObject.data.CodeHash)
			}
		}
		if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
			return err
		}
		// In parallel mode, untouched fields aren't in the versionMap's WriteSet; Normalize fills those in from the stateReader.
	}
	return nil
}

func printAccount(eip161Enabled bool, isAura bool, addr accounts.Address, stateObject *stateObject, isDirty bool) {
	emptyRemoval := EIP161EmptyRemoval(eip161Enabled, isAura, addr) && stateObject.data.Empty()
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
		fmt.Printf("delete: %x\n", addr)
	}
	if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		if stateObject.code.Bytes != nil && stateObject.dirtyCode {
			fmt.Printf("UpdateCode: %x,%x\n", addr, stateObject.data.CodeHash)
		}
		if stateObject.createdContract {
			fmt.Printf("CreateContract: %x\n", addr)
		}
		stateObject.printTrie()
		fmt.Printf("UpdateAccountData: %x, balance=%s, nonce=%d\n", addr, stateObject.data.Balance.String(), stateObject.data.Nonce)
	}
}

// FinalizeTx should be called after every transaction.
func (sdb *IntraBlockState) FinalizeTx(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr, bi := range sdb.balanceInc {
		if !bi.transferred {
			sdb.getStateObject(addr, true)
		}
	}
	for addr := range sdb.journal.dirties {
		so, exist := sdb.stateObjects[addr]
		if !exist {
			// ripeMD is a special case: an out-of-gas touch still journals it and survives a revert, so it can lack a stateObject — safe to skip.
			continue
		}

		if err := updateAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, stateWriter, addr, so, true, sdb.trace, sdb.tracingHooks, false, chainRules.IsAmsterdam); err != nil {
			return err
		}

		// EIP-6780+EIP-7928: a same-tx contract's SELFDESTRUCT wipes storage; zero the writes so the BAL folds them away via net-zero.
		if sdb.versionMap != nil && so.selfdestructed && so.newlyCreated {
			for key := range so.dirtyStorage {
				sdb.recordWriteStorage(addr, key, uint256.Int{})
			}
		}

		// EIP-8246 leaves a balance-preserving SELFDESTRUCT alive; replace it with a clean balance-only object since the assembler reuses one IBS across txs.
		if so.selfdestructed && !so.deleted {
			preserved := accounts.NewAccount()
			preserved.Balance = so.data.Balance
			sdb.stateObjects[addr] = newObject(sdb, addr, &preserved, &preserved)
		}

		so.newlyCreated = false
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

func (sdb *IntraBlockState) SoftFinalise() {
	for addr := range sdb.journal.dirties {
		// Parallel path: dirtiness must come from the journal, not stateObject existence, or MakeWriteSet drops writes.
		if _, exist := sdb.stateObjects[addr]; !exist && sdb.versionMap == nil {
			continue
		}
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	sdb.clearJournalAndRefund()
}

func (sdb *IntraBlockState) CommitBlock(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr, bi := range sdb.balanceInc {
		if !bi.transferred {
			sdb.getStateObject(addr, true)
		}
	}
	return sdb.MakeWriteSet(chainRules, stateWriter)
}

// ExtractAndClearDirty snapshots and clears stateObjectsDirty, so eth_simulateV1 can skip EIP-161 on override-only accounts.
func (sdb *IntraBlockState) ExtractAndClearDirty() map[accounts.Address]struct{} {
	dirty := maps.Clone(sdb.stateObjectsDirty)
	clear(sdb.stateObjectsDirty)
	return dirty
}

// CommitOverrideDirtyAccounts writes override accounts CommitBlock didn't reach; EIP-161 is disabled so they aren't dropped for looking empty.
func (sdb *IntraBlockState) CommitOverrideDirtyAccounts(chainRules *chain.Rules, stateWriter StateWriter, overrideDirty map[accounts.Address]struct{}) error {
	for addr := range overrideDirty {
		if _, alsoTxDirty := sdb.stateObjectsDirty[addr]; alsoTxDirty {
			continue // CommitBlock already handled this address
		}
		so, exists := sdb.stateObjects[addr]
		if !exists || so.deleted {
			continue
		}
		if err := updateAccount(false, chainRules.IsAura, stateWriter, addr, so, true, sdb.trace, sdb.tracingHooks, true, chainRules.IsAmsterdam); err != nil {
			return err
		}
	}
	return nil
}

func (sdb *IntraBlockState) BalanceIncreaseSet() map[accounts.Address]uint256.Int {
	s := make(map[accounts.Address]uint256.Int, len(sdb.balanceInc))
	for addr, bi := range sdb.balanceInc {
		if !bi.transferred {
			s[addr] = bi.increase
		}
	}
	return s
}

func (sdb *IntraBlockState) MakeWriteSet(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr := range sdb.journal.dirties {
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		if dbg.TraceAccount(addr.Handle()) {
			var updated *uint256.Int
			if w, ok := sdb.versionedWrites.GetBalance(addr); ok {
				val := w.Val
				updated = &val
			}
			var dirty string
			if isDirty {
				dirty = " (dirty)"
			}
			if updated != nil {
				fmt.Printf("%d (%d.%d) Updated Balance: %x%s: %s (%d)\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, dirty, stateObject.data.Balance.String(), updated)
			} else {
				fmt.Printf("%d (%d.%d) Updated Balance: %x%s: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, dirty, stateObject.data.Balance.String())
			}
		}
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) Update Account %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
		}
		if err := updateAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, stateWriter, addr, stateObject, isDirty, sdb.trace, sdb.tracingHooks, true, chainRules.IsAmsterdam); err != nil {
			return err
		}
		// EIP-6780/7928: a same-tx contract's SELFDESTRUCT wipes storage; zero the writes here too so AsBlockAccessList nets them away.
		if sdb.versionMap != nil && stateObject.selfdestructed && stateObject.newlyCreated {
			for key := range stateObject.dirtyStorage {
				sdb.recordWriteStorage(addr, key, uint256.Int{})
			}
		}
	}

	var reverted []accounts.Address

	sdb.versionedWrites.forEachAddr(func(addr accounts.Address) {
		if _, isDirty := sdb.stateObjectsDirty[addr]; !isDirty {
			reverted = append(reverted, addr)
		}
	})

	for _, addr := range reverted {
		sdb.versionMap.DeleteAll(addr, sdb.txIndex)
		sdb.versionedWrites.deleteAddr(addr)
	}

	sdb.clearJournalAndRefund()
	return nil
}

// FinalizedWrites applies EIP-6780 normalization and EIP-161 filtering, returning a detached committable snapshot.
func (sdb *IntraBlockState) FinalizedWrites(chainRules *chain.Rules) *WriteSet {
	writes := sdb.versionedWrites.Finalize()
	sdb.withholdCreatedEmptyAccounts(chainRules, writes)
	return writes
}

// withholdCreatedEmptyAccounts drops writes for an account absent both before and after the tx (EIP-161).
func (sdb *IntraBlockState) withholdCreatedEmptyAccounts(chainRules *chain.Rules, writes *WriteSet) {
	if sdb.blockNum == 0 || chainRules == nil {
		return
	}
	for addr := range writes.address {
		if !EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr) {
			continue
		}
		read, ok := sdb.versionedReads.GetAddress(addr)
		if !ok || (read.Val != nil && !read.Val.IsNil()) || !writes.createdEmpty(addr) {
			continue
		}
		writes.deleteAddr(addr)
	}
}

func (sdb *IntraBlockState) MergeTxIOInto(io *VersionedIO, writes *WriteSet) {
	version := Version{BlockNum: sdb.blockNum, TxIndex: sdb.txIndex, Incarnation: sdb.version}
	io.mergeTx(version, sdb.versionedReads, writes)
}

func (sdb *IntraBlockState) FlushWritesToVersionMap(writes *WriteSet) {
	if sdb.versionMap == nil {
		return
	}
	sdb.versionMap.FlushVersionedWrites(writes, true, "")
}

func (sdb *IntraBlockState) Print(chainRules chain.Rules, all bool) {
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		_, isDirty2 := sdb.journal.dirties[addr]

		printAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr, stateObject, all || isDirty || isDirty2)
	}
}

// SetTxContext sets the tx index used for new state logs; call before transaction execution.
func (sdb *IntraBlockState) SetTxContext(bn uint64, ti int) {
	sdb.txIndex = ti
	sdb.blockNum = bn
	sdb.sdProbeEpoch++
}

func (sdb *IntraBlockState) clearJournalAndRefund() {
	sdb.journal.Reset()
	sdb.revisions.reset()
	sdb.refund = uint64(0)
	if dbg.AssertEnabled && !sdb.noMaterialize && !sdb.stateObjectArena.empty() {
		// Slots are rewound per transaction, so only the path that caches nothing may draw them.
		panic("stateObjectArena not empty with noMaterialize=false")
	}
	sdb.stateObjectArena.reset()
}

// Prepare runs the preparatory access-list/transient-storage steps (EIP-2929/2930/3651/1153); must be invoked before a state transition.
func (sdb *IntraBlockState) Prepare(rules *chain.Rules, sender, coinbase accounts.Address, dst accounts.Address,
	precompiles []accounts.Address, list types.AccessList) {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(sender.Handle()) || !dst.IsNil() && dbg.TraceAccount(dst.Handle())) {
		fmt.Printf("%d (%d.%d) ibs.Prepare: sender: %x, coinbase: %x, dest: %x, %x, %v, %v\n", sdb.blockNum, sdb.txIndex, sdb.version, sender, coinbase, dst, precompiles, list, rules)
	}
	sdb.eip8246 = rules.IsAmsterdam
	sdb.eip161 = rules.IsEIP161Enabled()
	sdb.isAura = rules.IsAura
	if rules.IsBerlin {
		sdb.accessList.Reset()
		al := &sdb.accessList

		al.AddAddress(sender)
		if !dst.IsNil() {
			al.AddAddress(dst)
		}
		for _, addr := range precompiles {
			al.AddAddress(addr)
		}
		for _, el := range list {
			address := accounts.InternAddress(el.Address)
			al.AddAddress(address)
			for _, key := range el.StorageKeys {
				al.AddSlot(address, accounts.InternKey(key))
			}
		}
		if rules.IsShanghai {
			al.AddAddress(coinbase)
		}
	}
	clear(sdb.transientStorage)
	sdb.versionedReads.access = nil
	sdb.recordAccess = true

	// EIP-7928 records the EIP-3651 coinbase access even without a priority fee.
	if rules.IsShanghai {
		sdb.MarkAddressAccess(coinbase, false)
	}
}

func (sdb *IntraBlockState) AddAddressToAccessList(addr accounts.Address) (addrMod bool) {
	addrMod = sdb.accessList.AddAddress(addr)
	if addrMod {
		sdb.journal.accessListAddAccountChange(addr)
	}
	return addrMod
}

func (sdb *IntraBlockState) AddSlotToAccessList(addr accounts.Address, slot accounts.StorageKey) (addrMod, slotMod bool) {
	addrMod, slotMod = sdb.accessList.AddSlot(addr, slot)
	if addrMod {
		// Defensive: entering 'address' scope should already imply it's access-listed.
		sdb.journal.accessListAddAccountChange(addr)
	}
	if slotMod {
		sdb.journal.accessListAddSlotChange(addr, slot)
	}
	return addrMod, slotMod
}

func (sdb *IntraBlockState) AddressInAccessList(addr accounts.Address) bool {
	return sdb.accessList.ContainsAddress(addr)
}

func (sdb *IntraBlockState) SlotInAccessList(addr accounts.Address, slot accounts.StorageKey) (addressPresent bool, slotPresent bool) {
	return sdb.accessList.Contains(addr, slot)
}

// SlotKnownWarm is conservative: true means warm, false means unknown, so the caller must fall back to AddSlotToAccessList.
func (sdb *IntraBlockState) SlotKnownWarm(addr accounts.Address, slot accounts.StorageKey) bool {
	return sdb.accessList.lastSlots != nil && sdb.accessList.lastAddr == addr && slot == sdb.accessList.lastWarmSlot
}

func (sdb *IntraBlockState) MarkAddressAccess(addr accounts.Address, revertable bool) {
	if !sdb.recordAccess {
		return
	}
	if sdb.versionedReads.access == nil {
		sdb.versionedReads.access = make(AccessSet)
	}
	if opts, ok := sdb.versionedReads.access[addr]; ok {
		if opts.revertable && !revertable {
			opts.revertable = false
			sdb.versionedReads.access[addr] = opts
		}
	} else {
		sdb.versionedReads.access[addr] = accessOptions{revertable: revertable}
	}
}

// StartAccessRecording enables versioned access tracking until ResetVersionedIO, so a touched-but-absent address still reaches the BAL.
func (sdb *IntraBlockState) StartAccessRecording() {
	sdb.recordAccess = true
}

// MarkReadsInternal marks addr's reads as internal: kept for conflict detection but excluded from the BAL.
func (sdb *IntraBlockState) MarkReadsInternal(addr accounts.Address) {
	sdb.versionedReads.ScanAddr(addr, func(_ AccountPath, _ accounts.StorageKey, hdr *ReadHeader) {
		hdr.internal = true
	})
}

func (sdb *IntraBlockState) AccessedAddr(addr accounts.Address) bool {
	_, ok := sdb.versionedReads.access[addr]
	return ok
}

func (sdb *IntraBlockState) accountRead(addr accounts.Address, account *accounts.Account, source ReadSource, version Version) {
	if sdb.versionMap != nil {
		sdb.MarkAddressAccess(addr, true)
		if source == WriteSetRead {
			// A read from this tx's own earlier write has no cross-tx dependency; recording it would wrongly invalidate the tx.
			return
		}
		if source == ReadSetRead {
			// Served from the read set: already recorded with its real source; re-recording would launder it past validation.
			return
		}
		data := *account
		// Demotes a sub-field MapRead promotion when AddressPath itself has no cell, or the validator non-converges.
		if source == MapRead {
			if _, res, ok := sdb.versionMap.ReadAddress(addr, sdb.txIndex); !ok || res.Status() != MVReadResultDone {
				source = StorageRead
				version = UnknownVersion
			}
		}
		sdb.versionedReads.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: source, Version: version},
			Val:        NewAccountView(&data),
		})
	}
}

// recordWrite* helpers are monomorphic per AccountPath: a repeat write reuses the existing *VersionedWrite[T] with no alloc.

func (sdb *IntraBlockState) recordWriteBalance(addr accounts.Address, val uint256.Int) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetBalance(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWBalance()
	vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetBalance(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteNonce(addr accounts.Address, val uint64, reason tracing.NonceChangeReason) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetNonce(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		vw.NonceReason = reason
		traceWrite(sdb, vw)
		return
	}
	vw := getVWNonce()
	vw.WriteHeader = WriteHeader{Address: addr, Path: NoncePath, Version: sdb.Version(), NonceReason: reason}
	vw.Val = val
	sdb.versionedWrites.SetNonce(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteIncarnation(addr accounts.Address, val uint64) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetIncarnation(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWIncarnation()
	vw.WriteHeader = WriteHeader{Address: addr, Path: IncarnationPath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetIncarnation(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteSelfDestruct(addr accounts.Address, val bool) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetSelfDestruct(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWSelfDestruct()
	vw.WriteHeader = WriteHeader{Address: addr, Path: SelfDestructPath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetSelfDestruct(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteCreateContract(addr accounts.Address, val bool) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetCreateContract(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWCreateContract()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CreateContractPath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetCreateContract(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteCode(addr accounts.Address, val accounts.Code) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetCode(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWCode()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodePath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetCode(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteCodeHash(addr accounts.Address, val accounts.CodeHash) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetCodeHash(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWCodeHash()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodeHashPath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetCodeHash(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteCodeSize(addr accounts.Address, val int) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetCodeSize(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWCodeSize()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodeSizePath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetCodeSize(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteAddress(addr accounts.Address, val *accounts.Account) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetAddress(addr); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWAddress()
	vw.WriteHeader = WriteHeader{Address: addr, Path: AddressPath, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetAddress(addr, vw)
	traceWrite(sdb, vw)
}

func (sdb *IntraBlockState) recordWriteStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap == nil {
		return
	}
	if vw, ok := sdb.versionedWrites.GetStorage(addr, key); ok {
		vw.Version = sdb.Version()
		vw.Val = val
		traceWrite(sdb, vw)
		return
	}
	vw := getVWStorage()
	vw.WriteHeader = WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: sdb.Version()}
	vw.Val = val
	sdb.versionedWrites.SetStorage(addr, key, vw)
	traceWrite(sdb, vw)
}

func traceWrite[T any](sdb *IntraBlockState, vw *VersionedWrite[T]) {
	if !dbg.TraceTransactionIO {
		return
	}
	hdr := vw.WriteHeader
	if !(sdb.trace || (dbg.TraceAccount(hdr.Address.Handle()) && (hdr.Key == accounts.NilKey || traceKey(hdr.Key)))) {
		return
	}
	fmt.Printf("%d (%d.%d) WRT %x %s: %v (%d.%d)\n", sdb.blockNum, sdb.txIndex, sdb.version,
		hdr.Address, AccountKey{Path: hdr.Path, Key: hdr.Key}, vw.Val, hdr.Version.TxIndex, hdr.Version.Incarnation)
}

// accountLifecycle layers this tx's own SelfDestruct write over the versionMap floor to give the self-destruct verdict.
func (sdb *IntraBlockState) accountLifecycle(addr accounts.Address) (destroyed bool) {
	if own, ok := sdb.versionedWriteSelfDestruct(addr); ok {
		return own
	}
	d, _, revived := sdb.versionMap.AccountLifecycle(addr, sdb.txIndex)
	return d && !revived
}

func (sdb *IntraBlockState) versionedWriteSelfDestruct(addr accounts.Address) (bool, bool) {
	if sdb.versionMap == nil {
		return false, false
	}
	vw, ok := sdb.versionedWrites.GetSelfDestruct(addr)
	if !ok {
		return false, false
	}
	if _, isDirty := sdb.journal.dirties[addr]; !isDirty {
		return false, false
	}
	return vw.Val, true
}

// versionedWriteCreateContract reports whether this tx's own writes created a contract at addr, guarded by journal.dirties.
func (sdb *IntraBlockState) versionedWriteCreateContract(addr accounts.Address) (bool, bool) {
	if sdb.versionMap == nil {
		return false, false
	}
	if _, isDirty := sdb.journal.dirties[addr]; !isDirty {
		return false, false
	}
	vw, ok := sdb.versionedWrites.GetCreateContract(addr)
	if !ok {
		return false, false
	}
	return vw.Val, true
}

// reconstructCellFlags stamps a transient stateObject's create/self-destruct flags from this tx's own write cells.
func (sdb *IntraBlockState) reconstructCellFlags(obj *stateObject, addr accounts.Address) {
	if obj == nil {
		return
	}
	if cc, ok := sdb.versionedWriteCreateContract(addr); ok && cc {
		obj.createdContract = true
		obj.newlyCreated = true
	}
	if sd, ok := sdb.versionedWriteSelfDestruct(addr); ok && sd {
		obj.selfdestructed = true
	}
	// The transient's CodeHash can lag CodePath/CodeHashPath cells; seed code from this tx's own write or the floor cell.
	if _, isDirty := sdb.journal.dirties[addr]; isDirty {
		if vw, ok := sdb.versionedWrites.GetCode(addr); ok {
			obj.code = vw.Val
			obj.data.CodeHash = vw.Val.Hash
			return
		}
	}
	if obj.code.Bytes != nil {
		return
	}
	code, codeSource, _, err := refreshCode(sdb, addr)
	if err != nil || code.Bytes == nil {
		return
	}
	codeHash := code.codeHash(codeSource, obj.data.CodeHash)
	obj.code = accounts.Code{Hash: codeHash, Bytes: code.Bytes}
	obj.data.CodeHash = codeHash
	obj.original.CodeHash = codeHash
}

// versionedWriteHit probes the dirty write set for (addr, path, key) and populates the matching typed pointer field on r.
func (sdb *IntraBlockState) versionedWriteHit(addr accounts.Address, path AccountPath, key accounts.StorageKey, r *readPathResult) bool {
	if sdb.versionMap == nil {
		return false
	}
	if _, isDirty := sdb.journal.dirties[addr]; !isDirty {
		return false
	}
	switch path {
	case AddressPath:
		if vw, ok := sdb.versionedWrites.GetAddress(addr); ok {
			r.vwAddress = vw
			return true
		}
	case BalancePath:
		if vw, ok := sdb.versionedWrites.GetBalance(addr); ok {
			r.vwBalance = vw
			return true
		}
	case NoncePath:
		if vw, ok := sdb.versionedWrites.GetNonce(addr); ok {
			r.vwNonce = vw
			return true
		}
	case IncarnationPath:
		if vw, ok := sdb.versionedWrites.GetIncarnation(addr); ok {
			r.vwIncarnation = vw
			return true
		}
	case SelfDestructPath:
		if vw, ok := sdb.versionedWrites.GetSelfDestruct(addr); ok {
			r.vwSelfDestruct = vw
			return true
		}
	case CreateContractPath:
		if vw, ok := sdb.versionedWrites.GetCreateContract(addr); ok {
			r.vwCreateContract = vw
			return true
		}
	case CodePath:
		if vw, ok := sdb.versionedWrites.GetCode(addr); ok {
			r.vwCode = vw
			return true
		}
	case CodeHashPath:
		if vw, ok := sdb.versionedWrites.GetCodeHash(addr); ok {
			r.vwCodeHash = vw
			return true
		}
	case CodeSizePath:
		if vw, ok := sdb.versionedWrites.GetCodeSize(addr); ok {
			r.vwCodeSize = vw
			return true
		}
	case StoragePath:
		if vw, ok := sdb.versionedWrites.GetStorage(addr, key); ok {
			r.vwStorage = vw
			return true
		}
	}
	return false
}

func (sdb *IntraBlockState) HadInvalidRead() bool {
	return sdb.dep >= 0
}

func (sdb *IntraBlockState) DepTxIndex() int {
	return sdb.dep
}

func (sdb *IntraBlockState) SetVersion(inc int) {
	sdb.version = inc
}

func (sdb *IntraBlockState) Version() Version {
	return Version{
		BlockNum:    sdb.blockNum,
		TxIndex:     sdb.txIndex,
		Incarnation: sdb.version,
	}
}

// VersionedReads returns the in-flight read set, sharing its maps with the IBS until ResetVersionedIO rebinds it.
func (sdb *IntraBlockState) VersionedReads() ReadSet {
	return sdb.versionedReads
}

func (sdb *IntraBlockState) ResetVersionedIO() {
	sdb.versionedReads = ReadSet{}
	sdb.versionedWrites.ReleaseAndReset()
	sdb.dep = UnknownDep
	sdb.recordAccess = false
}

func (sdb *IntraBlockState) ResetVersionedReads() {
	sdb.versionedReads = ReadSet{}
}

func (sdb *IntraBlockState) VersionedWrites() *WriteSet {
	return sdb.versionedWrites.Snapshot()
}

// ApplyVersionedWrites applies entries to StateDB without changing MVHashMap or this tx's write set.
func (sdb *IntraBlockState) ApplyVersionedWrites(writes *WriteSet) error {
	if writes == nil {
		return nil
	}
	// Deterministic (Address, Path, Key) order matters: load order changes whether Code/SelfDestruct records an extra read, altering the EIP-7928 BAL hash.
	headers := make([]WriteHeader, 0, writes.Count())
	for h := range writes.AllHeaders() {
		headers = append(headers, h)
	}
	sortWriteHeaders(headers)
	for _, hdr := range headers {
		addr := hdr.Address

		switch hdr.Path {
		case AddressPath:
			continue
		case StoragePath:
			vw, ok := writes.GetStorage(addr, hdr.Key)
			if !ok {
				continue
			}
			if err := sdb.setState(addr, hdr.Key, vw.Val, true); err != nil {
				return err
			}
		case BalancePath:
			vw, ok := writes.GetBalance(addr)
			if !ok {
				continue
			}
			if err := sdb.SetBalance(addr, vw.Val, hdr.Reason); err != nil {
				return err
			}
		case NoncePath:
			vw, ok := writes.GetNonce(addr)
			if !ok {
				continue
			}
			if err := sdb.SetNonce(addr, vw.Val, hdr.NonceReason); err != nil {
				return err
			}
		case IncarnationPath:
			vw, ok := writes.GetIncarnation(addr)
			if !ok {
				continue
			}
			if err := sdb.SetIncarnation(addr, vw.Val); err != nil {
				return err
			}
			// Re-emit so the finalize IBS's writes flush to the global versionMap.
			sdb.recordWriteIncarnation(addr, vw.Val)
		case CodePath:
			vwCode, ok := writes.GetCode(addr)
			if !ok {
				continue
			}
			code := vwCode.Val
			stateObject, err := sdb.GetOrNewStateObject(addr)
			if err != nil {
				return err
			}
			// Force-sets code, bypassing SetCode's equality check, since the finalize IBS's ReadSet may already hold the post-write value.
			sdb.journal.codeChange(addr, stateObject.code.Bytes, stateObject.data.CodeHash, !sdb.hasWrite(addr, CodePath, accounts.NilKey))
			stateObject.setCode(code)
			sdb.recordWriteCode(addr, code)
			sdb.recordWriteCodeHash(addr, code.Hash)
			sdb.recordWriteCodeSize(addr, code.Len())
		case CodeHashPath, CodeSizePath:
			// set by CodePath case above
		case SelfDestructPath:
			vw, ok := writes.GetSelfDestruct(addr)
			if !ok {
				continue
			}
			if vw.Val {
				// Materializes the object first: for a newly-created account, Selfdestruct would otherwise silently no-op.
				if _, err := sdb.GetOrNewStateObject(addr); err != nil {
					return err
				}
				if _, err := sdb.Selfdestruct(addr, true); err != nil {
					return err
				}
			} else {
				// SelfDestructPath=false means resurrection; the worker's createdContract=true isn't a versioned write path, so it's restored below.
				so, err := sdb.GetOrNewStateObject(addr)
				if err != nil {
					return err
				}
				if so != nil {
					so.selfdestructed = false
					so.createdContract = true
				}
				// Re-emits SelfDestructPath=false so later readers won't mistake the account for still-destructed.
				sdb.recordWriteSelfDestruct(addr, false)
			}
		case CreateContractPath:
			// A same-tx self-destruct dominates the creation marker: skip CreateContract so the account isn't resurrected as live.
			if sw, ok := writes.GetSelfDestruct(addr); ok && sw.Val {
				continue
			}
			so, err := sdb.GetOrNewStateObject(addr)
			if err != nil {
				return err
			}
			if so != nil {
				so.createdContract = true
			}
		default:
			return fmt.Errorf("unknown key type: %d", hdr.Path)
		}
	}
	return nil
}
