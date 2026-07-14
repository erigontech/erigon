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
	"slices"
	"sort"
	"strings"
	"sync"
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

var _ evmtypes.IntraBlockState = new(IntraBlockState) // compile-time interface-check

type revision struct {
	id           int
	journalIndex int
}

type revisions struct {
	nextId int
	valid  []revision
}

func (r *revisions) snapshot(journal *journal) int {
	id := r.nextId
	r.nextId++
	r.valid = append(r.valid, revision{id, journal.length()})
	return id
}

func (r *revisions) returnSnapshot(id int) {
	if r == nil {
		return
	}
	if lv := len(r.valid); lv > 0 && r.valid[lv-1].id == id {
		r.valid = r.valid[0 : lv-1]
		if r.nextId == id+1 {
			r.nextId = id
		}
	}
}

func (r *revisions) reset() {
	if r != nil {
		r.valid = r.valid[:0]
		r.nextId = 0
	}
}

func (r *revisions) put() *revisions {
	if r != nil {
		r.reset()
		if len(r.valid) < 128 {
			revisionsPool.Put(r)
		}
	}
	return nil
}

func (r *revisions) revertToSnapshot(revid int) int {
	// Find the snapshot in the stack of valid snapshots.
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

var revisionsPool = sync.Pool{
	New: func() any {
		return &revisions{0, make([]revision, 0, 2048)}
	},
}

// BalanceIncrease represents the increase of balance of an account that did not require
// reading the account first
type BalanceIncrease struct {
	increase    uint256.Int
	transferred bool // Set to true when the corresponding stateObject is created and balance increase is transferred to the stateObject
	count       int  // Number of increases - this needs tracking for proper reversion
}

type accessOptions struct {
	revertable bool
}

type AccessSet map[accounts.Address]*accessOptions

func (aa AccessSet) Merge(other AccessSet) AccessSet {
	if len(other) == 0 {
		return aa
	}
	dst := make(AccessSet, len(aa)+len(other))
	maps.Copy(dst, aa)
	maps.Copy(dst, other)
	return dst
}

// IntraBlockState is responsible for caching and managing state changes
// that occur during block's execution.
// NOT THREAD SAFE!
type IntraBlockState struct {
	stateReader StateReader

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[accounts.Address]*stateObject
	stateObjectsDirty map[accounts.Address]struct{}

	nilAccounts map[accounts.Address]struct{} // Remember non-existent account to avoid reading them again

	// The refund counter, also used by state transitioning.
	refund uint64

	txIndex  int
	blockNum uint64
	logs     []types.Logs
	logSize  uint

	// Per-transaction access list
	accessList accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal      *journal
	revisions    *revisions
	trace        bool
	tracingHooks *tracing.Hooks
	balanceInc   map[accounts.Address]*BalanceIncrease // Map of balance increases (without first reading the account)
	recordAccess bool                                  // gates MarkAddressAccess — enabled in Prepare

	// Versioned storage used for parallel tx processing, versions
	// are maintaned across transactions until they are reset
	// at the block level.  Per-path typed maps give single-level lookups for
	// non-storage paths; the AccountKey{Path,Key} struct allocation is gone
	// from the probe hot path.
	versionMap      *VersionMap
	versionedWrites WriteSet
	versionedReads  ReadSet
	// committedBase memoizes the per-tx committed (pre-block) account fallback
	// used by versionedAccountBase when the versionMap has no cell for addr.
	// The committed view is block-immutable and this branch is only reached on
	// a versionMap miss (a written account returns via the write-set), so the
	// cached pointer is safe to share across the tx's read-only callers. Reset
	// per tx.
	committedBase       map[accounts.Address]*accounts.Account
	accountReadDuration time.Duration
	accountReadCount    int64
	storageReadDuration time.Duration
	storageReadCount    int64
	codeReadDuration    time.Duration
	codeReadCount       int64
	version             int
	dep                 int

	// Per-attempt memo of the shared-versionMap SelfDestruct probe. The probe
	// (read_paths.go) fires on every versionedReadCore call but reads only
	// prior-tx SD writes — stable within one execution attempt — so a warm
	// multi-field refresh repeats the same locked read. sdProbeEpoch is bumped
	// on every Reset/SetTxContext, discarding the memo across txs and
	// re-executions without a per-tx map clear.
	sdProbe      map[accounts.Address]sdProbeEntry
	sdProbeEpoch uint64

	// noMaterialize suppresses the stateObject cache on the parallel execution
	// path: create/write flows record only versioned cells and committed reads
	// resolve from the state reader, gated by this tx's own CreateContract /
	// SelfDestruct cells. Left false for genesis/RPC/serial, which still commit
	// via FinalizeTx→so.data.
	noMaterialize bool

	// eip8246 pins whether SELFDESTRUCT preserves the account (EIP-8246 removes
	// the balance burn). Set per-tx from the block rules in Prepare; under it a
	// SelfDestructPath=true account must read as a live, balance-preserving,
	// empty-code account rather than a destroyed one.
	eip8246 bool
}

type sdProbeEntry struct {
	epoch      uint64
	res        ReadResult
	destructed bool
	ok         bool
}

// Create a new state from a given trie
func New(stateReader StateReader) *IntraBlockState {
	return &IntraBlockState{
		stateReader:       stateReader,
		stateObjects:      map[accounts.Address]*stateObject{},
		stateObjectsDirty: map[accounts.Address]struct{}{},
		nilAccounts:       map[accounts.Address]struct{}{},
		logs:              []types.Logs{},
		journal:           newJournal(),
		accessList:        accessList{addresses: make(map[accounts.Address]int)},
		transientStorage:  newTransientStorage(),
		balanceInc:        map[accounts.Address]*BalanceIncrease{},
		recordAccess:      false,
		txIndex:           0,
		trace:             false,
		dep:               UnknownDep,
	}
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

// SetNoMaterialize enables the cache-free parallel path: create/write flows
// record only versioned cells and never populate the stateObject map.
func (sdb *IntraBlockState) SetNoMaterialize(v bool) {
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

	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if len(so.fakeStorage) > 0 {
		for _, v := range so.fakeStorage {
			if !v.IsZero() {
				return true, nil
			}
		}

		return false, nil
	}

	// If we know of at least one non-empty cached storage slot, then the object has storage
	for _, v := range so.originStorage {
		if !v.IsZero() {
			return true, nil
		}
	}

	// If we know of at least one non-empty dirty storage slot, then the object has storage
	for _, v := range so.dirtyStorage {
		if !v.IsZero() {
			return true, nil
		}
	}

	// In parallel execution mode, check if a prior TX wrote IncarnationPath.
	// IncarnationPath is written ONLY by CreateAccount and Selfdestruct —
	// both operations that clear all storage.  If a prior TX wrote it, the
	// account was created or destroyed in this block and HasStorage should
	// return false. Mirrors the StoragePath check in versionedReadCore.
	if sdb.versionMap != nil {
		if inc, incRes, ok := sdb.versionMap.ReadIncarnation(addr, sdb.txIndex); ok && incRes.Status() == MVReadResultDone {
			// Record IncarnationPath dependency for validation.
			sdb.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{
				ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: incRes.DepIdx(), Incarnation: incRes.Incarnation()}},
				Val:        inc,
			})
			return false, nil
		}
	}

	// EIP-684 CREATE-collision fall-through: the in-memory checks missed, so ask
	// the reader — on snapshot-backed storage this is a kv.HasPrefix(StorageDomain)
	// walk through the .bt index, a validation hot-path cost.
	result, err := sdb.stateReader.HasStorage(addr)
	return result, err
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (sdb *IntraBlockState) Reset() {
	sdb.nilAccounts = map[accounts.Address]struct{}{}
	for _, so := range sdb.stateObjects {
		so.release()
	}
	sdb.stateObjects = map[accounts.Address]*stateObject{}
	sdb.stateObjectsDirty = map[accounts.Address]struct{}{}
	for i := range sdb.logs {
		clear(sdb.logs[i]) // free p¬ointers
		sdb.logs[i] = sdb.logs[i][:0]
	}
	sdb.balanceInc = map[accounts.Address]*BalanceIncrease{}
	sdb.journal.Reset()
	sdb.revisions = sdb.revisions.put()
	sdb.refund = uint64(0)
	sdb.txIndex = 0
	sdb.sdProbeEpoch++
	sdb.logSize = 0
	sdb.accessList.Reset()
	sdb.transientStorage = newTransientStorage()
	sdb.versionMap = nil
	clear(sdb.committedBase)
	// Read side rebinds to a fresh empty set: VersionedReads() at end of
	// tx hands the per-path maps to result.TxIn, so rebinding leaves the
	// handed-over maps intact while the next tx lazily reallocs.
	sdb.versionedReads = ReadSet{}
	// Write side: VersionedWrites() returns Cloned snapshots, so the
	// originals in sdb.versionedWrites are no longer referenced after the
	// boundary call.  Walk the per-path maps and return every VW to its
	// typed pool before resetting.
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

// Release returns pooled resources (like journal, stateObjects) back to their pools.
// Call this when the IntraBlockState is no longer needed.
// If parallel is true, cleanup happens in a goroutine for faster return.
func (sdb *IntraBlockState) Release(parallel bool) {
	stateObjects := sdb.stateObjects
	journal := sdb.journal
	sdb.stateObjects = nil
	sdb.journal = nil

	if parallel {
		go releaseResources(stateObjects, journal)
	} else {
		releaseResources(stateObjects, journal)
	}
}

func releaseResources(stateObjects map[accounts.Address]*stateObject, journal *journal) {
	for _, so := range stateObjects {
		so.release()
	}
	if journal != nil {
		journal.release()
	}
}

func (sdb *IntraBlockState) AddLog(log *types.Log) {
	sdb.journal.append(addLogChange{txIndex: sdb.txIndex})
	log.TxIndex = hexutil.Uint(sdb.txIndex)
	log.Index = hexutil.Uint(sdb.logSize)
	if dbg.TraceLogs && (sdb.trace || dbg.TraceAccount(accounts.InternAddress(log.Address).Handle())) {
		var topics string
		for i := 0; i < 4 && i < len(log.Topics); i++ {
			topics += "[" + hex.EncodeToString(log.Topics[i][:]) + "]"
		}
		if topics == "" {
			topics = "[]"
		}
		fmt.Printf("%d (%d.%d) Log: Index:%d Account:%x Topics: %s Data:%x\n", sdb.blockNum, sdb.txIndex, sdb.version, log.Index, log.Address, topics, log.Data)
	}
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnLog != nil {
		sdb.tracingHooks.OnLog(log)
	}
	sdb.logSize++
	for len(sdb.logs) <= sdb.txIndex+1 {
		sdb.logs = append(sdb.logs, nil)
	}
	sdb.logs[sdb.txIndex+1] = append(sdb.logs[sdb.txIndex+1], log)
}

func (sdb *IntraBlockState) GetLogs(txIndex int, txnHash common.Hash, blockNumber uint64, blockHash common.Hash) types.Logs {
	if txIndex+1 >= len(sdb.logs) {
		return nil
	}
	logs := sdb.logs[txIndex+1]
	for _, l := range logs {
		l.TxHash = txnHash
		l.BlockNumber = hexutil.Uint64(blockNumber)
		l.BlockHash = blockHash
	}
	return slices.Clone(logs)
}

// GetRawLogs - is like GetLogs, but allow postpone calculation of `txn.Hash()`.
// Example: if you need filter logs and only then set `txn.Hash()` for filtered logs - then no reason to calc for all transactions.
func (sdb *IntraBlockState) GetRawLogs(txIndex int) types.Logs {
	if txIndex+1 >= len(sdb.logs) {
		return nil
	}
	return slices.Clone(sdb.logs[txIndex+1])
}

func (sdb *IntraBlockState) Logs() types.Logs {
	var logs types.Logs
	for _, lgs := range sdb.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddRefund adds gas to the refund counter
func (sdb *IntraBlockState) AddRefund(gas uint64) {
	sdb.journal.append(refundChange{prev: sdb.refund})
	sdb.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (sdb *IntraBlockState) SubRefund(gas uint64) error {
	sdb.journal.append(refundChange{prev: sdb.refund})
	if gas > sdb.refund {
		return errors.New("refund counter below zero")
	}
	sdb.refund -= gas
	return nil
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for self destructed accounts.
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

	// Existence needs only the base record + self-destruct gate, not the
	// per-field overlay.
	// Same-tx self-destruct: the account is still alive (EIP-6780).
	// Cross-tx self-destruct: versionedAccountBase returns nil.
	readAccount, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	return readAccount != nil, nil
}

var emptyAccount = accounts.NewAccount()

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
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
	// Existence + the self-destruct/revival gate, without reconstructing the
	// whole account: the EIP-161 verdict needs only the current balance, nonce
	// and code hash, read per-field below (short-circuiting), so the per-field
	// overlay and a full-account allocation are avoided.
	account, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	if account == nil {
		sdb.touchAccount(addr)
		// Do NOT call accountRead here: versionedAccountBase already recorded
		// the AddressPath read (via versionedReadCore) with Val=nil.  Calling
		// accountRead(&emptyAccount) would overwrite that nil with a non-nil
		// pointer to an empty Account.  Downstream code (getBalance →
		// versionedReadCore for BalancePath → recursive AddressPath lookup) treats
		// non-nil as "account exists", creating a stateObject instead of going
		// through createObject.  When createObject is skipped, AddressPath is
		// never written to the version map, and other txs that read this
		// address miss the conflict during validation.
		return true, nil
	}

	return sdb.emptyFromVersionedFields(addr, account)
}

// emptyFromVersionedFields computes the EIP-161 emptiness verdict for an
// account that versionedAccountBase resolved as existing, reading the current
// balance/nonce/codeHash per-field (short-circuiting on the first non-empty
// field) instead of reconstructing the whole account. The per-field refresh
// reads apply the same self-destruct gate as the whole-account path.
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

// GetBalance retrieves the balance from the given address or 0 if object not found
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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

// TxIndex returns the current transaction index set by Prepare.
func (sdb *IntraBlockState) TxnIndex() int {
	return sdb.txIndex
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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
	// When commited=true (used by ResolveCode for EIP-7702 delegation),
	// versionedReadCore skips local versionedWrites and may return a stale
	// ReadSet value. If the CURRENT tx has set this account's code (e.g.,
	// via EIP-7702 authorization processing), return the dirty code directly.
	// We must also check hasWrite to ensure the code was set in this tx,
	// not in a previous tx sharing the same IBS (block generator reuses IBS).
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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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
		// Size-only read: ReadAccountCodeSize, not ReadAccountCode. It routes
		// through the size-only cache layer, and is correct on the Stateless
		// reader — a size-only witness node has the size but no bytes, so
		// ReadAccountCode there returns nil (EXTCODESIZE 0) and diverges from
		// consensus.
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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
// codeAccessTracker lets a stateReader observe code accesses (EIP-7928 BAL /
// EIP-7702 delegation). No-op when the reader doesn't implement it.
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
	if sdb.eip8246 && hash == accounts.NilCodeHash {
		// A prior tx's EIP-8246 SELFDESTRUCT leaves an existing empty-code
		// account, but its CodeHashPath is dropped from the version map, so
		// recover the codehash from the reconstructed account (EmptyCodeHash),
		// distinguishing it from a genuinely absent account (NilCodeHash).
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
	// eip-7702
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
	// committed=false so the tx's own writes (e.g. from EIP-7702 authorization
	// list) are visible. With committed=true the parallel executor reads stale
	// delegation code from the version map instead of the current tx's SetCode.
	// CodePath exemptions in versionedReadCore already handle SelfDestruct cases.
	code, err := sdb.getCode(addr, false)
	// eip-7702
	if delegation, ok := types.ParseDelegation(code); ok {
		return sdb.getCode(delegation, false)
	}
	if err != nil {
		return nil, err
	}
	return code, nil
}

func (sdb *IntraBlockState) GetDelegatedDesignation(addr accounts.Address) (accounts.Address, bool, error) {
	// eip-7702 - for account read recording we don't count this as
	// it may not result in an actual gas recorded access - if it
	// is it will be marked via a direct call
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

// GetState retrieves a value from the given account's storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := readState(sdb, addr, key)

	if dbg.TraceTransactionIO && (sdb.trace || (dbg.TraceAccount(addr.Handle()) && traceKey(key))) {
		fmt.Printf("%d (%d.%d) GetState (%s) %x, %x=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, key, versionedValue.Hex()[2:])
	}

	return versionedValue, err
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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

// writeBalanceVersioned records a balance change on the versionMap write-set and
// the journal without materializing the stateObject on the common existing-alive
// path. An absent or destroyed-no-revival account is materialized via
// GetOrNewStateObject so createObject records the AddressPath write OCC needs; the
// create path never reads balance (matching the old stateObject path). The journal
// prev is read only in the existing branch so a create does not widen the OCC
// read-set with a spurious BalancePath read.
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
		// A destroyed-then-revived account's transient is rebuilt from the base
		// record and lags this tx's own balance write, so SetBalance's journal
		// entry would capture a stale prev and a revert would restore the wrong
		// balance. Seed the live balance first. The base==nil create path never
		// read balance, so leave it untouched (avoids widening the OCC read-set).
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
	sdb.journal.append(balanceChange{account: addr, prev: prev, wasCommited: wasCommited})
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
		sdb.tracingHooks.OnBalanceChange(addr, prev, update, reason)
	}
	sdb.recordWriteBalance(addr, update)
	return nil
}

// AddBalance adds amount to the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) AddBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if sdb.versionMap == nil {
		// If this account has not been read, add to the balance increment map
		if _, needAccount := sdb.stateObjects[addr]; !needAccount && addr == ripemd && amount.IsZero() {
			sdb.journal.append(balanceIncrease{
				account:  addr,
				increase: amount,
			})

			bi, ok := sdb.balanceInc[addr]
			if !ok {
				bi = &BalanceIncrease{}
				sdb.balanceInc[addr] = bi
			}

			if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
				// TODO: discuss if we should ignore error
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

	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.IsZero() {
		return sdb.TouchAccount(addr)
	}

	prev, wasCommited, _ := sdb.getBalance(addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			prev := prev     // avoid capture allocation unless we're tracing
			amount := amount // avoid capture allocation unless we're tracing
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
	sdb.journal.append(touchAccount{
		account: addr,
	})
	if addr == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		sdb.journal.dirty(addr)
	}
}

// TouchAccount materializes an empty account and records the zero-balance touch
// needed for state clearing and trie consistency.
func (sdb *IntraBlockState) TouchAccount(addr accounts.Address) error {
	markTouched := func() {
		sdb.recordWriteBalance(addr, uint256.Int{})
		if _, ok := sdb.journal.dirties[addr]; !ok {
			if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) Touch %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
			}
			sdb.touchAccount(addr)
		}
	}

	if sdb.versionMap != nil {
		// The touch only depends on emptiness. For an existing account compute
		// it from field reads without materializing/reconstructing the
		// stateObject; only an absent account needs GetOrNewStateObject so
		// createObject records the AddressPath write (OCC create detection).
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

// readSelfDestructMemo returns the shared-versionMap SelfDestruct probe for the
// current execution attempt, caching it so a warm multi-field read does not
// re-acquire the versionMap RWMutex per field. The probe reads only prior-tx SD
// writes; the tx's own SelfDestruct lives in versionedWrites and is consulted
// separately, so the memoized value is stable for the attempt.
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

// eip8246PreservedAccount reconstructs the live account a prior tx left behind
// when EIP-8246 removed the SELFDESTRUCT burn: the balance survives, code and
// nonce are cleared at destruction, and any later per-field map writes overlay
// the reconstruction so account-level reads agree with the field-level ones.
// Returns nil when the balance was moved out, leaving an empty account that
// EIP-161 removes.
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

// getVersionedAccount returns the account reconstructed from the base record
// plus the versionMap field overlays. Whole-account consumers (stateObject
// construction) need the reconstructed record; field-oriented callers
// (GetBalance/Empty/Exist) read what they need without it.
func (sdb *IntraBlockState) getVersionedAccount(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	return sdb.versionedAccountBase(addr, readStorage)
}

// versionedAccountBase resolves account existence via the AddressPath read (and
// storage fallback), applying the self-destruct/revival gate, but does NOT
// overlay the per-field versionMap cells. It returns nil when the account is
// absent or was destroyed with no revival. The AddressPath read it performs
// records the nil-read that OCC uses to detect create/absent conflicts.
func (sdb *IntraBlockState) versionedAccountBase(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	if sdb.versionMap == nil {
		return nil, UnknownSource, UnknownVersion, nil
	}

	readAccount, source, version, err := readAccount(sdb, addr)

	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}

	// EIP-8246: a prior tx's SELFDESTRUCT preserves the account (balance kept,
	// code/nonce cleared) rather than destroying it. AddressPath reads zero
	// under SD, so reconstruct the surviving account from the version map here,
	// covering both committed and in-block-created accounts — unless a later tx
	// re-created it, in which case fall through to the normal read.
	if sdb.eip8246 && readAccount == nil {
		if destructed, sdRes, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex); ok && sdRes.Status() == MVReadResultDone && destructed {
			destructTxIndex := sdRes.DepIdx()
			// Only a genuine re-creation (a later CreateAccount, which writes
			// AddressPath) skips reconstruction. Later Balance/Nonce/CodeHash
			// writes are updates to the still-preserved account, not a revival:
			// reconstruct it and let eip8246PreservedAccount overlay the latest
			// balance, nonce and code hash, so e.g. an account funded after its
			// SELFDESTRUCT still reads as existing — matching serial.
			revived := false
			if hi, ok := sdb.versionMap.LatestTxIndex(addr, AddressPath, accounts.NilKey, sdb.txIndex-1); ok && hi > destructTxIndex {
				revived = true
			}
			if !revived {
				preserved, err := sdb.eip8246PreservedAccount(addr)
				if err != nil {
					return nil, StorageRead, UnknownVersion, err
				}
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
			return nil, StorageRead, UnknownVersion, err
		}

		// CachedReaderV3 bypasses the versionMap, so a prior in-block SD'd
		// address still returns its pre-SD record. Without this gate the
		// stale nonce/codeHash flows through the per-field refresh (which
		// only overwrites fields a versionMap cell exists for), so Empty()
		// returns false and the EVM misses CallNewAccountGas.
		if destroyed, _, revived := sdb.versionMap.AccountLifecycle(addr, sdb.txIndex); destroyed && !revived {
			return nil, StorageRead, UnknownVersion, nil
		}
	}

	return readAccount, source, version, nil
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SubBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if amount.IsZero() {
		if addr == params.SystemAddress {
			// Gnosis/AuRa keeps an empty system account even after
			// Spurious Dragon (see PR 5645 and Issue 18276).
			//
			// The primary syscall path in evm.call() handles this via
			// TouchAccount directly; this branch is retained as
			// defense-in-depth for other callers (AuRa engine,
			// consensus callbacks).
			return sdb.TouchAccount(addr)
		}
		return nil
	}

	prev, wasCommited, _ := sdb.getBalance(addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			prev := prev     // avoid capture allocation unless we're tracing
			amount := amount // avoid capture allocation unless we're tracing
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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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

// writeNonceVersioned records a nonce write on the parallel (versionMap) path
// without materializing a stateObject for an existing, live account. A nonce SET
// does not depend on the prior value, so prev is read WITHOUT recording an OCC
// read (versionedWrites for this tx's own prior write, else the base record) —
// matching the materialized path's AddressPath-only footprint. Absent/destroyed
// accounts still materialize (account creation).
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
	if vw, ok := sdb.versionedWrites.GetNonce(addr); ok {
		prev = vw.Val
	}
	// Keep an already-materialized stateObject's so.data in step so the
	// so.data-based commit paths (genesis FinalizeTx, RPC) stay correct. We
	// don't materialize one that isn't present — that's the whole point.
	if so, ok := sdb.stateObjects[addr]; ok {
		prev = so.data.Nonce
		so.setNonce(nonce)
	}
	sdb.journal.append(nonceChange{account: addr, prev: prev, wasCommited: wasCommited})
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

// DESCRIBED: docs/programmers_guide/guide.md#code-hash
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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
		// so.data/so.original are the base record and miss a prior-tx
		// CodeHashPath-only write (the per-field reads no longer rebuild a
		// full account).
		// baseCodeHash ("what this SetCode saw") = the current cell, including
		// this tx's own earlier code writes. origHash (the cumulative net-zero
		// baseline) = the versionMap floor at txIndex — the tx-start value,
		// excluding this tx's unflushed writes.
		if ch, chErr := sdb.GetCodeHash(addr); chErr == nil {
			baseCodeHash = ch
		}
		if ch, res, ok := sdb.versionMap.ReadCodeHash(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone {
			origHash = ch
		} else if sdb.noMaterialize {
			// The rebuilt transient's original reflects this tx's own code cell
			// (readAccount folds CodeHashPath), not the tx-start value. With no
			// prior-tx floor entry the cumulative baseline is the committed hash.
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
		// Skip when the new code matches either (1) the value seen by THIS
		// SetCode call (revert to in-tx base), or (2) the pre-tx original
		// (cumulative net-zero — e.g. EIP-7702 authority that delegates and
		// then resets within the same tx). Case (2) is disabled for newly
		// created stateObjects: original holds the pre-creation snapshot,
		// and deleting CodePath/CodeHashPath writes would corrupt the trie.
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
	return sdb.trace
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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	return sdb.setState(addr, key, value, false)
}

func (sdb *IntraBlockState) setState(addr accounts.Address, key accounts.StorageKey, value uint256.Int, force bool) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetState %x, %x=%s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, key, value.Hex())
	}

	// The EVM SSTORE path (force==false) writes through cells without
	// materializing a stateObject. force==true (ApplyVersionedWrites replay) and
	// a fakeStorage override (eth_simulate) still need the object.
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
		// Always record the write even when the value equals the origin.
		// Deleting the write entry when value == origin broke revert semantics:
		// if a nested call writes a value and the outer call reverts, the journal
		// must restore the previous write entry. With the deletion optimization,
		// the entry was gone and the revert had nothing to restore.
		sdb.recordWriteStorage(addr, key, value)
	}
	return nil
}

// setStateVersioned records a storage write on the parallel (versionMap) path
// without materializing a stateObject. It mirrors stateObject.SetState's set
// decision and journalling; the prev value comes from the cell-based
// readStateForSet. An already-materialized stateObject is kept in step so the
// so.data-based commit paths (genesis FinalizeTx, RPC) stay correct.
func (sdb *IntraBlockState) setStateVersioned(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	prev, source, _, commited, err := readStateForSet(sdb, addr, key)
	if err != nil {
		return err
	}
	// See stateObject.SetState: a value resolved from a cached read or the
	// version map has no versioned write for this key this tx, so this is the
	// first write and commited must be true for storageChange.revert to delete
	// (not update) the cell.
	if source != WriteSetRead && source != UnknownSource && source != StorageRead {
		commited = true
	}
	if source != UnknownSource && prev == value {
		return nil
	}
	sdb.journal.append(storageChange{account: addr, key: key, prevalue: prev, wasCommited: commited})
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnStorageChange != nil {
		sdb.tracingHooks.OnStorageChange(addr, key, prev, value)
	}
	if so, ok := sdb.stateObjects[addr]; ok {
		so.setState(key, value)
	}
	sdb.recordWriteStorage(addr, key, value)
	return nil
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
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

// SetIncarnation sets incarnation for account if account exists
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

// Selfdestruct marks the given account as suicided. When preserveBalance is
// false the account balance is burned (pre-EIP-6780/6780 behaviour); when true
// the balance is left untouched (EIP-8246) and only cleared at finalization if
// the account ends up empty.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
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
	sdb.journal.append(selfdestructChange{
		account:     addr,
		prev:        stateObject.selfdestructed,
		prevbalance: prevBalance,
		wasCommited: !sdb.hasWrite(addr, SelfDestructPath, accounts.NilKey),
	})

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

	// NOTE: we intentionally do NOT versionWritten(StoragePath, key, 0) for the
	// dirty slots here. Pre-Cancun (and for CALL-based SELFDESTRUCT generally)
	// the account stays alive until end-of-tx, so a re-entry's GetState must
	// still see the dirty values — and versionedReadCore consults versionedWrites
	// before the stateObject, so a spurious StoragePath=0 here would make those
	// reads return 0 (wrong gas: SSTORE_SET vs dirty-update, and wrong value).
	// The parallel commitment calculator gets the per-slot DELETE entries from
	// normalizeWriteSet's SD cascade (sdStorageSlots = vm.StorageKeys ∪
	// domainStorageKeys), so they don't need to be emitted here.

	return true, nil
}

// selfdestructVersioned records a self-destruct on the parallel (versionMap)
// path without materializing a stateObject. Existence and the prior
// self-destruct flag / balance / incarnation are read from the base record plus
// this tx's own versioned writes, never a cached object. An already-materialized
// stateObject is kept in step for the so.data-based commit paths (genesis
// FinalizeTx, RPC).
func (sdb *IntraBlockState) selfdestructVersioned(addr accounts.Address, preserveBalance bool) (bool, error) {
	base, _, _, err := sdb.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	// base is nil for an absent account and for one destroyed in a prior tx and
	// not revived (versionedAccountBase applies that gate) — the serial path's
	// stateObject.deleted check. A same-tx repeat SELFDESTRUCT still proceeds:
	// the serial object stays deleted==false until finalize, so it re-runs.
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

	sdb.journal.append(selfdestructChange{
		account:     addr,
		prev:        prev,
		prevbalance: prevBalance,
		wasCommited: !sdb.hasWrite(addr, SelfDestructPath, accounts.NilKey),
	})

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
		// Pre-EIP-8246: SELFDESTRUCT burns the balance and the account is deleted;
		// keep the pre-destruct incarnation for the storage-delete cascade.
		sdb.recordWriteIncarnation(addr, inc)
		sdb.recordWriteBalance(addr, uint256.Int{})
		return true, nil
	}
	// EIP-8246: the balance is preserved but code, nonce and incarnation are
	// cleared, leaving a balance-only account. Record the cleared cells so a later
	// tx reconstructing the preserved account reads empty code / zero nonce, and a
	// re-creation bumps the incarnation from 0 (matching serial), not the stale
	// pre-destruct value.
	sdb.recordWriteIncarnation(addr, 0)
	sdb.recordWriteNonce(addr, 0, tracing.NonceChangeUnspecified)
	sdb.recordWriteCodeHash(addr, accounts.EmptyCodeHash)

	return true, nil
}

var zeroBalance uint256.Int

// Used for EIP-6780
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

// SetTransientState sets transient storage for a given account. It
// adds the change to the journal so that it can be rolled back
// to its previous value if there is a revert.
func (sdb *IntraBlockState) SetTransientState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	prev := sdb.GetTransientState(addr, key)
	if prev == value {
		return
	}

	sdb.journal.append(transientStorageChange{
		account:  addr,
		key:      key,
		prevalue: prev,
	})

	sdb.setTransientState(addr, key, value)
}

// setTransientState is a lower level setter for transient storage. It
// is called during a revert to prevent modifications to the journal.
func (sdb *IntraBlockState) setTransientState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	sdb.transientStorage.Set(addr, key, value)
}

// GetTransientState gets transient storage for a given account.
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
	if so, ok := sdb.stateObjects[addr]; ok {
		return so, nil
	}

	// Load the object from the database.
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
	accountVersion := sdb.Version()

	if err != nil {
		return nil, err
	}

	if readAccount == nil {
		if sdb.versionMap != nil {
			readAccount, accountSource, accountVersion, err = refreshAccount(sdb, addr)

			if readAccount == nil || err != nil {
				return nil, err
			}

			destructed, _, _, err := refreshSelfDestruct(sdb, addr)

			if destructed || err != nil {
				if !sdb.noMaterialize {
					so := stateObjectPool.Get().(*stateObject)
					so.db = sdb
					so.address = addr
					so.selfdestructed = destructed
					so.deleted = destructed
					sdb.setStateObject(addr, so)
				}
				return nil, err
			}
		} else {
			sdb.nilAccounts[addr] = struct{}{}
			if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred {
				return sdb.createObject(addr, nil), nil
			}
			return nil, nil
		}
	}

	var code []byte

	if sdb.versionMap != nil {
		account = readAccount

		// Check if a prior tx selfdestructed this account. The AddressPath
		// versionedReadCore above returned nil (SelfDestructPath early-exit), but
		// stateReader returned a committed value from SharedDomains. Read
		// SelfDestructPath directly from the versionMap (not via versionedReadCore
		// which itself short-circuits on the same flag). Use the same pattern
		// as CreateAccount (line 1628).
		if destructed, res, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone && destructed {
			// Only honour if the current tx hasn't already resurrected.
			localResurrected := false
			if sdVal, ok := sdb.versionedWriteSelfDestruct(addr); ok {
				if !sdVal {
					localResurrected = true
				}
			}
			if !localResurrected {
				if !sdb.noMaterialize {
					so := stateObjectPool.Get().(*stateObject)
					so.db = sdb
					so.address = addr
					so.selfdestructed = true
					so.deleted = true
					sdb.setStateObject(addr, so)
				}
				return nil, nil
			}
		}

		code, _, _, err = refreshCode(sdb, addr)
		if err != nil {
			return nil, err
		}
	} else {
		account = readAccount
	}

	if recordRead {
		sdb.accountRead(addr, account, accountSource, accountVersion)
	}
	obj := newObject(sdb, addr, account, account)
	if code != nil {
		// When code is loaded from the version map (written by a prior tx),
		// synchronise the stateObject's CodeHash with the actual code.
		// Without this fix, the stale CodeHash causes the "revert to original"
		// optimisation in SetCode to incorrectly delete code writes when
		// clearing a delegation that was set by a prior transaction in the
		// same block.
		codeHash := accounts.InternCodeHash(crypto.HashData(code))
		obj.code = accounts.Code{Hash: codeHash, Bytes: code}
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
	if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred && sdb.versionMap == nil {
		object.data.Balance = u256.Add(object.data.Balance, bi.increase)
		bi.transferred = true
		sdb.journal.append(balanceIncreaseTransfer{bi: bi})
	}
	sdb.stateObjects[addr] = object
}

// Retrieve a state object or create a new state object if nil.
func (sdb *IntraBlockState) GetOrNewStateObject(addr accounts.Address) (*stateObject, error) {
	stateObject, err := sdb.getStateObject(addr, true)
	if err != nil {
		return nil, err
	}
	if stateObject == nil || stateObject.deleted {
		stateObject = sdb.createObject(addr, stateObject /* previous */)
	}
	return stateObject, nil
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten.
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
	newobj.setNonce(0) // sets the object to dirty
	if previous == nil {
		sdb.journal.append(createObjectChange{account: addr})
	} else {
		reset := resetObjectChange{account: addr, prev: previous}
		if sdb.versionMap != nil {
			reset.prevWrites = sdb.versionedWrites.snapshotCreateFields(addr)
		}
		sdb.journal.append(reset)
	}
	newobj.newlyCreated = true
	if !sdb.noMaterialize {
		sdb.setStateObject(addr, newobj)
	}
	data := newobj.data
	sdb.recordWriteAddress(addr, &data)
	// Write CodeHashPath so that any stale versionedReads cache entry
	// (e.g. from the pre-creation GetCodeHash check in EVM create()) is
	// invalidated.  newObject normalises the zero-value CodeHash to
	// EmptyCodeHash, so this records keccak256("") for a fresh account.
	sdb.recordWriteCodeHash(addr, newobj.data.CodeHash)
	return newobj
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//  1. sends funds to sha(account ++ (nonce + 1))
//  2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
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

			destructed, _, _, err := refreshSelfDestruct(sdb, addr)

			if err != nil {
				return err
			}

			// Reuse the cached stateObject directly `previous` so that (a) selfdestructed=true is captured,
			// (b) the accumulated incarnation is used for the new object's PrevIncarnation (important when the
			// account was created and destroyed multiple times within the same block), and
			// (c) after a REVERT CommitBlock can still emit DeleteAccount for it.
			if !destructed {
				if so, ok := sdb.stateObjects[addr]; ok && so.selfdestructed {
					// Accumulated-IBS path (e.g. GenerateChain): stateObjects cache marks the
					// account as selfdestructed but versionedReadCore returned false due to the
					// so.deleted early exit.  Reuse the cached stateObject to preserve the
					// correct selfdestructed flag and accumulated incarnation.
					previous = so
				} else if sdb.versionMap != nil {
					// Fresh-IBS worker path (e.g. InsertChain parallel executor): no stateObjects
					// cache, but the versionMap may have SelfDestructPath=true from a prior tx.
					// versionedReadCore returns false for SelfDestructPath via the early-exit at
					// lines 459-462 — bypass it here so we correctly set selfdestructed=true.
					if d, res, ok := sdb.versionMap.ReadSelfDestruct(addr, sdb.txIndex); ok && res.Status() == MVReadResultDone && d {
						destructed = true
					}
				}
			}

			// Honour same-block revival (#21319): a prior tx's self-destruct is
			// overridden by a later tx that revived the account to a non-empty
			// state (a value transfer leaving balance/nonce/code behind). A value-0
			// no-op transfer that leaves it empty does NOT revive it (EIP-161
			// removes it again). account is the version-map-refreshed record, so
			// its emptiness is the authoritative revival test. Without this,
			// CreateAccount keeps previous.selfdestructed set and skips the balance
			// carry below — losing the revived funds.
			if destructed && sdb.versionMap != nil && !account.Empty() {
				destructed = false
			}

			if previous == nil {
				previous = newObject(sdb, addr, account, account)
				previous.selfdestructed = destructed
			}
		} else if so, ok := sdb.stateObjects[addr]; ok && so.deleted {
			// The account was selfdestructed in an earlier transaction within the
			// same block (accumulated IBS, e.g. GenerateChain) AND the underlying
			// storage has no record of it (e.g. it was created within this block).
			// getVersionedAccount returned nil; preserve the deleted stateObject as
			// `previous` so that after a REVERT CommitBlock can still emit
			// DeleteAccount for it.
			previous = so
		} else if so, ok := sdb.stateObjects[addr]; ok {
			// The serial block builder runs with a version map but does not flush
			// per-tx writes to it, so a same-block credit on this IBS lives only in
			// the cache; reuse it as `previous` to keep the balance carry-over below.
			previous = so
		} else if sd, ok := sdb.versionedWriteSelfDestruct(addr); ok && sd {
			// Cache-free parallel path: a within-tx create→self-destruct leaves no
			// committed base record and no cached stateObject. Rebuild `previous`
			// from this tx's own cells so the recreated account's incarnation still
			// accumulates and the resurrect write is emitted.
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
	// Capture each path's own (source, version) for the synthetic reads stamped
	// at the bottom of the function — inheriting the account-record version
	// would trip the validator on the recursive AddressPath check.
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
	// Writer.DeleteAccount stores the selfdestructed incarnation in rs.selfdestructedByTx.
	// Recover it here so that CreateAccount in the next tx computes newInc = prevInc+1 correctly.
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

	// Capture the address's current balance BEFORE createObject writes the fresh
	// zero-balance record. versionedAccountBase returns the base record without
	// overlaying this tx's own field writes, so previous.data.Balance can lag
	// either an in-block credit (genesis Constructor: AddBalance then SysCreate)
	// or a committed prefund (CREATE at a pre-funded address). Reading after
	// createObject would see the just-written zero and drop the balance.
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
		// resetObjectChange.dirtied() returns false, so without this the
		// parallel worker's MakeWriteSet drops the resurrect write. Confined to
		// CreateAccount — the GetOrNewStateObject AddBalance path must NOT mark
		// dirty here.
		sdb.journal.dirty(addr)
	}
	if carryBalanceValid {
		newObj.data.Balance.Set(&carryBalance)
	}
	newObj.data.PrevIncarnation = prevInc

	if contractCreation {
		newObj.createdContract = true
		newObj.data.Incarnation = prevInc + 1
		// Record contract creation in the versioned writes so that
		// normalizeWriteSet knows this address was created (prevents
		// empty account deletion for newly deployed contracts).
		sdb.recordWriteCreateContract(addr, true)
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) New Incarnation %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, newObj.data.Incarnation)
		}
	} else {
		newObj.selfdestructed = false
	}

	// for newly created accounts these synthetic read/writes are used so that account
	// creation clashes between trnascations get detected
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap != nil {
		sdb.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader{Source: balSource, Version: balVersion}, newObj.Balance()})
		sdb.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{ReadHeader{Source: incSource, Version: incVersion}, prevInc})
	}
	sdb.recordWriteBalance(addr, newObj.Balance())
	sdb.recordWriteIncarnation(addr, newObj.data.Incarnation)
	if previous == nil || previous.selfdestructed && !newObj.selfdestructed {
		sdb.recordWriteSelfDestruct(addr, false)
	}

	return nil
}

// Snapshot returns an identifier for the current revision of the state.
func (sdb *IntraBlockState) PushSnapshot() int {
	if sdb.revisions == nil {
		sdb.revisions = revisionsPool.Get().(*revisions)
	}
	return sdb.revisions.snapshot(sdb.journal)
}

func (sdb *IntraBlockState) PopSnapshot(snapshot int) {
	sdb.revisions.returnSnapshot(snapshot)
}

// RevertToSnapshot reverts all state changes made since the given revision.
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
	// Replay the journal to undo changes and remove invalidated snapshots
	sdb.journal.revert(sdb, snapshot)

	if traced {
		fmt.Printf("%d (%d.%d) Reverted: %d:%d\n", sdb.blockNum, sdb.txIndex, sdb.version, revid, snapshot)
	}
}

// GetRefund returns the current value of the refund counter.
func (sdb *IntraBlockState) GetRefund() uint64 {
	return sdb.refund
}

// EIP161EmptyRemoval reports whether an empty account at addr is removed under
// EIP-161 (SpuriousDragon). AuRa retains its SystemAddress even when empty, to
// match the reference implementation.
func EIP161EmptyRemoval(eip161Enabled, isAura bool, addr accounts.Address) bool {
	return eip161Enabled && (!isAura || addr != params.SystemAddress)
}

func updateAccount(eip161Enabled bool, isAura bool, stateWriter StateWriter, addr accounts.Address, stateObject *stateObject, isDirty bool, trace bool, tracingHooks *tracing.Hooks, useBlockOrigin bool, eip8246 bool) error {
	emptyRemoval := EIP161EmptyRemoval(eip161Enabled, isAura, addr) && stateObject.data.Empty()
	// EIP-8246: a self-destructed account that still holds a balance is reset to
	// a balance-only account (nonce 0, empty code, empty storage) not deleted.
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
		// Supersede Selfdestruct's pre-destruct IncarnationPath: extraction keeps
		// incarnation for self-destructed accounts (unlike nonce/code/codeHash,
		// which the extraction filter drops), and a later CREATE2 must see the
		// persisted balance-only record's 0 in every execution mode.
		stateObject.db.recordWriteIncarnation(addr, 0)
		if err := stateWriter.CreateContract(addr); err != nil {
			return err
		}
		if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
			return err
		}
	} else if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		stateObject.deleted = false
		// Write any contract code associated with the state object; dirtyCode is
		// set only when code actually changed, so a clear-to-empty must still
		// write through (empty CodeDomain, consistent with the empty codeHash).
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
		// Note: in parallel mode, individual setters (AddBalance, SetNonce)
		// call versionWritten for their specific field. Fields not modified
		// by the TX (e.g., CodeHash when only balance changed) are NOT in
		// the versionMap's WriteSet. The normalizeWriteSet function handles
		// this by reading missing account fields from the stateReader.
	}
	return nil
}

func printAccount(eip161Enabled bool, isAura bool, addr accounts.Address, stateObject *stateObject, isDirty bool) {
	emptyRemoval := EIP161EmptyRemoval(eip161Enabled, isAura, addr) && stateObject.data.Empty()
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
		fmt.Printf("delete: %x\n", addr)
	}
	if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		// Write any contract code associated with the state object
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
			// ripeMD is 'touched' at block 1714175, in txn 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That txn goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `sdb.journal.dirties` but not in `sdb.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}

		if err := updateAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, stateWriter, addr, so, true, sdb.trace, sdb.tracingHooks, false, chainRules.IsAmsterdam); err != nil {
			return err
		}

		// Per EIP-6780 + EIP-7928: SELFDESTRUCT of a SAME-TX created contract
		// wipes storage at end-of-tx, so the BAL must record dirty slots as
		// reads, not changes. Zero storage versionedWrites so AsBlockAccessList
		// folds them away via net-zero. Must run BEFORE so.newlyCreated = false.
		// The block assembler's BAL is built per-tx from ibs.TxIO() and never
		// fires the MakeWriteSet hook, so this per-tx hook is required for
		// assembler/validator BAL convergence.
		if sdb.versionMap != nil && so.selfdestructed && so.newlyCreated {
			for key := range so.dirtyStorage {
				sdb.recordWriteStorage(addr, key, uint256.Int{})
			}
		}

		// EIP-8246: a balance-preserving SELFDESTRUCT leaves the account alive
		// (balance kept, code/nonce/storage cleared). The block assembler reuses
		// one IBS across txs without Reset, so replace the destroyed object with
		// a clean balance-only one — done after the storage/BAL cleanup above,
		// which still needs the selfdestructed marker. Otherwise a later tx's
		// CREATE2 at this address sees a stale selfdestructed flag and drops the
		// preserved balance, building an invalid block.
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
		// versionMap (parallel) path: a write can be recorded to versionedWrites
		// without materializing a stateObject, so dirtiness must come from the
		// journal (populated alongside every recordWrite), not stateObject
		// existence — else MakeWriteSet's revert reconciliation drops the write.
		// Serial path keeps the stateObject gate: a touched-but-reverted address
		// (ripeMD, out-of-gas) lingers in journal.dirties without a stateObject.
		if _, exist := sdb.stateObjects[addr]; !exist && sdb.versionMap == nil {
			continue
		}
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
}

// CommitBlock finalizes the state by removing the self destructed objects
// and clears the journal as well as the refunds.
func (sdb *IntraBlockState) CommitBlock(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr, bi := range sdb.balanceInc {
		if !bi.transferred {
			sdb.getStateObject(addr, true)
		}
	}
	return sdb.MakeWriteSet(chainRules, stateWriter)
}

// ExtractAndClearDirty snapshots the current stateObjectsDirty set and clears it.
// Used by eth_simulateV1 to separate accounts dirtied by stateOverrides from those
// dirtied by actual transaction execution, so CommitBlock does not apply EIP-161 to
// override-only accounts.
func (sdb *IntraBlockState) ExtractAndClearDirty() map[accounts.Address]struct{} {
	dirty := maps.Clone(sdb.stateObjectsDirty)
	clear(sdb.stateObjectsDirty)
	return dirty
}

// CommitOverrideDirtyAccounts writes state-override accounts that were not subsequently
// touched by any transaction (and therefore not handled by CommitBlock).  EIP-161 is
// intentionally disabled: override accounts are simulation-only mutations and must not
// be removed simply because they are "empty" by consensus rules.
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
		// Per EIP-6780 + EIP-7928: a SELFDESTRUCT against a SAME-TX created
		// contract clears storage at end-of-tx, so the BAL must record the
		// dirty slots as reads, not changes. Zero the storage versionedWrite
		// values so AsBlockAccessList's net-zero check folds them away.
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

	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

// FinalizedWrites returns the tx's committable write-set on the parallel
// (versionMap) path: WriteSet.Finalize applies the EIP-6780 wipe and snapshots
// the recorded IO. No journal reset here — Reset() does that before the next tx.
func (sdb *IntraBlockState) FinalizedWrites() *WriteSet {
	return sdb.versionedWrites.Finalize()
}

// MergeTxIOInto folds the current transaction's recorded reads, writes and
// accesses into io at the current tx index, without building an intermediate
// VersionedIO.
func (sdb *IntraBlockState) MergeTxIOInto(io *VersionedIO) {
	version := Version{BlockNum: sdb.blockNum, TxIndex: sdb.txIndex, Incarnation: sdb.version}
	io.mergeTx(version, sdb.versionedReads, sdb.VersionedWrites())
}

// FlushWritesToVersionMap publishes the current tx's writes into this IBS's own
// versionMap, positioned by each write's (txIndex, incarnation). The single-IBS
// block assembler resets the per-tx versionedWrites between txs (for per-tx BAL
// recording), so without this a later tx would not observe an earlier tx's state
// — the versionMap is the cross-tx carrier, matching how the parallel executor
// persists each committed tx before the next reads it.
func (sdb *IntraBlockState) FlushWritesToVersionMap() {
	if sdb.versionMap == nil {
		return
	}
	sdb.versionMap.FlushVersionedWrites(&sdb.versionedWrites, true, "")
}

// ApplyEIP6780StorageWipe zeroes the current tx's storage writes for any account
// it both created and self-destructed (EIP-6780/EIP-7928), so the BAL records the
// slots as reads (net-zero) rather than changes. On the noMaterialize builder path
// this replaces the stateObject-driven wipe MakeWriteSet performs; it is derived
// purely from the CreateContract/SelfDestruct write cells.
func (sdb *IntraBlockState) ApplyEIP6780StorageWipe() {
	sdb.versionedWrites.zeroSameTxCreateDestructStorage()
}

func (sdb *IntraBlockState) Print(chainRules chain.Rules, all bool) {
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		_, isDirty2 := sdb.journal.dirties[addr]

		printAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr, stateObject, all || isDirty || isDirty2)
	}
}

// SetTxContext sets the current transaction index which
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (sdb *IntraBlockState) SetTxContext(bn uint64, ti int) {
	/* Not sure what this test is for it seems to break some tests
	if len(sdb.logs) > 0 && ti == 0 {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. len(sdb.logs)=%d, ti=%d", len(sdb.logs), ti)
		panic(err)
	}
	if sdb.txIndex >= 0 && sdb.txIndex > ti {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. sdb.txIndex=%d, ti=%d", sdb.txIndex, ti)
		panic(err)
	}
	*/
	sdb.txIndex = ti
	sdb.blockNum = bn
	sdb.sdProbeEpoch++
}

// no not lock
func (sdb *IntraBlockState) clearJournalAndRefund() {
	sdb.journal.Reset()
	sdb.revisions = sdb.revisions.put()
	sdb.refund = uint64(0)
}

// Prepare handles the preparatory steps for executing a state transition.
// This method must be invoked before state transition.
//
// Berlin fork:
// - Add sender to access list (EIP-2929)
// - Add destination to access list (EIP-2929)
// - Add precompiles to access list (EIP-2929)
// - Add the contents of the optional txn access list (EIP-2930)
//
// Shanghai fork:
// - Add coinbase to access list (EIP-3651)
//
// Cancun fork:
// - Reset transient storage (EIP-1153)
//
// Prague fork:
// - Add authorities to access list (EIP-7702)
// - Add delegated designation (if it exists for dst) to access list (EIP-7702)
func (sdb *IntraBlockState) Prepare(rules *chain.Rules, sender, coinbase accounts.Address, dst accounts.Address,
	precompiles []accounts.Address, list types.AccessList, authorities []accounts.Address) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(sender.Handle()) || !dst.IsNil() && dbg.TraceAccount(dst.Handle())) {
		fmt.Printf("%d (%d.%d) ibs.Prepare: sender: %x, coinbase: %x, dest: %x, %x, %v, %v, %v\n", sdb.blockNum, sdb.txIndex, sdb.version, sender, coinbase, dst, precompiles, list, rules, authorities)
	}
	sdb.eip8246 = rules.IsAmsterdam
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		sdb.accessList.Reset()
		al := &sdb.accessList

		al.AddAddress(sender)
		if !dst.IsNil() {
			al.AddAddress(dst)
			// If it's a create-tx, the destination will be added inside evm.create
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
		if rules.IsShanghai { // EIP-3651: warm coinbase
			al.AddAddress(coinbase)
		}
	}
	if rules.IsPrague {
		for _, addr := range authorities {
			sdb.AddAddressToAccessList(addr)
		}

		if !dst.IsNil() {
			dd, ok, err := sdb.GetDelegatedDesignation(dst)
			if err != nil {
				return err
			}
			if ok {
				sdb.AddAddressToAccessList(dd)
			}
		}
	}
	// Reset transient storage at the beginning of transaction execution
	sdb.transientStorage = newTransientStorage()
	sdb.versionedReads.access = nil
	sdb.recordAccess = true

	// EIP-3651 makes the coinbase warm (Shanghai+). EIP-7928 BAL must include
	// it even when the block has no priority-fee transfer to the coinbase
	// (i.e. nothing else in the tx writes to the coinbase). Without this, txns
	// that produce no fee for the coinbase leave its address out of the BAL
	// and the validator-side BAL hash diverges from the spec sidecar.
	// recordAccess was just enabled and the access set reset, so
	// MarkAddressAccess will actually take effect here (unlike when called
	// from verifyAuthorities, which runs before Prepare).
	if rules.IsShanghai {
		sdb.MarkAddressAccess(coinbase, true)
	}
	// EIP-7702 authorities: txn_executor.verifyAuthorities calls
	// MarkAddressAccess for each recovered authority before Prepare runs, so
	// that mark is a no-op. Re-mark here so the authority is captured in the
	// BAL even when the EVM never touches the authority during execution
	// (e.g. authorization fails the nonce check after recovery).
	if rules.IsPrague {
		for _, addr := range authorities {
			sdb.MarkAddressAccess(addr, false)
		}
	}
	return nil
}

// AddAddressToAccessList adds the given address to the access list
func (sdb *IntraBlockState) AddAddressToAccessList(addr accounts.Address) (addrMod bool) {
	addrMod = sdb.accessList.AddAddress(addr)
	if addrMod {
		sdb.journal.append(accessListAddAccountChange{addr})
	}
	return addrMod
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (sdb *IntraBlockState) AddSlotToAccessList(addr accounts.Address, slot accounts.StorageKey) (addrMod, slotMod bool) {
	addrMod, slotMod = sdb.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		sdb.journal.append(accessListAddAccountChange{addr})
	}
	if slotMod {
		sdb.journal.append(accessListAddSlotChange{
			address: addr,
			slot:    slot,
		})
	}
	return addrMod, slotMod
}

// AddressInAccessList returns true if the given address is in the access list.
func (sdb *IntraBlockState) AddressInAccessList(addr accounts.Address) bool {
	return sdb.accessList.ContainsAddress(addr)
}

func (sdb *IntraBlockState) SlotInAccessList(addr accounts.Address, slot accounts.StorageKey) (addressPresent bool, slotPresent bool) {
	return sdb.accessList.Contains(addr, slot)
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
		}
	} else {
		sdb.versionedReads.access[addr] = &accessOptions{revertable}
	}
}

// MarkReadsInternal marks all versioned reads for addr as internal.
// Internal reads are kept for parallel-execution conflict detection
// but excluded from the block access list (BAL).  This is used when
// a state read was performed for gas calculation but the operation
// was rejected (e.g. CALL with value inside STATICCALL).
func (sdb *IntraBlockState) MarkReadsInternal(addr accounts.Address) {
	sdb.versionedReads.ScanAddr(addr, func(_ AccountPath, _ accounts.StorageKey, hdr *ReadHeader) {
		hdr.internal = true
	})
}

// AccessedAddresses returns and resets the set of addresses touched during the current transaction.
func (sdb *IntraBlockState) AccessedAddresses() AccessSet {
	access := sdb.versionedReads.access
	if len(access) == 0 {
		sdb.recordAccess = false
		sdb.versionedReads.access = nil
		return nil
	}
	out := make(AccessSet, len(access))
	maps.Copy(out, access)
	sdb.recordAccess = false
	sdb.versionedReads.access = nil
	return out
}

func (sdb *IntraBlockState) accountRead(addr accounts.Address, account *accounts.Account, source ReadSource, version Version) {
	if sdb.versionMap != nil {
		sdb.MarkAddressAccess(addr, true)
		if source == WriteSetRead {
			// A read satisfied by this tx's own earlier write carries no
			// cross-tx dependency; recording it would make the validator
			// (floored below the tx's own writes) return None and wrongly
			// invalidate the tx.
			return
		}
		data := *account
		// Demote a sub-field MapRead promotion when AddressPath itself has no cell,
		// or the validator non-converges on its recursive AddressPath check.
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

// recordWriteX helpers record a versioned write at the specified path
// directly into the typed per-path map.  No generic dispatcher / runtime
// type switch — each helper is monomorphic by path.

// recordWrite* — typed write recorders for each AccountPath.  Pool fast
// path: a repeat write to the same (addr[,key]) reuses the existing
// *VersionedWrite[T] in place (no alloc, no map churn).  Only the first
// write per (addr[,key]) per tx hits getVW* + SetX.  WriteSet.ReleaseAndReset
// returns every VW to its pool.

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

// versionedWriteSelfDestruct returns the SelfDestructPath write for addr
// in the dirty per-tx write set, if any.
// accountLifecycle returns the complete self-destruct verdict for the current
// tx, layering the tx's own field-level SelfDestruct write over the versionMap
// floor — the account-level analogue of what versionedReadCore does per field.
// It consults the read/write collections and the versionMap only, never the
// stateObject (whose deleted flag is a redundant cache of the own SelfDestruct
// write). An own-tx SelfDestruct write is authoritative (newest): true after a
// same-tx SD, false after a same-tx recreate. With no own write, the floor's
// destroyed-and-not-revived verdict applies.
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

// versionedWriteCreateContract reports whether this tx's own writes created a
// contract at addr (the CreateContract cell). Guarded by journal.dirties so a
// stale entry from a reverted create is ignored.
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

// reconstructCellFlags stamps the transient (uncached) stateObject's
// create/self-destruct flags from this tx's own versioned-write cells. Under
// noMaterialize the stateObject is rebuilt on every getStateObject call, so the
// createdContract / newlyCreated / selfdestructed state that a materialized
// object would have carried must be recovered from the cells instead.
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
	// The transient is rebuilt from the AddressPath account, whose CodeHash can
	// lag the CodePath/CodeHashPath cells (e.g. a delegation set by a prior tx
	// whose AddressPath record was published with an empty code hash). Seed the
	// code from this tx's own Code write, else the versionMap floor cell, so
	// object code reads (GetDelegatedDesignation, stateObject.Code()) agree with
	// the cells — matching the refresh-and-sync the fall-through getStateObject
	// path performs for accounts it materializes from scratch. An own write is
	// authoritative even when it clears code to empty (EIP-7702 delegation
	// clearing writes nil bytes / EmptyCodeHash); falling through to the floor
	// there would resurrect the prior-tx delegation.
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
	code, _, _, err := refreshCode(sdb, addr)
	if err != nil || code == nil {
		return
	}
	codeHash := accounts.InternCodeHash(crypto.HashData(code))
	obj.code = accounts.Code{Hash: codeHash, Bytes: code}
	obj.data.CodeHash = codeHash
	obj.original.CodeHash = codeHash
}

// versionedWriteHit probes the dirty per-tx write set for a write at
// (addr, path, key) and, when present, populates the corresponding
// per-typed pointer field on r.  Returns true when a write was found.
// The non-storage paths share a single non-nil typed field; the storage
// path uses r.vwStorage.
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

// VersionedReads returns the in-flight per-path read set.  The returned
// value shares the underlying maps with the IBS; it is handed over at
// end of tx (RecordReads / TxIn), after which ResetVersionedIO rebinds
// the IBS field to a fresh set.
func (sdb *IntraBlockState) VersionedReads() ReadSet {
	return sdb.versionedReads
}

func (sdb *IntraBlockState) ResetVersionedIO() {
	sdb.versionedReads = ReadSet{}
	sdb.versionedWrites.ReleaseAndReset()
	sdb.dep = UnknownDep
	sdb.recordAccess = false
}

// ResetVersionedReads clears tracked versioned reads without affecting writes.
func (sdb *IntraBlockState) ResetVersionedReads() {
	sdb.versionedReads = ReadSet{}
}

// VersionedWrites returns a frozen typed snapshot of this tx's recorded writes.
// The snapshot logic lives on the write-set itself (WriteSet.Snapshot); this is
// the IntraBlockState accessor for it.
func (sdb *IntraBlockState) VersionedWrites() *WriteSet {
	return sdb.versionedWrites.Snapshot()
}

// Apply entries in a given write set to StateDB. Note that this function does not change MVHashMap nor write set
// of the current StateDB.
func (sdb *IntraBlockState) ApplyVersionedWrites(writes *WriteSet) error {
	if writes == nil {
		return nil
	}
	// Deterministic (Address, Path, Key) order: Code/SelfDestruct load the state
	// object and may record an extra read depending on whether a prior
	// same-address write already loaded it, which changes the EIP-7928 BAL hash.
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
			// Force-set code bypassing stateObject.SetCode's equality check.
			// The finalize IBS uses a VersionedStateReader whose ReadSet may
			// contain the post-write code value (when the worker read the code
			// after a SetCodeTx modified it), causing SetCode's equality
			// comparison to incorrectly skip the update and leave dirtyCode unset.
			sdb.journal.append(codeChange{
				account:     addr,
				prevhash:    stateObject.data.CodeHash,
				prevcode:    stateObject.code.Bytes,
				wasCommited: !sdb.hasWrite(addr, CodePath, accounts.NilKey),
			})
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
				// Ensure the state object exists before calling Selfdestruct.
				// For newly-created accounts (with no pre-block DB entry)
				// getStateObject returns nil and Selfdestruct silently no-ops, so
				// materialize the object first to keep the selfdestructed marking.
				if _, err := sdb.GetOrNewStateObject(addr); err != nil {
					return err
				}
				if _, err := sdb.Selfdestruct(addr, true); err != nil {
					return err
				}
			} else {
				// SelfDestructPath=false indicates account resurrection in this block.
				// The worker IBS set createdContract=true (ensuring CreateContract is called
				// during commit to clear old storage), but that flag is not a versioned write
				// path and is lost in the finalize IBS.
				so, err := sdb.GetOrNewStateObject(addr)
				if err != nil {
					return err
				}
				if so != nil {
					so.selfdestructed = false
					so.createdContract = true
				}
				// Re-emit SelfDestructPath=false so the global versionMap reflects the
				// resurrection; subsequent workers reading SelfDestructPath will see the
				// updated value and not mistake the account for still being selfdestructed.
				sdb.recordWriteSelfDestruct(addr, false)
			}
		case CreateContractPath:
			// Contract creation: set createdContract flag on the stateObject.
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
