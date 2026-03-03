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
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
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
	for addr, opt := range aa {
		dst[addr] = opt
	}
	for addr, opt := range other {
		dst[addr] = opt
	}
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
	accessList *accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal       *journal
	revisions     *revisions
	trace         bool
	tracingHooks  *tracing.Hooks
	balanceInc    map[accounts.Address]*BalanceIncrease // Map of balance increases (without first reading the account)
	addressAccess AccessSet
	recordAccess  bool

	// Versioned storage used for parallel tx processing, versions
	// are maintaned across transactions until they are reset
	// at the block level
	versionMap          *VersionMap
	versionedWrites     WriteSet
	versionedReads      ReadSet
	accountReadDuration time.Duration
	accountReadCount    int64
	storageReadDuration time.Duration
	storageReadCount    int64
	codeReadDuration    time.Duration
	codeReadCount       int64
	version             int
	dep                 int
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
		accessList:        newAccessList(),
		transientStorage:  newTransientStorage(),
		balanceInc:        map[accounts.Address]*BalanceIncrease{},
		addressAccess:     nil,
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
	_, ok := sdb.versionedWrites[addr][AccountKey{path, key}]
	return ok
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
	// return false.  This mirrors the same check in versionedRead for
	// StoragePath (versionedio.go:660-703).
	if sdb.versionMap != nil {
		incRes := sdb.versionMap.Read(addr, IncarnationPath, accounts.NilKey, sdb.txIndex)
		if incRes.Status() == MVReadResultDone {
			// Record IncarnationPath dependency for validation.
			if sdb.versionedReads == nil {
				sdb.versionedReads = ReadSet{}
			}
			sdb.versionedReads.Set(VersionedRead{
				Address: addr,
				Path:    IncarnationPath,
				Key:     accounts.NilKey,
				Source:  MapRead,
				Version: Version{TxIndex: incRes.DepIdx(), Incarnation: incRes.Incarnation()},
				Val:     incRes.Value(),
			})
			return false, nil
		}
	}

	// Otherwise check in the DB
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
	sdb.refund = 0
	sdb.txIndex = 0
	sdb.logSize = 0
	sdb.versionMap = nil
	sdb.versionedReads = nil
	sdb.versionedWrites = nil
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
	log.TxIndex = uint(sdb.txIndex)
	log.Index = sdb.logSize
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
		l.BlockNumber = blockNumber
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

	if s, ok := sdb.stateObjects[addr]; ok {
		return s != nil && !s.deleted, nil
	}

	readAccount, _, _, err := sdb.getVersionedAccount(addr, true)
	if err != nil {
		return false, err
	}

	// Same-tx self-destruct: the account is still alive (EIP-6780).
	// Cross-tx self-destruct: getVersionedAccount returns nil.
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
	if so, ok := sdb.stateObjects[addr]; ok {
		return so == nil || so.deleted || so.data.Empty(), nil
	}

	account, accountSource, accountVersion, err := sdb.getVersionedAccount(addr, true)
	if err != nil {
		return false, err
	}
	if account == nil {
		sdb.touchAccount(addr)
		sdb.accountRead(addr, &emptyAccount, accountSource, accountVersion)
	}
	// Do not use SelfDestructPath here: a self-destructed account is still
	// "alive" during the same tx (EIP-6780) and should not appear empty
	// until end-of-tx cleanup.  Cross-tx destructs are already handled by
	// getVersionedAccount returning nil (the versionedRead short-circuit
	// returns default values for all paths when a previous tx destroyed the
	// account).
	return account == nil || account.Empty(), nil
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
				fmt.Printf("%d (%d.%d) GetBalance %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &balance)
			}
			return stateObject.Balance(), true, nil
		}
		return u256.Num0, false, nil
	}

	balance, source, _, err := versionedRead(sdb, addr, BalancePath, accounts.NilKey, false, uint256.Int{},
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			if s != nil && !s.deleted {
				return s.Balance(), nil
			}
			return uint256.Int{}, nil
		})

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetBalance %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &balance)
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

	nonce, _, _, err := versionedRead(sdb, addr, NoncePath, accounts.NilKey, false, 0,
		func(v uint64) uint64 { return v },
		func(s *stateObject) (uint64, error) {
			if s != nil && !s.deleted {
				return s.Nonce(), nil
			}
			return 0, nil
		})

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
			return code, err
		}
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, StorageRead, addr, 0)
		}
		return nil, nil
	}
	// When commited=true (used by ResolveCode for EIP-7702 delegation),
	// versionedRead skips local versionedWrites and may return a stale
	// ReadSet value. If the CURRENT tx has set this account's code (e.g.,
	// via EIP-7702 authorization processing), return the dirty code directly.
	// We must also check hasWrite to ensure the code was set in this tx,
	// not in a previous tx sharing the same IBS (block generator reuses IBS).
	if commited {
		if so, ok := sdb.stateObjects[addr]; ok && so.dirtyCode && sdb.hasWrite(addr, CodePath, accounts.NilKey) {
			return so.code, nil
		}
	}
	code, source, _, err := versionedRead(sdb, addr, CodePath, accounts.NilKey, commited, nil,
		func(v []byte) []byte {
			return v
		},
		func(s *stateObject) ([]byte, error) {
			if s != nil && !s.deleted {
				code, err := s.Code()
				return code, err
			}
			return nil, nil
		})

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		if err != nil {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: err: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, err)
		} else {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, len(code))
		}
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
		if stateObject.code != nil {
			return len(stateObject.code), nil
		}
		if stateObject.data.CodeHash.IsEmpty() {
			return 0, nil
		}
		return sdb.stateReader.ReadAccountCodeSize(addr)
	}

	size, source, _, err := versionedRead(sdb, addr, CodeSizePath, accounts.NilKey, false, 0,
		func(v int) int { return v },
		func(s *stateObject) (int, error) {
			if s == nil || s.deleted {
				return 0, nil
			}
			if s.code != nil {
				return len(s.code), nil
			}
			if s.data.CodeHash.IsEmpty() {
				return 0, nil
			}
			if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle()))) {
				sdb.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", sdb.blockNum, sdb.txIndex, sdb.version))
			}
			var readStart time.Time
			if dbg.KVReadLevelledMetrics {
				readStart = time.Now()
			}
			l, err := sdb.stateReader.ReadAccountCodeSize(addr)
			if dbg.KVReadLevelledMetrics {
				sdb.codeReadDuration += time.Since(readStart)
				sdb.codeReadCount++
			}
			sdb.stateReader.SetTrace(false, "")
			if err != nil {
				return l, err
			}
			return l, err
		})

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetCodeSize (%s) %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, size)
	}

	return size, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
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

	hash, _, _, err := versionedRead(sdb, addr, CodeHashPath, accounts.NilKey, false, accounts.NilCodeHash,
		func(v accounts.CodeHash) accounts.CodeHash { return v },
		func(s *stateObject) (accounts.CodeHash, error) {
			if s == nil || s.deleted {
				return accounts.NilCodeHash, nil
			}
			return s.data.CodeHash, nil
		})
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
	// CodePath exemptions in versionedRead already handle SelfDestruct cases.
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
			return delegation, true, nil
		}
	}
	return accounts.ZeroAddress, false, nil
}

// GetState retrieves a value from the given account's storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := versionedRead(sdb, addr, StoragePath, key, false, u256.N0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			var value uint256.Int
			if s != nil && !s.deleted {
				value, _ = s.GetState(key)
			}
			return value, nil
		})

	if dbg.TraceTransactionIO && (sdb.trace || (dbg.TraceAccount(addr.Handle()) && traceKey(key))) {
		fmt.Printf("%d (%d.%d) GetState (%s) %x, %x=%x\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, key, &versionedValue)
	}

	return versionedValue, err
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCommittedState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := versionedRead(sdb, addr, StoragePath, key, true, u256.N0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			var value uint256.Int
			if s != nil && !s.deleted {
				return s.GetCommittedState(key)
			}
			return value, nil
		})

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetCommittedState (%s) %x, %x=%x\n", sdb.blockNum, sdb.txIndex, sdb.version, source, addr, key, &versionedValue)
	}

	return versionedValue, err
}

func (sdb *IntraBlockState) HasSelfdestructed(addr accounts.Address) (bool, error) {
	destructed, _, _, err := versionedRead(sdb, addr, SelfDestructPath, accounts.NilKey, false, false,
		func(v bool) bool { return v },
		func(s *stateObject) (bool, error) {
			if s == nil {
				return false, nil
			}
			if s.deleted {
				return false, nil
			}
			if s.createdContract {
				return false, nil
			}
			return s.selfdestructed, nil
		})

	return destructed, err
}

func (sdb *IntraBlockState) ReadVersion(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) ReadResult {
	return sdb.versionMap.Read(addr, path, key, txIdx)
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
		stateObject, err := sdb.GetOrNewStateObject(addr)
		if err != nil {
			return err
		}

		if stateObject.data.Empty() {
			versionWritten(sdb, addr, BalancePath, accounts.NilKey, uint256.Int{})
			if _, ok := sdb.journal.dirties[addr]; !ok {
				if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) Touch %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
				}
				sdb.touchAccount(addr)
			}
		}

		// BAL: record coinbase/selfdestruct recipients even with 0 value
		sdb.MarkAddressAccess(addr, true)
		return nil
	}

	prev, wasCommited, _ := sdb.getBalance(addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			prev := prev     // avoid capture allocation unless we're tracing
			amount := amount // avoid capture allocation unless we're tracing
			expected := (&uint256.Int{}).Add(&prev, &amount)
			if bal.Cmp(expected) != 0 {
				panic(fmt.Sprintf("add failed: expected: %d got: %d", expected, &bal))
			}
			fmt.Printf("%d (%d.%d) AddBalance %x, %d+%d=%d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &prev, &amount, &bal)
		}()
	}

	update := u256.Add(prev, amount)

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(update, wasCommited, reason)
	versionWritten(sdb, addr, BalancePath, accounts.NilKey, update)
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

func (sdb *IntraBlockState) getVersionedAccount(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	if sdb.versionMap == nil {
		return nil, UnknownSource, UnknownVersion, nil
	}

	readAccount, source, version, err := versionedRead(sdb, addr, AddressPath, accounts.NilKey, false, nil,
		func(v *accounts.Account) *accounts.Account { return v }, nil)

	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}

	if readAccount == nil {
		if readStorage {
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
			source = StorageRead
			sdb.stateReader.SetTrace(false, "")
		}

		if readAccount == nil || err != nil {
			return nil, StorageRead, UnknownVersion, err
		}
	}

	return sdb.refreshVersionedAccount(addr, readAccount, source, version)
}

func (sdb *IntraBlockState) refreshVersionedAccount(addr accounts.Address, readAccount *accounts.Account, readSource ReadSource, readVersion Version) (*accounts.Account, ReadSource, Version, error) {
	account := readAccount
	version := readVersion
	source := readSource

	balance, bsource, bversion, err := versionedRead(sdb, addr, BalancePath, accounts.NilKey, false, account.Balance, nil, nil)
	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}
	if bversion.TxIndex > readVersion.TxIndex || (bversion.TxIndex == readVersion.TxIndex && bversion.Incarnation >= readVersion.Incarnation) {
		if balance.Cmp(&account.Balance) != 0 {
			if account == readAccount {
				account = &accounts.Account{}
				account.Copy(readAccount)
			}
			account.Balance = balance
		}
		if bversion.TxIndex > version.TxIndex || (bversion.TxIndex == version.TxIndex && bversion.Incarnation > version.Incarnation) {
			version = bversion
			if bsource != source {
				source = bsource
			}
		}
	}

	nonce, nsource, nversion, err := versionedRead(sdb, addr, NoncePath, accounts.NilKey, false, account.Nonce, nil, nil)
	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}
	if nversion.TxIndex > readVersion.TxIndex || (nversion.TxIndex == readVersion.TxIndex && nversion.Incarnation >= readVersion.Incarnation) {
		if nonce > account.Nonce {
			if account == readAccount {
				account = &accounts.Account{}
				account.Copy(readAccount)
			}
			account.Nonce = nonce
		}
		if nversion.TxIndex > version.TxIndex || (nversion.TxIndex == version.TxIndex && nversion.Incarnation > version.Incarnation) {
			version = nversion
			if nsource != source {
				source = nsource
			}
		}
	}

	incarnation, isource, iversion, err := versionedRead(sdb, addr, IncarnationPath, accounts.NilKey, false, account.Incarnation, nil, nil)
	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}
	if iversion.TxIndex > readVersion.TxIndex || (iversion.TxIndex == readVersion.TxIndex && iversion.Incarnation >= readVersion.Incarnation) {
		if incarnation > account.Incarnation {
			if account == readAccount {
				account = &accounts.Account{}
				account.Copy(readAccount)
			}
			account.Incarnation = incarnation
		}
		if iversion.TxIndex > version.TxIndex || (iversion.TxIndex == version.TxIndex && iversion.Incarnation > version.Incarnation) {
			version = iversion
			if isource != source {
				source = isource
			}
		}
	}

	codeHash, csource, cversion, err := versionedRead(sdb, addr, CodeHashPath, accounts.NilKey, false, account.CodeHash, nil, nil)
	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}

	if cversion.TxIndex > readVersion.TxIndex || (cversion.TxIndex == readVersion.TxIndex && cversion.Incarnation >= readVersion.Incarnation) {
		if codeHash != account.CodeHash {
			if account == readAccount {
				account = &accounts.Account{}
				account.Copy(readAccount)
			}
			account.CodeHash = codeHash
		}
		if cversion.TxIndex > version.TxIndex || (cversion.TxIndex == version.TxIndex && cversion.Incarnation > version.Incarnation) {
			version = cversion
			if csource != source {
				source = csource
			}
		}
	}

	return account, source, version, nil
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SubBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if amount.IsZero() && addr != params.SystemAddress {
		// We skip this early exit if the sender is the system address
		// because Gnosis has a special logic to create an empty system account
		// even after Spurious Dragon (see PR 5645 and Issue 18276).
		return nil
	}

	prev, wasCommited, _ := sdb.getBalance(addr)

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			prev := prev     // avoid capture allocation unless we're tracing
			amount := amount // avoid capture allocation unless we're tracing
			fmt.Printf("%d (%d.%d) SubBalance %x, %d-%d=%d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &prev, &amount, &bal)
		}()
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	update := u256.Sub(prev, amount)
	stateObject.SetBalance(update, wasCommited, reason)
	if sdb.versionMap != nil {
		versionWritten(sdb, addr, BalancePath, accounts.NilKey, update)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		amount := amount
		fmt.Printf("%d (%d.%d) SetBalance %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &amount)
	}
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(amount, !sdb.hasWrite(addr, BalancePath, accounts.NilKey), reason)
	versionWritten(sdb, addr, BalancePath, accounts.NilKey, stateObject.Balance())
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetNonce(addr accounts.Address, nonce uint64) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetNonce %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, nonce)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}

	stateObject.SetNonce(nonce, !sdb.hasWrite(addr, NoncePath, accounts.NilKey))
	versionWritten(sdb, addr, NoncePath, accounts.NilKey, stateObject.Nonce())
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
func (sdb *IntraBlockState) SetCode(addr accounts.Address, code []byte) error {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		lenc, cs := printCode(code)
		fmt.Printf("%d (%d.%d) SetCode %x, %d: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, lenc, cs)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
	written, err := stateObject.SetCode(codeHash, code, !sdb.hasWrite(addr, CodePath, accounts.NilKey))
	if err != nil {
		return err
	}
	if written {
		if codeHash == stateObject.original.CodeHash {
			sdb.versionedWrites.Delete(addr, AccountKey{Path: CodePath})
			sdb.versionedWrites.Delete(addr, AccountKey{Path: CodeHashPath})
			sdb.versionedWrites.Delete(addr, AccountKey{Path: CodeSizePath})
		} else {
			versionWritten(sdb, addr, CodePath, accounts.NilKey, code)
			versionWritten(sdb, addr, CodeHashPath, accounts.NilKey, codeHash)
			versionWritten(sdb, addr, CodeSizePath, accounts.NilKey, len(code))
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
		versionWritten(sdb, addr, StoragePath, key, value)
	}
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
		versionWritten(sdb, addr, IncarnationPath, accounts.NilKey, stateObject.data.Incarnation)
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

	incarnation, _, _, err := versionedRead(sdb, addr, IncarnationPath, accounts.NilKey, false, 0,
		func(v uint64) uint64 { return v },
		func(s *stateObject) (uint64, error) {
			if s != nil && !s.deleted {
				return s.data.Incarnation, nil
			}
			return 0, nil
		})

	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetIncarnation %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, incarnation)
	}

	return incarnation, err
}

// Selfdestruct marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (sdb *IntraBlockState) Selfdestruct(addr accounts.Address) (bool, error) {
	if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SelfDestruct %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
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

	if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		sdb.tracingHooks.OnBalanceChange(addr, prevBalance, zeroBalance, tracing.BalanceDecreaseSelfdestruct)
	}

	stateObject.markSelfdestructed()
	stateObject.createdContract = false
	stateObject.data.Balance.Clear()

	versionWritten(sdb, addr, IncarnationPath, accounts.NilKey, stateObject.data.Incarnation)
	versionWritten(sdb, addr, SelfDestructPath, accounts.NilKey, stateObject.selfdestructed)
	versionWritten(sdb, addr, BalancePath, accounts.NilKey, uint256.Int{})

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
			readAccount, accountSource, accountVersion, err = versionedRead[*accounts.Account](sdb, addr, AddressPath, accounts.NilKey, false, nil, nil, nil)

			if readAccount == nil || err != nil {
				return nil, err
			}

			destructed, _, _, err := versionedRead(sdb, addr, SelfDestructPath, accounts.NilKey, false, false, nil, nil)

			if destructed || err != nil {
				so := stateObjectPool.Get().(*stateObject)
				so.db = sdb
				so.address = addr
				so.selfdestructed = destructed
				so.deleted = destructed
				sdb.setStateObject(addr, so)
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
		account, accountSource, accountVersion, err = sdb.refreshVersionedAccount(addr, readAccount, accountSource, accountVersion)
		if err != nil {
			return nil, err
		}

		// Check if a prior tx selfdestructed this account. The AddressPath
		// versionedRead above returned nil (SelfDestructPath early-exit), but
		// stateReader returned a committed value from SharedDomains. Read
		// SelfDestructPath directly from the versionMap (not via versionedRead
		// which itself short-circuits on the same flag). Use the same pattern
		// as CreateAccount (line 1628).
		if res := sdb.versionMap.Read(addr, SelfDestructPath, accounts.NilKey, sdb.txIndex); res.Status() == MVReadResultDone {
			if destructed, ok := res.Value().(bool); ok && destructed {
				// Only honour if the current tx hasn't already resurrected.
				localResurrected := false
				if vw, ok := sdb.versionedWrite(addr, SelfDestructPath, accounts.NilKey); ok {
					if v, ok := vw.Val.(bool); ok && !v {
						localResurrected = true
					}
				}
				if !localResurrected {
					so := stateObjectPool.Get().(*stateObject)
					so.db = sdb
					so.address = addr
					so.selfdestructed = true
					so.deleted = true
					sdb.setStateObject(addr, so)
					return nil, nil
				}
			}
		}

		code, _, _, err = versionedRead[[]byte](sdb, addr, CodePath, accounts.NilKey, false, nil, nil, nil)
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
		obj.code = code
		// When code is loaded from the version map (written by a prior tx),
		// synchronise the stateObject's CodeHash with the actual code.
		// refreshVersionedAccount above may not have updated the account's
		// CodeHash because the base-reader version (sdb.Version()) makes the
		// version check (cversion.TxIndex > readVersion.TxIndex) fail for
		// entries from earlier transactions.  Without this fix, the stale
		// CodeHash causes the "revert to original" optimisation in SetCode
		// to incorrectly delete code writes when clearing a delegation that
		// was set by a prior transaction in the same block.
		codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
		if codeHash != obj.data.CodeHash {
			obj.data.CodeHash = codeHash
			obj.original.CodeHash = codeHash
		}
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
		sdb.journal.append(resetObjectChange{account: addr, prev: previous})
	}
	newobj.newlyCreated = true
	sdb.setStateObject(addr, newobj)
	data := newobj.data
	versionWritten(sdb, addr, AddressPath, accounts.NilKey, &data)
	// Write CodeHashPath so that any stale versionedReads cache entry
	// (e.g. from the pre-creation GetCodeHash check in EVM create()) is
	// invalidated.  newObject normalises the zero-value CodeHash to
	// EmptyCodeHash, so this records keccak256("") for a fresh account.
	versionWritten(sdb, addr, CodeHashPath, accounts.NilKey, newobj.data.CodeHash)
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
				fmt.Printf("%d (%d.%d) Create Account%s: %x, balance=%d\n", sdb.blockNum, sdb.txIndex, sdb.version, creatingContract, addr, &bal)
			}
		}()
	}

	source := StorageRead
	version := UnknownVersion

	if sdb.versionMap == nil {
		previous, err = sdb.getStateObject(addr, true)
		if err != nil {
			return err
		}
	} else {
		readAccount, accountSource, accountVersion, err := sdb.getVersionedAccount(addr, true)

		if err != nil {
			return err
		}

		if readAccount != nil {
			account := readAccount

			destructed, _, _, err := versionedRead(sdb, addr, SelfDestructPath, accounts.NilKey, false, false,
				func(v bool) bool { return v }, nil)

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
					// account as selfdestructed but versionedRead returned false due to the
					// so.deleted early exit.  Reuse the cached stateObject to preserve the
					// correct selfdestructed flag and accumulated incarnation.
					previous = so
					source = accountSource
					version = accountVersion
				} else if sdb.versionMap != nil {
					// Fresh-IBS worker path (e.g. InsertChain parallel executor): no stateObjects
					// cache, but the versionMap may have SelfDestructPath=true from a prior tx.
					// versionedRead returns false for SelfDestructPath via the early-exit at
					// lines 459-462 — bypass it here so we correctly set selfdestructed=true.
					if res := sdb.versionMap.Read(addr, SelfDestructPath, accounts.NilKey, sdb.txIndex); res.Status() == MVReadResultDone && res.value.(bool) {
						destructed = true
					}
				}
			}

			if previous == nil {
				previous = newObject(sdb, addr, account, account)
				previous.selfdestructed = destructed
				source = accountSource
				version = accountVersion
			}
		} else if so, ok := sdb.stateObjects[addr]; ok && so.deleted {
			// The account was selfdestructed in an earlier transaction within the
			// same block (accumulated IBS, e.g. GenerateChain) AND the underlying
			// storage has no record of it (e.g. it was created within this block).
			// getVersionedAccount returned nil; preserve the deleted stateObject as
			// `previous` so that after a REVERT CommitBlock can still emit
			// DeleteAccount for it.
			previous = so
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
	// Read IncarnationPath directly from the versionMap to get the
	// incarnation written by the prior CreateAccount, giving the correct prevInc.
	if sdb.versionMap != nil && (previous == nil || previous.selfdestructed) {
		if res := sdb.versionMap.Read(addr, IncarnationPath, accounts.NilKey, sdb.txIndex); res.Status() == MVReadResultDone {
			if inc, ok := res.value.(uint64); ok && inc > prevInc {
				prevInc = inc
			}
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

	newObj := sdb.createObject(addr, previous)
	if previous != nil && !previous.selfdestructed {
		newObj.data.Balance.Set(&previous.data.Balance)
	}
	newObj.data.PrevIncarnation = prevInc

	if contractCreation {
		newObj.createdContract = true
		newObj.data.Incarnation = prevInc + 1
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) New Incarnation %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, newObj.data.Incarnation)
		}
	} else {
		newObj.selfdestructed = false
	}

	// for newly created accounts these synthetic read/writes are used so that account
	// creation clashes between trnascations get detected
	versionRead[uint256.Int](sdb, addr, BalancePath, accounts.NilKey, source, version, newObj.Balance())
	versionRead[uint256.Int](sdb, addr, IncarnationPath, accounts.NilKey, source, version, prevInc)
	versionWritten(sdb, addr, BalancePath, accounts.NilKey, newObj.Balance())
	versionWritten(sdb, addr, IncarnationPath, accounts.NilKey, newObj.data.Incarnation)
	if previous == nil || previous.selfdestructed && !newObj.selfdestructed {
		versionWritten(sdb, addr, SelfDestructPath, accounts.NilKey, false)
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

func updateAccount(EIP161Enabled bool, isAura bool, stateWriter StateWriter, addr accounts.Address, stateObject *stateObject, isDirty bool, trace bool, tracingHooks *tracing.Hooks) error {
	emptyRemoval := EIP161Enabled && stateObject.data.Empty() && (!isAura || addr != params.SystemAddress)
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
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
	if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		stateObject.deleted = false
		// Write any contract code associated with the state object
		if stateObject.code != nil && stateObject.dirtyCode {
			if err := stateWriter.UpdateAccountCode(addr, stateObject.data.Incarnation, stateObject.data.CodeHash, stateObject.code); err != nil {
				return err
			}
		}
		if stateObject.createdContract {
			if err := stateWriter.CreateContract(addr); err != nil {
				return err
			}
		}
		if err := stateObject.updateStorage(stateWriter); err != nil {
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
	}
	return nil
}

func printAccount(EIP161Enabled bool, addr accounts.Address, stateObject *stateObject, isDirty bool) {
	emptyRemoval := EIP161Enabled && stateObject.data.Empty()
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
		fmt.Printf("delete: %x\n", addr)
	}
	if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		// Write any contract code associated with the state object
		if stateObject.code != nil && stateObject.dirtyCode {
			fmt.Printf("UpdateCode: %x,%x\n", addr, stateObject.data.CodeHash)
		}
		if stateObject.createdContract {
			fmt.Printf("CreateContract: %x\n", addr)
		}
		stateObject.printTrie()
		fmt.Printf("UpdateAccountData: %x, balance=%d, nonce=%d\n", addr, &stateObject.data.Balance, stateObject.data.Nonce)
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

		if err := updateAccount(chainRules.IsSpuriousDragon, chainRules.IsAura, stateWriter, addr, so, true, sdb.trace, sdb.tracingHooks); err != nil {
			return err
		}

		so.newlyCreated = false
		sdb.stateObjectsDirty[addr] = struct{}{}
		if so.deleted {
			delete(sdb.versionedReads, addr)
		}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

// GetRemovedAccountsWithBalance returns a list of accounts scheduled for
// removal which still have positive balance. The purpose of this function is
// to handle a corner case of EIP-7708 where a self-destructed account might
// still receive funds between sending/burning its previous balance and actual
// removal. In this case the burning of these remaining balances still need to
// be logged.
// Specification EIP-7708: https://eips.ethereum.org/EIPS/eip-7708
func (sdb *IntraBlockState) GetRemovedAccountsWithBalance() (list []evmtypes.AddressAndBalance) {
	for addr := range sdb.journal.dirties {
		if obj, exist := sdb.stateObjects[addr]; exist && obj.selfdestructed {
			if balance := obj.Balance(); !balance.IsZero() {
				list = append(list, evmtypes.AddressAndBalance{Address: obj.address.Value(), Balance: balance})
			}
		}
	}
	return list
}

func (sdb *IntraBlockState) SoftFinalise() {
	for addr := range sdb.journal.dirties {
		_, exist := sdb.stateObjects[addr]
		if !exist {
			// ripeMD is 'touched' at block 1714175, in txn 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That txn goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `sdb.journal.dirties` but not in `sdb.stateObjects`.
			// Thus, we can safely ignore it here
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
			if sdb.versionedWrites != nil {
				if w, ok := sdb.versionedWrites[addr][AccountKey{Path: BalancePath}]; ok {
					val := w.Val.(uint256.Int)
					updated = &val
				}
			}
			var dirty string
			if isDirty {
				dirty = " (dirty)"
			}
			if updated != nil {
				fmt.Printf("%d (%d.%d) Updated Balance: %x%s: %d (%d)\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, dirty, &stateObject.data.Balance, updated)
			} else {
				fmt.Printf("%d (%d.%d) Updated Balance: %x%s: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, dirty, &stateObject.data.Balance)
			}
		}
		if dbg.TraceTransactionIO && (sdb.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) Update Account %x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr)
		}
		if err := updateAccount(chainRules.IsSpuriousDragon, chainRules.IsAura, stateWriter, addr, stateObject, isDirty, sdb.trace, sdb.tracingHooks); err != nil {
			return err
		}
	}

	var reverted []accounts.Address

	for addr := range sdb.versionedWrites {
		if _, isDirty := sdb.stateObjectsDirty[addr]; !isDirty {
			reverted = append(reverted, addr)
		}
	}

	for _, addr := range reverted {
		sdb.versionMap.DeleteAll(addr, sdb.txIndex)
		delete(sdb.versionedWrites, addr)
	}

	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

func (sdb *IntraBlockState) TxIO() *VersionedIO {
	var io VersionedIO
	version := Version{BlockNum: sdb.blockNum, TxIndex: sdb.txIndex, Incarnation: sdb.version}
	io.RecordReads(version, sdb.versionedReads)
	io.RecordWrites(version, sdb.VersionedWrites(false))
	io.RecordAccesses(version, sdb.addressAccess)
	return &io
}

func (sdb *IntraBlockState) Print(chainRules chain.Rules, all bool) {
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		_, isDirty2 := sdb.journal.dirties[addr]

		printAccount(chainRules.IsSpuriousDragon, addr, stateObject, all || isDirty || isDirty2)
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
}

// no not lock
func (sdb *IntraBlockState) clearJournalAndRefund() {
	sdb.journal.Reset()
	sdb.revisions = sdb.revisions.put()
	sdb.refund = 0
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
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := newAccessList()
		sdb.accessList = al
		//sdb.accessList.Reset()
		//al := sdb.accessList

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
	sdb.addressAccess = make(map[accounts.Address]*accessOptions)
	sdb.recordAccess = true
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
	if !sdb.recordAccess || sdb.addressAccess == nil {
		return
	}
	if opts, ok := sdb.addressAccess[addr]; ok {
		if opts.revertable && !revertable {
			opts.revertable = false
		}
	} else {
		sdb.addressAccess[addr] = &accessOptions{revertable}
	}
}

// AccessedAddresses returns and resets the set of addresses touched during the current transaction.
func (sdb *IntraBlockState) AccessedAddresses() AccessSet {
	if len(sdb.addressAccess) == 0 {
		sdb.recordAccess = false
		sdb.addressAccess = nil
		return nil
	}
	out := make(AccessSet, len(sdb.addressAccess))
	for addr := range sdb.addressAccess {
		out[addr] = nil
	}
	sdb.recordAccess = false
	sdb.addressAccess = nil
	return out
}

func (sdb *IntraBlockState) accountRead(addr accounts.Address, account *accounts.Account, source ReadSource, version Version) {
	if sdb.versionMap != nil {
		data := *account
		versionRead[*accounts.Account](sdb, addr, AddressPath, accounts.NilKey, source, version, &data)
	}
}

func versionWritten[T any](sdb *IntraBlockState, addr accounts.Address, path AccountPath, key accounts.StorageKey, val T) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap != nil {
		if sdb.versionedWrites == nil {
			sdb.versionedWrites = WriteSet{}
		}

		val := val // avoid escape if no versioned map

		vw := VersionedWrite{
			Address: addr,
			Path:    path,
			Key:     key,
			Version: sdb.Version(),
			Val:     val,
		}

		sdb.versionedWrites.Set(vw)

		if dbg.TraceTransactionIO && (sdb.trace || (dbg.TraceAccount(addr.Handle()) && (key == accounts.NilKey || traceKey(key)))) {
			fmt.Printf("%d (%d.%d) WRT %s\n", sdb.blockNum, sdb.txIndex, sdb.version, vw.String())
		}
	}
}

func versionRead[T any](sdb *IntraBlockState, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version, val any) {
	sdb.MarkAddressAccess(addr, true)
	if sdb.versionMap != nil {
		if sdb.versionedReads == nil {
			sdb.versionedReads = ReadSet{}
		}

		sdb.versionedReads.Set(VersionedRead{
			Address: addr,
			Path:    path,
			Key:     key,
			Source:  source,
			Version: version,
			Val:     val,
		})
	}
}

func (sdb *IntraBlockState) versionedWrite(addr accounts.Address, path AccountPath, key accounts.StorageKey) (*VersionedWrite, bool) {
	if sdb.versionMap == nil || sdb.versionedWrites == nil {
		return nil, false
	}

	v, ok := sdb.versionedWrites[addr][AccountKey{Path: path, Key: key}]

	if !ok {
		return nil, ok
	}

	if _, isDirty := sdb.journal.dirties[addr]; !isDirty {
		return nil, false
	}

	return v, ok
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

func (sdb *IntraBlockState) VersionedReads() ReadSet {
	return sdb.versionedReads
}

func (sdb *IntraBlockState) ResetVersionedIO() {
	sdb.versionedReads = nil
	sdb.versionedWrites = nil
	sdb.dep = UnknownDep
	sdb.recordAccess = false
	sdb.addressAccess = nil
}

// VersionedWrites returns the current versioned write set if this block
// checkDirty - is mainly for testing, for block processing this is called
// after the block execution is completed and non dirty writes (due to reversions)
// will already have been cleaned in MakeWriteSet
func (sdb *IntraBlockState) VersionedWrites(checkDirty bool) VersionedWrites {
	var writes VersionedWrites

	if sdb.versionedWrites != nil {
		writes = make(VersionedWrites, 0, sdb.versionedWrites.Len())

		for addr, vwrites := range sdb.versionedWrites {
			if checkDirty {
				if _, isDirty := sdb.journal.dirties[addr]; !isDirty {
					for _, v := range vwrites {
						sdb.versionMap.Delete(v.Address, v.Path, v.Key, sdb.txIndex, false)
					}
					continue
				}
			}

			// If an account was selfdestructed, strip all writes except
			// SelfDestructPath itself (and any zero-balance BalancePath writes).
			// Non-zero BalancePath writes after selfdestruct represent residual ETH
			// (EIP-7708 case 2); these are carried via
			// ExecutionResult.SelfDestructedWithBalance captured before SoftFinalise
			// clears the journal, and must NOT appear here to avoid polluting the
			// EIP-7928 block access list.
			var appends = make(VersionedWrites, 0, len(vwrites))
			var selfDestructed bool
			for _, v := range vwrites {
				if v.Path == SelfDestructPath && v.Val.(bool) {
					selfDestructed = true
					prevs := appends
					appends = VersionedWrites{v}
					for _, prev := range prevs {
						if prev.Path == BalancePath && prev.Val.(uint256.Int) == (uint256.Int{}) {
							appends = append(appends, prev)
						} else if prev.Path == IncarnationPath {
							// Preserve incarnation so resurrection txs can find the prior incarnation
							appends = append(appends, prev)
						}
					}
				} else {
					if selfDestructed {
						if v.Path == BalancePath && v.Val.(uint256.Int) == (uint256.Int{}) {
							appends = append(appends, v)
						} else if v.Path == IncarnationPath {
							// Preserve incarnation so resurrection txs can find the prior incarnation
							appends = append(appends, v)
						}
					} else {
						appends = append(appends, v)
					}
				}
			}
			writes = append(writes, appends...)
		}
	}

	return writes
}

// Apply entries in a given write set to StateDB. Note that this function does not change MVHashMap nor write set
// of the current StateDB.
func (sdb *IntraBlockState) ApplyVersionedWrites(writes VersionedWrites) error {
	// Sort writes by (Address, Path, Key) to ensure deterministic processing
	// order.  VersionedWrites come from WriteSet map iteration (Go maps have
	// non-deterministic order).  Processing order matters because some paths
	// (CodePath, SelfDestructPath) call GetOrNewStateObject which triggers a
	// read from the stateReader.  If a BalancePath write for the same address
	// has already been processed, the state object is already loaded and no
	// read occurs; otherwise an extra read is recorded.  Different reads
	// produce different EIP-7928 BAL hashes.
	sort.Slice(writes, func(i, j int) bool {
		if c := writes[i].Address.Cmp(writes[j].Address); c != 0 {
			return c < 0
		}
		if writes[i].Path != writes[j].Path {
			return writes[i].Path < writes[j].Path
		}
		return writes[i].Key.Cmp(writes[j].Key) < 0
	})
	for i := range writes {
		path := writes[i].Path
		val := writes[i].Val
		addr := writes[i].Address

		if val != nil {
			switch path {
			case AddressPath:
				continue
			case StoragePath:
				stateKey := writes[i].Key
				state := val.(uint256.Int)
				if err := sdb.setState(addr, stateKey, state, true); err != nil {
					return err
				}
			case BalancePath:
				balance := val.(uint256.Int)
				if err := sdb.SetBalance(addr, balance, writes[i].Reason); err != nil {
					return err
				}
			case NoncePath:
				nonce := val.(uint64)
				if err := sdb.SetNonce(addr, nonce); err != nil {
					return err
				}
			case IncarnationPath:
				incarnation := val.(uint64)
				if err := sdb.SetIncarnation(addr, incarnation); err != nil {
					return err
				}
				// Re-emit the IncarnationPath write into versionedWrites so that the
				// finalize IBS's writes are correctly flushed to the global versionMap.
				versionWritten(sdb, addr, IncarnationPath, accounts.NilKey, incarnation)
			case CodePath:
				code := val.([]byte)
				stateObject, err := sdb.GetOrNewStateObject(addr)
				if err != nil {
					return err
				}
				codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
				// Force-set code bypassing stateObject.SetCode's equality check.
				// The finalize IBS uses a VersionedStateReader whose ReadSet may
				// contain the post-write code value (when the worker read the code
				// after a SetCodeTx modified it), causing SetCode's bytes.Equal
				// comparison to incorrectly skip the update and leave dirtyCode unset.
				sdb.journal.append(codeChange{
					account:     addr,
					prevhash:    stateObject.data.CodeHash,
					prevcode:    stateObject.code,
					wasCommited: !sdb.hasWrite(addr, CodePath, accounts.NilKey),
				})
				stateObject.setCode(codeHash, code)
				versionWritten(sdb, addr, CodePath, accounts.NilKey, code)
				versionWritten(sdb, addr, CodeHashPath, accounts.NilKey, codeHash)
				versionWritten(sdb, addr, CodeSizePath, accounts.NilKey, len(code))
			case CodeHashPath, CodeSizePath:
				// set by CodePath case above
			case SelfDestructPath:
				deleted := val.(bool)
				if deleted {
					// Ensure the state object exists before calling Selfdestruct.
					// For newly-created accounts (e.g. coinbase born via CREATE in the
					// same transaction, with no pre-block DB entry), getStateObject
					// returns nil and Selfdestruct silently no-ops.  This matters for
					// the EIP-7708 finalize IBS: without a stateObject, the account will
					// not be marked as selfdestructed and GetRemovedAccountsWithBalance
					// will miss it, omitting the residual-balance burn log.
					if _, err := sdb.GetOrNewStateObject(addr); err != nil {
						return err
					}
					if _, err := sdb.Selfdestruct(addr); err != nil {
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
					versionWritten(sdb, addr, SelfDestructPath, accounts.NilKey, false)
				}
			default:
				return fmt.Errorf("unknown key type: %d", path)
			}
		}
	}
	return nil
}
