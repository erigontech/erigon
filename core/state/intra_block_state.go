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
	"errors"
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

var _ evmtypes.IntraBlockState = new(IntraBlockState) // compile-time interface-check

type revision struct {
	id           int
	journalIndex int
}

// SystemAddress - sender address for internal state updates.
var SystemAddress = libcommon.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe")

var EmptyAddress = libcommon.Address{}

// BalanceIncrease represents the increase of balance of an account that did not require
// reading the account first
type BalanceIncrease struct {
	increase    uint256.Int
	transferred bool // Set to true when the corresponding stateObject is created and balance increase is transferred to the stateObject
	count       int  // Number of increases - this needs tracking for proper reversion
}

// IntraBlockState is responsible for caching and managing state changes
// that occur during block's execution.
// NOT THREAD SAFE!
type IntraBlockState struct {
	stateReader StateReader

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[libcommon.Address]*stateObject
	stateObjectsDirty map[libcommon.Address]struct{}

	nilAccounts map[libcommon.Address]struct{} // Remember non-existent account to avoid reading them again

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
	journal        *journal
	validRevisions []revision
	nextRevisionId int
	trace          bool
	tracingHooks   *tracing.Hooks
	balanceInc     map[libcommon.Address]*BalanceIncrease // Map of balance increases (without first reading the account)

	// Versioned storage used for parallel tx processing, versions
	// are maintaned across transactions until they are reset
	// at the block level
	versionMap      *VersionMap
	versionedWrites WriteSet
	versionedReads  ReadSet
	version         int
	dep             int
}

// Create a new state from a given trie
func New(stateReader StateReader) *IntraBlockState {
	return &IntraBlockState{
		stateReader:       stateReader,
		stateObjects:      map[libcommon.Address]*stateObject{},
		stateObjectsDirty: map[libcommon.Address]struct{}{},
		nilAccounts:       map[libcommon.Address]struct{}{},
		logs:              []types.Logs{},
		journal:           newJournal(),
		accessList:        newAccessList(),
		transientStorage:  newTransientStorage(),
		balanceInc:        map[libcommon.Address]*BalanceIncrease{},
		txIndex:           0,
		trace:             false,
	}
}

func NewWithVersionMap(stateReader StateReader, mvhm *VersionMap) *IntraBlockState {
	ibs := New(stateReader)
	ibs.versionMap = mvhm
	ibs.dep = -1
	return ibs
}

// Copy intra block state
func (sdb *IntraBlockState) Copy() *IntraBlockState {
	state := New(sdb.stateReader)
	state.stateObjects = make(map[libcommon.Address]*stateObject, len(sdb.stateObjectsDirty))
	state.stateObjectsDirty = make(map[libcommon.Address]struct{}, len(sdb.stateObjectsDirty))

	for addr := range sdb.journal.dirties {
		if object, exist := sdb.stateObjects[addr]; exist {
			state.stateObjects[addr] = object.deepCopy(state)

			state.stateObjectsDirty[addr] = struct{}{} // Mark the copy dirty to force internal (code/state) commits
		}
	}

	state.validRevisions = append(state.validRevisions, sdb.validRevisions...)
	state.refund = sdb.refund

	for addr := range sdb.stateObjectsDirty {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = sdb.stateObjects[addr].deepCopy(state)
		}
		state.stateObjectsDirty[addr] = struct{}{}
	}

	state.logs = make([]types.Logs, len(sdb.logs))
	for hash, logs := range sdb.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		state.logs[hash] = cpy
	}

	state.accessList = sdb.accessList.Copy()

	state.txIndex = sdb.txIndex

	if sdb.versionMap != nil {
		state.versionMap = sdb.versionMap
	}

	return state
}

func (sdb *IntraBlockState) SetVersionMap(versionMap *VersionMap) {
	sdb.versionMap = versionMap
}

func (sdb *IntraBlockState) SetHooks(hooks *tracing.Hooks) {
	sdb.tracingHooks = hooks
}

func (sdb *IntraBlockState) SetTrace(trace bool) {
	sdb.trace = trace
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (sdb *IntraBlockState) Reset() {
	sdb.nilAccounts = map[libcommon.Address]struct{}{}
	sdb.stateObjects = map[libcommon.Address]*stateObject{}
	sdb.stateObjectsDirty = map[libcommon.Address]struct{}{}
	clear(sdb.logs) // free pointers
	sdb.logs = sdb.logs[:0]
	sdb.balanceInc = map[libcommon.Address]*BalanceIncrease{}
	sdb.journal.Reset()
	sdb.nextRevisionId = 0
	sdb.validRevisions = sdb.validRevisions[:0]
	sdb.refund = 0
	sdb.txIndex = 0
	sdb.logSize = 0
	sdb.versionMap = nil
	sdb.versionedReads = nil
	sdb.versionedWrites = nil
	sdb.dep = -1
}

func (sdb *IntraBlockState) AddLog(log2 *types.Log) {
	sdb.journal.append(addLogChange{txIndex: sdb.txIndex})
	log2.TxIndex = uint(sdb.txIndex)
	log2.Index = sdb.logSize
	if sdb.tracingHooks != nil && sdb.tracingHooks.OnLog != nil {
		sdb.tracingHooks.OnLog(log2)
	}
	sdb.logSize++
	for len(sdb.logs) <= sdb.txIndex {
		sdb.logs = append(sdb.logs, nil)
	}
	sdb.logs[sdb.txIndex] = append(sdb.logs[sdb.txIndex], log2)
}

func (sdb *IntraBlockState) GetLogs(txIndex int, txnHash libcommon.Hash, blockNumber uint64, blockHash libcommon.Hash) types.Logs {
	if txIndex >= len(sdb.logs) {
		return nil
	}
	logs := sdb.logs[txIndex]
	for _, l := range logs {
		l.TxHash = txnHash
		l.BlockNumber = blockNumber
		l.BlockHash = blockHash
	}
	return logs
}

// GetRawLogs - is like GetLogs, but allow postpone calculation of `txn.Hash()`.
// Example: if you need filter logs and only then set `txn.Hash()` for filtered logs - then no reason to calc for all transactions.
func (sdb *IntraBlockState) GetRawLogs(txIndex int) types.Logs {
	if txIndex >= len(sdb.logs) {
		return nil
	}
	return sdb.logs[txIndex]
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
// Notably this also returns true for suicided accounts.
func (sdb *IntraBlockState) Exist(addr libcommon.Address) (bool, error) {
	s, err := sdb.getStateObject(addr)
	if err != nil {
		return false, err
	}
	return s != nil && !s.deleted, err
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (sdb *IntraBlockState) Empty(addr libcommon.Address) (bool, error) {
	so, err := sdb.getStateObject(addr)
	if err != nil {
		return false, err
	}
	return so == nil || so.deleted || so.empty(), nil
}

// GetBalance retrieves the balance from the given address or 0 if object not found
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetBalance(addr libcommon.Address) (uint256.Int, error) {
	return versionedRead(sdb, addr, BalancePath, libcommon.Hash{}, false, *u256.Num0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			if s != nil && !s.deleted {
				return s.Balance(), nil
			}
			return uint256.Int{}, nil
		})
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetNonce(addr libcommon.Address) (uint64, error) {
	nonce, err := versionedRead(sdb, addr, NoncePath, libcommon.Hash{}, false, 0,
		func(v uint64) uint64 { return v },
		func(s *stateObject) (uint64, error) {
			if s != nil && !s.deleted {
				return s.Nonce(), nil
			}
			return 0, nil
		})

	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) GetNonce %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, nonce)
	}

	return nonce, err
}

// TxIndex returns the current transaction index set by Prepare.
func (sdb *IntraBlockState) TxnIndex() int {
	return sdb.txIndex
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCode(addr libcommon.Address) ([]byte, error) {
	code, err := versionedRead(sdb, addr, CodePath, libcommon.Hash{}, false, nil,
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

	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) GetCode %x: size: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, len(code))
	}

	return code, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeSize(addr libcommon.Address) (int, error) {
	size, err := versionedRead(sdb, addr, CodeSizePath, libcommon.Hash{}, false, 0,
		func(v int) int { return v },
		func(s *stateObject) (int, error) {
			if s == nil || s.deleted {
				return 0, nil
			}
			if s.code != nil {
				return len(s.code), nil
			}
			l, err := sdb.stateReader.ReadAccountCodeSize(addr, s.data.Incarnation)
			if err != nil {
				return l, err
			}
			return l, err
		})

	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) GetCodeSize %x: %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, size)
	}

	return size, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeHash(addr libcommon.Address) (libcommon.Hash, error) {
	return versionedRead(sdb, addr, CodeHashPath, libcommon.Hash{}, false, libcommon.Hash{},
		func(v libcommon.Hash) libcommon.Hash { return v },
		func(s *stateObject) (libcommon.Hash, error) {
			if s == nil || s.deleted {
				return libcommon.Hash{}, nil
			}
			return s.data.CodeHash, nil
		})
}

func (sdb *IntraBlockState) ResolveCodeHash(addr libcommon.Address) (libcommon.Hash, error) {
	// eip-7702
	dd, ok, err := sdb.GetDelegatedDesignation(addr)

	if ok {
		return sdb.GetCodeHash(dd)
	}

	if err != nil {
		return libcommon.Hash{}, err
	}

	return sdb.GetCodeHash(addr)
}

func (sdb *IntraBlockState) ResolveCode(addr libcommon.Address) ([]byte, error) {
	// eip-7702
	dd, ok, err := sdb.GetDelegatedDesignation(addr)
	if ok {
		return sdb.GetCode(dd)
	}
	if err != nil {
		return nil, err
	}
	return sdb.GetCode(addr)
}

func (sdb *IntraBlockState) GetDelegatedDesignation(addr libcommon.Address) (libcommon.Address, bool, error) {
	// eip-7702
	code, err := sdb.GetCode(addr)
	if err != nil {
		return EmptyAddress, false, err
	}
	if delegation, ok := types.ParseDelegation(code); ok {
		return delegation, true, nil
	}
	return EmptyAddress, false, nil
}

// GetState retrieves a value from the given account's storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetState(addr libcommon.Address, key libcommon.Hash, value *uint256.Int) error {
	versionedValue, err := versionedRead(sdb, addr, StatePath, key, false, *u256.N0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			var value uint256.Int
			if s != nil && !s.deleted {
				s.GetState(key, &value)
			}
			return value, nil
		})

	*value = versionedValue

	if sdb.trace || (traceAccount(addr) && traceKey(key)) {
		fmt.Printf("%d (%d.%d) GetState %x, %x=%x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, key, value)
	}

	return err
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCommittedState(addr libcommon.Address, key libcommon.Hash, value *uint256.Int) error {
	versionedValue, err := versionedRead(sdb, addr, StatePath, key, true, *u256.N0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			var value uint256.Int
			if s != nil && !s.deleted {
				s.GetCommittedState(key, &value)
			}
			return value, nil
		})

	*value = versionedValue

	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) GetCommittedState %x, %x=%x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, key, value)
	}

	return err
}

func (sdb *IntraBlockState) HasSelfdestructed(addr libcommon.Address) (bool, error) {
	return versionedRead(sdb, addr, SelfDestructPath, libcommon.Hash{}, false, false,
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
}

func (sdb *IntraBlockState) ReadVersion(k VersionKey, txIdx int) ReadResult {
	return sdb.versionMap.Read(k, txIdx)
}

// AddBalance adds amount to the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) AddBalance(addr libcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) error {
	prev0, _ := sdb.GetBalance(addr)

	defer func() {
		bal, _ := sdb.GetBalance(addr)
		expected := (&uint256.Int{}).Add(&prev0, amount)
		if bal.Cmp(expected) != 0 {
			panic(fmt.Sprintf("add failed: expected: %d got: %d", expected, &bal))
		}
		if sdb.trace || traceAccount(addr) {
			fmt.Printf("%d (%d.%d) AddBalance %x, %d+%d=%d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &prev0, amount, &bal)
		}
	}()

	if sdb.versionMap == nil {
		// If this account has not been read, add to the balance increment map
		if _, needAccount := sdb.stateObjects[addr]; !needAccount && addr == ripemd && amount.IsZero() {
			sdb.journal.append(balanceIncrease{
				account:  &addr,
				increase: *amount,
			})

			bi, ok := sdb.balanceInc[addr]
			if !ok {
				bi = &BalanceIncrease{}
				sdb.balanceInc[addr] = bi
			}

			if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
				// TODO: discuss if we should ignore error
				prev := new(uint256.Int)
				account, _ := sdb.stateReader.ReadAccountDataForDebug(addr)
				if account != nil {
					prev.Add(&account.Balance, &bi.increase)
				} else {
					prev.Add(prev, &bi.increase)
				}

				sdb.tracingHooks.OnBalanceChange(addr, prev, new(uint256.Int).Add(prev, amount), reason)
			}

			bi.increase.Add(&bi.increase, amount)
			bi.count++
			return nil
		}
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}

	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.IsZero() {
		if stateObject.empty() {
			stateObject.touch()
		}

		return nil
	}

	prev, err := sdb.GetBalance(addr)
	if err != nil {
		return err
	}

	update := new(uint256.Int).Add(&prev, amount)
	stateObject.SetBalance(update, reason)
	sdb.versionWritten(SubpathKey(&addr, BalancePath), *update)
	return nil
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SubBalance(addr libcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) error {
	if sdb.trace || traceAccount(addr) {
		prev, _ := sdb.GetBalance(addr)
		defer func() {
			bal, _ := sdb.GetBalance(addr)
			fmt.Printf("%d (%d.%d) SubBalance %x, %d-%d=%d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, &prev, amount, &bal)
		}()
	}

	if amount.IsZero() {
		return nil
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}

	prev, err := sdb.GetBalance(addr)
	if err != nil {
		return err
	}

	update := new(uint256.Int).Sub(&prev, amount)
	stateObject.SetBalance(update, reason)
	sdb.versionWritten(SubpathKey(&addr, BalancePath), *update)

	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetBalance(addr libcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) error {
	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) SetBalance %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, amount)
	}
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(amount, reason)
	sdb.versionWritten(SubpathKey(&addr, BalancePath), stateObject.Balance())
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetNonce(addr libcommon.Address, nonce uint64) error {
	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) SetNonce %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, nonce)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}

	stateObject.SetNonce(nonce)
	sdb.versionWritten(SubpathKey(&addr, NoncePath), stateObject.Nonce())
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#code-hash
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetCode(addr libcommon.Address, code []byte) error {
	if sdb.trace || traceAccount(addr) {
		v := code
		lenv := len(code)
		if lenv > 41 {
			v = v[0:40]
		}
		fmt.Printf("%d (%d.%d) SetCode %x, %d: %x...\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, lenv, v)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	codeHash := crypto.Keccak256Hash(code)
	stateObject.SetCode(codeHash, code)
	sdb.versionWritten(SubpathKey(&addr, CodePath), code)
	sdb.versionWritten(SubpathKey(&addr, CodeHashPath), codeHash)
	sdb.versionWritten(SubpathKey(&addr, CodeSizePath), len(code))
	return nil
}

var tracedKeys = map[libcommon.Hash]struct{}{
	//libcommon.HexToHash("42b18621a78e3da7af1e2ba633cbabe8986f856a8a1342ee35958ff9e6afca40"): {},
	//libcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000000"): {},
}

func traceKey(key libcommon.Hash) bool {
	_, ok := tracedKeys[key]
	return len(tracedKeys) == 0 || ok
}

var tracedAccounts = map[libcommon.Address]struct{}{
	libcommon.HexToAddress("dfc3280009e736b55b2b869001487d96d9adff1b"): {},
	//libcommon.HexToAddress("9b08288c3be4f62bbf8d1c20ac9c5e6f9467d8b7"): {},
	//libcommon.HexToAddress("3cad627d8cc7ca1dd31bb7b0411b7cfda15571f2"): {},
	//libcommon.HexToAddress("47c4002f8554fec15828af5386fc63555393650e"): {},
	//libcommon.HexToAddress("749e27557966db1a6932e60a5dfbde7615b8c503"): {},
	//libcommon.HexToAddress("343300b5d84d444b2adc9116fef1bed02be49cf2"): {},
}

func traceAccount(addr libcommon.Address) bool {
	_, ok := tracedAccounts[addr]
	return ok
}

func (sdb *IntraBlockState) TraceAccount(addr libcommon.Address) bool {
	return traceAccount(addr)
}

func (sdb *IntraBlockState) Trace() bool {
	return sdb.trace
}

func (sdb *IntraBlockState) TxIndex() int {
	return sdb.txIndex
}

func (sdb *IntraBlockState) Incarnation() int {
	return sdb.version
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetState(addr libcommon.Address, key libcommon.Hash, value uint256.Int) error {
	if sdb.trace || (traceAccount(addr) && traceKey(key)) {
		fmt.Printf("%d (%d.%d) SetState %x, %x=%x\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, key.Bytes(), &value)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject.SetState(key, value) {
		sdb.versionWritten(StateKey(&addr, &key), value)
	}
	return nil
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (sdb *IntraBlockState) SetStorage(addr libcommon.Address, storage Storage) error {
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
func (sdb *IntraBlockState) SetIncarnation(addr libcommon.Address, incarnation uint64) error {
	if sdb.trace || traceAccount(addr) {
		fmt.Printf("%d (%d.%d) SetIncarnation %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, incarnation)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.setIncarnation(incarnation)
	}
	return nil
}

func (sdb *IntraBlockState) GetIncarnation(addr libcommon.Address) (uint64, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return 0, err
	}
	if stateObject != nil {
		return stateObject.data.Incarnation, nil
	}
	return 0, nil
}

// Selfdestruct marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (sdb *IntraBlockState) Selfdestruct(addr libcommon.Address) (bool, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return false, err
	}
	if stateObject == nil || stateObject.deleted {
		return false, nil
	}
	prevBalance := stateObject.Balance()
	sdb.journal.append(selfdestructChange{
		account:     &addr,
		prev:        stateObject.selfdestructed,
		prevbalance: prevBalance,
	})

	if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		sdb.tracingHooks.OnBalanceChange(addr, &prevBalance, uint256.NewInt(0), tracing.BalanceDecreaseSelfdestruct)
	}

	stateObject.markSelfdestructed()
	stateObject.createdContract = false
	stateObject.data.Balance.Clear()

	sdb.versionWritten(SubpathKey(&addr, SelfDestructPath), stateObject.selfdestructed)
	sdb.versionWritten(SubpathKey(&addr, BalancePath), uint256.Int{})

	return true, nil
}

func (sdb *IntraBlockState) Selfdestruct6780(addr libcommon.Address) error {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject == nil {
		return nil
	}
	if stateObject.newlyCreated {
		code, err := sdb.GetCode(addr)
		if err != nil {
			return err
		}
		if _, ok := types.ParseDelegation(code); !ok {
			sdb.Selfdestruct(addr)
		}
	}
	return nil
}

// SetTransientState sets transient storage for a given account. It
// adds the change to the journal so that it can be rolled back
// to its previous value if there is a revert.
func (sdb *IntraBlockState) SetTransientState(addr libcommon.Address, key libcommon.Hash, value uint256.Int) {
	prev := sdb.GetTransientState(addr, key)
	if prev == value {
		return
	}

	sdb.journal.append(transientStorageChange{
		account:  &addr,
		key:      key,
		prevalue: prev,
	})

	sdb.setTransientState(addr, key, value)
}

// setTransientState is a lower level setter for transient storage. It
// is called during a revert to prevent modifications to the journal.
func (sdb *IntraBlockState) setTransientState(addr libcommon.Address, key libcommon.Hash, value uint256.Int) {
	sdb.transientStorage.Set(addr, key, value)
}

// GetTransientState gets transient storage for a given account.
func (sdb *IntraBlockState) GetTransientState(addr libcommon.Address, key libcommon.Hash) uint256.Int {
	return sdb.transientStorage.Get(addr, key)
}

func (sdb *IntraBlockState) getStateObject(addr libcommon.Address) (*stateObject, error) {
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

	account, err := sdb.stateReader.ReadAccountData(addr)
	if err != nil {
		return nil, err
	}

	if account == nil {
		if sdb.versionMap != nil {
			account, _ = versionedRead[*accounts.Account](sdb, addr, AddressPath, libcommon.Hash{}, false, nil, nil, nil)

			if account == nil {
				return nil, nil
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
		// need to do a versioned read of balance/nonce/codehash
		if balance, _ := versionedRead[uint256.Int](sdb, addr, BalancePath, libcommon.Hash{}, false, account.Balance, nil, nil); balance.Cmp(&account.Balance) != 0 {
			account.Balance = balance
		}

		if nonce, _ := versionedRead[uint64](sdb, addr, NoncePath, libcommon.Hash{}, false, 0, nil, nil); nonce > account.Nonce {
			account.Nonce = nonce
		}

		if codeHash, _ := versionedRead[libcommon.Hash](sdb, addr, CodeHashPath, libcommon.Hash{}, false, libcommon.Hash{}, nil, nil); (codeHash != libcommon.Hash{}) {
			account.CodeHash = codeHash
		}

		code, _ = versionedRead[[]byte](sdb, addr, CodePath, libcommon.Hash{}, false, nil, nil, nil)
	}

	sdb.accountRead(addr, account)
	// Insert into the live set.
	obj := newObject(sdb, addr, account, account)

	if code != nil {
		obj.code = code
	}

	sdb.setStateObject(addr, obj)
	return obj, nil
}

func (sdb *IntraBlockState) setStateObject(addr libcommon.Address, object *stateObject) {
	if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred && sdb.versionMap == nil {
		object.data.Balance.Add(&object.data.Balance, &bi.increase)
		bi.transferred = true
		sdb.journal.append(balanceIncreaseTransfer{bi: bi})
	}
	sdb.stateObjects[addr] = object
}

// Retrieve a state object or create a new state object if nil.
func (sdb *IntraBlockState) GetOrNewStateObject(addr libcommon.Address) (*stateObject, error) {
	stateObject, err := sdb.getStateObject(addr)
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
func (sdb *IntraBlockState) createObject(addr libcommon.Address, previous *stateObject) (newobj *stateObject) {
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
		sdb.journal.append(createObjectChange{account: &addr})
	} else {
		sdb.journal.append(resetObjectChange{account: &addr, prev: previous})
	}
	newobj.newlyCreated = true
	sdb.setStateObject(addr, newobj)
	sdb.versionWritten(AddressKey(&addr), newobj.deepCopy(nil))
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
func (sdb *IntraBlockState) CreateAccount(addr libcommon.Address, contractCreation bool) error {
	var prevInc uint64
	previous, err := sdb.getStateObject(addr)
	if err != nil {
		return err
	}
	if previous != nil && previous.selfdestructed {
		prevInc = previous.data.Incarnation
	} else {
		if inc, err := sdb.stateReader.ReadAccountIncarnation(addr); err == nil {
			prevInc = inc
		} else {
			return err
		}
	}
	if previous != nil && prevInc < previous.data.PrevIncarnation {
		prevInc = previous.data.PrevIncarnation
	}

	newObj := sdb.createObject(addr, previous)
	if previous != nil && !previous.selfdestructed {
		newObj.data.Balance.Set(&previous.data.Balance)
	}
	newObj.data.Initialised = true
	newObj.data.PrevIncarnation = prevInc

	if contractCreation {
		newObj.createdContract = true
		newObj.data.Incarnation = prevInc + 1
	} else {
		newObj.selfdestructed = false
	}

	account := newObj.data
	sdb.versionWritten(AddressKey(&addr), &account)
	sdb.versionWritten(SubpathKey(&addr, BalancePath), newObj.Balance())
	return nil
}

// Snapshot returns an identifier for the current revision of the state.
func (sdb *IntraBlockState) Snapshot() int {
	id := sdb.nextRevisionId
	sdb.nextRevisionId++
	sdb.validRevisions = append(sdb.validRevisions, revision{id, sdb.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (sdb *IntraBlockState) RevertToSnapshot(revid int, err error) {
	var traced bool
	for addr := range tracedAccounts {
		if _, isDirty := sdb.journal.dirties[addr]; !isDirty {
			traced = true
			if err == nil {
				fmt.Printf("%d (%d.%d) Reverted %x, %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, revid)
			} else {
				fmt.Printf("%d (%d.%d) Reverted %x, %d: %s\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, revid, err)
			}
		}
	}

	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(sdb.validRevisions), func(i int) bool {
		return sdb.validRevisions[i].id >= revid
	})
	if idx == len(sdb.validRevisions) || sdb.validRevisions[idx].id != revid {
		var id int
		if idx < len(sdb.validRevisions) {
			id = sdb.validRevisions[idx].id
		}
		panic(fmt.Errorf("revision id %v cannot be reverted (idx=%v,len=%v,id=%v)", revid, idx, len(sdb.validRevisions), id))
	}
	snapshot := sdb.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	sdb.journal.revert(sdb, snapshot)
	sdb.validRevisions = sdb.validRevisions[:idx]

	if sdb.versionMap != nil {
		var revertedWrites []VersionKey

		if sdb.versionedWrites != nil {
			sdb.versionedWrites.Scan(func(v *VersionedWrite) bool {
				if _, isDirty := sdb.journal.dirties[v.Path.GetAddress()]; !isDirty {
					revertedWrites = append(revertedWrites, v.Path)
				}
				return true
			})
		}

		for _, key := range revertedWrites {
			sdb.versionMap.Delete(key, sdb.txIndex, false)
			sdb.versionedWrites.Delete(key.GetAddress(), AccountKey{key.subpath, key.GetStateKey()})
		}
	}

	if traced && sdb.txIndex == 8 && sdb.version == 1 {
		fmt.Printf("%d (%d.%d) Reverted: %d:%d\n", sdb.blockNum, sdb.txIndex, sdb.version, revid, snapshot)
	}
}

// GetRefund returns the current value of the refund counter.
func (sdb *IntraBlockState) GetRefund() uint64 {
	return sdb.refund
}

func updateAccount(EIP161Enabled bool, isAura bool, stateWriter StateWriter, addr libcommon.Address, stateObject *stateObject, isDirty bool, tracingHooks *tracing.Hooks) error {
	emptyRemoval := EIP161Enabled && stateObject.empty() && (!isAura || addr != SystemAddress)
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
		balance := stateObject.Balance()
		if tracingHooks != nil && tracingHooks.OnBalanceChange != nil && !(&balance).IsZero() && stateObject.selfdestructed {
			tracingHooks.OnBalanceChange(stateObject.address, &balance, uint256.NewInt(0), tracing.BalanceDecreaseSelfdestructBurn)
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
		if err := stateObject.updateTrie(stateWriter); err != nil {
			return err
		}
		if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
			return err
		}
	}
	return nil
}

func printAccount(EIP161Enabled bool, addr libcommon.Address, stateObject *stateObject, isDirty bool) {
	emptyRemoval := EIP161Enabled && stateObject.empty()
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
		if stateObject.data.Balance.IsUint64() {
			fmt.Printf("UpdateAccountData: %x, balance=%d, nonce=%d\n", addr, stateObject.data.Balance.Uint64(), stateObject.data.Nonce)
		} else {
			div := uint256.NewInt(1_000_000_000)
			fmt.Printf("UpdateAccountData: %x, balance=%d*%d, nonce=%d\n", addr, uint256.NewInt(0).Div(&stateObject.data.Balance, div).Uint64(), div.Uint64(), stateObject.data.Nonce)
		}
	}
}

// FinalizeTx should be called after every transaction.
func (sdb *IntraBlockState) FinalizeTx(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr, bi := range sdb.balanceInc {
		if !bi.transferred {
			sdb.getStateObject(addr)
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

		//fmt.Printf("FinalizeTx: %x, balance=%d %T\n", addr, so.data.Balance.Uint64(), stateWriter)
		if err := updateAccount(chainRules.IsSpuriousDragon, chainRules.IsAura, stateWriter, addr, so, true, sdb.tracingHooks); err != nil {
			return err
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
			sdb.getStateObject(addr)
		}
	}
	return sdb.MakeWriteSet(chainRules, stateWriter)
}

func (sdb *IntraBlockState) BalanceIncreaseSet() map[libcommon.Address]uint256.Int {
	s := make(map[libcommon.Address]uint256.Int, len(sdb.balanceInc))
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
		if traceAccount(addr) {
			var updated *uint256.Int
			if sdb.versionedWrites != nil {
				if w, ok := sdb.versionedWrites[addr][AccountKey{Path: BalancePath}]; ok {
					val := w.Val.(uint256.Int)
					updated = &val
				}
			}
			if updated != nil {
				fmt.Printf("%d (%d.%d) Balance: %x (%v): %d (%d)\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, isDirty, &stateObject.data.Balance, &updated)
			} else {
				fmt.Printf("%d (%d.%d) Balance: %x (%v): %d\n", sdb.blockNum, sdb.txIndex, sdb.version, addr, isDirty, &stateObject.data.Balance)
			}
		}
		if err := updateAccount(chainRules.IsSpuriousDragon, chainRules.IsAura, stateWriter, addr, stateObject, isDirty, sdb.tracingHooks); err != nil {
			return err
		}
	}

	var revertedWrites []VersionKey

	if sdb.versionedWrites != nil {
		sdb.versionedWrites.Scan(func(v *VersionedWrite) bool {
			if _, isDirty := sdb.stateObjectsDirty[v.Path.GetAddress()]; !isDirty {
				revertedWrites = append(revertedWrites, v.Path)
			}
			return true
		})
	}

	for _, key := range revertedWrites {
		sdb.versionMap.Delete(key, sdb.txIndex, false)
		sdb.versionedWrites.Delete(key.GetAddress(), AccountKey{Path: key.subpath, Key: key.GetStateKey()})
	}

	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
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
	if len(sdb.logs) > 0 && ti == 0 {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. len(sdb.logs)=%d, ti=%d", len(sdb.logs), ti)
		panic(err)
	}
	if sdb.txIndex >= 0 && sdb.txIndex > ti {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. sdb.txIndex=%d, ti=%d", sdb.txIndex, ti)
		panic(err)
	}
	sdb.txIndex = ti
	sdb.blockNum = bn
}

// no not lock
func (sdb *IntraBlockState) clearJournalAndRefund() {
	sdb.journal.Reset()
	sdb.validRevisions = sdb.validRevisions[:0]
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
func (sdb *IntraBlockState) Prepare(rules *chain.Rules, sender, coinbase libcommon.Address, dst *libcommon.Address,
	precompiles []libcommon.Address, list types.AccessList, authorities []libcommon.Address) error {
	if sdb.trace || traceAccount(sender) || dst != nil && traceAccount(*dst) {
		fmt.Printf("%d (%d.%d) ibs.Prepare: sender: %x, coinbase: %x, dest: %x, %x, %v, %v, %v\n", sdb.blockNum, sdb.txIndex, sdb.version, sender, coinbase, dst, precompiles, list, rules, authorities)
	}
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := newAccessList()
		sdb.accessList = al
		//sdb.accessList.Reset()
		//al := sdb.accessList

		al.AddAddress(sender)
		if dst != nil {
			al.AddAddress(*dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range precompiles {
			al.AddAddress(addr)
		}
		for _, el := range list {
			al.AddAddress(el.Address)
			for _, key := range el.StorageKeys {
				al.AddSlot(el.Address, key)
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

		if dst != nil {
			dd, ok, err := sdb.GetDelegatedDesignation(*dst)
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
	return nil
}

// AddAddressToAccessList adds the given address to the access list
func (sdb *IntraBlockState) AddAddressToAccessList(addr libcommon.Address) (addrMod bool) {
	addrMod = sdb.accessList.AddAddress(addr)
	if addrMod {
		sdb.journal.append(accessListAddAccountChange{&addr})
	}
	return addrMod
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (sdb *IntraBlockState) AddSlotToAccessList(addr libcommon.Address, slot libcommon.Hash) (addrMod, slotMod bool) {
	addrMod, slotMod = sdb.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		sdb.journal.append(accessListAddAccountChange{&addr})
	}
	if slotMod {
		sdb.journal.append(accessListAddSlotChange{
			address: &addr,
			slot:    &slot,
		})
	}
	return addrMod, slotMod
}

// AddressInAccessList returns true if the given address is in the access list.
func (sdb *IntraBlockState) AddressInAccessList(addr libcommon.Address) bool {
	return sdb.accessList.ContainsAddress(addr)
}

func (sdb *IntraBlockState) SlotInAccessList(addr libcommon.Address, slot libcommon.Hash) (addressPresent bool, slotPresent bool) {
	return sdb.accessList.Contains(addr, slot)
}

func (s *IntraBlockState) accountRead(addr libcommon.Address, account *accounts.Account) {
	if s.versionMap != nil {
		// record the originating account data so that
		// re-reads will return a complete account - note
		// this is not used by the version map wich works
		// at the level of individual account elements
		if s.versionedReads == nil {
			s.versionedReads = ReadSet{}
		}

		k := AddressKey(&addr)

		s.versionedReads.Set(&VersionedRead{
			Path:    k,
			Source:  StorageRead,
			Version: s.Version(),
			Val:     *account,
		})
	}
}

func (s *IntraBlockState) versionWritten(k VersionKey, val any) {
	if s.versionMap != nil {
		if s.versionedWrites == nil {
			s.versionedWrites = WriteSet{}
		}

		s.versionedWrites.Set(&VersionedWrite{
			Path:    k,
			Version: s.Version(),
			Val:     val,
		})
	}
}

func (sdb *IntraBlockState) versionedWrite(addr libcommon.Address, path AccountPath, key libcommon.Hash) (*VersionedWrite, bool) {
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

func (ibs *IntraBlockState) HadInvalidRead() bool {
	return ibs.dep >= 0
}

func (ibs *IntraBlockState) DepTxIndex() int {
	return ibs.dep
}

func (ibs *IntraBlockState) SetVersion(inc int) {
	ibs.version = inc
}

func (s *IntraBlockState) Version() Version {
	return Version{
		TxIndex:     s.txIndex,
		Incarnation: s.version,
	}
}

func (ibs *IntraBlockState) VersionedReads() ReadSet {
	return ibs.versionedReads
}

// VersionedWrites returns the current versioned write set if this block
// checkDirty - is mainly for testing, for block processing this is called
// after the block execution is completed and non dirty writes (due to reversions)
// will already have been cleaned in MakeWriteSet
// (TODO the writes should probably be unset in revert - but this requires extended revert logic)
func (ibs *IntraBlockState) VersionedWrites(checkDirty bool) VersionedWrites {
	var writes VersionedWrites

	if ibs.versionedWrites != nil {
		writes = make(VersionedWrites, 0, ibs.versionedWrites.Len())

		ibs.versionedWrites.Scan(func(v *VersionedWrite) bool {
			if checkDirty {
				if _, isDirty := ibs.journal.dirties[v.Path.GetAddress()]; isDirty {
					writes = append(writes, v)
				} else {
					ibs.versionMap.Delete(v.Path, ibs.txIndex, false)
				}
			} else {
				writes = append(writes, v)
			}
			return true
		})
	}

	return writes
}

// Apply entries in a given write set to StateDB. Note that this function does not change MVHashMap nor write set
// of the current StateDB.
func (s *IntraBlockState) ApplyVersionedWrites(writes VersionedWrites) error {
	for i := range writes {
		path := writes[i].Path
		val := writes[i].Val
		addr := path.GetAddress()

		if val != nil {
			if path.IsState() {
				stateKey := path.GetStateKey()
				state := val.(uint256.Int)
				s.SetState(addr, stateKey, state)
			} else if path.IsAddress() {
				continue
			} else {
				switch path.GetSubpath() {
				case BalancePath:
					balance := val.(uint256.Int)
					s.SetBalance(addr, &balance, writes[i].Reason)
				case NoncePath:
					nonce := val.(uint64)
					s.SetNonce(addr, nonce)
				case CodePath:
					code := val.([]byte)
					s.SetCode(addr, code)
				case CodeHashPath, CodeSizePath:
					// set by SetCode
				case SelfDestructPath:
					deleted := val.(bool)
					if deleted {
						s.Selfdestruct(addr)
					}
				default:
					panic(fmt.Errorf("unknown key type: %d", path.GetSubpath()))
				}
			}
		}
	}
	return nil
}
