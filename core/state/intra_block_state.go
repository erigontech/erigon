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
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/turbo/ethdb/wasmdb"
)

var _ evmtypes.IntraBlockState = new(IntraBlockState) // compile-time interface-check

type revision struct {
	id           int
	journalIndex int

	// Arbiturm: track the total balance change across all accounts
	unexpectedBalanceDelta *uint256.Int
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
	isEscrow    bool // Arbiturm: true if increase related to escrow account
}

// IntraBlockState is responsible for caching and managing state changes
// that occur during block's execution.
// NOT THREAD SAFE!
type IntraBlockState struct {
	stateReader StateReader

	arbExtraData *ArbitrumExtraData // TODO make sure this field not used for other chains

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[libcommon.Address]*stateObject
	stateObjectsDirty map[libcommon.Address]struct{}

	nilAccounts map[libcommon.Address]struct{} // Remember non-existent account to avoid reading them again

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by IntraBlockState.Commit.
	savedErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	txIndex int
	logs    []types.Logs
	logSize uint

	// Per-transaction access list
	accessList *accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *journal
	validRevisions []revision
	nextRevisionID int
	trace          bool
	tracingHooks   *tracing.Hooks
	balanceInc     map[libcommon.Address]*BalanceIncrease // Map of balance increases (without first reading the account)

	// Arbitrum stylus
	wasmDB wasmdb.WasmIface
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
		arbExtraData: &ArbitrumExtraData{
			unexpectedBalanceDelta: libcommon.Num0,
			userWasms:              UserWasms{},
			openWasmPages:          0,
			everWasmPages:          0,
			activatedWasms:         make(map[libcommon.Hash]ActivatedWasm),
			recentWasms:            RecentWasms{},
		},
		//trace:             true,
	}
}

func (sdb *IntraBlockState) SetWasmDB(wasmDB wasmdb.WasmIface) {
	sdb.wasmDB = wasmDB
}

func (sdb *IntraBlockState) SetHooks(hooks *tracing.Hooks) {
	sdb.tracingHooks = hooks
}

func (sdb *IntraBlockState) SetTrace(trace bool) {
	sdb.trace = trace
}

// setErrorUnsafe sets error but should be called in medhods that already have locks
// Deprecated: The IBS api now returns errors directly
func (sdb *IntraBlockState) setErrorUnsafe(err error) {
	if sdb.savedErr == nil {
		sdb.savedErr = err
	}
}

// Deprecated: The IBS api now returns errors directly
func (sdb *IntraBlockState) Error() error {
	return sdb.savedErr
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (sdb *IntraBlockState) Reset() {
	//if len(sdb.nilAccounts) == 0 || len(sdb.stateObjects) == 0 || len(sdb.stateObjectsDirty) == 0 || len(sdb.balanceInc) == 0 {
	//	log.Warn("zero", "len(sdb.nilAccounts)", len(sdb.nilAccounts),
	//		"len(sdb.stateObjects)", len(sdb.stateObjects),
	//		"len(sdb.stateObjectsDirty)", len(sdb.stateObjectsDirty),
	//		"len(sdb.balanceInc)", len(sdb.balanceInc))
	//}

	/*
		sdb.nilAccounts = make(map[libcommon.Address]struct{})
		sdb.stateObjects = make(map[libcommon.Address]*stateObject)
		sdb.stateObjectsDirty = make(map[libcommon.Address]struct{})
		sdb.logs = make(map[libcommon.Hash][]*types.Log)
		sdb.balanceInc = make(map[libcommon.Address]*BalanceIncrease)
	*/

	sdb.nilAccounts = make(map[libcommon.Address]struct{})
	//clear(sdb.nilAccounts)
	sdb.stateObjects = make(map[libcommon.Address]*stateObject)
	//clear(sdb.stateObjects)
	sdb.stateObjectsDirty = make(map[libcommon.Address]struct{})
	//clear(sdb.stateObjectsDirty)
	clear(sdb.logs) // free pointers
	sdb.logs = sdb.logs[:0]
	sdb.balanceInc = make(map[libcommon.Address]*BalanceIncrease)
	//clear(sdb.balanceInc)
	sdb.txIndex = 0
	sdb.logSize = 0
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
func (sdb *IntraBlockState) SubRefund(gas uint64) {
	sdb.journal.append(refundChange{prev: sdb.refund})
	if gas > sdb.refund {
		sdb.setErrorUnsafe(errors.New("refund counter below zero"))
	}
	sdb.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (sdb *IntraBlockState) Exist(addr libcommon.Address) (bool, error) {
	s, err := sdb.getStateObject(addr)
	if err != nil {
		return false, err
	}
	return s != nil && !s.deleted, nil
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
func (sdb *IntraBlockState) GetBalance(addr libcommon.Address) (*uint256.Int, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return nil, err
	}
	if stateObject != nil && !stateObject.deleted {
		return stateObject.Balance(), nil
	}
	return u256.Num0, nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetNonce(addr libcommon.Address) (uint64, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return 0, err
	}
	if stateObject != nil && !stateObject.deleted {
		return stateObject.Nonce(), nil
	}
	return 0, nil
}

// TxIndex returns the current transaction index set by Prepare.
func (sdb *IntraBlockState) TxnIndex() int {
	return sdb.txIndex
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCode(addr libcommon.Address) ([]byte, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return nil, err
	}
	if stateObject != nil && !stateObject.deleted {
		if sdb.trace {
			fmt.Printf("GetCode %x, returned %d\n", addr, len(stateObject.Code()))
		}
		return stateObject.Code(), nil
	}
	if sdb.trace {
		fmt.Printf("GetCode %x, returned nil\n", addr)
	}
	return nil, nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeSize(addr libcommon.Address) (int, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return 0, err
	}
	if stateObject == nil || stateObject.deleted {
		return 0, nil
	}
	if stateObject.code != nil {
		return len(stateObject.code), nil
	}
	if stateObject.data.CodeHash == emptyCodeHashH {
		return 0, nil
	}
	l, err := sdb.stateReader.ReadAccountCodeSize(addr, stateObject.data.Incarnation)
	if err != nil {
		sdb.setErrorUnsafe(err)
		return l, err
	}
	return l, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeHash(addr libcommon.Address) (libcommon.Hash, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return libcommon.Hash{}, err
	}
	if stateObject == nil || stateObject.deleted {
		return libcommon.Hash{}, nil
	}
	return stateObject.data.CodeHash, nil
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
func (sdb *IntraBlockState) GetState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int) error {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil && !stateObject.deleted {
		stateObject.GetState(key, value)
	} else {
		value.Clear()
	}
	return nil
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCommittedState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int) error {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil && !stateObject.deleted {
		stateObject.GetCommittedState(key, value)
	} else {
		value.Clear()
	}
	return nil
}

func (sdb *IntraBlockState) HasSelfdestructed(addr libcommon.Address) (bool, error) {
	stateObject, err := sdb.getStateObject(addr)
	if err != nil {
		return false, err
	}
	if stateObject == nil {
		return false, nil
	}
	if stateObject.deleted {
		return false, nil
	}
	if stateObject.createdContract {
		return false, nil
	}
	return stateObject.selfdestructed, nil
}

/*
 * SETTERS
 */

func (sdb *IntraBlockState) RemoveEscrowProtection(addr libcommon.Address) {
	bi, ok := sdb.balanceInc[addr]
	if ok {
		if sdb.trace {
			fmt.Printf("RemoveEscrowProtection %x, isEscrow=%v\n", addr, bi.isEscrow)
		}
		bi.isEscrow = false
		sdb.balanceInc[addr] = bi
	}
}

// AddBalance adds amount to the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) AddBalance(addr libcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) error {
	if sdb.trace {
		fmt.Printf("AddBalance %x, %d\n", addr, amount)
	}

	// If this account has not been read, add to the balance increment map
	_, needAccount := sdb.stateObjects[addr]
	if !needAccount && addr == ripemd && amount.IsZero() {
		needAccount = true
	}
	if !needAccount {
		sdb.journal.append(balanceIncrease{
			account:  &addr,
			increase: *amount,
		})

		bi, ok := sdb.balanceInc[addr]
		if !ok {
			bi = &BalanceIncrease{
				isEscrow: reason == tracing.BalanceIncreaseEscrow, // arbitrum specific protection
			}
			if sdb.trace && bi.isEscrow {
				fmt.Printf("ESCROW protected %x\n", addr)
			}
			sdb.balanceInc[addr] = bi
		} else {
			bi.isEscrow = reason == tracing.BalanceIncreaseEscrow // arbitrum specific protection
			sdb.balanceInc[addr] = bi
			if sdb.trace && bi.isEscrow {
				fmt.Printf("ESCROW 2protected %x\n", addr)
			}

		}

		if !amount.IsZero() && sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
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

		sdb.arbExtraData.unexpectedBalanceDelta.Add(sdb.arbExtraData.unexpectedBalanceDelta, amount)

		bi.increase.Add(&bi.increase, amount)
		bi.count++
		return nil
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	sdb.arbExtraData.unexpectedBalanceDelta.Add(sdb.arbExtraData.unexpectedBalanceDelta, amount)
	stateObject.AddBalance(amount, reason)
	return nil
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SubBalance(addr libcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) error {
	if sdb.trace {
		fmt.Printf("SubBalance %x, %d\n", addr, amount)
	}

	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		sdb.arbExtraData.unexpectedBalanceDelta.Sub(sdb.arbExtraData.unexpectedBalanceDelta, amount)
		stateObject.SubBalance(amount, reason)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetBalance(addr libcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) error {
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		if sdb.arbExtraData != nil {
			if amount == nil {
				amount = uint256.NewInt(0)
			}
			prevBalance := stateObject.Balance()
			sdb.arbExtraData.unexpectedBalanceDelta.Add(sdb.arbExtraData.unexpectedBalanceDelta, amount)
			sdb.arbExtraData.unexpectedBalanceDelta.Sub(sdb.arbExtraData.unexpectedBalanceDelta, prevBalance)
		}
		stateObject.SetBalance(amount, reason)
	}
	return nil
}

// Arbitrum
func (sdb *IntraBlockState) ExpectBalanceBurn(amount *uint256.Int) {
	if amount.Sign() < 0 {
		panic(fmt.Sprintf("ExpectBalanceBurn called with negative amount %v", amount))
	}
	sdb.arbExtraData.unexpectedBalanceDelta.Add(sdb.arbExtraData.unexpectedBalanceDelta, amount)
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetNonce(addr libcommon.Address, nonce uint64) error {
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#code-hash
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetCode(addr libcommon.Address, code []byte) error {
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetState(addr libcommon.Address, key *libcommon.Hash, value uint256.Int) error {
	stateObject, err := sdb.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.SetState(key, value)
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

	prevBalance := *stateObject.Balance()
	sdb.journal.append(selfdestructChange{
		account:     &addr,
		prev:        stateObject.selfdestructed,
		prevbalance: prevBalance,
	})

	if sdb.tracingHooks != nil && sdb.tracingHooks.OnBalanceChange != nil {
		if sdb.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
			sdb.tracingHooks.OnBalanceChange(addr, &prevBalance, uint256.NewInt(0), tracing.BalanceDecreaseSelfdestruct)
		}
		if sdb.tracingHooks.CaptureArbitrumTransfer != nil {
			sdb.tracingHooks.CaptureArbitrumTransfer(&addr, nil, &prevBalance, false, "selfDestruct")
		}
	}

	stateObject.markSelfdestructed()
	sdb.arbExtraData.unexpectedBalanceDelta.Sub(sdb.arbExtraData.unexpectedBalanceDelta, &stateObject.data.Balance)
	if bi, exist := sdb.balanceInc[addr]; exist && bi.isEscrow {
		fmt.Printf("ESCROW unprotected by selfdestruct %x\n", addr)
		bi.isEscrow = false
	}

	stateObject.createdContract = false
	stateObject.data.Balance.Clear()

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

func (sdb *IntraBlockState) getStateObject(addr libcommon.Address) (stateObject *stateObject, err error) {
	// Prefer 'live' objects.
	if obj := sdb.stateObjects[addr]; obj != nil {
		return obj, nil
	}

	// Load the object from the database.
	if _, ok := sdb.nilAccounts[addr]; ok {
		if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred {
			return sdb.createObject(addr, nil), nil
		}
		return nil, nil
	}
	account, err := sdb.stateReader.ReadAccountData(addr)
	if err != nil {
		sdb.setErrorUnsafe(err)
		return nil, err
	}
	if account == nil {
		sdb.nilAccounts[addr] = struct{}{}
		if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred {
			return sdb.createObject(addr, nil), nil
		}
		return nil, nil
	}

	// Insert into the live set.
	obj := newObject(sdb, addr, account, account)
	sdb.setStateObject(addr, obj)
	return obj, nil
}

func (sdb *IntraBlockState) setStateObject(addr libcommon.Address, object *stateObject) {
	if bi, ok := sdb.balanceInc[addr]; ok && !bi.transferred {
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
	account := new(accounts.Account)
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
			sdb.savedErr = err
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
	return nil
}

// Snapshot returns an identifier for the current revision of the state.
func (sdb *IntraBlockState) Snapshot() int {
	id := sdb.nextRevisionID
	sdb.nextRevisionID++
	sdb.validRevisions = append(sdb.validRevisions,
		revision{id, sdb.journal.length(), sdb.arbExtraData.unexpectedBalanceDelta.Clone()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (sdb *IntraBlockState) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(sdb.validRevisions), func(i int) bool {
		return sdb.validRevisions[i].id >= revid
	})
	if idx == len(sdb.validRevisions) || sdb.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	revision := sdb.validRevisions[idx]
	snapshot := revision.journalIndex
	//fmt.Printf("Reverting to snapshot %d: set unexpected delta (d=%s)\n", revid,
	//	uint256.NewInt(0).Sub(sdb.arbExtraData.unexpectedBalanceDelta, revision.unexpectedBalanceDelta))
	if sdb.arbExtraData != nil {
		if sdb.arbExtraData.unexpectedBalanceDelta == nil {
			sdb.arbExtraData.unexpectedBalanceDelta = uint256.NewInt(0)
		}
		sdb.arbExtraData.unexpectedBalanceDelta.Set(revision.unexpectedBalanceDelta)
	}

	// Replay the journal to undo changes and remove invalidated snapshots
	sdb.journal.revert(sdb, snapshot)
	sdb.validRevisions = sdb.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
func (sdb *IntraBlockState) GetRefund() uint64 {
	return sdb.refund
}

func updateAccount(EIP161Enabled bool, isAura bool, stateWriter StateWriter, addr libcommon.Address, stateObject *stateObject, isDirty bool, tracingHooks *tracing.Hooks) error {
	emptyRemoval := EIP161Enabled && stateObject.empty() && (!isAura || addr != SystemAddress)
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
		if tracingHooks != nil && tracingHooks.OnBalanceChange != nil && !stateObject.Balance().IsZero() && stateObject.selfdestructed {
			tracingHooks.OnBalanceChange(stateObject.address, stateObject.Balance(), uint256.NewInt(0), tracing.BalanceDecreaseSelfdestructBurn)
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

	if sdb.wasmDB != nil && sdb.arbExtraData != nil {
		if db := sdb.wasmDB.WasmStore(); db != nil && len(sdb.arbExtraData.activatedWasms) > 0 {
			if err := db.Update(context.TODO(), func(tx kv.RwTx) error {
				// Arbitrum: write Stylus programs to disk
				for moduleHash, asmMap := range sdb.arbExtraData.activatedWasms {
					wasmdb.WriteActivation(tx, moduleHash, asmMap)
				}
				return nil
			}); err != nil {
				return err
			}
			sdb.arbExtraData.activatedWasms = make(map[libcommon.Hash]ActivatedWasm)
		}
		sdb.arbExtraData.unexpectedBalanceDelta.Clear()
	}
	return sdb.MakeWriteSet(chainRules, stateWriter)
}

type BalanceIncreaseEntry struct {
	Amount   uint256.Int
	IsEscrow bool
}

func (sdb *IntraBlockState) BalanceIncreaseSet() map[libcommon.Address]BalanceIncreaseEntry {
	s := make(map[libcommon.Address]BalanceIncreaseEntry, len(sdb.balanceInc))
	for addr, bi := range sdb.balanceInc {
		if bi.isEscrow {
			s[addr] = BalanceIncreaseEntry{
				Amount:   uint256.Int{},
				IsEscrow: bi.isEscrow,
			}
		}

		if !bi.transferred {
			s[addr] = BalanceIncreaseEntry{
				Amount:   bi.increase,
				IsEscrow: bi.isEscrow,
			}
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
		if err := updateAccount(chainRules.IsSpuriousDragon, chainRules.IsAura, stateWriter, addr, stateObject, isDirty, sdb.tracingHooks); err != nil {
			return err
		}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

func (sdb *IntraBlockState) Print(chainRules chain.Rules) {
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		_, isDirty2 := sdb.journal.dirties[addr]

		printAccount(chainRules.IsSpuriousDragon, addr, stateObject, isDirty || isDirty2)
	}
}

// SetTxContext sets the current transaction index which
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (sdb *IntraBlockState) SetTxContext(ti int) {
	if len(sdb.logs) > 0 && ti == 0 {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. len(sdb.logs)=%d, ti=%d", len(sdb.logs), ti)
		panic(err)
	}
	if sdb.txIndex >= 0 && sdb.txIndex > ti {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. sdb.txIndex=%d, ti=%d", sdb.txIndex, ti)
		panic(err)
	}
	sdb.txIndex = ti

	// Arbitrum: clear memory charging state for new tx
	if sdb.arbExtraData == nil {
		sdb.arbExtraData = &ArbitrumExtraData{
			unexpectedBalanceDelta: new(uint256.Int),
			userWasms:              map[libcommon.Hash]ActivatedWasm{},
			activatedWasms:         map[libcommon.Hash]ActivatedWasm{},
			recentWasms:            NewRecentWasms(),
		}
	} else {
		sdb.arbExtraData.openWasmPages = 0
		sdb.arbExtraData.everWasmPages = 0
	}
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
	if sdb.trace {
		fmt.Printf("ibs.Prepare %x, %x, %x, %x, %v, %v, %v\n", sender, coinbase, dst, precompiles, list, rules, authorities)
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
