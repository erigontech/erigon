// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"fmt"
	"sort"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
)

type revision struct {
	id           int
	journalIndex int
}

type StateTracer interface {
	CaptureAccountRead(account common.Address) error
	CaptureAccountWrite(account common.Address) error
}

// SystemAddress - sender address for internal state updates.
var SystemAddress = common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe")

// IntraBlockState is responsible for caching and managing state changes
// that occur during block's execution.
// NOT THREAD SAFE!
type IntraBlockState struct {
	stateReader StateReader

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[common.Address]*stateObject
	stateObjectsDirty map[common.Address]struct{}

	nilAccounts map[common.Address]struct{} // Remember non-existent account to avoid reading them again

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by IntraBlockState.Commit.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	thash, bhash common.Hash
	txIndex      int
	logs         map[common.Hash][]*types.Log
	logSize      uint

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *journal
	validRevisions []revision
	nextRevisionID int
	tracer         StateTracer
	trace          bool
	accessList     *accessList
}

// Create a new state from a given trie
func New(stateReader StateReader) *IntraBlockState {
	return &IntraBlockState{
		stateReader:       stateReader,
		stateObjects:      make(map[common.Address]*stateObject),
		stateObjectsDirty: make(map[common.Address]struct{}),
		nilAccounts:       make(map[common.Address]struct{}),
		logs:              make(map[common.Hash][]*types.Log),
		journal:           newJournal(),
		accessList:        newAccessList(),
	}
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (sdb *IntraBlockState) Copy() *IntraBlockState {
	// Copy all the basic fields, initialize the memory ones
	ibs := &IntraBlockState{
		stateReader:       sdb.stateReader,
		stateObjects:      make(map[common.Address]*stateObject, len(sdb.journal.dirties)),
		stateObjectsDirty: make(map[common.Address]struct{}, len(sdb.journal.dirties)),
		nilAccounts:       make(map[common.Address]struct{}),
		refund:            sdb.refund,
		logs:              make(map[common.Hash][]*types.Log, len(sdb.logs)),
		logSize:           sdb.logSize,
		journal:           newJournal(),
	}
	// Copy the dirty states, logs, and preimages
	for addr := range sdb.journal.dirties {
		// As documented [here](https://github.com/ethereum/go-ethereum/pull/16485#issuecomment-380438527),
		// and in the Finalise-method, there is a case where an object is in the journal but not
		// in the stateObjects: OOG after touch on ripeMD prior to Byzantium. Thus, we need to check for
		// nil
		if object, exist := sdb.stateObjects[addr]; exist {
			// Even though the original object is dirty, we are not copying the journal,
			// so we need to make sure that anyside effect the journal would have caused
			// during a commit (or similar op) is already applied to the copy.
			ibs.stateObjects[addr] = object.deepCopy(ibs)
			ibs.stateObjectsDirty[addr] = struct{}{} // Mark the copy dirty to force internal (code/state) commits
		}
	}
	// Above, we don't copy the actual journal. This means that if the copy is copied, the
	// loop above will be a no-op, since the copy's journal is empty.
	// Thus, here we iterate over stateObjects, to enable copies of copies
	for addr := range sdb.stateObjectsDirty {
		if _, exist := ibs.stateObjects[addr]; !exist {
			ibs.stateObjects[addr] = sdb.stateObjects[addr].deepCopy(ibs)
		}
		ibs.stateObjectsDirty[addr] = struct{}{}
	}
	for hash, logs := range sdb.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		ibs.logs[hash] = cpy
	}
	// comment from https://github.com/ethereum/go-ethereum/commit/6487c002f6b47e08cb9814f16712c6789b313a97#diff-c3757dc9e9d868f63bc84a0cc67159c1d5c22cc5d8c9468757098f0492e0658cR705
	// Do we need to copy the access list? In practice: No. At the start of a
	// transaction, the access list is empty. In practice, we only ever copy state
	// _between_ transactions/blocks, never in the middle of a transaction.
	// However, it doesn't cost us much to copy an empty list, so we do it anyway
	// to not blow up if we ever decide copy it in the middle of a transaction
	ibs.accessList = sdb.accessList.Copy()
	return ibs
}

func (sdb *IntraBlockState) SetTracer(tracer StateTracer) {
	sdb.tracer = tracer
}

func (sdb *IntraBlockState) SetTrace(trace bool) {
	sdb.trace = trace
}

// setErrorUnsafe sets error but should be called in medhods that already have locks
func (sdb *IntraBlockState) setErrorUnsafe(err error) {
	if sdb.dbErr == nil {
		sdb.dbErr = err
	}
}

func (sdb *IntraBlockState) Error() error {
	return sdb.dbErr
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (sdb *IntraBlockState) Reset() {
	sdb.stateObjects = make(map[common.Address]*stateObject)
	sdb.stateObjectsDirty = make(map[common.Address]struct{})
	sdb.thash = common.Hash{}
	sdb.bhash = common.Hash{}
	sdb.txIndex = 0
	sdb.logs = make(map[common.Hash][]*types.Log)
	sdb.logSize = 0
	sdb.clearJournalAndRefund()
	sdb.accessList = newAccessList()
}

func (sdb *IntraBlockState) AddLog(log *types.Log) {
	sdb.journal.append(addLogChange{txhash: sdb.thash})

	log.TxHash = sdb.thash
	log.BlockHash = sdb.bhash
	log.TxIndex = uint(sdb.txIndex)
	log.Index = sdb.logSize
	sdb.logs[sdb.thash] = append(sdb.logs[sdb.thash], log)
	sdb.logSize++
}

func (sdb *IntraBlockState) GetLogs(hash common.Hash) []*types.Log {
	return sdb.logs[hash]
}

func (sdb *IntraBlockState) Logs() []*types.Log {
	var logs []*types.Log
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
		panic("Refund counter below zero")
	}
	sdb.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (sdb *IntraBlockState) Exist(addr common.Address) bool {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	s := sdb.getStateObject(addr)
	return s != nil && !s.deleted
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (sdb *IntraBlockState) Empty(addr common.Address) bool {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	so := sdb.getStateObject(addr)
	return so == nil || so.deleted || so.empty()
}

// GetBalance retrieves the balance from the given address or 0 if object not found
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetBalance(addr common.Address) *uint256.Int {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		return stateObject.Balance()
	}
	return u256.Num0
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetNonce(addr common.Address) uint64 {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		return stateObject.Nonce()
	}

	return 0
}

// TxIndex returns the current transaction index set by Prepare.
func (sdb *IntraBlockState) TxIndex() int {
	return sdb.txIndex
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCode(addr common.Address) []byte {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		if sdb.trace {
			fmt.Printf("GetCode %x, returned %d\n", addr, len(stateObject.Code()))
		}
		return stateObject.Code()
	}
	if sdb.trace {
		fmt.Printf("GetCode %x, returned nil\n", addr)
	}
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeSize(addr common.Address) int {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil || stateObject.deleted {
		return 0
	}
	if stateObject.code != nil {
		return len(stateObject.code)
	}
	len, err := sdb.stateReader.ReadAccountCodeSize(addr, stateObject.data.Incarnation, common.BytesToHash(stateObject.CodeHash()))
	if err != nil {
		sdb.setErrorUnsafe(err)
	}
	return len
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeHash(addr common.Address) common.Hash {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil || stateObject.deleted {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

// GetState retrieves a value from the given account's storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetState(addr common.Address, key *common.Hash, value *uint256.Int) {
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		stateObject.GetState(key, value)
	} else {
		value.Clear()
	}
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCommittedState(addr common.Address, key *common.Hash, value *uint256.Int) {
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		stateObject.GetCommittedState(key, value)
	} else {
		value.Clear()
	}
}

func (sdb *IntraBlockState) HasSuicided(addr common.Address) bool {
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil {
		return false
	}
	if stateObject.deleted {
		return false
	}
	if stateObject.created {
		return false
	}
	return stateObject.suicided
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) AddBalance(addr common.Address, amount *uint256.Int) {
	if sdb.trace {
		fmt.Printf("AddBalance %x, %d\n", addr, amount)
	}
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SubBalance(addr common.Address, amount *uint256.Int) {
	if sdb.trace {
		fmt.Printf("SubBalance %x, %d\n", addr, amount)
	}
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}

	}

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetBalance(addr common.Address, amount *uint256.Int) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetNonce(addr common.Address, nonce uint64) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#code-hash
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetCode(addr common.Address, code []byte) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetState(addr common.Address, key *common.Hash, value uint256.Int) {
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(key, value)
	}
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (sdb *IntraBlockState) SetStorage(addr common.Address, storage Storage) {
	fmt.Printf("SetStorage: %x, %s\n ", addr, storage.String())
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetStorage(storage)
	}
}

// SetIncarnation sets incarnation for account if account exists
func (sdb *IntraBlockState) SetIncarnation(addr common.Address, incarnation uint64) {
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.setIncarnation(incarnation)
	}
}

func (sdb *IntraBlockState) GetIncarnation(addr common.Address) uint64 {
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.data.Incarnation
	}
	return 0
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (sdb *IntraBlockState) Suicide(addr common.Address) bool {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}
		err = sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil || stateObject.deleted {
		return false
	}
	sdb.journal.append(suicideChange{
		account:     &addr,
		prev:        stateObject.suicided,
		prevbalance: *stateObject.Balance(),
	})
	stateObject.markSuicided()
	stateObject.created = false
	stateObject.data.Balance.Clear()

	return true
}

// Retrieve a state object given my the address. Returns nil if not found.
func (sdb *IntraBlockState) getStateObject(addr common.Address) (stateObject *stateObject) {
	// Prefer 'live' objects.
	if obj := sdb.stateObjects[addr]; obj != nil {
		return obj
	}

	// Load the object from the database.
	if _, ok := sdb.nilAccounts[addr]; ok {
		return nil
	}
	account, err := sdb.stateReader.ReadAccountData(addr)
	if err != nil {
		sdb.setErrorUnsafe(err)
		return nil
	}
	if account == nil {
		sdb.nilAccounts[addr] = struct{}{}
		return nil
	}

	// Insert into the live set.
	obj := newObject(sdb, addr, account, account)
	sdb.setStateObject(obj)
	return obj
}

func (sdb *IntraBlockState) setStateObject(object *stateObject) {
	sdb.stateObjects[object.Address()] = object
}

// Retrieve a state object or create a new state object if nil.
func (sdb *IntraBlockState) GetOrNewStateObject(addr common.Address) *stateObject {
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil || stateObject.deleted {
		stateObject = sdb.createObject(addr, nil /* previous */)
	}
	return stateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten.
func (sdb *IntraBlockState) createObject(addr common.Address, previous *stateObject) (newobj *stateObject) {
	account := new(accounts.Account)
	if previous != nil {
		account.Balance.Set(&previous.data.Balance)
		account.Initialised = true
	}
	var original *accounts.Account
	if previous == nil {
		original = &accounts.Account{}
	} else {
		original = &previous.original
	}
	account.Root.SetBytes(trie.EmptyRoot[:]) // old storage should be ignored
	newobj = newObject(sdb, addr, account, original)
	if previous != nil && previous.suicided {
		newobj.suicided = true
	}
	newobj.setNonce(0) // sets the object to dirty
	if previous == nil {
		sdb.journal.append(createObjectChange{account: &addr})
	} else {
		sdb.journal.append(resetObjectChange{prev: previous})
	}
	sdb.setStateObject(newobj)
	return newobj
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//   1. sends funds to sha(account ++ (nonce + 1))
//   2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (sdb *IntraBlockState) CreateAccount(addr common.Address, contractCreation bool) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace && err != nil {
			log.Error("error while CaptureAccountRead", "err", err)
		}

		err = sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace && err != nil {
			log.Error("error while CaptureAccountWrite", "err", err)
		}
	}

	var prevInc uint64
	previous := sdb.getStateObject(addr)
	if contractCreation {
		if previous != nil && previous.suicided {
			prevInc = previous.data.Incarnation
		} else {
			inc, err := sdb.stateReader.ReadAccountIncarnation(addr)
			if sdb.trace && err != nil {
				log.Error("error while ReadAccountIncarnation", "err", err)
			}
			if err == nil {
				prevInc = inc
			}
		}
	}

	newObj := sdb.createObject(addr, previous)

	if contractCreation {
		newObj.created = true
		newObj.data.Incarnation = prevInc + 1
	} else {
		newObj.suicided = false
	}
}

// Snapshot returns an identifier for the current revision of the state.
func (sdb *IntraBlockState) Snapshot() int {
	id := sdb.nextRevisionID
	sdb.nextRevisionID++
	sdb.validRevisions = append(sdb.validRevisions, revision{id, sdb.journal.length()})
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
	snapshot := sdb.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	sdb.journal.revert(sdb, snapshot)
	sdb.validRevisions = sdb.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
func (sdb *IntraBlockState) GetRefund() uint64 {
	return sdb.refund
}

func updateAccount(EIP158Enabled bool, stateWriter StateWriter, addr common.Address, stateObject *stateObject, isDirty bool) error {
	emptyRemoval := EIP158Enabled && stateObject.empty()
	if stateObject.suicided || (isDirty && emptyRemoval) {
		if err := stateWriter.DeleteAccount(addr, &stateObject.original); err != nil {
			return err
		}
		stateObject.deleted = true
	}
	if isDirty && (stateObject.created || !stateObject.suicided) && !emptyRemoval {
		stateObject.deleted = false
		// Write any contract code associated with the state object
		if stateObject.code != nil && stateObject.dirtyCode {
			if err := stateWriter.UpdateAccountCode(addr, stateObject.data.Incarnation, common.BytesToHash(stateObject.CodeHash()), stateObject.code); err != nil {
				return err
			}
		}
		if stateObject.created {
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

func printAccount(EIP158Enabled bool, addr common.Address, stateObject *stateObject, isDirty bool) {
	emptyRemoval := EIP158Enabled && stateObject.empty()
	if stateObject.suicided || (isDirty && emptyRemoval) {
		fmt.Printf("delete: %x\n", addr)
	}
	if isDirty && (stateObject.created || !stateObject.suicided) && !emptyRemoval {
		// Write any contract code associated with the state object
		if stateObject.code != nil && stateObject.dirtyCode {
			fmt.Printf("UpdateCode: %x,%x\n", addr, stateObject.CodeHash())
		}
		if stateObject.created {
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
func (sdb *IntraBlockState) FinalizeTx(chainRules params.Rules, stateWriter StateWriter) error {
	for addr := range sdb.journal.dirties {
		stateObject, exist := sdb.stateObjects[addr]
		if !exist {
			// ripeMD is 'touched' at block 1714175, in tx 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That tx goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `sdb.journal.dirties` but not in `sdb.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}

		if err := updateAccount(chainRules.IsEIP158, stateWriter, addr, stateObject, true); err != nil {
			return err
		}

		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

// CommitBlock finalizes the state by removing the self destructed objects
// and clears the journal as well as the refunds.
func (sdb *IntraBlockState) CommitBlock(chainRules params.Rules, stateWriter StateWriter) error {
	for addr := range sdb.journal.dirties {
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		if err := updateAccount(chainRules.IsEIP158, stateWriter, addr, stateObject, isDirty); err != nil {
			return err
		}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

func (sdb *IntraBlockState) Print(chainRules params.Rules) {
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		_, isDirty2 := sdb.journal.dirties[addr]

		printAccount(chainRules.IsEIP158, addr, stateObject, isDirty || isDirty2)
	}
}

// Prepare sets the current transaction hash and index and block hash which is
// used when the EVM emits new state logs.
func (sdb *IntraBlockState) Prepare(thash, bhash common.Hash, ti int) {
	sdb.thash = thash
	sdb.bhash = bhash
	sdb.txIndex = ti
	sdb.accessList = newAccessList()
}

// no not lock
func (sdb *IntraBlockState) clearJournalAndRefund() {
	sdb.journal = newJournal()
	sdb.validRevisions = sdb.validRevisions[:0]
	sdb.refund = 0
}

// PrepareAccessList handles the preparatory steps for executing a state transition with
// regards to both EIP-2929 and EIP-2930:
//
// - Add sender to access list (2929)
// - Add destination to access list (2929)
// - Add precompiles to access list (2929)
// - Add the contents of the optional tx access list (2930)
//
// This method should only be called if Yolov3/Berlin/2929+2930 is applicable at the current number.
func (sdb *IntraBlockState) PrepareAccessList(sender common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	sdb.AddAddressToAccessList(sender)
	if dst != nil {
		sdb.AddAddressToAccessList(*dst)
		// If it's a create-tx, the destination will be added inside evm.create
	}
	for _, addr := range precompiles {
		sdb.AddAddressToAccessList(addr)
	}
	for _, el := range list {
		sdb.AddAddressToAccessList(el.Address)
		for _, key := range el.StorageKeys {
			sdb.AddSlotToAccessList(el.Address, key)
		}
	}
}

// AddAddressToAccessList adds the given address to the access list
func (sdb *IntraBlockState) AddAddressToAccessList(addr common.Address) {
	if sdb.accessList.AddAddress(addr) {
		sdb.journal.append(accessListAddAccountChange{&addr})
	}
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (sdb *IntraBlockState) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrMod, slotMod := sdb.accessList.AddSlot(addr, slot)
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
}

// AddressInAccessList returns true if the given address is in the access list.
func (sdb *IntraBlockState) AddressInAccessList(addr common.Address) bool {
	return sdb.accessList.ContainsAddress(addr)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (sdb *IntraBlockState) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return sdb.accessList.Contains(addr, slot)
}
