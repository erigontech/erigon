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
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

type revision struct {
	id           int
	journalIndex int
}

var (
	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)

	// emptyCode is the known hash of the empty EVM bytecode.
	// DESCRIBED: docs/programmers_guide/guide.md#code-hash
	emptyCode = crypto.Keccak256Hash(nil)
)

type proofList [][]byte

func (n *proofList) Put(key []byte, value []byte) error {
	*n = append(*n, value)
	return nil
}

type StateTracer interface {
	CaptureAccountRead(account common.Address) error
	CaptureAccountWrite(account common.Address) error
}

// IntraBlockState is responsible for caching and managing state changes
// that occur during block's execution.
type IntraBlockState struct {
	sync.RWMutex
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

	preimages map[common.Hash][]byte

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *journal
	validRevisions []revision
	nextRevisionID int
	tracer         StateTracer
	trace          bool
}

// Create a new state from a given trie
func New(stateReader StateReader) *IntraBlockState {
	return &IntraBlockState{
		stateReader:       stateReader,
		stateObjects:      make(map[common.Address]*stateObject),
		stateObjectsDirty: make(map[common.Address]struct{}),
		nilAccounts:       make(map[common.Address]struct{}),
		logs:              make(map[common.Hash][]*types.Log),
		preimages:         make(map[common.Hash][]byte),
		journal:           newJournal(),
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
		preimages:         make(map[common.Hash][]byte, len(sdb.preimages)),
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
	for hash, preimage := range sdb.preimages {
		ibs.preimages[hash] = preimage
	}
	return ibs
}

func (sdb *IntraBlockState) SetTracer(tracer StateTracer) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.tracer = tracer
}

func (sdb *IntraBlockState) SetTrace(trace bool) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.trace = trace
}

// setError remembers the first non-nil error it is called with.
func (sdb *IntraBlockState) setError(err error) {
	sdb.Lock()
	defer sdb.Unlock()
	sdb.setErrorUnsafe(err)
}

// setErrorUnsafe sets error but should be called in medhods that already have locks
func (sdb *IntraBlockState) setErrorUnsafe(err error) {
	if sdb.dbErr == nil {
		sdb.dbErr = err
	}
}

func (sdb *IntraBlockState) Error() error {
	sdb.RLock()
	defer sdb.RUnlock()
	return sdb.dbErr
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (sdb *IntraBlockState) Reset() error {
	sdb.Lock()
	defer sdb.Unlock()

	sdb.stateObjects = make(map[common.Address]*stateObject)
	sdb.stateObjectsDirty = make(map[common.Address]struct{})
	sdb.thash = common.Hash{}
	sdb.bhash = common.Hash{}
	sdb.txIndex = 0
	sdb.logs = make(map[common.Hash][]*types.Log)
	sdb.logSize = 0
	sdb.preimages = make(map[common.Hash][]byte)
	sdb.clearJournalAndRefund()
	return nil
}

func (sdb *IntraBlockState) AddLog(log *types.Log) {
	sdb.Lock()
	defer sdb.Unlock()

	sdb.journal.append(addLogChange{txhash: sdb.thash})

	log.TxHash = sdb.thash
	log.BlockHash = sdb.bhash
	log.TxIndex = uint(sdb.txIndex)
	log.Index = sdb.logSize
	sdb.logs[sdb.thash] = append(sdb.logs[sdb.thash], log)
	sdb.logSize++
}

func (sdb *IntraBlockState) GetLogs(hash common.Hash) []*types.Log {
	sdb.RLock()
	defer sdb.RUnlock()

	return sdb.logs[hash]
}

func (sdb *IntraBlockState) Logs() []*types.Log {
	sdb.RLock()
	defer sdb.RUnlock()
	var logs []*types.Log
	for _, lgs := range sdb.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (sdb *IntraBlockState) AddPreimage(hash common.Hash, preimage []byte) {
	sdb.Lock()
	defer sdb.Unlock()
	if _, ok := sdb.preimages[hash]; !ok {
		sdb.journal.append(addPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		sdb.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (sdb *IntraBlockState) Preimages() map[common.Hash][]byte {
	sdb.Lock()
	defer sdb.Unlock()
	return sdb.preimages
}

// AddRefund adds gas to the refund counter
func (sdb *IntraBlockState) AddRefund(gas uint64) {
	sdb.Lock()
	defer sdb.Unlock()

	sdb.journal.append(refundChange{prev: sdb.refund})
	sdb.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (sdb *IntraBlockState) SubRefund(gas uint64) {
	sdb.Lock()
	defer sdb.Unlock()

	sdb.journal.append(refundChange{prev: sdb.refund})
	if gas > sdb.refund {
		panic("Refund counter below zero")
	}
	sdb.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (sdb *IntraBlockState) Exist(addr common.Address) bool {
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()

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

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCode(addr common.Address) []byte {
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()

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
	len, err := sdb.stateReader.ReadAccountCodeSize(addr, common.BytesToHash(stateObject.CodeHash()))
	if err != nil {
		sdb.setErrorUnsafe(err)
	}
	return len
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCodeHash(addr common.Address) common.Hash {
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()

	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		stateObject.GetState(key, value)
	} else {
		value.Clear()
	}
}

// GetProof returns the Merkle proof for a given account
func (sdb *IntraBlockState) GetProof(a common.Address) ([][]byte, error) {
	//sdb.Lock()
	//defer sdb.Unlock()
	//var proof proofList
	//err := sdb.trie.Prove(crypto.Keccak256(a.Bytes()), 0, &proof)
	//return [][]byte(proof), err
	return nil, nil
}

// GetStorageProof returns the storage proof for a given key
func (sdb *IntraBlockState) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
	//sdb.Lock()
	//defer sdb.Unlock()
	//var proof proofList
	//trie := sdb.StorageTrie(a)
	//if trie == nil {
	//	return proof, errors.New("storage trie for requested address does not exist")
	//}
	//err := trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	//return [][]byte(proof), err
	return nil, nil
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) GetCommittedState(addr common.Address, key *common.Hash, value *uint256.Int) {
	sdb.Lock()
	defer sdb.Unlock()

	stateObject := sdb.getStateObject(addr)
	if stateObject != nil && !stateObject.deleted {
		stateObject.GetCommittedState(key, value)
	} else {
		value.Clear()
	}
}

func (sdb *IntraBlockState) HasSuicided(addr common.Address) bool {
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()

	if sdb.trace {
		fmt.Printf("AddBalance %x, %d\n", addr, amount)
	}
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}
	sdb.Unlock()

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SubBalance(addr common.Address, amount *uint256.Int) {
	sdb.Lock()
	if sdb.trace {
		fmt.Printf("SubBalance %x, %d\n", addr, amount)
	}
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}

	}
	sdb.Unlock()

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetBalance(addr common.Address, amount *uint256.Int) {
	sdb.Lock()
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}
	sdb.Unlock()

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetNonce(addr common.Address, nonce uint64) {
	sdb.Lock()
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}
	sdb.Unlock()

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

// DESCRIBED: docs/programmers_guide/guide.md#code-hash
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (sdb *IntraBlockState) SetCode(addr common.Address, code []byte) {
	sdb.Lock()
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}
	sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()
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

var nullLocation = common.Hash{}
var nullValue = common.Big0

// do not lock!!!
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
	sdb.Lock()
	defer sdb.Unlock()

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
	sdb.Lock()
	defer sdb.Unlock()
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
	sdb.Lock()
	defer sdb.Unlock()
	id := sdb.nextRevisionID
	sdb.nextRevisionID++
	sdb.validRevisions = append(sdb.validRevisions, revision{id, sdb.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (sdb *IntraBlockState) RevertToSnapshot(revid int) {
	sdb.Lock()
	defer sdb.Unlock()
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
	sdb.RLock()
	defer sdb.RUnlock()
	return sdb.refund
}

func updateAccount(ctx context.Context, stateWriter StateWriter, addr common.Address, stateObject *stateObject, isDirty bool) error {
	emptyRemoval := params.GetForkFlag(ctx, params.IsEIP158Enabled) && stateObject.empty()
	if stateObject.suicided || (isDirty && emptyRemoval) {
		if err := stateWriter.DeleteAccount(ctx, addr, &stateObject.original); err != nil {
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
		if err := stateObject.updateTrie(ctx, stateWriter); err != nil {
			return err
		}
		if err := stateWriter.UpdateAccountData(ctx, addr, &stateObject.original, &stateObject.data); err != nil {
			return err
		}
	}
	return nil
}

// FinalizeTx should be called after every transaction.
func (sdb *IntraBlockState) FinalizeTx(ctx context.Context, stateWriter StateWriter) error {
	sdb.Lock()
	defer sdb.Unlock()

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

		if err := updateAccount(ctx, stateWriter, addr, stateObject, true); err != nil {
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
func (sdb *IntraBlockState) CommitBlock(ctx context.Context, stateWriter StateWriter) error {
	sdb.Lock()
	defer sdb.Unlock()

	for addr := range sdb.journal.dirties {
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		if err := updateAccount(ctx, stateWriter, addr, stateObject, isDirty); err != nil {
			return err
		}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

// Prepare sets the current transaction hash and index and block hash which is
// used when the EVM emits new state logs.
func (sdb *IntraBlockState) Prepare(thash, bhash common.Hash, ti int) {
	sdb.Lock()
	defer sdb.Unlock()

	sdb.thash = thash
	sdb.bhash = bhash
	sdb.txIndex = ti
}

// no not lock
func (sdb *IntraBlockState) clearJournalAndRefund() {
	sdb.journal = newJournal()
	sdb.validRevisions = sdb.validRevisions[:0]
	sdb.refund = 0
}
