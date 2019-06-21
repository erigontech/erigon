// Copyright 2014 The go-ethereum Authors
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
	"bytes"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"

	//"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"

	"github.com/petar/GoLLRB/llrb"
)

type revision struct {
	id           int
	journalIndex int
}

var (
	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)

	// emptyCode is the known hash of the empty EVM bytecode.
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

// StateDBs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type StateDB struct {
	stateReader StateReader

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[common.Address]*stateObject
	stateObjectsDirty map[common.Address]struct{}

	nilAccounts map[common.Address]struct{} // Remember non-existent account to avoid reading them again

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by StateDB.Commit.
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
	nextRevisionId int
	tracer         StateTracer
	trace          bool
}

// Create a new state from a given trie
func New(stateReader StateReader) *StateDB {
	return &StateDB{
		stateReader:       stateReader,
		stateObjects:      make(map[common.Address]*stateObject),
		stateObjectsDirty: make(map[common.Address]struct{}),
		nilAccounts:       make(map[common.Address]struct{}),
		logs:              make(map[common.Hash][]*types.Log),
		preimages:         make(map[common.Hash][]byte),
		journal:           newJournal(),
	}
}

func (sdb *StateDB) SetTracer(tracer StateTracer) {
	sdb.tracer = tracer
}

func (sdb *StateDB) SetTrace(trace bool) {
	sdb.trace = trace
}

// setError remembers the first non-nil error it is called with.
func (sdb *StateDB) setError(err error) {
	if sdb.dbErr == nil {
		sdb.dbErr = err
	}
}

func (sdb *StateDB) Error() error {
	return sdb.dbErr
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (sdb *StateDB) Reset() error {
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

func (sdb *StateDB) AddLog(log *types.Log) {
	sdb.journal.append(addLogChange{txhash: sdb.thash})

	log.TxHash = sdb.thash
	log.BlockHash = sdb.bhash
	log.TxIndex = uint(sdb.txIndex)
	log.Index = sdb.logSize
	sdb.logs[sdb.thash] = append(sdb.logs[sdb.thash], log)
	sdb.logSize++
}

func (sdb *StateDB) GetLogs(hash common.Hash) []*types.Log {
	return sdb.logs[hash]
}

func (sdb *StateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range sdb.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (sdb *StateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := sdb.preimages[hash]; !ok {
		sdb.journal.append(addPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		sdb.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (sdb *StateDB) Preimages() map[common.Hash][]byte {
	return sdb.preimages
}

// AddRefund adds gas to the refund counter
func (sdb *StateDB) AddRefund(gas uint64) {
	sdb.journal.append(refundChange{prev: sdb.refund})
	sdb.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (sdb *StateDB) SubRefund(gas uint64) {
	sdb.journal.append(refundChange{prev: sdb.refund})
	if gas > sdb.refund {
		panic("Refund counter below zero")
	}
	sdb.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (sdb *StateDB) Exist(addr common.Address) bool {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	//fmt.Printf("Checking existence of %s\n", hex.EncodeToString(addr[:]))
	return sdb.getStateObject(addr) != nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (sdb *StateDB) Empty(addr common.Address) bool {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	so := sdb.getStateObject(addr)
	return so == nil || so.empty()
}

// Retrieve the balance from the given address or 0 if object not found
func (sdb *StateDB) GetBalance(addr common.Address) *big.Int {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.Big0
}

func (sdb *StateDB) GetNonce(addr common.Address) uint64 {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Nonce()
	}

	return 0
}

func (sdb *StateDB) GetCode(addr common.Address) []byte {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
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

func (sdb *StateDB) GetCodeSize(addr common.Address) int {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil {
		return 0
	}
	if stateObject.code != nil {
		return len(stateObject.code)
	}
	len, err := sdb.stateReader.ReadAccountCodeSize(common.BytesToHash(stateObject.CodeHash()))
	if err != nil {
		sdb.setError(err)
	}
	return len
}

func (sdb *StateDB) GetCodeHash(addr common.Address) common.Hash {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

// GetState retrieves a value from the given account's storage trie.
func (sdb *StateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetState(hash)
	}
	return common.Hash{}
}

// GetProof returns the MerkleProof for a given Account
func (sdb *StateDB) GetProof(a common.Address) ([][]byte, error) {
	//var proof proofList
	//err := sdb.trie.Prove(crypto.Keccak256(a.Bytes()), 0, &proof)
	//return [][]byte(proof), err
	return nil, nil
}

// GetProof returns the StorageProof for given key
func (sdb *StateDB) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
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
func (sdb *StateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetCommittedState(hash)
	}
	return common.Hash{}
}

func (sdb *StateDB) HasSuicided(addr common.Address) bool {
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.suicided
	}
	return false
}

func (sdb *StateDB) StorageSize(addr common.Address) *uint64 {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountRead err", err)
		}

	}
	stateObject := sdb.getStateObject(addr)
	if stateObject != nil {
		return stateObject.StorageSize()
	}

	return nil
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr.
func (sdb *StateDB) AddBalance(addr common.Address, amount *big.Int) {
	if sdb.trace {
		fmt.Printf("AddBalance %x, %d\n", addr, amount)
	}
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}
	}
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (sdb *StateDB) SubBalance(addr common.Address, amount *big.Int) {
	if sdb.trace {
		fmt.Printf("SubBalance %x, %d\n", addr, amount)
	}
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}

	}
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

func (sdb *StateDB) SetBalance(addr common.Address, amount *big.Int) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}

	}
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (sdb *StateDB) SetNonce(addr common.Address, nonce uint64) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}

	}
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (sdb *StateDB) SetCode(addr common.Address, code []byte) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}

	}
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (sdb *StateDB) SetState(addr common.Address, key, value common.Hash) {
	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(key, value)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (sdb *StateDB) Suicide(addr common.Address) bool {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
		err = sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}
	}
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil {
		return false
	}
	sdb.journal.append(suicideChange{
		account:     &addr,
		prev:        stateObject.suicided,
		prevbalance: new(big.Int).Set(stateObject.Balance()),
	})
	stateObject.markSuicided()
	stateObject.data.Balance = new(big.Int)

	return true
}

func (sdb *StateDB) IncreaseStorageSize(addr common.Address) {
	sdb.changeStorageSize(addr, 1)
}

func (sdb *StateDB) DecreaseStorageSize(addr common.Address) {
	sdb.changeStorageSize(addr, -1)
}

// todo check is it correct 0 location value
var nullLocation = common.Hash{}
var nullValue = common.Big0

func (sdb *StateDB) SetStorageSize(addr common.Address, loc common.Hash, val *big.Int) {
	if loc == nullLocation && val == nullValue {
		sdb.IncreaseStorageSize(addr)
	}
	if loc != nullLocation && val == nullValue {
		sdb.DecreaseStorageSize(addr)
	}
}

func (sdb *StateDB) changeStorageSize(addr common.Address, sizeDiff int64) {
	if sizeDiff == 0 {
		return
	}

	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Println("CaptureAccountWrite err", err)
		}
	}

	stateObject := sdb.GetOrNewStateObject(addr)
	if stateObject == nil {
		return
	}

	size := sdb.StorageSize(addr)
	if size == nil {
		size = new(uint64)
		*size = HugeNumber
	}
	if size != nil && *size == 0 {
		*size = HugeNumber
	}

	if sizeDiff >= 0 {
		*size = *size + uint64(sizeDiff)
	} else {
		*size = *size - uint64(-sizeDiff)
	}

	stateObject.SetStorageSize(size)
}

// Retrieve a state object given my the address. Returns nil if not found.
func (sdb *StateDB) getStateObject(addr common.Address) (stateObject *stateObject) {
	// Prefer 'live' objects.
	if obj := sdb.stateObjects[addr]; obj != nil {
		if obj.deleted {
			return nil
		}
		return obj
	}

	// Load the object from the database.
	if _, ok := sdb.nilAccounts[addr]; ok {
		return nil
	}
	account, err := sdb.stateReader.ReadAccountData(addr)
	if err != nil {
		sdb.setError(err)
		return nil
	}
	if account == nil {
		sdb.nilAccounts[addr] = struct{}{}
		return nil
	}
	// Insert into the live set.
	original := *account // Copy
	obj := newObject(sdb, addr, *account, original)
	sdb.setStateObject(obj)
	return obj
}

type AccountItem struct {
	SecKey  []byte
	Balance *big.Int
}

func (a *AccountItem) Less(b llrb.Item) bool {
	bi := b.(*AccountItem)
	c := a.Balance.Cmp(bi.Balance)
	if c == 0 {
		return bytes.Compare(a.SecKey, bi.SecKey) < 0
	} else {
		return c < 0
	}
}

func (sdb *StateDB) setStateObject(object *stateObject) {
	sdb.stateObjects[object.Address()] = object
}

// Retrieve a state object or create a new state object if nil.
func (sdb *StateDB) GetOrNewStateObject(addr common.Address) *stateObject {
	stateObject := sdb.getStateObject(addr)
	if stateObject == nil || stateObject.deleted {
		stateObject, _ = sdb.createObject(addr, stateObject)
	}
	return stateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.
func (sdb *StateDB) createObject(addr common.Address, previous *stateObject) (newobj, prev *stateObject) {
	//fmt.Printf("CREATE %x\n", addr[:])
	prev = previous
	var account accounts.Account
	var original accounts.Account
	if previous == nil {
		account = accounts.Account{}
		account.Root.SetBytes(emptyRoot[:])
		original = accounts.Account{}
	} else {
		account = previous.data
		original = previous.original
	}
	newobj = newObject(sdb, addr, account, original)
	newobj.setNonce(0) // sets the object to dirty
	if prev == nil {
		sdb.journal.append(createObjectChange{account: &addr})
	} else {
		sdb.journal.append(resetObjectChange{prev: prev})
	}
	sdb.setStateObject(newobj)
	return newobj, prev
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
func (sdb *StateDB) CreateAccount(addr common.Address, checkPrev bool) {
	if sdb.tracer != nil {
		err := sdb.tracer.CaptureAccountRead(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountRead err", err)
		}
		err = sdb.tracer.CaptureAccountWrite(addr)
		if sdb.trace {
			fmt.Printf("CaptureAccountWrite err", err)
		}
	}
	var previous *stateObject
	if checkPrev {
		previous = sdb.getStateObject(addr)
	}
	newObj, prev := sdb.createObject(addr, previous)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
	}
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (sdb *StateDB) Copy() *StateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &StateDB{
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
		// As documented [here](https://github.com/ledgerwatch/turbo-geth/pull/16485#issuecomment-380438527),
		// and in the Finalise-method, there is a case where an object is in the journal but not
		// in the stateObjects: OOG after touch on ripeMD prior to Byzantium. Thus, we need to check for
		// nil
		if object, exist := sdb.stateObjects[addr]; exist {
			state.stateObjects[addr] = object.deepCopy(state)
			state.stateObjectsDirty[addr] = struct{}{}
		}
	}
	// Above, we don't copy the actual journal. This means that if the copy is copied, the
	// loop above will be a no-op, since the copy's journal is empty.
	// Thus, here we iterate over stateObjects, to enable copies of copies
	for addr := range sdb.stateObjectsDirty {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = sdb.stateObjects[addr].deepCopy(state)
			state.stateObjectsDirty[addr] = struct{}{}
		}
	}
	for hash, logs := range sdb.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		state.logs[hash] = cpy
	}
	for hash, preimage := range sdb.preimages {
		state.preimages[hash] = preimage
	}
	return state
}

// Snapshot returns an identifier for the current revision of the state.
func (sdb *StateDB) Snapshot() int {
	id := sdb.nextRevisionId
	sdb.nextRevisionId++
	sdb.validRevisions = append(sdb.validRevisions, revision{id, sdb.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (sdb *StateDB) RevertToSnapshot(revid int) {
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
func (sdb *StateDB) GetRefund() uint64 {
	return sdb.refund
}

type Addresses []common.Address

func (a *Addresses) Len() int {
	return len(*a)
}
func (a *Addresses) Less(i, j int) bool {
	return bytes.Compare((*a)[i][:], (*a)[j][:]) == -1
}
func (a *Addresses) Swap(i, j int) {
	(*a)[i], (*a)[j] = (*a)[j], (*a)[i]
}

func (sdb *StateDB) Finalise(deleteEmptyObjects bool, stateWriter StateWriter) error {
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

		if stateObject.suicided || (deleteEmptyObjects && stateObject.empty()) {
			if err := stateWriter.DeleteAccount(addr, &stateObject.original); err != nil {
				return err
			}
			stateObject.deleted = true
		} else {
			if err := stateObject.updateTrie(stateWriter); err != nil {
				return err
			}
			if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
				return err
			}
		}
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

// Finalise finalises the state by removing the self destructed objects
// and clears the journal as well as the refunds.
func (sdb *StateDB) Commit(deleteEmptyObjects bool, stateWriter StateWriter) error {
	for addr := range sdb.journal.dirties {
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	for addr, stateObject := range sdb.stateObjects {
		_, isDirty := sdb.stateObjectsDirty[addr]
		//fmt.Printf("%x %d %x %x\n", addr[:], stateObject.data.Balance, stateObject.data.CodeHash, stateObject.data.Root[:])

		if stateObject.suicided || (isDirty && deleteEmptyObjects && stateObject.empty()) {
			if err := stateWriter.DeleteAccount(addr, &stateObject.original); err != nil {
				return err
			}
			stateObject.deleted = true
		} else if isDirty {
			// Write any contract code associated with the state object
			if stateObject.code != nil && stateObject.dirtyCode {
				if err := stateWriter.UpdateAccountCode(common.BytesToHash(stateObject.CodeHash()), stateObject.code); err != nil {
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
	}
	// Invalidate journal because reverting across transactions is not allowed.
	sdb.clearJournalAndRefund()
	return nil
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
/*
func (tds *TrieDbState) IntermediateRoot(s *StateDB, deleteEmptyObjects bool) (common.Hash, error) {
	if err := s.Finalise(deleteEmptyObjects, tds.TrieStateWriter()); err != nil {
		return common.Hash{}, err
	}
	return tds.TrieRoot()
}
*/

// Prepare sets the current transaction hash and index and block hash which is
// used when the EVM emits new state logs.
func (sdb *StateDB) Prepare(thash, bhash common.Hash, ti int) {
	sdb.thash = thash
	sdb.bhash = bhash
	sdb.txIndex = ti
}

func (sdb *StateDB) clearJournalAndRefund() {
	sdb.journal = newJournal()
	sdb.validRevisions = sdb.validRevisions[:0]
	sdb.refund = 0
}
