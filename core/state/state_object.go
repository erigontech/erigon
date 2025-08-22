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

package state

import (
	"fmt"
	"io"
	"maps"
	"math/big"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage map[common.Hash]uint256.Int

func (s Storage) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}

	return
}

func (s Storage) Copy() Storage {
	return maps.Clone(s)
}

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// First you need to obtain a state object.
// Account values can be accessed and modified through the object.
type stateObject struct {
	address  common.Address
	data     accounts.Account
	original accounts.Account
	db       *IntraBlockState

	// Write caches.
	//trie Trie // storage trie, which becomes non-nil on first access
	code Code // contract bytecode, which gets set when code is loaded

	originStorage Storage // Storage cache of original entries to dedup rewrites
	// blockOriginStorage keeps the values of storage items at the beginning of the block
	// Used to make decision on whether to make a write to the
	// database (value != origin) or not (value == origin)
	blockOriginStorage Storage
	dirtyStorage       Storage // Storage entries that need to be flushed to disk
	fakeStorage        Storage // Fake storage which constructed by caller for debugging purpose.

	// Cache flags.
	// When an object is marked selfdestructed it will be delete from the trie
	// during the "update" phase of the state transition.
	dirtyCode       bool // true if the code was updated
	selfdestructed  bool
	deleted         bool // true if account was deleted during the lifetime of this object
	newlyCreated    bool // true if this object was created in the current transaction
	createdContract bool // true if this object represents a newly created contract
}

// empty returns whether the account is considered empty.
func (so *stateObject) empty() bool {
	return so.data.Nonce == 0 && so.data.Balance.IsZero() && (so.data.CodeHash == empty.CodeHash)
}

func (s *stateObject) deepCopy(db *IntraBlockState) *stateObject {
	stateObject := &stateObject{db: db, address: s.address}
	stateObject.data.Copy(&s.data)
	stateObject.original.Copy(&s.original)
	stateObject.code = s.code
	stateObject.dirtyStorage = s.dirtyStorage.Copy()
	stateObject.originStorage = s.originStorage.Copy()
	stateObject.blockOriginStorage = s.blockOriginStorage.Copy()
	stateObject.selfdestructed = s.selfdestructed
	stateObject.dirtyCode = s.dirtyCode
	stateObject.deleted = s.deleted
	stateObject.newlyCreated = s.newlyCreated
	stateObject.createdContract = s.createdContract
	return stateObject
}

// newObject creates a state object.
func newObject(db *IntraBlockState, address common.Address, data, original *accounts.Account) *stateObject {
	var so = stateObject{
		db:                 db,
		address:            address,
		originStorage:      make(Storage),
		blockOriginStorage: make(Storage),
		dirtyStorage:       make(Storage),
	}
	so.data.Copy(data)
	if !so.data.Initialised {
		so.data.Balance.SetUint64(0)
		so.data.Initialised = true
	}
	if so.data.CodeHash == (common.Hash{}) {
		so.data.CodeHash = empty.CodeHash
	}
	if so.data.Root == (common.Hash{}) {
		so.data.Root = empty.RootHash
	}
	so.original.Copy(original)
	return &so
}

// EncodeRLP implements rlp.Encoder.
func (so *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, so.data)
}

func (so *stateObject) markSelfdestructed() {
	so.selfdestructed = true
}

func (so *stateObject) touch() {
	so.db.journal.append(touchChange{
		account: so.address,
	})
	if so.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		so.db.journal.dirty(so.address)
	}
}

// GetState returns a value from account storage.
func (so *stateObject) GetState(key common.Hash, out *uint256.Int) bool {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if so.fakeStorage != nil {
		*out = so.fakeStorage[key]
		return false
	}
	value, dirty := so.dirtyStorage[key]
	if dirty {
		*out = value
		return false
	}
	// Otherwise return the entry's original value
	so.GetCommittedState(key, out)
	return true
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (so *stateObject) GetCommittedState(key common.Hash, out *uint256.Int) error {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if so.fakeStorage != nil {
		*out = so.fakeStorage[key]
		return nil
	}
	// If we have the original value cached, return that
	{
		value, cached := so.originStorage[key]
		if cached {
			*out = value
			return nil
		}
	}
	if so.createdContract {
		out.Clear()
		return nil
	}
	// Load from DB in case it is missing.
	readStart := time.Now()
	res, ok, err := so.db.stateReader.ReadAccountStorage(so.address, key)
	so.db.storageReadDuration += time.Since(readStart)
	so.db.storageReadCount++

	if err != nil {
		out.Clear()
		return err
	}
	if ok {
		*out = res
	} else {
		out.Clear()
	}

	so.originStorage[key] = *out
	so.blockOriginStorage[key] = *out

	return err
}

// SetState updates a value in account storage.
func (so *stateObject) SetState(key common.Hash, value uint256.Int, force bool) bool {
	// If the fake storage is set, put the temporary state update here.
	if so.fakeStorage != nil {
		so.db.journal.append(fakeStorageChange{
			account:  &so.address,
			key:      key,
			prevalue: so.fakeStorage[key],
		})
		so.fakeStorage[key] = value
		return true
	}
	// If the new value is the same as old, don't set
	var prev uint256.Int
	var commited bool

	// we need to use versioned read here otherwise we will miss versionmap entries
	prev, _, _ = versionedRead(so.db, so.address, StatePath, key, false, *u256.N0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			var value uint256.Int
			if s != nil && !s.deleted {
				commited = s.GetState(key, &value)
			}
			return value, nil
		})

	if !force && prev == value {
		return false
	}

	// New value is different, update and journal the change
	so.db.journal.append(storageChange{
		account:     &so.address,
		key:         key,
		prevalue:    prev,
		wasCommited: commited,
	})

	if so.db.tracingHooks != nil && so.db.tracingHooks.OnStorageChange != nil {
		so.db.tracingHooks.OnStorageChange(so.address, key, prev, value)
	}
	so.setState(key, value)

	return true
}

// SetStorage replaces the entire state storage with the given one.
//
// After this function is called, all original state will be ignored and state
// lookup only happens in the fake state storage.
//
// Note this function should only be used for debugging purpose.
func (so *stateObject) SetStorage(storage Storage) {
	// Allocate fake storage if it's nil.
	if so.fakeStorage == nil {
		so.fakeStorage = make(Storage)
	}
	for key, value := range storage {
		so.fakeStorage[key] = value
	}
	// Don't bother journal since this function should only be used for
	// debugging and the `fake` storage won't be committed to database.
}

func (so *stateObject) setState(key common.Hash, value uint256.Int) {
	so.dirtyStorage[key] = value
}

// updateStotage writes cached storage modifications into the object's storage trie.
func (so *stateObject) updateStotage(stateWriter StateWriter) error {
	for key, value := range so.dirtyStorage {
		if err := stateWriter.WriteAccountStorage(so.address, so.data.GetIncarnation(), key, so.blockOriginStorage[key], value); err != nil {
			return err
		}
		so.originStorage[key] = value
	}
	return nil
}
func (so *stateObject) printTrie() {
	for key, value := range so.dirtyStorage {
		fmt.Printf("UpdateStorage: %x,%x,%s\n", so.address, key, value.Hex())
	}
}

func (so *stateObject) SetBalance(amount uint256.Int, reason tracing.BalanceChangeReason) {
	so.db.journal.append(balanceChange{
		account: &so.address,
		prev:    so.data.Balance,
	})
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnBalanceChange != nil {
		so.db.tracingHooks.OnBalanceChange(so.address, so.data.Balance, amount, reason)
	}
	so.setBalance(amount)
}

func (so *stateObject) setBalance(amount uint256.Int) {
	so.data.Balance = amount
	so.data.Initialised = true
}

// Return the gas back to the origin. Used by the Virtual machine or Closures
func (so *stateObject) ReturnGas(gas *big.Int) {}

func (so *stateObject) setIncarnation(incarnation uint64) {
	so.data.SetIncarnation(incarnation)
}

//
// Attribute accessors
//

// Returns the address of the contract/account
func (so *stateObject) Address() common.Address {
	return so.address
}

// Code returns the contract code associated with this object, if any.
func (so *stateObject) Code() ([]byte, error) {
	if so.code != nil {
		return so.code, nil
	}
	if so.data.CodeHash == empty.CodeHash {
		return nil, nil
	}

	readStart := time.Now()
	code, err := so.db.stateReader.ReadAccountCode(so.Address())
	so.db.storageReadDuration += time.Since(readStart)
	so.db.storageReadCount++

	if err != nil {
		return nil, fmt.Errorf("can't code for %x: %w", so.Address(), err)
	}
	so.code = code
	return code, nil
}

func (so *stateObject) SetCode(codeHash common.Hash, code []byte) error {
	prevcode, err := so.Code()
	if err != nil {
		return err
	}
	so.db.journal.append(codeChange{
		account:  &so.address,
		prevhash: so.data.CodeHash,
		prevcode: prevcode,
	})
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnCodeChange != nil {
		so.db.tracingHooks.OnCodeChange(so.address, so.data.CodeHash, prevcode, codeHash, code)
	}
	so.setCode(codeHash, code)
	return nil
}

func (so *stateObject) setCode(codeHash common.Hash, code []byte) {
	so.code = code
	so.data.CodeHash = codeHash
	so.dirtyCode = true
}

func (so *stateObject) SetNonce(nonce uint64) {
	so.db.journal.append(nonceChange{
		account: &so.address,
		prev:    so.data.Nonce,
	})
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnNonceChange != nil {
		so.db.tracingHooks.OnNonceChange(so.address, so.data.Nonce, nonce)
	}
	so.setNonce(nonce)
}

func (so *stateObject) setNonce(nonce uint64) {
	so.data.Nonce = nonce
}

func (so *stateObject) Balance() uint256.Int {
	return so.data.Balance
}

func (so *stateObject) Nonce() uint64 {
	return so.data.Nonce
}

func (so *stateObject) IsDirty() bool {
	return so.dirtyCode || len(so.dirtyStorage) > 0 || so.data != so.original
}

// Never called, but must be present to allow stateObject to be used
// as a vm.Account interface that also satisfies the vm.ContractRef
// interface. Interfaces are awesome.
func (so *stateObject) Value() *big.Int {
	panic("Value on stateObject should never be called")
}
