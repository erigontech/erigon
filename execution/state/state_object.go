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
	"bytes"
	"fmt"
	"io"
	"maps"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var stateObjectPool = sync.Pool{
	New: func() any {
		return &stateObject{
			originStorage:      make(Storage),
			blockOriginStorage: make(Storage),
			dirtyStorage:       make(Storage),
		}
	},
}

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage map[accounts.StorageKey]uint256.Int

func (s Storage) String() string {
	var str strings.Builder
	for key, value := range s {
		str.WriteString(fmt.Sprintf("%X : %X\n", key, value))
	}

	return str.String()
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
	address  accounts.Address
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

func (so *stateObject) deepCopy(db *IntraBlockState) *stateObject {
	obj := stateObjectPool.Get().(*stateObject)
	obj.db = db
	obj.address = so.address
	obj.data.Copy(&so.data)
	obj.original.Copy(&so.original)
	obj.code = so.code
	// Clear and copy storage maps
	clear(obj.dirtyStorage)
	maps.Copy(obj.dirtyStorage, so.dirtyStorage)
	clear(obj.originStorage)
	maps.Copy(obj.originStorage, so.originStorage)
	clear(obj.blockOriginStorage)
	maps.Copy(obj.blockOriginStorage, so.blockOriginStorage)
	if so.fakeStorage != nil {
		obj.fakeStorage = so.fakeStorage.Copy()
	}
	obj.selfdestructed = so.selfdestructed
	obj.dirtyCode = so.dirtyCode
	obj.deleted = so.deleted
	obj.newlyCreated = so.newlyCreated
	obj.createdContract = so.createdContract
	return obj
}

// newObject creates a state object from the pool.
func newObject(db *IntraBlockState, address accounts.Address, data, original *accounts.Account) *stateObject {
	so := stateObjectPool.Get().(*stateObject)
	so.db = db
	so.address = address
	so.data.Copy(data)

	if so.data.CodeHash.IsEmpty() {
		so.data.CodeHash = accounts.EmptyCodeHash
	}
	if so.data.Root == (common.Hash{}) {
		so.data.Root = empty.RootHash
	}
	so.original.Copy(original)
	return so
}

// release returns the stateObject to the pool after resetting it.
func (so *stateObject) release() {
	so.db = nil
	so.address = accounts.NilAddress
	so.data = accounts.Account{}
	so.original = accounts.Account{}
	so.code = nil
	clear(so.originStorage)
	clear(so.blockOriginStorage)
	clear(so.dirtyStorage)
	so.fakeStorage = nil
	so.dirtyCode = false
	so.selfdestructed = false
	so.deleted = false
	so.newlyCreated = false
	so.createdContract = false
	stateObjectPool.Put(so)
}

// EncodeRLP implements rlp.Encoder.
func (so *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, so.data)
}

func (so *stateObject) markSelfdestructed() {
	so.selfdestructed = true
}

// GetState returns a value from account storage.
func (so *stateObject) GetState(key accounts.StorageKey) (uint256.Int, bool) {
	// If the fake storage is set, only lookup the state here (in the debugging mode)
	if so.fakeStorage != nil {
		return so.fakeStorage[key], false
	}
	value, dirty := so.dirtyStorage[key]
	if dirty {
		return value, false
	}
	// Otherwise return the entry's original value
	value, _ = so.GetCommittedState(key)
	return value, true
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (so *stateObject) GetOriginState(key accounts.StorageKey) (uint256.Int, bool) {
	value, cached := so.originStorage[key]
	return value, cached
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (so *stateObject) GetCommittedState(key accounts.StorageKey) (uint256.Int, error) {
	// If the fake storage is set, only lookup the state here (in the debugging mode)
	if so.fakeStorage != nil {
		return so.fakeStorage[key], nil
	}
	// If we have the original value cached, return that
	{
		value, cached := so.originStorage[key]
		if cached {
			return value, nil
		}
	}
	if so.createdContract {
		return uint256.Int{}, nil
	}
	// Load from DB in case it is missing.
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (so.db.trace || dbg.TraceAccount(so.address.Handle()))) {
		so.db.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", so.db.blockNum, so.db.txIndex, so.db.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	res, ok, err := so.db.stateReader.ReadAccountStorage(so.address, key)
	if dbg.KVReadLevelledMetrics {
		so.db.storageReadDuration += time.Since(readStart)
	}
	so.db.storageReadCount++
	so.db.stateReader.SetTrace(false, "")

	if err != nil {
		return uint256.Int{}, err
	}

	if !ok {
		res.Clear()
	}

	so.originStorage[key] = res
	so.blockOriginStorage[key] = res

	return res, err
}

// SetState updates a value in account storage.
func (so *stateObject) SetState(key accounts.StorageKey, value uint256.Int, force bool) (_ bool, err error) {
	// If the fake storage is set, put the temporary state update here.
	if so.fakeStorage != nil {
		so.db.journal.append(fakeStorageChange{
			account:  so.address,
			key:      key,
			prevalue: so.fakeStorage[key],
		})
		so.fakeStorage[key] = value
		return true, nil
	}
	// If the new value is the same as old, don't set
	var prev uint256.Int
	var commited bool
	var source ReadSource

	// we need to use versioned read here otherwise we will miss versionmap entries
	prev, source, _, err = versionedRead(so.db, so.address, StoragePath, key, false, u256.N0,
		func(v uint256.Int) uint256.Int {
			return v
		},
		func(s *stateObject) (uint256.Int, error) {
			var value uint256.Int
			if s != nil && !s.deleted {
				value, commited = s.GetState(key)
			}
			return value, nil
		})

	if err != nil {
		return false, err
	}

	if !force && source != UnknownSource && prev == value {
		return false, nil
	}

	// New value is different, update and journal the change
	so.db.journal.append(storageChange{
		account:     so.address,
		key:         key,
		prevalue:    prev,
		wasCommited: commited,
	})

	if so.db.tracingHooks != nil && so.db.tracingHooks.OnStorageChange != nil {
		so.db.tracingHooks.OnStorageChange(so.address, key, prev, value)
	}
	so.setState(key, value)

	return true, nil
}

// SetStorage replaces the entire state storage with the given one.
//
// After this function is called, all the original state will be ignored and state
// lookup only happens in the fake state storage.
//
// Note this function should only be used for call/block simulation and debugging purpose.
func (so *stateObject) SetStorage(storage Storage) {
	// Allocate fake storage if it's nil.
	if so.fakeStorage == nil {
		so.fakeStorage = make(Storage)
	}
	// Set the fake storage through SetState to ensure journalling is done correctly.
	for key, value := range storage {
		so.SetState(key, value, false)
	}
}

func (so *stateObject) setState(key accounts.StorageKey, value uint256.Int) {
	so.dirtyStorage[key] = value
}

// updateStorage writes cached storage modifications into the object's storage trie.
func (so *stateObject) updateStorage(stateWriter StateWriter) error {
	// When using full state override, only the fake storage matters (see also SetStorage)
	if so.fakeStorage != nil {
		// First, delete the account to wipe out the original storage
		err := stateWriter.DeleteAccount(so.address, &so.original)
		if err != nil {
			return err
		}
		// Then, we need to apply the fake storage changes to compute the state root correctly
		err = so.applyStorageChanges(stateWriter, so.fakeStorage)
		if err != nil {
			return err
		}
		return nil
	}
	// Normal behaviour, apply the dirty storage changes
	err := so.applyStorageChanges(stateWriter, so.dirtyStorage)
	if err != nil {
		return err
	}
	return nil
}

func (so *stateObject) applyStorageChanges(stateWriter StateWriter, updatedStorage Storage) error {
	for key, value := range updatedStorage {
		blockOriginValue := so.blockOriginStorage[key]
		if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (so.db.trace || dbg.TraceAccount(so.address.Handle()))) {
			if _, ok := stateWriter.(*NoopWriter); !ok || dbg.TraceNoopIO {
				fmt.Printf("%d (%d.%d) Update Storage (%T): %x,%x,%s->%s\n", so.db.blockNum, so.db.txIndex, so.db.version,
					stateWriter, so.address, key, blockOriginValue.Hex(), value.Hex())
			}
		}
		if err := stateWriter.WriteAccountStorage(so.address, so.data.GetIncarnation(), key, blockOriginValue, value); err != nil {
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

func (so *stateObject) SetBalance(amount uint256.Int, wasCommited bool, reason tracing.BalanceChangeReason) {
	so.db.journal.append(balanceChange{
		account:     so.address,
		prev:        so.data.Balance,
		wasCommited: wasCommited,
	})
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnBalanceChange != nil {
		so.db.tracingHooks.OnBalanceChange(so.address, so.data.Balance, amount, reason)
	}
	so.setBalance(amount)
}

func (so *stateObject) setBalance(amount uint256.Int) {
	so.data.Balance = amount
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
func (so *stateObject) Address() accounts.Address {
	return so.address
}

// Code returns the contract code associated with this object, if any.
func (so *stateObject) Code() ([]byte, error) {
	if so.code != nil {
		return so.code, nil
	}
	if so.data.CodeHash.IsEmpty() {
		return nil, nil
	}

	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (so.db.trace || dbg.TraceAccount(so.address.Handle()))) {
		so.db.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", so.db.blockNum, so.db.txIndex, so.db.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	code, err := so.db.stateReader.ReadAccountCode(so.Address())
	if dbg.KVReadLevelledMetrics {
		so.db.codeReadDuration += time.Since(readStart)
		so.db.codeReadCount++
	}
	so.db.stateReader.SetTrace(false, "")

	if err != nil {
		return nil, fmt.Errorf("can't read code for %x: %w", so.Address(), err)
	}
	so.code = code
	return code, nil
}

func (so *stateObject) SetCode(codeHash accounts.CodeHash, code []byte, wasCommited bool) (bool, error) {
	prevcode, err := so.Code()
	if err != nil {
		return false, err
	}

	if bytes.Equal(prevcode, code) {
		return false, nil
	}

	so.db.journal.append(codeChange{
		account:     so.address,
		prevhash:    so.data.CodeHash,
		prevcode:    prevcode,
		wasCommited: wasCommited,
	})
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnCodeChange != nil {
		so.db.tracingHooks.OnCodeChange(so.address, so.data.CodeHash, prevcode, codeHash, code)
	}
	so.setCode(codeHash, code)
	return true, nil
}

func (so *stateObject) setCode(codeHash accounts.CodeHash, code []byte) {
	so.code = code
	so.data.CodeHash = codeHash
	so.dirtyCode = true
}

func (so *stateObject) SetNonce(nonce uint64, wasCommited bool) {
	so.db.journal.append(nonceChange{
		account:     so.address,
		prev:        so.data.Nonce,
		wasCommited: wasCommited,
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
