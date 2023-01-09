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

package state

import (
	"bytes"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

var emptyCodeHash = crypto.Keccak256(nil)
var emptyCodeHashH = common.BytesToHash(emptyCodeHash)

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
	cpy := make(Storage)
	for key, value := range s {
		cpy[key] = value
	}

	return cpy
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
	dirtyCode      bool // true if the code was updated
	selfdestructed bool
	deleted        bool // true if account was deleted during the lifetime of this object
	created        bool // true if this object represents a newly created contract
}

// empty returns whether the account is considered empty.
func (so *stateObject) empty() bool {
	return so.data.Nonce == 0 && so.data.Balance.IsZero() && bytes.Equal(so.data.CodeHash[:], emptyCodeHash)
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
		so.data.CodeHash = emptyCodeHashH
	}
	if so.data.Root == (common.Hash{}) {
		so.data.Root = trie.EmptyRoot
	}
	so.original.Copy(original)

	return &so
}

// EncodeRLP implements rlp.Encoder.
func (so *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, so.data)
}

// setError remembers the first non-nil error it is called with.
func (so *stateObject) setError(err error) {
	if so.db.savedErr == nil {
		so.db.savedErr = err
	}
}

func (so *stateObject) markSelfdestructed() {
	so.selfdestructed = true
}

func (so *stateObject) touch() {
	so.db.journal.append(touchChange{
		account: &so.address,
	})
	if so.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		so.db.journal.dirty(so.address)
	}
}

// GetState returns a value from account storage.
func (so *stateObject) GetState(key *common.Hash, out *uint256.Int) {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if so.fakeStorage != nil {
		*out = so.fakeStorage[*key]
		return
	}
	value, dirty := so.dirtyStorage[*key]
	if dirty {
		*out = value
		return
	}
	// Otherwise return the entry's original value
	so.GetCommittedState(key, out)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (so *stateObject) GetCommittedState(key *common.Hash, out *uint256.Int) {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if so.fakeStorage != nil {
		*out = so.fakeStorage[*key]
		return
	}
	// If we have the original value cached, return that
	{
		value, cached := so.originStorage[*key]
		if cached {
			*out = value
			return
		}
	}
	if so.created {
		out.Clear()
		return
	}
	// Load from DB in case it is missing.
	enc, err := so.db.stateReader.ReadAccountStorage(so.address, so.data.GetIncarnation(), key)
	if err != nil {
		so.setError(err)
		out.Clear()
		return
	}
	if enc != nil {
		out.SetBytes(enc)
	} else {
		out.Clear()
	}
	so.originStorage[*key] = *out
	so.blockOriginStorage[*key] = *out
}

// SetState updates a value in account storage.
func (so *stateObject) SetState(key *common.Hash, value uint256.Int) {
	// If the fake storage is set, put the temporary state update here.
	if so.fakeStorage != nil {
		so.db.journal.append(fakeStorageChange{
			account:  &so.address,
			key:      *key,
			prevalue: so.fakeStorage[*key],
		})
		so.fakeStorage[*key] = value
		return
	}
	// If the new value is the same as old, don't set
	var prev uint256.Int
	so.GetState(key, &prev)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	so.db.journal.append(storageChange{
		account:  &so.address,
		key:      *key,
		prevalue: prev,
	})
	so.setState(key, value)
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

func (so *stateObject) setState(key *common.Hash, value uint256.Int) {
	so.dirtyStorage[*key] = value
}

// updateTrie writes cached storage modifications into the object's storage trie.
func (so *stateObject) updateTrie(stateWriter StateWriter) error {
	for key, value := range so.dirtyStorage {
		value := value
		original := so.blockOriginStorage[key]
		so.originStorage[key] = value
		if err := stateWriter.WriteAccountStorage(so.address, so.data.GetIncarnation(), &key, &original, &value); err != nil {
			return err
		}
	}
	return nil
}
func (so *stateObject) printTrie() {
	for key, value := range so.dirtyStorage {
		fmt.Printf("WriteAccountStorage: %x,%x,%s\n", so.address, key, value.Hex())
	}
}

// AddBalance adds amount to so's balance.
// It is used to add funds to the destination account of a transfer.
func (so *stateObject) AddBalance(amount *uint256.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.IsZero() {
		if so.empty() {
			so.touch()
		}

		return
	}

	so.SetBalance(new(uint256.Int).Add(so.Balance(), amount))
}

// SubBalance removes amount from so's balance.
// It is used to remove funds from the origin account of a transfer.
func (so *stateObject) SubBalance(amount *uint256.Int) {
	if amount.IsZero() {
		return
	}
	so.SetBalance(new(uint256.Int).Sub(so.Balance(), amount))
}

func (so *stateObject) SetBalance(amount *uint256.Int) {
	so.db.journal.append(balanceChange{
		account: &so.address,
		prev:    so.data.Balance,
	})
	so.setBalance(amount)
}

func (so *stateObject) setBalance(amount *uint256.Int) {
	so.data.Balance.Set(amount)
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
func (so *stateObject) Code() []byte {
	if so.code != nil {
		return so.code
	}
	if bytes.Equal(so.CodeHash(), emptyCodeHash) {
		return nil
	}
	code, err := so.db.stateReader.ReadAccountCode(so.Address(), so.data.Incarnation, common.BytesToHash(so.CodeHash()))
	if err != nil {
		so.setError(fmt.Errorf("can't load code hash %x: %w", so.CodeHash(), err))
	}
	so.code = code
	return code
}

func (so *stateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := so.Code()
	so.db.journal.append(codeChange{
		account:  &so.address,
		prevhash: so.data.CodeHash,
		prevcode: prevcode,
	})
	so.setCode(codeHash, code)
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
	so.setNonce(nonce)
}

func (so *stateObject) setNonce(nonce uint64) {
	so.data.Nonce = nonce
}

func (so *stateObject) CodeHash() []byte {
	return so.data.CodeHash[:]
}

func (so *stateObject) Balance() *uint256.Int {
	return &so.data.Balance
}

func (so *stateObject) Nonce() uint64 {
	return so.data.Nonce
}

// Never called, but must be present to allow stateObject to be used
// as a vm.Account interface that also satisfies the vm.ContractRef
// interface. Interfaces are awesome.
func (so *stateObject) Value() *big.Int {
	panic("Value on stateObject should never be called")
}
