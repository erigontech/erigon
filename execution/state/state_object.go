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
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var stateObjectPool = sync.Pool{
	New: func() any { return newHeapObject() },
}

func newHeapObject() *stateObject {
	return &stateObject{}
}

type Storage map[accounts.StorageKey]uint256.Int

// set allocates the map on first write, so an object that never writes keeps a nil Storage.
func (s *Storage) set(key accounts.StorageKey, value uint256.Int) {
	if *s == nil {
		*s = make(Storage)
	}
	(*s)[key] = value
}

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

type stateObject struct {
	address  accounts.Address
	data     accounts.Account
	original accounts.Account
	db       *IntraBlockState

	code accounts.Code

	originStorage      Storage
	blockOriginStorage Storage
	dirtyStorage       Storage
	fakeStorage        Storage // overrides all storage reads/writes, for debugging and call simulation

	dirtyCode       bool
	selfdestructed  bool
	deleted         bool
	newlyCreated    bool
	createdContract bool

	// Set by stateObjectArena.alloc; keeps release from pooling a slot the arena owns.
	arena bool
}

func newObject(db *IntraBlockState, address accounts.Address, data, original *accounts.Account) *stateObject {
	so := db.allocStateObject()
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

// reset clears every per-use field, keeping any storage map already allocated.
func (so *stateObject) reset() {
	so.db = nil
	so.address = accounts.NilAddress
	so.data = accounts.Account{}
	so.original = accounts.Account{}
	so.code = accounts.Code{}
	clear(so.originStorage)
	clear(so.blockOriginStorage)
	clear(so.dirtyStorage)
	so.fakeStorage = nil
	so.dirtyCode = false
	so.selfdestructed = false
	so.deleted = false
	so.newlyCreated = false
	so.createdContract = false
}

// release resets the object and pools it, unless the arena owns the slot.
func (so *stateObject) release() {
	so.reset()
	if so.arena {
		return
	}
	stateObjectPool.Put(so)
}

func (so *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &so.data)
}

func (so *stateObject) markSelfdestructed() {
	so.selfdestructed = true
}

func (so *stateObject) GetState(key accounts.StorageKey) (uint256.Int, bool) {
	if so.fakeStorage != nil {
		return so.fakeStorage[key], false
	}
	value, dirty := so.dirtyStorage[key]
	if dirty {
		return value, false
	}
	value, _ = so.GetCommittedState(key)
	return value, true
}

func (so *stateObject) GetOriginState(key accounts.StorageKey) (uint256.Int, bool) {
	value, cached := so.originStorage[key]
	return value, cached
}

func (so *stateObject) GetCommittedState(key accounts.StorageKey) (uint256.Int, error) {
	if so.fakeStorage != nil {
		return so.fakeStorage[key], nil
	}
	{
		value, cached := so.originStorage[key]
		if cached {
			return value, nil
		}
	}
	if so.createdContract {
		if dbg.TraceTransactionIO && (so.db.trace || dbg.TraceAccount(so.address.Handle())) {
			fmt.Printf("%d (%d.%d) GetCommittedState SKIP (createdContract) %x key=%x\n",
				so.db.blockNum, so.db.txIndex, so.db.version, so.address, key)
		}
		return uint256.Int{}, nil
	}
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

	so.originStorage.set(key, res)
	so.blockOriginStorage.set(key, res)

	return res, err
}

func (so *stateObject) SetState(key accounts.StorageKey, value uint256.Int, force bool) (_ bool, err error) {
	if so.fakeStorage != nil {
		so.db.journal.fakeStorageChange(so.address, key, so.fakeStorage[key])
		so.fakeStorage[key] = value
		return true, nil
	}
	var prev uint256.Int
	var commited bool
	var source ReadSource

	// Must use a versioned read here, or version-map entries get missed.
	prev, source, _, commited, err = readStateForSet(so.db, so.address, key)
	if err != nil {
		return false, err
	}

	// commited stays false when the previous value came from a cached read or the
	// version map rather than the readStorage callback; force it true so a revert
	// deletes the versioned write instead of restoring it to prevalue.
	if source != WriteSetRead && source != UnknownSource && source != StorageRead {
		commited = true
	}

	if !force && source != UnknownSource && prev == value {
		return false, nil
	}

	so.db.journal.storageChange(so.address, key, prev, commited)

	if so.db.tracingHooks != nil && so.db.tracingHooks.OnStorageChange != nil {
		so.db.tracingHooks.OnStorageChange(so.address, key, prev, value)
	}
	so.setState(key, value)

	return true, nil
}

func (so *stateObject) SetStorage(storage Storage) {
	if so.fakeStorage == nil {
		so.fakeStorage = make(Storage)
	}
	for key, value := range storage {
		so.SetState(key, value, false)
	}
}

func (so *stateObject) setState(key accounts.StorageKey, value uint256.Int) {
	so.dirtyStorage.set(key, value)
}

func (so *stateObject) updateStorage(stateWriter StateWriter, useBlockOrigin bool) error {
	if so.fakeStorage != nil {
		err := stateWriter.DeleteAccount(so.address, &so.original)
		if err != nil {
			return err
		}
		err = so.applyStorageChanges(stateWriter, so.fakeStorage, useBlockOrigin)
		if err != nil {
			return err
		}
		return nil
	}
	err := so.applyStorageChanges(stateWriter, so.dirtyStorage, useBlockOrigin)
	if err != nil {
		return err
	}
	return nil
}

func (so *stateObject) applyStorageChanges(stateWriter StateWriter, updatedStorage Storage, useBlockOrigin bool) error {
	for key, value := range updatedStorage {
		// CommitBlock must compare against blockOriginStorage, not originStorage: its
		// system-txNum write is required by ComputeCommitment and must never be skipped.
		var originValue uint256.Int
		if useBlockOrigin {
			originValue = so.blockOriginStorage[key]
		} else {
			originValue = so.originStorage[key]
		}
		if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (so.db.trace || dbg.TraceAccount(so.address.Handle()))) {
			if _, ok := stateWriter.(*NoopWriter); !ok || dbg.TraceNoopIO {
				fmt.Printf("%d (%d.%d) Update Storage (%T): %x,%x,%s->%s\n", so.db.blockNum, so.db.txIndex, so.db.version,
					stateWriter, so.address, key, originValue.Hex(), value.Hex())
			}
		}
		if err := stateWriter.WriteAccountStorage(so.address, so.data.GetIncarnation(), key, originValue, value); err != nil {
			return err
		}
		so.originStorage.set(key, value)
	}
	return nil
}

func (so *stateObject) printTrie() {
	for key, value := range so.dirtyStorage {
		fmt.Printf("UpdateStorage: %x,%x,%s\n", so.address, key, value.Hex())
	}
}

func (so *stateObject) SetBalance(amount uint256.Int, wasCommited bool, reason tracing.BalanceChangeReason) {
	so.db.journal.balanceChange(so.address, so.data.Balance, wasCommited)
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnBalanceChange != nil {
		so.db.tracingHooks.OnBalanceChange(so.address, so.data.Balance, amount, reason)
	}
	so.setBalance(amount)
}

func (so *stateObject) setBalance(amount uint256.Int) {
	so.data.Balance = amount
}

func (so *stateObject) ReturnGas(gas *big.Int) {}

func (so *stateObject) setIncarnation(incarnation uint64) {
	so.data.SetIncarnation(incarnation)
}

func (so *stateObject) Address() accounts.Address {
	return so.address
}

func (so *stateObject) Code() ([]byte, error) {
	c, err := so.CodeTyped()
	if err != nil {
		return nil, err
	}
	return c.Bytes, nil
}

func (so *stateObject) CodeTyped() (accounts.Code, error) {
	if so.code.Bytes != nil {
		return so.code, nil
	}
	if so.data.CodeHash.IsEmpty() {
		return accounts.Code{Hash: so.data.CodeHash}, nil
	}

	// versionMap can hold synthetic code from a prior tx's EIP-7702 SetCode that the domain/stateReader doesn't have yet.
	if so.db.versionMap != nil {
		if c, rr, ok := so.db.versionMap.ReadCode(so.address, so.db.txIndex); ok && rr.Status() == MVReadResultDone {
			so.code = c
			return c, nil
		}
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
		return accounts.Code{}, fmt.Errorf("can't read code for %x: %w", so.Address(), err)
	}
	// Trusts the committed (CodeHash, bytes) pair over re-hashing; a codeHash-without-code mismatch reports as empty so SetCode's compare still heals it.
	var c accounts.Code
	if len(code) == 0 {
		c = accounts.EmptyCode
	} else {
		c = accounts.Code{Hash: so.data.CodeHash, Bytes: code}
	}
	so.code = c
	return c, nil
}

func (so *stateObject) SetCode(code accounts.Code, wasCommited bool, reason tracing.CodeChangeReason) (bool, error) {
	prev, err := so.CodeTyped()
	if err != nil {
		return false, err
	}

	// Guards the codeHash-without-code case: a hash match against empty prev bytes must still heal the CodeDomain, not skip.
	if prev.Hash == code.Hash && bytes.Equal(prev.Bytes, code.Bytes) {
		return false, nil
	}

	so.db.journal.codeChange(so.address, prev.Bytes, so.data.CodeHash, wasCommited)
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnCodeChangeV2 != nil {
		so.db.tracingHooks.OnCodeChangeV2(so.address, so.data.CodeHash, prev.Bytes, code.Hash, code.Bytes, reason)
	} else if so.db.tracingHooks != nil && so.db.tracingHooks.OnCodeChange != nil {
		so.db.tracingHooks.OnCodeChange(so.address, so.data.CodeHash, prev.Bytes, code.Hash, code.Bytes)
	}
	so.setCode(code)
	return true, nil
}

func (so *stateObject) setCode(code accounts.Code) {
	so.code = code
	so.data.CodeHash = code.Hash
	so.dirtyCode = true
}

func (so *stateObject) SetNonce(nonce uint64, wasCommited bool, reason tracing.NonceChangeReason) {
	so.db.journal.nonceChange(so.address, so.data.Nonce, wasCommited)
	if so.db.tracingHooks != nil && so.db.tracingHooks.OnNonceChangeV2 != nil {
		so.db.tracingHooks.OnNonceChangeV2(so.address, so.data.Nonce, nonce, reason)
	} else if so.db.tracingHooks != nil && so.db.tracingHooks.OnNonceChange != nil {
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

// Never called; required so stateObject satisfies both vm.Account and vm.ContractRef.
func (so *stateObject) Value() *big.Int {
	panic("Value on stateObject should never be called")
}
