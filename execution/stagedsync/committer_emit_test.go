// Copyright 2026 The Erigon Authors
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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestEmitTouched_ProducesExpectedUpdates pins the commitment fold the calculator runs
// per block: accumulateTouched + emitTouched read each touched account/slot straight from
// the version map and emit the leaf update, with no EIP knowledge. A present account emits
// its full {balance, nonce, codeHash}; one the write side resolved absent (self-destruct or
// EIP-161 empty) emits a leaf+subtree delete; a slot emits its value or a delete when zeroed.
func TestEmitTouched_ProducesExpectedUpdates(t *testing.T) {
	t.Parallel()

	const txIdx = 3
	const T = txIdx + 1 // baseTxNum 0 ⇒ GetAccount floor includes the write at txIdx
	ver := state.Version{TxIndex: txIdx}
	a := func(b byte) accounts.Address { return accounts.InternAddress([20]byte{b}) }
	k := func(b byte) accounts.StorageKey { return accounts.InternKey([32]byte{b}) }
	codeHash := accounts.NewCode([]byte{0x60, 0x00}).Hash
	acctKey := func(addr accounts.Address) string { v := addr.Value(); return string(v[:]) }
	storKey := func(addr accounts.Address, slot accounts.StorageKey) string {
		av, sv := addr.Value(), slot.Value()
		return string(av[:]) + string(sv[:])
	}
	full := func(bal uint64, nonce uint64, ch accounts.CodeHash) commitment.Update {
		return commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Balance: *uint256.NewInt(bal), Nonce: nonce, CodeHash: ch.Value()}
	}
	del := commitment.Update{Flags: commitment.DeleteUpdate, CodeHash: empty.CodeHash, DeleteStorageSubtree: true}
	storVal := func(v uint64) commitment.Update {
		b := uint256.NewInt(v).Bytes()
		u := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(b))}
		copy(u.Storage[:], b)
		return u
	}
	storDel := commitment.Update{Flags: commitment.DeleteUpdate}

	scenarios := []struct {
		name   string
		build  func() *state.WriteSet
		expect func() map[string]commitment.Update
	}{
		{"live account full update", func() *state.WriteSet {
			return newWS().bal(a(1), ver, *uint256.NewInt(100)).nonce(a(1), ver, 7).codeHash(a(1), ver, codeHash).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(1)): full(100, 7, codeHash)}
		}},
		{"storage plus account", func() *state.WriteSet {
			return newWS().bal(a(3), ver, *uint256.NewInt(5)).nonce(a(3), ver, 1).
				stor(a(3), k(1), ver, *uint256.NewInt(1)).stor(a(3), k(2), ver, *uint256.NewInt(2)).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(3)): full(5, 1, accounts.EmptyCodeHash), storKey(a(3), k(1)): storVal(1), storKey(a(3), k(2)): storVal(2)}
		}},
		{"storage cleared to zero", func() *state.WriteSet {
			return newWS().bal(a(9), ver, *uint256.NewInt(5)).nonce(a(9), ver, 1).stor(a(9), k(3), ver, uint256.Int{}).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(9)): full(5, 1, accounts.EmptyCodeHash), storKey(a(9), k(3)): storDel}
		}},
		{"balance-deleted verdict removes leaf and subtree", func() *state.WriteSet {
			return newWS().emptyRemoval(a(4), ver).build()
		}, func() map[string]commitment.Update { return map[string]commitment.Update{acctKey(a(4)): del} }},
		{"self-destruct preserving balance keeps leaf but wipes subtree", func() *state.WriteSet {
			return newWS().selfDestruct(a(5), ver, true).bal(a(5), ver, *uint256.NewInt(3)).nonce(a(5), ver, 1).build()
		}, func() map[string]commitment.Update {
			u := full(3, 1, accounts.EmptyCodeHash)
			u.DeleteStorageSubtree = true
			return map[string]commitment.Update{acctKey(a(5)): u}
		}},
		{"created contract", func() *state.WriteSet {
			return newWS().createContract(a(7), ver, true).bal(a(7), ver, *uint256.NewInt(9)).nonce(a(7), ver, 1).codeHash(a(7), ver, codeHash).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(7)): full(9, 1, codeHash)}
		}},
		{"zero-balance live touch (nonce keeps it alive)", func() *state.WriteSet {
			return newWS().bal(a(8), ver, uint256.Int{}).nonce(a(8), ver, 1).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(8)): full(0, 1, accounts.EmptyCodeHash)}
		}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			ws := sc.build()
			vm := state.NewVersionMap(nil)
			vm.SetBaseTxNum(0)
			vm.FlushVersionedWrites(ws, true)
			vm.RecordWriteKeysAtTxIndex(txIdx, false, ws)

			got := emitTouchedUpdates(t, vm, T, nil, vm.WritesAtTxIndex(txIdx, false))
			require.Equal(t, sc.expect(), got)
		})
	}
}

// testStorageBase is a base reader that serves both accounts and storage, for the
// cross-block batch-fold resolution emitTouched relies on.
type testStorageBase struct {
	accounts map[accounts.Address]*accounts.Account
	storage  map[string]uint256.Int
}

func (b *testStorageBase) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	return b.accounts[addr], nil
}

func (b *testStorageBase) ReadStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	av, kv := addr.Value(), key.Value()
	v, ok := b.storage[string(av[:])+string(kv[:])]
	return v, ok, nil
}

// TestEmitTouched_CrossBlockStorageResolvesFromBase pins the storage read-through: a slot
// touched in an earlier batch block has no cell in the current block's version map, so its
// value is taken from the committed base. A non-zero base value writes the slot; a base that
// no longer carries it (zero/absent) deletes the leaf. Without the fallback the slot would be
// wrongly deleted on every cross-block fold.
func TestEmitTouched_CrossBlockStorageResolvesFromBase(t *testing.T) {
	t.Parallel()

	const txIdx = 2
	const T = txIdx + 1
	ver := state.Version{TxIndex: txIdx}
	addr := accounts.InternAddress([20]byte{0xcb})
	slot := accounts.InternKey([32]byte{0x7})
	av, sv := addr.Value(), slot.Value()
	storKey := string(av[:]) + string(sv[:])
	acctKey := string(av[:])

	// The access list records the account and the slot (so both are touched), but only the
	// account is flushed — the slot has no cell this block, exactly the cross-block shape.
	recorded := newWS().bal(addr, ver, *uint256.NewInt(5)).nonce(addr, ver, 1).stor(addr, slot, ver, *uint256.NewInt(0)).build()
	flushed := newWS().bal(addr, ver, *uint256.NewInt(5)).nonce(addr, ver, 1).build()

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(5)
	acc.Nonce = 1

	t.Run("non-zero base value writes the slot", func(t *testing.T) {
		vm := state.NewVersionMap(nil)
		vm.SetBaseTxNum(0)
		vm.FlushVersionedWrites(flushed, true)
		vm.RecordWriteKeysAtTxIndex(txIdx, false, recorded)
		base := &testStorageBase{
			accounts: map[accounts.Address]*accounts.Account{addr: &acc},
			storage:  map[string]uint256.Int{storKey: *uint256.NewInt(42)},
		}
		got := emitTouchedUpdates(t, vm, T, base, vm.WritesAtTxIndex(txIdx, false))
		b := uint256.NewInt(42).Bytes()
		want := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(b))}
		copy(want.Storage[:], b)
		require.Equal(t, want, got[storKey], "cross-block slot must take its value from the base")
	})

	t.Run("absent base value deletes the slot", func(t *testing.T) {
		vm := state.NewVersionMap(nil)
		vm.SetBaseTxNum(0)
		vm.FlushVersionedWrites(flushed, true)
		vm.RecordWriteKeysAtTxIndex(txIdx, false, recorded)
		base := &testStorageBase{
			accounts: map[accounts.Address]*accounts.Account{addr: &acc},
			storage:  map[string]uint256.Int{},
		}
		got := emitTouchedUpdates(t, vm, T, base, vm.WritesAtTxIndex(txIdx, false))
		require.Equal(t, commitment.Update{Flags: commitment.DeleteUpdate}, got[storKey], "a slot the base no longer carries is deleted")
		require.Contains(t, got, acctKey, "the account is still emitted")
	})
}
