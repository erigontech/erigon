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
	"errors"
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
	full := func(bal uint64, nonce uint64, ch accounts.CodeHash) commitment.Update {
		return commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Balance: *uint256.NewInt(bal), Nonce: nonce, CodeHash: ch.Value()}
	}
	del := commitment.Update{Flags: commitment.DeleteUpdate, CodeHash: empty.CodeHash, DeleteStorageSubtree: true}
	storDel := commitment.Update{Flags: commitment.DeleteUpdate}

	wiped := full(3, 1, accounts.EmptyCodeHash)
	wiped.DeleteStorageSubtree = true

	scenarios := []struct {
		name   string
		ws     *state.WriteSet
		expect map[string]commitment.Update
	}{
		{
			"live account full update",
			newWS().bal(a(1), ver, *uint256.NewInt(100)).nonce(a(1), ver, 7).codeHash(a(1), ver, codeHash).build(),
			map[string]commitment.Update{acctPlainKey(a(1)): full(100, 7, codeHash)},
		},
		{
			"storage plus account",
			newWS().bal(a(3), ver, *uint256.NewInt(5)).nonce(a(3), ver, 1).
				stor(a(3), k(1), ver, *uint256.NewInt(1)).stor(a(3), k(2), ver, *uint256.NewInt(2)).build(),
			map[string]commitment.Update{acctPlainKey(a(3)): full(5, 1, accounts.EmptyCodeHash), storPlainKey(a(3), k(1)): storVal(1), storPlainKey(a(3), k(2)): storVal(2)},
		},
		{
			"storage cleared to zero",
			newWS().bal(a(9), ver, *uint256.NewInt(5)).nonce(a(9), ver, 1).stor(a(9), k(3), ver, uint256.Int{}).build(),
			map[string]commitment.Update{acctPlainKey(a(9)): full(5, 1, accounts.EmptyCodeHash), storPlainKey(a(9), k(3)): storDel},
		},
		{
			"balance-deleted verdict removes leaf and subtree",
			newWS().emptyRemoval(a(4), ver).build(),
			map[string]commitment.Update{acctPlainKey(a(4)): del},
		},
		{
			"self-destruct preserving balance keeps leaf but wipes subtree",
			newWS().selfDestruct(a(5), ver, true).bal(a(5), ver, *uint256.NewInt(3)).nonce(a(5), ver, 1).build(),
			map[string]commitment.Update{acctPlainKey(a(5)): wiped},
		},
		{
			"created contract",
			newWS().createContract(a(7), ver, true).bal(a(7), ver, *uint256.NewInt(9)).nonce(a(7), ver, 1).codeHash(a(7), ver, codeHash).build(),
			map[string]commitment.Update{acctPlainKey(a(7)): full(9, 1, codeHash)},
		},
		{
			"zero-balance live touch (nonce keeps it alive)",
			newWS().bal(a(8), ver, uint256.Int{}).nonce(a(8), ver, 1).build(),
			map[string]commitment.Update{acctPlainKey(a(8)): full(0, 1, accounts.EmptyCodeHash)},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			ws := sc.ws
			vm := state.NewVersionMap(nil)
			vm.FlushVersionedWrites(ws, true)

			got := emitTouchedUpdates(t, vm, T, nil, state.NewVersionMapWriteView(ws, vm, txIdx))
			require.Equal(t, sc.expect, got)
		})
	}
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
	storKey := storPlainKey(addr, slot)
	acctKey := acctPlainKey(addr)

	// The access list records the account and the slot (so both are touched), but only the
	// account is flushed — the slot has no cell this block, exactly the cross-block shape.
	recorded := newWS().bal(addr, ver, *uint256.NewInt(5)).nonce(addr, ver, 1).stor(addr, slot, ver, *uint256.NewInt(0)).build()
	flushed := newWS().bal(addr, ver, *uint256.NewInt(5)).nonce(addr, ver, 1).build()

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(5)
	acc.Nonce = 1

	t.Run("non-zero base value writes the slot", func(t *testing.T) {
		vm := state.NewVersionMap(nil)
		vm.FlushVersionedWrites(flushed, true)
		base := newMapStateReader()
		base.accounts[addr] = &acc
		base.storage[addr] = map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(42)}
		got := emitTouchedUpdates(t, vm, T, base, state.NewVersionMapWriteView(recorded, vm, txIdx))
		require.Equal(t, storVal(42), got[storKey], "cross-block slot must take its value from the base")
	})

	t.Run("absent base value deletes the slot", func(t *testing.T) {
		vm := state.NewVersionMap(nil)
		vm.FlushVersionedWrites(flushed, true)
		base := newMapStateReader()
		base.accounts[addr] = &acc
		got := emitTouchedUpdates(t, vm, T, base, state.NewVersionMapWriteView(recorded, vm, txIdx))
		require.Equal(t, commitment.Update{Flags: commitment.DeleteUpdate}, got[storKey], "a slot the base no longer carries is deleted")
		require.Contains(t, got, acctKey, "the account is still emitted")
	})
}

// TestEmitTouched_BatchKeepsEarlierBlockDestruct pins the cross-block storage reset: an
// account destroyed in an earlier batch block and funded again in a later one keeps
// DeleteStorageSubtree, although the later block's version map has no destruct for it.
func TestEmitTouched_BatchKeepsEarlierBlockDestruct(t *testing.T) {
	t.Parallel()

	const txIdx = 1
	const T = txIdx + 1
	ver := state.Version{TxIndex: txIdx}
	addr := accounts.InternAddress([20]byte{0xd5})
	av := addr.Value()

	wsA := newWS().selfDestruct(addr, ver, true).bal(addr, ver, uint256.Int{}).build()
	vmA := state.NewVersionMap(nil)
	vmA.FlushVersionedWrites(wsA, true)

	wsB := newWS().bal(addr, ver, *uint256.NewInt(7)).build()
	vmB := state.NewVersionMap(nil)
	vmB.FlushVersionedWrites(wsB, true)

	got := emitTouchedUpdates(t, vmB, T, newMapStateReader(),
		state.NewVersionMapWriteView(wsA, vmA, txIdx), state.NewVersionMapWriteView(wsB, vmB, txIdx))[string(av[:])]
	require.Equal(t, *uint256.NewInt(7), got.Balance)
	require.True(t, got.DeleteStorageSubtree, "a destruct in an earlier batch block must reset the storage subtree")
}

// TestEmitTouched_ZeroCodeHashFoldsAsEmpty pins that an account record carrying the
// zero CodeHash (a balance-only account synthesized from accounts.Account{}) folds
// with the empty-code hash, as the domain round-trip does.
func TestEmitTouched_ZeroCodeHashFoldsAsEmpty(t *testing.T) {
	t.Parallel()

	const txIdx = 0
	ver := state.Version{TxIndex: txIdx}
	addr := accounts.InternAddress([20]byte{0xb0})
	av := addr.Value()

	ws := newWS().bal(addr, ver, *uint256.NewInt(5)).addr(addr, ver, &accounts.Account{Balance: *uint256.NewInt(5)}).build()
	vm := state.NewVersionMap(nil)
	vm.FlushVersionedWrites(ws, true)

	got := emitTouchedUpdates(t, vm, txIdx+1, newMapStateReader(), state.NewVersionMapWriteView(ws, vm, txIdx))[string(av[:])]
	require.Equal(t, *uint256.NewInt(5), got.Balance)
	require.Equal(t, empty.CodeHash, got.CodeHash)
}

func storVal(v uint64) commitment.Update {
	b := uint256.NewInt(v).Bytes()
	u := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(b))}
	copy(u.Storage[:], b)
	return u
}

type errStorageBase struct {
	*mapStateReader
	err error
}

func (b *errStorageBase) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, b.err
}

// TestEmitTouched_BaseReadErrorFailsTheFold pins that a failed base read fails the fold
// instead of emitting a delete for the account or slot it could not read.
func TestEmitTouched_BaseReadErrorFailsTheFold(t *testing.T) {
	t.Parallel()

	const txIdx = 1
	ver := state.Version{TxIndex: txIdx}
	addr := accounts.InternAddress([20]byte{0xe1})
	slot := accounts.InternKey([32]byte{0x2})
	boom := errors.New("boom")

	t.Run("account", func(t *testing.T) {
		vm := state.NewVersionMap(nil)
		cc := &commitmentCalculator{}
		cc.accumulateTouched(newWS().nonce(addr, ver, 1).build(), nil)
		err := cc.emitTouched(vm.View(txIdx+1, &failingAccountReader{mapStateReader: newMapStateReader(), err: boom}), newTestUpdates())
		require.ErrorIs(t, err, boom)
	})

	t.Run("storage", func(t *testing.T) {
		ws := newWS().bal(addr, ver, *uint256.NewInt(1)).stor(addr, slot, ver, *uint256.NewInt(1)).build()
		vm := state.NewVersionMap(nil)
		vm.FlushVersionedWrites(newWS().bal(addr, ver, *uint256.NewInt(1)).build(), true)
		cc := &commitmentCalculator{}
		cc.accumulateTouched(ws, nil)
		err := cc.emitTouched(vm.View(txIdx+1, &errStorageBase{mapStateReader: newMapStateReader(), err: boom}), newTestUpdates())
		require.ErrorIs(t, err, boom)
	})
}

type countingAccountReader struct {
	*mapStateReader
	reads int
}

func (r *countingAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	r.reads++
	return r.mapStateReader.ReadAccountData(addr)
}

// TestEmitTouched_FoldReusesFirstTouchBaseRead pins that the fold composes from the base
// account read when the account was first touched, instead of reading the base again at
// block end, and that a fold over another block's version map reads its own base.
func TestEmitTouched_FoldReusesFirstTouchBaseRead(t *testing.T) {
	t.Parallel()

	const txIdx = 1
	ver := state.Version{TxIndex: txIdx}
	addr := accounts.InternAddress([20]byte{0xf1})
	av := addr.Value()
	acc := accounts.NewAccount()
	acc.Nonce = 3
	acc.Balance = *uint256.NewInt(1)

	ws := newWS().bal(addr, ver, *uint256.NewInt(5)).build()
	vm := state.NewVersionMap(nil)
	vm.FlushVersionedWrites(ws, true)
	base := &countingAccountReader{mapStateReader: newMapStateReader()}
	base.accounts[addr] = &acc

	cc := &commitmentCalculator{}
	cc.setBlockBase(vm.BaseTxNum(), base)
	cc.accumulateTouched(state.NewVersionMapWriteView(ws, vm, txIdx), base)
	readsBeforeFold := base.reads

	u := newTestUpdates()
	require.NoError(t, cc.emitTouched(cc.foldView(vm, txIdx+1, base), u))
	require.Equal(t, readsBeforeFold, base.reads, "the fold must not read the base again")
	got := collectUpdates(t, u)[string(av[:])]
	require.Equal(t, *uint256.NewInt(5), got.Balance)
	require.Equal(t, uint64(3), got.Nonce)

	other := state.NewVersionMap(nil)
	other.SetBaseTxNum(100)
	other.FlushVersionedWrites(ws, true)
	require.NoError(t, cc.emitTouched(cc.foldView(other, 100+txIdx+1, base), newTestUpdates()))
	require.Equal(t, readsBeforeFold+1, base.reads, "a fold over another block must read its own base")
}

// TestEmitTouched_FoldReusesFirstTouchKeyHash pins that the fold inserts each key under
// the hash computed when the key was first touched, instead of hashing it again at
// block end.
func TestEmitTouched_FoldReusesFirstTouchKeyHash(t *testing.T) {
	t.Parallel()

	const txIdx = 1
	ver := state.Version{TxIndex: txIdx}
	addr := accounts.InternAddress([20]byte{0xf2})
	slot := accounts.InternKey([32]byte{0x3})
	ws := newWS().bal(addr, ver, *uint256.NewInt(5)).nonce(addr, ver, 1).stor(addr, slot, ver, *uint256.NewInt(9)).build()
	vm := state.NewVersionMap(nil)
	vm.FlushVersionedWrites(ws, true)

	hashes := 0
	hasher := func(key []byte) []byte {
		hashes++
		return commitment.KeyToHexNibbleHash(key)
	}
	cc := &commitmentCalculator{updates: commitment.NewUpdates(commitment.ModeUpdate, "", hasher)}
	cc.accumulateTouched(state.NewVersionMapWriteView(ws, vm, txIdx), nil)
	hashesBeforeFold := hashes

	require.NoError(t, cc.emitTouched(vm.View(txIdx+1, nil), cc.updates))
	require.Equal(t, hashesBeforeFold, hashes, "the fold must not hash a touched key again")
	got := map[string][]byte{}
	require.NoError(t, cc.updates.HashSort(t.Context(), nil, func(hk, pk []byte, _ *commitment.Update) error {
		got[string(pk)] = append([]byte(nil), hk...)
		return nil
	}))
	for _, pk := range []string{acctPlainKey(addr), storPlainKey(addr, slot)} {
		require.Equal(t, commitment.KeyToHexNibbleHash([]byte(pk)), got[pk], "key %x", pk)
	}
}
