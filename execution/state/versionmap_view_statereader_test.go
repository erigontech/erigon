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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type viewEquivReader struct {
	minimalStateReader
	accounts map[accounts.Address]*accounts.Account
	storage  map[accounts.Address]map[accounts.StorageKey]uint256.Int
}

func (r *viewEquivReader) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	if acc, ok := r.accounts[a]; ok {
		c := *acc
		return &c, nil
	}
	return nil, nil
}

func (r *viewEquivReader) ReadAccountStorage(a accounts.Address, k accounts.StorageKey) (uint256.Int, bool, error) {
	if m, ok := r.storage[a]; ok {
		if v, ok := m[k]; ok {
			return v, true, nil
		}
	}
	return uint256.Int{}, false, nil
}

// TestVersionMapViewMatchesVersionedStateReader proves the common read interface —
// VersionMapView as StateReader — resolves identically to versionedStateReader over the
// same version map + base. This is the equivalence PR 0 relies on to let external readers
// and apply share one reader.
func TestVersionMapViewMatchesVersionedStateReader(t *testing.T) {
	t.Parallel()

	a := accounts.InternAddress(common.HexToAddress("0xa"))     // base account, balance overridden in-block
	b := accounts.InternAddress(common.HexToAddress("0xb"))     // block-created contract (code)
	cold := accounts.InternAddress(common.HexToAddress("0xc0")) // untouched base account
	slotW := accounts.InternKey(common.HexToHash("0x11"))       // written in-block
	slotB := accounts.InternKey(common.HexToHash("0x22"))       // only in base

	removed := accounts.InternAddress(common.HexToAddress("0xde1")) // base empty-ish, BAL drains to 0 -> EIP-161 removal
	absent := accounts.InternAddress(common.HexToAddress("0xab5"))  // neither base nor vm

	aBase := accounts.NewAccount()
	aBase.Balance = *uint256.NewInt(100)
	aBase.Nonce = 5
	coldBase := accounts.NewAccount()
	coldBase.Balance = *uint256.NewInt(7)
	coldBase.Nonce = 2
	removedBase := accounts.NewAccount() // nonce 0, empty code -> empty once balance is 0

	base := &viewEquivReader{
		accounts: map[accounts.Address]*accounts.Account{a: &aBase, cold: &coldBase, removed: &removedBase},
		storage: map[accounts.Address]map[accounts.StorageKey]uint256.Int{
			a: {slotB: *uint256.NewInt(9)},
		},
	}

	bal := types.BlockAccessList{
		{
			Address:        a.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(200)}},
			StorageChanges: []types.SlotChanges{{Slot: slotW, Changes: []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(42)}}}},
		},
		{
			Address:      b.Value(),
			NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}},
			CodeChanges:  []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60, 0x00}}},
		},
		{
			Address:        removed.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
		},
	}

	// Destruct edges the apply reader handles specially (EIP-8246 preserve vs burn).
	preserve := accounts.InternAddress(common.HexToAddress("0x9e5")) // destructed, keeps a non-zero balance
	burned := accounts.InternAddress(common.HexToAddress("0xb47"))   // destructed, balance burned -> absent
	// Reachable shape: a destruct that deletes (EIP-6780) only happens for a same-tx-created
	// account, and CREATE collides on code or nonce — so the target is code-less and nonce-0.
	preserveBase := accounts.NewAccount()
	preserveBase.Balance = *uint256.NewInt(10)
	burnedBase := accounts.NewAccount()
	burnedBase.Balance = *uint256.NewInt(10)
	burnedBase.Nonce = 1
	burnedBase.CodeHash = accounts.NewCode([]byte{0x60, 0x02}).Hash
	base.accounts[preserve] = &preserveBase
	base.accounts[burned] = &burnedBase

	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, base))

	// EIP-8246 preserve: self-destruct keeps a non-zero (non-deleted) balance at tx 0.
	wsP := &WriteSet{}
	wsP.SetSelfDestruct(preserve, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: preserve, Path: SelfDestructPath, Version: Version{TxIndex: 0}}, Val: true})
	wsP.SetBalance(preserve, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: preserve, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(5)})
	vm.FlushWrites(wsP, true)
	// Burn: contract removal (self-destruct + balance ValueDeleted) at tx 0.
	wsB := &WriteSet{}
	wsB.SetContractRemoval(burned, Version{TxIndex: 0})
	vm.FlushWrites(wsB, true)

	const txIdx = 3
	view := vm.View(uint64(txIdx), base) // baseTxNum defaults to 0, so txNum==txIdx
	ref := NewVersionedStateReader(txIdx, ReadSet{}, vm, base)

	for _, addr := range []accounts.Address{a, b, cold, removed, absent, preserve, burned} {
		wantSz, _ := ref.ReadAccountCodeSize(addr)
		gotSz, _ := view.ReadAccountCodeSize(addr)
		require.Equalf(t, wantSz, gotSz, "ReadAccountCodeSize %x", addr)
		wantAcc, wantErr := ref.ReadAccountData(addr)
		gotAcc, gotErr := view.ReadAccountData(addr)
		require.Equal(t, wantErr, gotErr, "ReadAccountData err %x", addr)
		// Root is a storage-trie field the versioned/commitment model does not consume
		// (storage is tracked as cells); normalize it to map any *other* divergence.
		// The readers agree up to two benign representation differences that the commit
		// boundary normalizes or ignores: Root (a storage-trie field the versioned model
		// does not consume) and an empty CodeHash (the view composes EmptyCodeHash via
		// NewAccount, the apply reader synthesizes a nil hash; commitment's accountUpdate
		// maps both to empty.CodeHash via CodeHash.IsEmpty). Normalize both, then assert
		// every consumed field matches.
		if wantAcc != nil && gotAcc != nil {
			wantAcc.Root, gotAcc.Root = common.Hash{}, common.Hash{}
			if wantAcc.CodeHash.IsEmpty() && gotAcc.CodeHash.IsEmpty() {
				wantAcc.CodeHash, gotAcc.CodeHash = accounts.EmptyCodeHash, accounts.EmptyCodeHash
			}
		}
		require.Equal(t, wantAcc, gotAcc, "ReadAccountData (consumed fields) %x", addr)

		wantInc, _ := ref.ReadAccountIncarnation(addr)
		gotInc, _ := view.ReadAccountIncarnation(addr)
		require.Equal(t, wantInc, gotInc, "ReadAccountIncarnation %x", addr)

		wantCode, _ := ref.ReadAccountCode(addr)
		gotCode, _ := view.ReadAccountCode(addr)
		require.Equal(t, wantCode, gotCode, "ReadAccountCode %x", addr)
	}

	for _, addr := range []accounts.Address{a, cold} {
		for _, k := range []accounts.StorageKey{slotW, slotB} {
			wantV, wantOk, _ := ref.ReadAccountStorage(addr, k)
			gotV, gotOk, _ := view.ReadAccountStorage(addr, k)
			require.Equal(t, wantOk, gotOk, "ReadAccountStorage ok %x/%x", addr, k)
			require.Equal(t, wantV, gotV, "ReadAccountStorage val %x/%x", addr, k)
		}
	}
}
