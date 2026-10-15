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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestFinalizedWritesWithholdCreatedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(addr))

	writes := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress)
	_, hasBalance := writes.GetBalance(addr)
	require.False(t, hasBalance)
	_, hasDelete := writes.GetSelfDestruct(addr)
	require.False(t, hasDelete)

	vm.FlushVersionedWrites(writes, true, "")
	next := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(next.Close)
	next.SetNoMaterialize(true)
	next.SetTxContext(1, 1)
	exists, err := next.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestFinalizedWritesEmptyRemovalClearsWholeAccountCell pins item 1's write-side
// invariant: when an EIP-161 account that existed at base is (re)created empty and
// removed, the removal must drop the whole-account AddressPath write too — not only the
// account fields. Otherwise the tx flushes both a SelfDestruct(true) and an AddressPath
// cell at the SAME tx index, which is the exact cell layout of a same-tx SD+CREATE2
// metamorphic recreate. AccountLifecycleAt then reads the removed account as Revived
// (its AddressPath-at-destroyedAt branch), undoing the removal and diverging the root.
// The fix must be write-side: the two cases are indistinguishable from cells, so
// tightening AccountLifecycleAt's >= would instead regress genuine metamorphic recreate.
func TestFinalizedWritesEmptyRemovalClearsWholeAccountCell(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe2})
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	base := accounts.NewAccount() // existing empty EOA: bal 0, nonce 0, EmptyCodeHash
	reader.accounts[addr] = &base

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.CreateAccount(addr, false))

	writes := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})

	_, hasDelete := writes.GetSelfDestruct(addr)
	require.True(t, hasDelete, "empty removal must emit a SelfDestruct delete")
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress,
		"empty removal must not leave a whole-account AddressPath cell alongside the delete "+
			"(same layout as a metamorphic recreate -> AccountLifecycleAt would read it as Revived)")
}

func TestEmptyAccountTouchInvalidatedByFunding(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xe2})
	vm := NewVersionMap(nil)
	// Model a real create+SELFDESTRUCT: the destroying tx marks the account
	// destructed and zeroes its balance at the same version.
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
	vm.WriteBalance(addr, Version{TxIndex: 0}, uint256.Int{}, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 2)

	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)
	require.NoError(t, ibs.TouchAccount(addr))

	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, ibs.VersionedReads())

	account := accounts.NewAccount()
	account.Balance = *uint256.NewInt(32649)
	vm.WriteAddress(addr, Version{TxIndex: 1}, &account, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, account.Balance, true)

	require.Equal(t, VersionInvalid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
}

func TestDestroyedAccountReadRemainsValid(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xe3})
	vm := NewVersionMap(nil)
	account := accounts.NewAccount()
	account.Balance = *uint256.NewInt(1)
	vm.WriteAddress(addr, Version{TxIndex: 0}, &account, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 2)

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)

	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
}

func TestFinalizedWritesLeavesVersionMapForApplyLoop(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(addr))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), false, "")

	writes := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	_, hasAddress := writes.GetAddress(addr)
	require.False(t, hasAddress)

	_, result, ok := vm.ReadAddress(addr, 1)
	require.True(t, ok)
	require.Equal(t, MVReadResultDependency, result.Status())
	require.Equal(t, 0, result.DepIdx())
}

func TestCreatedEmptyRequiresNoOtherWrites(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	newWrites := func() *WriteSet {
		account := accounts.NewAccount()
		writes := &WriteSet{}
		writes.SetAddress(addr, &VersionedWrite[*accounts.Account]{Val: &account})
		return writes
	}
	tests := []struct {
		name string
		add  func(*WriteSet)
	}{
		{
			name: "balance",
			add: func(writes *WriteSet) {
				writes.SetBalance(addr, &VersionedWrite[uint256.Int]{Val: *uint256.NewInt(1)})
			},
		},
		{
			name: "nonce",
			add: func(writes *WriteSet) {
				writes.SetNonce(addr, &VersionedWrite[uint64]{Val: 1})
			},
		},
		{
			name: "code hash",
			add: func(writes *WriteSet) {
				writes.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{Val: accounts.InternCodeHash(common.Hash{1})})
			},
		},
		{
			name: "code",
			add: func(writes *WriteSet) {
				writes.SetCode(addr, &VersionedWrite[accounts.Code]{})
			},
		},
		{
			name: "code size",
			add: func(writes *WriteSet) {
				writes.SetCodeSize(addr, &VersionedWrite[int]{})
			},
		},
		{
			name: "storage",
			add: func(writes *WriteSet) {
				writes.SetStorage(addr, accounts.InternKey([32]byte{1}), &VersionedWrite[uint256.Int]{})
			},
		},
		{
			name: "self-destruct",
			add: func(writes *WriteSet) {
				writes.SetSelfDestruct(addr, &VersionedWrite[bool]{})
			},
		},
		{
			name: "contract creation",
			add: func(writes *WriteSet) {
				writes.SetCreateContract(addr, &VersionedWrite[bool]{})
			},
		},
		{
			name: "incarnation",
			add: func(writes *WriteSet) {
				writes.SetIncarnation(addr, &VersionedWrite[uint64]{})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writes := newWrites()
			require.True(t, writes.createdEmpty(addr))
			test.add(writes)
			require.False(t, writes.createdEmpty(addr))
		})
	}
}

func TestFinalizedWritesKeepCreatedEmptyBeforeEIP161(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	addr := accounts.InternAddress([20]byte{0xe1})
	require.NoError(t, ibs.TouchAccount(addr))

	writes := ibs.FinalizedWrites(&chain.Rules{})
	_, ok := writes.GetAddress(addr)
	require.True(t, ok)
}

func TestFinalizedWritesKeepCreatedEmptyAtGenesis(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)

	addr := accounts.InternAddress([20]byte{0xe1})
	require.NoError(t, ibs.TouchAccount(addr))

	writes := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	_, ok := writes.GetAddress(addr)
	require.True(t, ok)
}

func TestFinalizedWritesKeepCreatedEmptyAuraSystemAccount(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(ibs.Close)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.TouchAccount(params.SystemAddress))

	writes := ibs.FinalizedWrites(&chain.Rules{
		IsSpuriousDragon: true,
		IsAura:           true,
	})
	_, ok := writes.GetAddress(params.SystemAddress)
	require.True(t, ok)
}
