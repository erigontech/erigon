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

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestFinalizedWritesWithholdCreatedEmptyAccount(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(func() { ibs.Release(false) })
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
	t.Cleanup(func() { next.Release(false) })
	next.SetNoMaterialize(true)
	next.SetTxContext(1, 1)
	exists, err := next.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestFinalizedWritesLeavesVersionMapForApplyLoop(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xe1})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	t.Cleanup(func() { ibs.Release(false) })
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
			test.add(writes)
			require.False(t, writes.createdEmpty(addr))
		})
	}
}

func TestFinalizedWritesKeepCreatedEmptyBeforeEIP161(t *testing.T) {
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	t.Cleanup(func() { ibs.Release(false) })
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
	t.Cleanup(func() { ibs.Release(false) })
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
	t.Cleanup(func() { ibs.Release(false) })
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
