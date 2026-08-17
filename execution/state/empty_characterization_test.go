// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/execution/types/accounts"
)

// Empty() keys off AddressPath existence and {Balance,Nonce,CodeHash}, never Incarnation; a same-tx
// self-destruct (EIP-6780) leaves the account alive.
func emptyCharIBS(reader *accountStateReader, vm *VersionMap, txIndex int) *IntraBlockState {
	ibs := New(NewVersionedStateReader(txIndex, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, txIndex)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	return ibs
}

func TestEmpty_Characterization(t *testing.T) {
	t.Parallel()

	t.Run("warm non-empty (balance>0)", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(1)
		reader := newAccountStateReader(addr)
		empty, err := emptyCharIBS(reader, NewVersionMap(nil), 3).Empty(addr)
		require.NoError(t, err)
		require.False(t, empty)
	})

	t.Run("warm empty (all zero)", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(2)
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
		zero := accounts.NewAccount()
		reader.accounts[addr] = &zero
		empty, err := emptyCharIBS(reader, NewVersionMap(nil), 3).Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
	})

	t.Run("absent account is empty and records a nil AddressPath read", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(3)
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
		ibs := emptyCharIBS(reader, NewVersionMap(nil), 3)
		empty, err := ibs.Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
		tr, ok := ibs.versionedReads.GetAddress(addr)
		require.True(t, ok, "absent Empty must record an AddressPath read for OCC create/absent detection")
		require.Nil(t, tr.Val, "the recorded AddressPath read must carry a nil account")
	})

	t.Run("cross-tx self-destruct (no revival) is empty", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(4)
		reader := newAccountStateReader(addr)
		vm := NewVersionMap(nil)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
		empty, err := emptyCharIBS(reader, vm, 3).Empty(addr)
		require.NoError(t, err)
		require.True(t, empty, "a prior-tx self-destruct with no revival reads as empty")
	})

	t.Run("field write without AddressPath zeroes balance -> empty", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(5)
		reader := newAccountStateReader(addr)
		vm := NewVersionMap(nil)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 1}, uint256.Int{}, true)
		empty, err := emptyCharIBS(reader, vm, 3).Empty(addr)
		require.NoError(t, err)
		require.True(t, empty)
	})
}
