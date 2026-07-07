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

// AccountLifecycle is the single revival definition the scattered consumers
// (getVersionedAccount, versionedStateReader, validateReadImpl, the create
// decision) converge onto. Pin it against the cases those sites currently
// compute ad-hoc — including the same-tx metamorphic SD+CREATE2 that only the
// AddressPath >= arm catches.
func TestAccountLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("not destroyed", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(1)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(10), true)
		destroyed, _, revived := vm.AccountLifecycle(addr, 5)
		require.False(t, destroyed)
		require.False(t, revived)
	})

	t.Run("destroyed, no revival", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(2)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(10), true)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		destroyed, at, revived := vm.AccountLifecycle(addr, 5)
		require.True(t, destroyed)
		require.Equal(t, 2, at)
		require.False(t, revived)
	})

	t.Run("revived via AddressPath at same tx (metamorphic SD+CREATE2)", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(3)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 3}, true, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, &accounts.Account{Nonce: 1}, true)
		destroyed, at, revived := vm.AccountLifecycle(addr, 5)
		require.True(t, destroyed)
		require.Equal(t, 3, at)
		require.True(t, revived, "AddressPath >= destroyedAt must catch same-tx metamorphic re-create")
	})

	t.Run("revived via Balance after destruct", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(4)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 3}, *uint256.NewInt(7), true)
		destroyed, _, revived := vm.AccountLifecycle(addr, 5)
		require.True(t, destroyed)
		require.True(t, revived)
	})

	t.Run("revived via CodeHash after destruct", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(5)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 4}, accounts.NewCode([]byte{0x60}).Hash, true)
		destroyed, _, revived := vm.AccountLifecycle(addr, 6)
		require.True(t, destroyed)
		require.True(t, revived)
	})

	t.Run("field write at same tx as destruct is not a revival (strict >)", func(t *testing.T) {
		t.Parallel()
		vm := NewVersionMap(nil)
		addr := getAddress(6)
		writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
		writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, uint256.Int{}, true)
		destroyed, _, revived := vm.AccountLifecycle(addr, 5)
		require.True(t, destroyed)
		require.False(t, revived, "a same-tx SD-zero balance write is not a revival; only AddressPath uses >=")
	})
}
