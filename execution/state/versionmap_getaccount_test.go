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

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestGetAccount_NewCodelessAccountHasEmptyCodeHash pins that a freshly-created
// codeless account (balance written, no code-hash cell, no base) projects with
// EmptyCodeHash, not the zero NilCodeHash. GetCodeHash reports a base-absent account
// as NilCodeHash, so Account must keep NewAccount's EmptyCodeHash unless the version
// map actually holds a code-hash write — otherwise the new EOA's trie leaf diverges.
func TestGetAccount_NewCodelessAccountHasEmptyCodeHash(t *testing.T) {
	t.Parallel()

	addr := getAddress(2)
	vm := NewVersionMap(nil)
	vm.SetBaseTxNum(0)
	// A value transfer to a new address: a balance cell, no code-hash cell, no base.
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0}, *uint256.NewInt(5), true)

	acc, ok := vm.GetAccount(addr, 1, nil)
	require.True(t, ok, "a funded new account exists")
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash, "a codeless new account projects EmptyCodeHash, not zero")
}

// TestVersionMap_GetAccountProjection pins the post-seal projection: GetAccount reads
// the account composed at a global txNum (txNum = baseTxNum + TxIndex + 1, so it
// includes that tx's own writes) and returns comma-ok false once the account is
// self-destructed with no revival — the deleted/absent signal commitment folds on.
func TestVersionMap_GetAccountProjection(t *testing.T) {
	t.Parallel()

	const base = 100
	addr := getAddress(1)
	vm := NewVersionMap(nil)
	vm.SetBaseTxNum(base)

	// Written by the tx at TxIndex 0 → global txNum base+1.
	vm.WriteAddress(addr, Version{TxIndex: 0}, &accounts.Account{Nonce: 3, Balance: *uint256.NewInt(100), Incarnation: 1}, true)

	// Before the write (block-start system tx, TxIndex -1 → txNum base): absent.
	_, ok := vm.GetAccount(addr, base, nil)
	require.False(t, ok, "account absent before its creating tx")

	// Including the creating tx's own writes (txNum base+1): present with its values.
	acc, ok := vm.GetAccount(addr, base+1, nil)
	require.True(t, ok, "account present once its creating tx is included")
	require.Equal(t, uint64(3), acc.Nonce)
	require.True(t, acc.Balance.Eq(uint256.NewInt(100)))

	// Self-destruct at TxIndex 2 (txNum base+3), no revival → absent.
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, true, true)
	_, ok = vm.GetAccount(addr, base+3, nil)
	require.False(t, ok, "self-destructed account projects as absent")
}
