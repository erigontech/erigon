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

// TestSeedEmptyRemovals pins the write-side artifact: stamping the EIP-161 removal into
// the BAL seed makes GetAccount read a touched-empty account as absent with no read-side
// rules, while respecting the committed base for untouched fields (a balance-only zeroing
// of an account whose base nonce is non-zero is NOT empty and must survive).
func TestSeedEmptyRemovals(t *testing.T) {
	t.Parallel()

	emptied := accounts.InternAddress(common.HexToAddress("0xe3111"))  // drained to empty
	survives := accounts.InternAddress(common.HexToAddress("0x5a71e")) // balance 0 but base nonce 5
	const baseTxNum = 0

	bal := types.BlockAccessList{
		{Address: emptied.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}},
		{Address: survives.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}},
	}

	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	emptiedBase := accounts.NewAccount() // nonce 0, EmptyCodeHash — becomes empty when balance is 0
	reader.accounts[emptied] = &emptiedBase
	survivesBase := accounts.NewAccount()
	survivesBase.Nonce = 5 // untouched by the BAL, keeps the account non-empty
	reader.accounts[survives] = &survivesBase

	vm := NewVersionMap(bal)
	vm.SetBaseTxNum(baseTxNum)
	vm.SeedEmptyRemovals(bal, true, false, reader)

	// GetAccount reads the seeded removal deterministically — no SetRules.
	_, ok := vm.View(1, reader).GetAccount(emptied)
	require.False(t, ok, "a touched account drained to empty is removed via the seeded self-destruct")

	acc, ok := vm.View(1, reader).GetAccount(survives)
	require.True(t, ok, "a balance-zeroed account with a non-zero base nonce is NOT empty and survives")
	require.Equal(t, uint64(5), acc.Nonce)
}

// An account whose last BAL change sets a non-zero balance, nonce or code cannot be empty,
// so seeding needs no committed-base read for it.
func TestSeedEmptyRemovalsSkipsBaseReadForProvenNonEmpty(t *testing.T) {
	t.Parallel()
	addr := func(s string) accounts.Address { return accounts.InternAddress(common.HexToAddress(s)) }
	funded, nonced, coded := addr("0xf1"), addr("0xf2"), addr("0xf3")
	bal := types.BlockAccessList{
		{Address: funded.Value(), BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(7)}, {Index: 1, Value: uint256.Int{}}}},
		{Address: nonced.Value(), NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}}},
		{Address: coded.Value(), CodeChanges: []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60}}}},
	}
	reader := &countingAccountReader{}

	vm := NewVersionMap(bal)
	vm.SeedEmptyRemovals(bal, true, false, reader)
	require.Zero(t, reader.reads)
}

// A non-zero value cleared by a later change does not prove the account non-empty: seeding
// reads the base and removes the account.
func TestSeedEmptyRemovalsReadsBaseWhenLastChangeClears(t *testing.T) {
	t.Parallel()
	addr := func(s string) accounts.Address { return accounts.InternAddress(common.HexToAddress(s)) }
	drained, renonced, uncoded := addr("0xc1"), addr("0xc2"), addr("0xc3")
	bal := types.BlockAccessList{
		{Address: drained.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(7)}, {Index: 2, Value: uint256.Int{}}}},
		{Address: renonced.Value(), NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}, {Index: 2, Value: 0}}},
		{Address: uncoded.Value(), CodeChanges: []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60}}, {Index: 2}}},
	}
	reader := &countingAccountReader{}

	vm := NewVersionMap(bal)
	vm.SeedEmptyRemovals(bal, true, false, reader)
	require.Equal(t, 3, reader.reads)
	for _, a := range []accounts.Address{drained, renonced, uncoded} {
		_, ok := vm.View(2, reader).GetAccount(a)
		require.False(t, ok, a)
	}
}
