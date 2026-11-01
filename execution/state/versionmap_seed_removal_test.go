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

// TestSeedAddresses pins the AddressPath seeding. The BAL carries no address record, so a
// block-created account (absent in the committed base) is seeded with field cells only;
// the address read resolves existence from the AddressPath cell plus the base — not the
// field cells — so without seeding it reads the created account as absent, which makes the
// EIP-161 withhold path stamp a spurious removal. SeedAddresses seeds the AddressPath (from
// the base when present, created otherwise) so the read resolves present.
func TestSeedAddresses(t *testing.T) {
	t.Parallel()

	created := accounts.InternAddress(common.HexToAddress("0xc0ffee")) // absent in base, created this block
	existing := accounts.InternAddress(common.HexToAddress("0xe5157")) // present in base, only touched
	const baseTxNum = 0

	bal := types.BlockAccessList{
		{
			Address:      created.Value(),
			NonceChanges: []*types.NonceChange{{Index: 3, Value: 1}},
			CodeChanges:  []*types.CodeChange{{Index: 3, Bytecode: []byte{0x60, 0x00}}},
		},
		{
			Address:        existing.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(7)}},
		},
	}

	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	existingBase := accounts.NewAccount()
	existingBase.Nonce = 9
	reader.accounts[existing] = &existingBase // `created` is intentionally absent from the base

	vm := NewVersionMap(bal)
	vm.SetBaseTxNum(baseTxNum)

	// Without seeding the BAL leaves only field cells, so the created account's address
	// read is absent even though it exists — the bug SeedAddresses fixes.
	if _, _, ok := vm.ReadAddress(created, 5); ok {
		t.Fatal("precondition: a BAL-created account's AddressPath read should be absent before SeedAddresses")
	}

	vm.SeedAddresses(bal, []AccountBaseReader{reader})

	acc, _, ok := vm.ReadAddress(created, 5)
	require.True(t, ok, "SeedAddresses seeds the AddressPath so a BAL-created account reads present")
	require.NotNil(t, acc)
	require.Equal(t, uint64(1), acc.Nonce)

	ex, _, ok := vm.ReadAddress(existing, 5)
	require.True(t, ok, "a base-present account is published as its origin and reads present")
	require.Equal(t, uint64(9), ex.Nonce)
}
