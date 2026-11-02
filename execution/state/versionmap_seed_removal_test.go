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
	"errors"
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
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))

	// GetAccount reads the seeded removal deterministically — no SetRules.
	_, ok := vm.View(1, reader).GetAccount(emptied)
	require.False(t, ok, "a touched account drained to empty is removed via the seeded self-destruct")

	acc, ok := vm.View(1, reader).GetAccount(survives)
	require.True(t, ok, "a balance-zeroed account with a non-zero base nonce is NOT empty and survives")
	require.Equal(t, uint64(5), acc.Nonce)
}

// TestSeedAddresses pins the DB-free existence seed. The BAL carries no address record, so
// a block-created account (absent in the committed base) has only field cells; the address
// existence read consults the AddressPath (not the field cells), so unseeded it reads absent
// and the EIP-161 withhold path stamps a spurious removal. SeedAddresses marks existence
// straight from the BAL — no base reader — stamping a ValueUnknown AddressPath so the
// authoritative value still defers to the committed base.
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

	// Before seeding the BAL leaves only field cells, so the created account's address
	// read is absent even though it exists — the bug SeedAddresses fixes.
	if _, _, ok := vm.ReadAddress(created, 5); ok {
		t.Fatal("precondition: a BAL-created account's address read should be absent before SeedAddresses")
	}

	vm.SeedAddresses() // no base reader: existence comes from the BAL alone

	if _, _, ok := vm.ReadAddress(created, 5); !ok {
		t.Fatal("SeedAddresses marks a BAL-created account present with no base lookup")
	}
	if _, _, ok := vm.ReadAddress(existing, 5); !ok {
		t.Fatal("a touched base-present account also reads present")
	}

	// The marker is ValueUnknown, so the authoritative value defers to the base: a
	// base-present account keeps its untouched base nonce, not the seed's zeroed marker.
	ex := NewVersionedAccountView(existing, 6, vm, reader).Account()
	require.NotNil(t, ex)
	require.Equal(t, uint64(9), ex.Nonce, "a base-present account's untouched nonce resolves from the base")

	cr := NewVersionedAccountView(created, 6, vm, reader).Account()
	require.NotNil(t, cr)
	require.Equal(t, uint64(1), cr.Nonce, "a created account composes from its field cells")
}

// TestSeedAddressesDoesNotLeakLaterCode pins the CREATE2-onto-funded fix: an account funded
// (balance) at an early tx and given code at a later tx must, before the deploying tx,
// compose with empty code. Seeding existence must not drag the later CodeHash into earlier
// reads — the collision that made the proposer and validator disagree.
func TestSeedAddressesDoesNotLeakLaterCode(t *testing.T) {
	t.Parallel()

	proxy := accounts.InternAddress(common.HexToAddress("0xc0ffee")) // funded early, CREATE2'd later
	const baseTxNum = 0

	bal := types.BlockAccessList{
		{
			Address:        proxy.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(256_000)}},
			NonceChanges:   []*types.NonceChange{{Index: 5, Value: 1}},
			CodeChanges:    []*types.CodeChange{{Index: 5, Bytecode: []byte{0x60, 0x00}}},
		},
	}

	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}} // proxy absent in base

	vm := NewVersionMap(bal)
	vm.SetBaseTxNum(baseTxNum)
	vm.SeedAddresses()

	// Compose after funding (BAL index 2 → TxIndex 1) but before the code deploy (index 5
	// → TxIndex 4): the funded, still-codeless account the CREATE2 collision check must see.
	acc := NewVersionedAccountView(proxy, 3, vm, reader).Account()
	require.NotNil(t, acc, "the funded proxy exists before the deploying tx")
	require.Equal(t, *uint256.NewInt(256_000), acc.Balance)
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash, "reads before the deploy must not see the later CREATE2 code")
}

func TestSeedEmptyRemovals_BaseReadErrorFails(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xe2})
	bal := types.BlockAccessList{{Address: addr.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}}}
	vm := NewVersionMap(bal)
	vm.SetBaseTxNum(0)
	boom := errors.New("boom")
	require.ErrorIs(t, vm.SeedEmptyRemovals(true, false, &failingBaseReader{err: boom}), boom)
}

// TestSeedEmptyRemovals_DrainThenRefund pins the removal at an interior index: an
// account drained to empty and funded again later in the block reads absent to a
// step checkpoint between the two, and present at block end.
func TestSeedEmptyRemovals_DrainThenRefund(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xd7a1"))
	bal := types.BlockAccessList{{Address: addr.Value(), BalanceChanges: []*types.BalanceChange{
		{Index: 1, Value: uint256.Int{}},
		{Index: 2, Value: *uint256.NewInt(7)},
	}}}
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	base := accounts.NewAccount()
	base.Balance = *uint256.NewInt(5)
	reader.accounts[addr] = &base

	vm := NewVersionMap(bal)
	vm.SetBaseTxNum(0)
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))

	_, ok := vm.View(1, reader).GetAccount(addr)
	require.False(t, ok, "drained to empty at index 1: absent before the re-fund")
	acc, ok := vm.View(2, reader).GetAccount(addr)
	require.True(t, ok, "funded again at index 2: present at block end")
	require.Equal(t, *uint256.NewInt(7), acc.Balance)
}

// TestSeedEmptyRemovals_DrainThenRecreateWithoutBalance pins that a re-creation with no
// balance change (CREATE2 onto a drained address) supersedes the interior removal, as
// createObject does for a worker.
func TestSeedEmptyRemovals_DrainThenRecreateWithoutBalance(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xd7a2"))
	bal := types.BlockAccessList{{
		Address:        addr.Value(),
		BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
		NonceChanges:   []*types.NonceChange{{Index: 2, Value: 1}},
		CodeChanges:    []*types.CodeChange{{Index: 2, Bytecode: []byte{0x60, 0x00}}},
	}}
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	base := accounts.NewAccount()
	base.Balance = *uint256.NewInt(5)
	reader.accounts[addr] = &base

	vm := NewVersionMap(bal)
	vm.SetBaseTxNum(0)
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))

	_, ok := vm.View(1, reader).GetAccount(addr)
	require.False(t, ok, "drained to empty at index 1")
	acc, ok := vm.View(2, reader).GetAccount(addr)
	require.True(t, ok, "re-created at index 2: present at block end")
	require.Equal(t, uint64(1), acc.Nonce)
}
