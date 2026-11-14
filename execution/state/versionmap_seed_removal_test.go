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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
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
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))

	// GetAccount reads the seeded removal deterministically — no SetRules.
	_, ok, _ := vm.View(1, reader).GetAccount(emptied)
	require.False(t, ok, "a touched account drained to empty is removed via the seeded self-destruct")

	acc, ok, _ := vm.View(1, reader).GetAccount(survives)
	require.True(t, ok, "a balance-zeroed account with a non-zero base nonce is NOT empty and survives")
	require.Equal(t, uint64(5), acc.Nonce)
}

// An account whose last BAL change sets a non-zero balance, nonce or code cannot be empty,
// and one with only storage changes runs code, so seeding needs no committed-base read for either.
func TestSeedEmptyRemovalsSkipsBaseReadForProvenNonEmpty(t *testing.T) {
	t.Parallel()
	addr := func(s string) accounts.Address { return accounts.InternAddress(common.HexToAddress(s)) }
	funded, nonced, coded, stored := addr("0xf1"), addr("0xf2"), addr("0xf3"), addr("0xf4")
	bal := types.BlockAccessList{
		{Address: funded.Value(), BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(7)}, {Index: 1, Value: *uint256.NewInt(3)}}},
		{Address: nonced.Value(), NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}}},
		{Address: coded.Value(), CodeChanges: []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60}}}},
		{Address: stored.Value(), StorageChanges: []types.SlotChanges{{Slot: accounts.InternKey(common.Hash{31: 1}), Changes: []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(9)}}}}},
	}
	reader := &countingAccountReader{}

	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
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
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
	require.Equal(t, 3, reader.reads)
	for _, a := range []accounts.Address{drained, renonced, uncoded} {
		_, ok, _ := vm.View(2, reader).GetAccount(a)
		require.False(t, ok, a)
	}
}

// TestSeedAddresses pins the DB-free existence seed. The BAL carries no address record, so
// a block-created account (absent in the committed base) has only field cells; the address
// existence read consults the AddressPath (not the field cells), so unseeded it reads absent
// and the EIP-161 withhold path stamps a spurious removal. SeedAddresses marks existence
// straight from the BAL — no base reader — marking the AddressPath AccountFromBAL so the
// authoritative value still defers to the committed base.
func TestSeedAddresses(t *testing.T) {
	t.Parallel()

	created := accounts.InternAddress(common.HexToAddress("0xc0ffee")) // absent in base, created this block
	existing := accounts.InternAddress(common.HexToAddress("0xe5157")) // present in base, only touched

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

	// The marker is AccountFromBAL, so the authoritative value defers to the base: a
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
	vm.SeedAddresses()

	// Compose after funding (BAL index 2 → TxIndex 1) but before the code deploy (index 5
	// → TxIndex 4): the funded, still-codeless account the CREATE2 collision check must see.
	acc := NewVersionedAccountView(proxy, 3, vm, reader).Account()
	require.NotNil(t, acc, "the funded proxy exists before the deploying tx")
	require.Equal(t, *uint256.NewInt(256_000), acc.Balance)
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash, "reads before the deploy must not see the later CREATE2 code")
}

func TestAuthoritativeAddressWriteDropsBALSource(t *testing.T) {
	t.Parallel()

	a := accounts.InternAddress(common.HexToAddress("0xacc5"))
	baseAcc := accounts.NewAccount()
	baseAcc.Balance = *uint256.NewInt(100)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{a: &baseAcc}}
	written := accounts.NewAccount()
	written.Nonce = 9
	written.Balance = *uint256.NewInt(7)
	marker := written

	flushOverMarker := func(vm *VersionMap, complete bool) *VersionMap {
		vm.WriteAddressExists(a, Version{TxIndex: 0}, &marker)
		ws := &WriteSet{}
		ws.SetAddress(a, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: a, Path: AddressPath, Version: Version{TxIndex: 0}}, Val: &written})
		vm.FlushVersionedWrites(ws, complete)
		return vm
	}
	other := accounts.InternAddress(common.HexToAddress("0x07e5"))
	cases := map[string]func() *VersionMap{
		"pooled marker cell": func() *VersionMap {
			prev := NewVersionMap(nil)
			prev.WriteAddressExists(a, Version{TxIndex: 0}, &marker)
			prev.Delete(a, AddressPath, accounts.NilKey, 0, false)
			vm := NewVersionMap(nil)
			vm.WriteAddress(a, Version{TxIndex: 0}, &written, true)
			return vm
		},
		"marker at the same txIdx": func() *VersionMap {
			vm := NewVersionMap(nil)
			vm.WriteAddressExists(a, Version{TxIndex: 0}, &marker)
			vm.WriteAddress(a, Version{TxIndex: 0}, &written, true)
			return vm
		},
		"equal-value flush over a marker": func() *VersionMap {
			return flushOverMarker(NewVersionMap(nil), true)
		},
		"equal-value estimate flush over a marker on a BAL block": func() *VersionMap {
			bal := types.BlockAccessList{{Address: other.Value(), NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}}}}
			return flushOverMarker(NewVersionMap(bal), false)
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok, _ := build().View(1, reader).GetAccount(a)
			require.True(t, ok)
			require.Equal(t, uint64(9), got.Nonce)
			require.Equal(t, *uint256.NewInt(7), got.Balance)
		})
	}
}

// TestEstimateFlushReplacesBALAddressMarker pins that the creating tx's in-flight flush may
// replace its Done existence marker with an Estimate (the marker value is a placeholder),
// and that its complete flush then publishes the tx value as Done.
func TestEstimateFlushReplacesBALAddressMarker(t *testing.T) {
	prevAssert := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = prevAssert })

	a := accounts.InternAddress(common.HexToAddress("0xacc5"))
	other := accounts.InternAddress(common.HexToAddress("0x07e5"))
	marker := accounts.NewAccount()
	marker.Balance = *uint256.NewInt(7)
	inFlight := accounts.NewAccount()

	vm := NewVersionMap(types.BlockAccessList{{Address: other.Value(), NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}}}})
	vm.WriteAddressExists(a, Version{TxIndex: 0}, &marker)
	ws := &WriteSet{}
	ws.SetAddress(a, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: a, Path: AddressPath, Version: Version{TxIndex: 0}}, Val: &inFlight})

	vm.FlushVersionedWrites(ws, false)
	_, res, ok := vm.ReadAddress(a, 1)
	require.True(t, ok)
	require.Equal(t, MVReadResultDependency, res.Status())

	vm.FlushVersionedWrites(ws, true)
	got, res, ok := vm.ReadAddress(a, 1)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, res.Status())
	require.Equal(t, inFlight.Balance, got.Balance)
}

// TestEstimateFlushKeepsBALSeededCode pins that the deploying tx's in-flight flush of the
// code its BAL entry already seeded leaves the seeded Done code cells in place.
func TestEstimateFlushKeepsBALSeededCode(t *testing.T) {
	prevAssert := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = prevAssert })

	a := accounts.InternAddress(common.HexToAddress("0xc0de"))
	bytecode := []byte{0x60, 0x00}
	vm := NewVersionMap(types.BlockAccessList{{Address: a.Value(), CodeChanges: []*types.CodeChange{{Index: 1, Bytecode: bytecode}}}})
	code := accounts.NewCode(bytecode)
	v := Version{TxIndex: 0}
	ws := &WriteSet{}
	ws.SetCode(a, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: a, Path: CodePath, Version: v}, Val: code})
	ws.SetCodeSize(a, &VersionedWrite[int]{WriteHeader: WriteHeader{Address: a, Path: CodeSizePath, Version: v}, Val: code.Len()})

	vm.FlushVersionedWrites(ws, false)
	got, res, ok := vm.ReadCode(a, 1)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, res.Status())
	require.Equal(t, code.Hash, got.Hash)
	size, res, ok := vm.ReadCodeSize(a, 1)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, res.Status())
	require.Equal(t, len(bytecode), size)
}

func TestBALMarkedAccountReadsCommittedBaseOncePerBlock(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xba5e"))
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Balance = *uint256.NewInt(100)
	reader := &countingAccountReader{staticAccountReader: staticAccountReader{addr: a, acc: &acc}}
	bal := types.BlockAccessList{{Address: a.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(50)}}}}
	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	for txIndex := 1; txIndex <= 5; txIndex++ {
		startNoMaterializeTx(ibs, vm, txIndex)
		_, err := ibs.GetCodeHash(a)
		require.NoError(t, err)
		exists, err := ibs.Exist(a)
		require.NoError(t, err)
		require.True(t, exists)
		ibs.PublishOrigins()
	}
	require.Equal(t, 1, reader.reads)
}

func TestSeedEmptyRemovals_BaseReadErrorFails(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xe2})
	bal := types.BlockAccessList{{Address: addr.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}}}
	vm := NewVersionMap(bal)
	boom := errors.New("boom")
	require.ErrorIs(t, vm.SeedEmptyRemovals(true, false, &failingBaseReader{err: boom}), boom)
}

// TestSeedEmptyRemovals_DrainThenRefund pins the removal at an interior index: an
// account drained to empty reads absent to a step checkpoint at index 1, then is
// present at block end once funded again at index 2, or once re-created at index 2
// with no balance change (CREATE2 onto a drained address, as createObject does for a
// worker).
func TestSeedEmptyRemovals_DrainThenRefund(t *testing.T) {
	t.Parallel()

	refunded := accounts.InternAddress(common.HexToAddress("0xd7a1"))
	recreated := accounts.InternAddress(common.HexToAddress("0xd7a2"))
	bal := types.BlockAccessList{
		{Address: refunded.Value(), BalanceChanges: []*types.BalanceChange{
			{Index: 1, Value: uint256.Int{}},
			{Index: 2, Value: *uint256.NewInt(7)},
		}},
		{
			Address:        recreated.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
			NonceChanges:   []*types.NonceChange{{Index: 2, Value: 1}},
			CodeChanges:    []*types.CodeChange{{Index: 2, Bytecode: []byte{0x60, 0x00}}},
		},
	}
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	for _, addr := range []accounts.Address{refunded, recreated} {
		base := accounts.NewAccount()
		base.Balance = *uint256.NewInt(5)
		reader.accounts[addr] = &base
	}

	vm := NewVersionMap(bal)
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))

	_, ok, _ := vm.View(1, reader).GetAccount(refunded)
	require.False(t, ok, "drained to empty at index 1: absent before the re-fund")
	acc, ok, _ := vm.View(2, reader).GetAccount(refunded)
	require.True(t, ok, "funded again at index 2: present at block end")
	require.Equal(t, *uint256.NewInt(7), acc.Balance)

	_, ok, _ = vm.View(1, reader).GetAccount(recreated)
	require.False(t, ok, "drained to empty at index 1")
	acc, ok, _ = vm.View(2, reader).GetAccount(recreated)
	require.True(t, ok, "re-created at index 2: present at block end")
	require.Equal(t, uint64(1), acc.Nonce)
}

func TestHasEmptyBaseAccount(t *testing.T) {
	t.Parallel()
	addr := func(s string) accounts.Address { return accounts.InternAddress(common.HexToAddress(s)) }
	empty, absent, funded := addr("0xe1"), addr("0xa1"), addr("0xf1")
	emptyAcc, systemAcc, fundedAcc := accounts.NewAccount(), accounts.NewAccount(), accounts.NewAccount()
	fundedAcc.Balance = *uint256.NewInt(1)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{
		empty: &emptyAcc, params.SystemAddress: &systemAcc, funded: &fundedAcc,
	}}
	listed := func(a accounts.Address) types.AccountChanges { return types.AccountChanges{Address: a.Value()} }
	changed := listed(empty)
	changed.BalanceChanges = []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(1)}}
	for _, tc := range []struct {
		name         string
		eip161, aura bool
		bal          types.BlockAccessList
		want         bool
	}{
		{"empty", true, false, types.BlockAccessList{listed(empty)}, true},
		{"empty with a change", true, false, types.BlockAccessList{changed}, true},
		{"absent and funded", true, false, types.BlockAccessList{listed(absent), listed(funded)}, false},
		{"before EIP-161", false, false, types.BlockAccessList{listed(empty)}, false},
		{"Aura system address", true, true, types.BlockAccessList{listed(params.SystemAddress)}, false},
		{"system address", true, false, types.BlockAccessList{listed(params.SystemAddress)}, true},
	} {
		got, err := NewVersionMap(tc.bal).HasEmptyBaseAccount(tc.eip161, tc.aura, reader)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, tc.name)
	}
}

// A BAL existence marker at the index that drains the account is not a revival.
func TestSeededRemovalAtMarkerIndexStaysAbsent(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress(common.HexToAddress("0x5a1e"))
	baseAcc := accounts.NewAccount()
	baseAcc.Balance = *uint256.NewInt(5)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &baseAcc}}
	bal := types.BlockAccessList{{Address: x.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}}}
	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))

	lc, _, _, _ := vm.AccountLifecycleAt(x, 1)
	require.Equal(t, LifecycleAbsent, lc)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	exists, err := ibs.Exist(x)
	require.NoError(t, err)
	require.False(t, exists)
}

// seededRemovalOfPrefundedAccount returns a version map seeded with the access list of a block in
// which tx 0 creates and destroys the prefunded x and tx 1 re-creates it.
func seededRemovalOfPrefundedAccount(t *testing.T) (accounts.Address, *accountStateReader, *VersionMap) {
	t.Helper()
	x := accounts.InternAddress(common.HexToAddress("0x5a1e"))
	baseAcc := accounts.NewAccount()
	baseAcc.Balance = *uint256.NewInt(5)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &baseAcc}}
	vm := NewVersionMap(types.BlockAccessList{{
		Address:        x.Value(),
		BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
		NonceChanges:   []*types.NonceChange{{Index: 2, Value: 1}},
		CodeChanges:    []*types.CodeChange{{Index: 2, Bytecode: []byte{0x00}}},
	}})
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
	return x, reader, vm
}

// Tx 0 creates and destroys the prefunded x, tx 1 re-creates it. Tx 1 reads x while the
// removal is still the seeded Done cell, then tx 0 flushes its own removal (Estimate) before
// tx 1 asks for the account: x must stay absent, so the re-creation pays for a new account.
func TestDestructObservedBeforeItsInFlightFlushKeepsTheAccountAbsent(t *testing.T) {
	t.Parallel()
	x, reader, vm := seededRemovalOfPrefundedAccount(t)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	balance, err := ibs.GetBalance(x)
	require.NoError(t, err)
	require.True(t, balance.IsZero())

	removal := &WriteSet{}
	removal.SetContractRemoval(x, Version{TxIndex: 0})
	vm.FlushVersionedWrites(removal, false)

	empty, err := ibs.Empty(x)
	require.NoError(t, err)
	require.True(t, empty)
	exists, err := ibs.Exist(x)
	require.NoError(t, err)
	require.False(t, exists)

	vm.MarkWritesValidated(removal, nil)
	require.Equal(t, VersionValid, validateReads(vm, ibs, 1))
	for _, path := range []AccountPath{SelfDestructPath, BalancePath, CodeHashPath} {
		vm.Delete(x, path, accounts.NilKey, 0, false)
	}
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 1), "a withdrawn destruct invalidates the absence")
}

// As above, but tx 1 credits x in the window: the credit creates a fresh account instead of
// reloading the pre-block one, so tx 1 stays valid once tx 0 is validated.
func TestCreditAfterObservedDestructCreatesAFreshAccount(t *testing.T) {
	t.Parallel()
	x, reader, vm := seededRemovalOfPrefundedAccount(t)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	_, err := ibs.GetBalance(x)
	require.NoError(t, err)

	removal := &WriteSet{}
	removal.SetContractRemoval(x, Version{TxIndex: 0})
	vm.FlushVersionedWrites(removal, false)

	require.NoError(t, ibs.AddBalance(x, *uint256.NewInt(1), tracing.BalanceChangeTransfer))
	balance, err := ibs.GetBalance(x)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1), balance)

	vm.MarkWritesValidated(removal, nil)
	require.Equal(t, VersionValid, validateReads(vm, ibs, 1))
}

// As above, but tx 1 funds x again between two destructs, so its account cell lies below the
// destruct of tx 2 that tx 3 reads; it is not a re-creation by the destroying tx.
func TestRepeatedDestructObservedBeforeItsInFlightFlushKeepsTheAccountAbsent(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress(common.HexToAddress("0x5a1f"))
	baseAcc := accounts.NewAccount()
	baseAcc.Balance = *uint256.NewInt(5)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &baseAcc}}
	bal := types.BlockAccessList{{
		Address: x.Value(),
		BalanceChanges: []*types.BalanceChange{
			{Index: 1, Value: uint256.Int{}},
			{Index: 2, Value: *uint256.NewInt(1)},
			{Index: 3, Value: uint256.Int{}},
		},
		NonceChanges: []*types.NonceChange{{Index: 4, Value: 1}},
		CodeChanges:  []*types.CodeChange{{Index: 4, Bytecode: []byte{0x00}}},
	}}
	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
	funded := accounts.NewAccount()
	funded.Balance = *uint256.NewInt(1)
	vm.WriteAddress(x, Version{TxIndex: 1}, &funded, true)

	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 3)
	balance, err := ibs.GetBalance(x)
	require.NoError(t, err)
	require.True(t, balance.IsZero())

	removal := &WriteSet{}
	removal.SetContractRemoval(x, Version{TxIndex: 2})
	vm.FlushVersionedWrites(removal, false)

	empty, err := ibs.Empty(x)
	require.NoError(t, err)
	require.True(t, empty)
	exists, err := ibs.Exist(x)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestVersionedReaderServesBALMarkedAccountFromOrigin pins that a versioned reader over an
// empty read set (the fee calculation) answers a BAL-marked account from its stored origin
// instead of reading the committed base again for every tx.
func TestVersionedReaderServesBALMarkedAccountFromOrigin(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xc0ba5e"))
	acc := accounts.NewAccount()
	acc.Nonce = 3
	acc.Balance = *uint256.NewInt(100)
	reader := &countingAccountReader{staticAccountReader: staticAccountReader{addr: a, acc: &acc}}
	bal := types.BlockAccessList{{Address: a.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(50)}}}}
	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	SeedOrigin(vm, a, &acc)

	for txIndex := 1; txIndex <= 3; txIndex++ {
		got, err := NewVersionedStateReader(txIndex, ReadSet{}, vm, reader).ReadAccountData(a)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, uint64(3), got.Nonce)
		require.Equal(t, *uint256.NewInt(50), got.Balance, "txIndex %d: the BAL balance cell overlays the origin", txIndex)
	}
	require.Zero(t, reader.reads, "the stored origin answers every tx")
}

// TestInFlightFlushOfSeededRemovalKeepsDoneFlags pins #26286: an in-flight flush of a
// destroying tx that matches the BAL-seeded empty removal keeps SelfDestructPath and
// BalancePath Done instead of turning them back into Estimate cells.
func TestInFlightFlushOfSeededRemovalKeepsDoneFlags(t *testing.T) {
	t.Parallel()
	t.Run("matching_removal_stays_done", func(t *testing.T) {
		t.Parallel()
		x, _, vm := seededRemovalOfPrefundedAccount(t)

		_, sdBefore, ok := vm.ReadSelfDestruct(x, 1)
		require.True(t, ok)
		require.Equal(t, MVReadResultDone, sdBefore.Status())
		_, balBefore, ok := vm.ReadBalance(x, 1)
		require.True(t, ok)
		require.Equal(t, MVReadResultDone, balBefore.Status())

		removal := &WriteSet{}
		removal.SetContractRemoval(x, Version{TxIndex: 0})
		vm.FlushVersionedWrites(removal, false)

		destructed, sdAfter, ok := vm.ReadSelfDestruct(x, 1)
		require.True(t, ok)
		require.True(t, destructed)
		require.Equal(t, MVReadResultDone, sdAfter.Status(), "seeded SelfDestruct cell must stay Done")

		bal, balAfter, ok := vm.ReadBalance(x, 1)
		require.True(t, ok)
		require.True(t, bal.IsZero())
		require.Equal(t, ValueDeleted, balAfter.ValStatus())
		require.Equal(t, MVReadResultDone, balAfter.Status(), "seeded Balance cell must stay Done")

		vm.MarkWritesValidated(removal, nil)
		_, sdVal, _ := vm.ReadSelfDestruct(x, 1)
		require.Equal(t, MVReadResultDone, sdVal.Status())
		_, balVal, _ := vm.ReadBalance(x, 1)
		require.Equal(t, MVReadResultDone, balVal.Status())

		vm.MarkWritesComplete(removal)
		_, sdDone, _ := vm.ReadSelfDestruct(x, 1)
		require.Equal(t, MVReadResultDone, sdDone.Status())
		_, balDone, _ := vm.ReadBalance(x, 1)
		require.Equal(t, MVReadResultDone, balDone.Status())
	})

	t.Run("mismatched_flush_becomes_estimate", func(t *testing.T) {
		t.Parallel()
		x, _, vm := seededRemovalOfPrefundedAccount(t)

		mismatch := &WriteSet{}
		mismatch.SetSelfDestruct(x, &VersionedWrite[bool]{
			WriteHeader: WriteHeader{Address: x, Path: SelfDestructPath, Version: Version{TxIndex: 0}},
			Val:         false,
		})
		mismatch.SetBalance(x, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: x, Path: BalancePath, Version: Version{TxIndex: 0}, valStatus: ValueChanged},
			Val:         *uint256.NewInt(1),
		})
		vm.FlushVersionedWrites(mismatch, false)

		_, sdAfter, ok := vm.ReadSelfDestruct(x, 1)
		require.True(t, ok)
		require.Equal(t, MVReadResultDependency, sdAfter.Status())
		_, balAfter, ok := vm.ReadBalance(x, 1)
		require.True(t, ok)
		require.Equal(t, MVReadResultDependency, balAfter.Status())
	})
}
