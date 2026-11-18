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
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
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

func TestHasAmbiguousBaseAccount(t *testing.T) {
	t.Parallel()
	addr := func(s string) accounts.Address { return accounts.InternAddress(common.HexToAddress(s)) }
	empty, absent, funded, storageHolder, contract := addr("0xe1"), addr("0xa1"), addr("0xf1"), addr("0x51"), addr("0xc1")
	emptyAcc, systemAcc, fundedAcc := accounts.NewAccount(), accounts.NewAccount(), accounts.NewAccount()
	fundedAcc.Balance = *uint256.NewInt(1)
	storageHolderAcc := fundedAcc
	storageHolderAcc.Incarnation = FirstContractIncarnation
	contractAcc := storageHolderAcc
	contractAcc.CodeHash = accounts.InternCodeHash(common.HexToHash("0xc0de"))
	codeless := addr("0xec")
	codelessAcc := storageHolderAcc
	codelessAcc.Nonce = 1
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{
		empty: &emptyAcc, params.SystemAddress: &systemAcc, funded: &fundedAcc, storageHolder: &storageHolderAcc, contract: &contractAcc, codeless: &codelessAcc,
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
		{"no nonce and no code with a contract incarnation", true, false, types.BlockAccessList{listed(storageHolder)}, true},
		{"contract with no nonce", true, false, types.BlockAccessList{listed(contract)}, false},
		{"before EIP-161, no nonce and no code with a contract incarnation", false, false, types.BlockAccessList{listed(storageHolder)}, true},
		{"a nonce and no code with a contract incarnation", true, false, types.BlockAccessList{listed(codeless)}, false},
	} {
		got, err := NewVersionMap(tc.bal).HasAmbiguousBaseAccount(tc.eip161, tc.aura, reader)
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

// redestruct puts the seeded destruct of x at txIdx back in flight, as the runs of the destroying
// tx can: the first run keeps the seed, the second drops the destruct (the flush deletes its
// cell), and the third writes it again.
func redestruct(vm *VersionMap, x accounts.Address, txIdx int) *WriteSet {
	var removal *WriteSet
	for _, inc := range []Incarnation{0, 2} {
		if inc > 0 {
			vm.Delete(x, SelfDestructPath, accounts.NilKey, txIdx, false)
		}
		removal = &WriteSet{}
		removal.SetContractRemoval(x, Version{TxIndex: txIdx, Incarnation: inc})
		vm.FlushVersionedWrites(removal, false)
	}
	return removal
}

// The destroying tx's in-flight flush of the removal the access list seeded keeps the seed
// Done, so a later tx that reads the account does not wait for the destroying tx.
func TestInFlightFlushOfTheSeededRemovalKeepsItDone(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		remove func(*WriteSet, accounts.Address, Version)
	}{
		{"contract removal", (*WriteSet).SetContractRemoval},
		{"empty removal", (*WriteSet).SetEmptyRemoval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, reader, vm := seededRemovalOfPrefundedAccount(t)
			removal := &WriteSet{}
			tc.remove(removal, x, Version{TxIndex: 0, Incarnation: 1})
			vm.FlushVersionedWrites(removal, false)
			_, res, _ := vm.ReadSelfDestruct(x, 1)
			require.Equal(t, MVReadResultDone, res.Status())
			_, res, _ = vm.ReadBalance(x, 1)
			require.Equal(t, MVReadResultDone, res.Status())

			ibs := NewWithVersionMap(reader, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 1)
			exists, err := ibs.Exist(x)
			require.NoError(t, err)
			require.False(t, ibs.HadInvalidRead(), "tx 1 waits for tx %d", ibs.DepTxIndex())
			require.False(t, exists)

			vm.MarkWritesValidated(removal, nil)
			vm.MarkWritesComplete(removal)
			require.Equal(t, VersionValid, validateReads(vm, ibs, 1))
		})
	}
}

// Only a seeded destruct keeps its version: the same destruct over a validated one that an
// earlier run of the tx left is a new version.
func TestReflushedDestructOfAnEarlierRunTakesTheNewVersion(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress(common.HexToAddress("0x5a1e"))
	other := accounts.InternAddress(common.HexToAddress("0x07e5"))
	vm := NewVersionMap(types.BlockAccessList{{Address: other.Value(), NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}}}})
	run0 := &WriteSet{}
	run0.SetEmptyRemoval(x, Version{TxIndex: 0})
	vm.FlushVersionedWrites(run0, false)
	vm.MarkWritesValidated(run0, nil)

	run1 := &WriteSet{}
	run1.SetEmptyRemoval(x, Version{TxIndex: 0, Incarnation: 1})
	vm.FlushVersionedWrites(run1, false)
	vm.MarkWritesComplete(run1)
	_, res, _ := vm.ReadSelfDestruct(x, 1)
	require.Equal(t, Incarnation(1), res.Incarnation())
}

// Tx 0 creates and destroys the prefunded x, tx 1 re-creates it. Tx 1 reads x while the
// removal is still the seeded Done cell, and the destruct goes back in flight before tx 1 asks
// for the account: x must stay absent, so the re-creation pays for a new account. Validation
// rejects the attempt, as the seed it read is replaced.
func TestDestructObservedBeforeItsInFlightFlushKeepsTheAccountAbsent(t *testing.T) {
	t.Parallel()
	x, reader, vm := seededRemovalOfPrefundedAccount(t)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	balance, err := ibs.GetBalance(x)
	require.NoError(t, err)
	require.True(t, balance.IsZero())

	removal := redestruct(vm, x, 0)

	empty, err := ibs.Empty(x)
	require.NoError(t, err)
	require.True(t, empty)
	exists, err := ibs.Exist(x)
	require.NoError(t, err)
	require.False(t, exists)

	vm.MarkWritesValidated(removal, nil)
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 1))
}

// Tx 1 reads x as absent while the seeded destruct of tx 0 holds. The read is anchored on that
// destruct, so it is invalid once the destruct is withdrawn or replaced, also when it is the
// only read: the existence marker the access list seeded at the same version records an
// account that exists.
func TestAbsentAccountReadIsInvalidOnceItsDestructIsGone(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*VersionMap, accounts.Address)
	}{
		{"withdrawn", func(vm *VersionMap, x accounts.Address) {
			for _, path := range []AccountPath{SelfDestructPath, BalancePath} {
				vm.Delete(x, path, accounts.NilKey, 0, false)
			}
		}},
		{"replaced", func(vm *VersionMap, x accounts.Address) {
			vm.MarkWritesValidated(redestruct(vm, x, 0), nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, reader, vm := seededRemovalOfPrefundedAccount(t)
			exists, absentRead := existenceRead(t, reader, vm, x, 1)
			require.False(t, exists)
			require.Equal(t, VersionValid, vm.ValidateReadSet(1, absentRead, validateEqualVersion, false, ""))

			tc.change(vm, x)
			require.Equal(t, VersionInvalid, vm.ValidateReadSet(1, absentRead, validateEqualVersion, false, ""))
		})
	}
}

// Tx 1 reads x as present from the existence marker while the seeded destruct of tx 0 is
// withdrawn. The marker has the version of that destruct, so the read is invalid once a run of
// tx 0 removes x again, also when it is the only read. A destruct that keeps the balance
// (EIP-8246) leaves x alive, so the read stays valid.
func TestPresentAccountReadFromTheMarkerIsInvalidOnceItsDestructReturns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		keepBalance bool
	}{
		{"balance moved out", false},
		{"balance kept", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, reader, vm := seededRemovalOfPrefundedAccount(t)
			for _, path := range []AccountPath{SelfDestructPath, BalancePath} {
				vm.Delete(x, path, accounts.NilKey, 0, false)
			}
			exists, presentRead := existenceRead(t, reader, vm, x, 1)
			require.True(t, exists)
			require.Equal(t, VersionValid, vm.ValidateReadSet(1, presentRead, validateEqualVersion, false, ""))

			createAndDestroyInTx0(t, reader, vm, x, tc.keepBalance)
			existsOnRerun, _ := existenceRead(t, reader, vm, x, 1)
			require.Equal(t, tc.keepBalance, existsOnRerun)
			valid := vm.ValidateReadSet(1, presentRead, validateEqualVersion, false, "") == VersionValid
			require.Equal(t, existsOnRerun, valid, "the read stays valid only while a re-run still finds x")
		})
	}
}

// A credit of x by tx 1, written after tx 0 removed x again, revives x for a read from the
// marker only once it is sealed: until then it can be withdrawn, and a withdrawn credit does not
// re-validate a tx that read only the account's existence.
func TestPresentAccountReadFromTheMarkerNeedsASealedRevival(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress(common.HexToAddress("0x5a1e"))
	baseAcc := accounts.NewAccount()
	baseAcc.Balance = *uint256.NewInt(5)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &baseAcc}}
	vm := NewVersionMap(types.BlockAccessList{{
		Address:        x.Value(),
		BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
	}})
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
	for _, path := range []AccountPath{SelfDestructPath, BalancePath} {
		vm.Delete(x, path, accounts.NilKey, 0, false)
	}
	exists, presentRead := existenceRead(t, reader, vm, x, 2)
	require.True(t, exists)
	require.Equal(t, VersionValid, vm.ValidateReadSet(2, presentRead, validateEqualVersion, false, ""))

	createAndDestroyInTx0(t, reader, vm, x, false)
	credit := &WriteSet{}
	credit.SetBalance(x, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: x, Path: BalancePath, Version: Version{TxIndex: 1, Incarnation: 1}},
		Val:         *uint256.NewInt(7),
	})
	vm.FlushVersionedWrites(credit, false)
	vm.MarkWritesValidated(credit, nil)
	require.False(t, presentRead.DependsOn(WriteHeader{Address: x, Path: BalancePath}))
	require.Equal(t, VersionInvalid, vm.ValidateReadSet(2, presentRead, validateEqualVersion, false, ""))
	rerun := NewWithVersionMap(reader, vm)
	defer rerun.Close()
	rerun.SetTxContext(1, 2)
	existsOnRerun, err := rerun.Exist(x)
	require.NoError(t, err)
	require.True(t, existsOnRerun)
	require.Equal(t, VersionValid, vm.ValidateReadSet(2, rerun.VersionedReads(), validateEqualVersion, false, ""), "a re-run validates, so the rule does not loop")

	vm.MarkWritesComplete(credit)
	require.Equal(t, VersionValid, vm.ValidateReadSet(2, presentRead, validateEqualVersion, false, ""))
}

// existenceRead runs Exist(x) in tx txIdx and returns its result with the read it recorded.
func existenceRead(t *testing.T, reader StateReader, vm *VersionMap, x accounts.Address, txIdx int) (bool, ReadSet) {
	t.Helper()
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, txIdx)
	exists, err := ibs.Exist(x)
	require.NoError(t, err)
	reads := ibs.VersionedReads()
	read, ok := reads.GetAddress(x)
	require.True(t, ok)
	return exists, ReadSet{address: map[accounts.Address]VersionedRead[AccountView]{x: read}}
}

// createAndDestroyInTx0 is a later run of tx 0 that deploys a contract at x and self-destructs it.
func createAndDestroyInTx0(t *testing.T, reader StateReader, vm *VersionMap, x accounts.Address, keepBalance bool) {
	t.Helper()
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(2)
	require.NoError(t, ibs.CreateAccount(x, true))
	if !keepBalance {
		require.NoError(t, ibs.SetBalance(x, uint256.Int{}, tracing.BalanceChangeUnspecified))
	}
	_, err := ibs.Selfdestruct(x, true)
	require.NoError(t, err)
	require.NoError(t, ibs.MakeWriteSet(&chain.Rules{IsAmsterdam: true}, NewNoopWriter()))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), false)
	vm.MarkWritesValidated(ibs.VersionedWrites(), nil)
}

// As above, but tx 1 credits x in the window: the credit creates a fresh account instead of
// reloading the pre-block one.
func TestCreditAfterObservedDestructCreatesAFreshAccount(t *testing.T) {
	t.Parallel()
	x, reader, vm := seededRemovalOfPrefundedAccount(t)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	_, err := ibs.GetBalance(x)
	require.NoError(t, err)

	redestruct(vm, x, 0)

	require.NoError(t, ibs.AddBalance(x, *uint256.NewInt(1), tracing.BalanceChangeTransfer))
	balance, err := ibs.GetBalance(x)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1), balance)
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

	redestruct(vm, x, 2)

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

// SeedEmptyRemovals derives zero balances from the access list and the pre-block state: a deleted
// balance where it removes the account, and a zero balance where a later tx revives it without a
// balance change. They are the tx's final writes, as the cells of the access list are. When a re-run
// of the tx drops a balance write that an earlier run made there, the derived balance is back as Done
// with its status, also when the earlier run wrote an equal value, which kept the cell Done and
// rewrote its status. A change takes the re-run's incarnation, so that a reader of the earlier run's
// cell is invalid.
func TestDropWritePutsBackADerivedZeroBalance(t *testing.T) {
	t.Parallel()
	prefunded := func(t *testing.T) (accounts.Address, *VersionMap) {
		x, _, vm := seededRemovalOfPrefundedAccount(t)
		return x, vm
	}
	uncoded := func(t *testing.T) (accounts.Address, *VersionMap) {
		x := accounts.InternAddress(common.HexToAddress("0xc3"))
		vm := NewVersionMap(types.BlockAccessList{{Address: x.Value(), CodeChanges: []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60}}, {Index: 2}}}})
		vm.SeedAddresses()
		require.NoError(t, vm.SeedEmptyRemovals(true, false, &countingAccountReader{}))
		return x, vm
	}
	// Seeding derives a removal at tx 0, then a zero balance of a revival at tx 1 and a removal at tx 1,
	// as the code change at index 2 revives the account without a balance change and leaves it empty.
	removedTwice := func(t *testing.T) (accounts.Address, *VersionMap) {
		x := accounts.InternAddress(common.HexToAddress("0xc4"))
		acc := accounts.NewAccount()
		acc.Balance = *uint256.NewInt(5)
		reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &acc}}
		vm := NewVersionMap(types.BlockAccessList{{
			Address:        x.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
			CodeChanges:    []*types.CodeChange{{Index: 2}},
		}})
		vm.SeedAddresses()
		require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
		return x, vm
	}
	balance := func(v uint64) func(*WriteSet, accounts.Address, Version) {
		return func(ws *WriteSet, x accounts.Address, ver Version) {
			ws.SetBalance(x, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: x, Path: BalancePath, Version: ver}, Val: *uint256.NewInt(v)})
		}
	}
	for _, tc := range []struct {
		name      string
		seed      func(*testing.T) (accounts.Address, *VersionMap)
		txIdx     int
		overwrite func(*WriteSet, accounts.Address, Version)
		status    valueStatus
	}{
		{"removal where the access list has a zero balance", prefunded, 0, balance(5), ValueDeleted},
		{"removal over an equal balance", prefunded, 0, balance(0), ValueDeleted},
		{"removal where the access list has no balance", uncoded, 1, balance(5), ValueDeleted},
		{"revival", prefunded, 1, balance(5), ValueChanged},
		{"revival over a removal", prefunded, 1, (*WriteSet).SetEmptyRemoval, ValueChanged},
		{"removal at the tx of a revival", removedTwice, 1, balance(5), ValueDeleted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, vm := tc.seed(t)
			run := &WriteSet{}
			tc.overwrite(run, x, Version{TxIndex: tc.txIdx, Incarnation: 1})
			vm.FlushVersionedWrites(run, false)

			vm.DropWrite(x, BalancePath, accounts.NilKey, Version{TxIndex: tc.txIdx, Incarnation: 2})
			c := requireDoneCell(t, vm.load(x).Balance, tc.txIdx, 2)
			require.True(t, c.Value.IsZero())
			require.Equal(t, tc.status, c.valStatus)
		})
	}
}

// A dropped balance write puts back the access list's zero with its status: a reader that saw the
// earlier run's deleted balance took the account as removed.
func TestDropWritePutsBackTheStatusOfASeededBalance(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress(common.HexToAddress("0x5a1e"))
	baseAcc := accounts.NewAccount()
	baseAcc.Balance, baseAcc.Nonce = *uint256.NewInt(5), 3
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &baseAcc}}
	vm := NewVersionMap(types.BlockAccessList{{Address: x.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}}})
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, reader))
	run := &WriteSet{}
	run.SetEmptyRemoval(x, Version{TxIndex: 0, Incarnation: 1})
	vm.FlushVersionedWrites(run, false)

	vm.DropWrite(x, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 2})
	c := requireDoneCell(t, vm.load(x).Balance, 0, 2)
	require.True(t, c.Value.IsZero())
	require.Equal(t, ValueChanged, c.valStatus)
}

func requireDoneCell[T any](t *testing.T, cells *btree.Map[int, *WriteCell[T]], txIdx int, inc Incarnation) *WriteCell[T] {
	t.Helper()
	require.NotNil(t, cells)
	c, ok := cells.Get(txIdx)
	require.True(t, ok, "no cell at tx %d", txIdx)
	require.Equal(t, FlagDone, c.loadFlag())
	require.Equal(t, inc, c.loadInc())
	return c
}

// The access list seeds a removal of x at tx 3. The first run of tx 3 writes a balance of 8 at the
// seed's version, and tx 4 reads it. The re-run removes x, which keeps the seeded destruct and moves
// the balance to a deleted zero at the re-run's incarnation, so the read of 8 is stale. A read of the
// deleted zero is recorded at the destruct's version, so it stays valid.
func TestABalanceReadAtTheSeededDestructsVersionIsStaleOnceTheBalanceMoves(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress(common.HexToAddress("0xd6"))
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(5)
	base := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{x: &acc}}
	vm := NewVersionMap(types.BlockAccessList{{Address: x.Value(), BalanceChanges: []*types.BalanceChange{{Index: 4, Value: uint256.Int{}}}}})
	vm.SeedAddresses()
	require.NoError(t, vm.SeedEmptyRemovals(true, false, base))
	readBalance := func(want uint64) ReadSet {
		ibs := NewWithVersionMap(base, vm)
		defer ibs.Close()
		ibs.SetTxContext(1, 4)
		balance, err := ibs.GetBalance(x)
		require.NoError(t, err)
		require.False(t, ibs.HadInvalidRead())
		require.Equal(t, *uint256.NewInt(want), balance)
		return ibs.VersionedReads()
	}

	first := &WriteSet{}
	first.SetBalance(x, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: x, Path: BalancePath, Version: Version{TxIndex: 3}}, Val: *uint256.NewInt(8)})
	vm.FlushVersionedWrites(first, false)
	vm.MarkWritesValidated(first, nil)
	reads := readBalance(8)

	final := &WriteSet{}
	final.SetEmptyRemoval(x, Version{TxIndex: 3, Incarnation: 1})
	vm.FlushVersionedWrites(final, false)
	vm.MarkWritesValidated(final, nil)
	require.Equal(t, VersionInvalid, vm.ValidateReadSet(4, reads, validateEqualVersion, false, ""))
	require.Equal(t, VersionValid, vm.ValidateReadSet(4, readBalance(0), validateEqualVersion, false, ""))
}
