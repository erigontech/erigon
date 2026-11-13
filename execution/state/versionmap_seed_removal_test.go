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

	vm := NewBALReadVersionMap(bal)
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

// TestBALExistence pins the DB-free existence read. The BAL carries no address record, so a
// block-created account (absent in the committed base) would read absent and the EIP-161
// withhold path would stamp a spurious removal. The address read takes existence straight
// from the BAL — no base reader — as an AccountFromBAL marker, so the authoritative value
// still defers to the committed base.
func TestBALExistence(t *testing.T) {
	t.Parallel()

	created := accounts.InternAddress(common.HexToAddress("0xc0ffee")) // absent in base, created this block
	existing := accounts.InternAddress(common.HexToAddress("0xe5157")) // present in base, only touched

	bal := types.BlockAccessList{
		{
			Address:        existing.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(7)}},
		},
		{
			Address:      created.Value(),
			NonceChanges: []*types.NonceChange{{Index: 3, Value: 1}},
			CodeChanges:  []*types.CodeChange{{Index: 3, Bytecode: []byte{0x60, 0x00}}},
		},
	}

	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	existingBase := accounts.NewAccount()
	existingBase.Nonce = 9
	reader.accounts[existing] = &existingBase // `created` is intentionally absent from the base

	vm := NewBALReadVersionMap(bal)

	if _, _, ok := vm.ReadAddress(created, 2); ok {
		t.Fatal("a BAL-created account is absent before its first change")
	}
	if _, _, ok := vm.ReadAddress(created, 5); !ok {
		t.Fatal("a BAL-created account reads present with no base lookup")
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

// TestBALExistenceDoesNotLeakLaterCode pins the CREATE2-onto-funded fix: an account funded
// (balance) at an early tx and given code at a later tx must, before the deploying tx,
// compose with empty code. Existence must not drag the later CodeHash into earlier
// reads — the collision that made the proposer and validator disagree.
func TestBALExistenceDoesNotLeakLaterCode(t *testing.T) {
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

	vm := NewBALReadVersionMap(bal)

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
	vm := NewBALReadVersionMap(bal)
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
	vm := NewBALReadVersionMap(bal)
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
	vm := NewBALReadVersionMap(bal)
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

// TestBALReadVersionMapReadsLatestChangeBelowTx pins the BAL read rule: tx i reads the change with
// the highest block-access index < i+1, else the pre-block state. A flushed write of an earlier tx
// does not change what it reads.
func TestBALReadVersionMapReadsLatestChangeBelowTx(t *testing.T) {
	t.Parallel()

	a := accounts.InternAddress(common.HexToAddress("0xba1"))
	slot := accounts.InternKey(common.HexToHash("0x05"))
	code := []byte{0x60, 0x01}
	bal := types.BlockAccessList{{
		Address:        a.Value(),
		StorageChanges: []types.SlotChanges{{Slot: slot, Changes: []*types.StorageChange{{Index: 2, Value: *uint256.NewInt(5)}}}},
		BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(10)}, {Index: 3, Value: *uint256.NewInt(30)}},
		NonceChanges:   []*types.NonceChange{{Index: 3, Value: 1}},
		CodeChanges:    []*types.CodeChange{{Index: 2, Bytecode: code}},
	}}
	vm := NewBALReadVersionMap(bal)

	_, _, ok := vm.ReadBalance(a, 0)
	require.False(t, ok, "tx 0 reads the pre-block balance")
	for txIdx, want := range map[int]uint64{1: 10, 2: 10, 3: 30, 4: 30} {
		b, res, ok := vm.ReadBalance(a, txIdx)
		require.True(t, ok)
		require.Equal(t, want, b.Uint64(), "tx %d", txIdx)
		require.Equal(t, MVReadResultDone, res.Status())
	}
	_, _, ok = vm.ReadStorage(a, slot, 1)
	require.False(t, ok, "tx 1 reads the pre-block slot")
	s, _, ok := vm.ReadStorage(a, slot, 2)
	require.True(t, ok)
	require.Equal(t, uint64(5), s.Uint64())
	n, _, ok := vm.ReadNonce(a, 3)
	require.True(t, ok)
	require.Equal(t, uint64(1), n)
	c, _, ok := vm.ReadCode(a, 2)
	require.True(t, ok)
	require.Equal(t, code, c.Bytes)

	vm.WriteBalance(a, Version{TxIndex: 1}, *uint256.NewInt(99), true)
	b, _, _ := vm.ReadBalance(a, 2)
	require.Equal(t, uint64(10), b.Uint64(), "a flushed write is not a BAL change")
}

// TestBALReadVersionMapReadStaysValid pins that a flush of an earlier tx never invalidates a
// BAL-served read: the BAL hash check, not re-execution, rejects a wrong list.
func TestBALReadVersionMapReadStaysValid(t *testing.T) {
	t.Parallel()

	a := accounts.InternAddress(common.HexToAddress("0xba2"))
	bal := types.BlockAccessList{{
		Address:        a.Value(),
		BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(10)}},
	}}
	vm := NewBALReadVersionMap(bal)

	io := NewVersionedIO(3)
	rs := ReadSet{}
	rs.SetBalance(a, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
		Val:        *uint256.NewInt(10),
	})
	io.RecordReads(Version{TxIndex: 2}, rs)
	vm.WriteBalance(a, Version{TxIndex: 1}, *uint256.NewInt(99), false)
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, false, ""))
}

// TestReadersSkipWrites pins which flushes can reach a reader of a BAL-read map: only a
// lifecycle write, since BAL-listed paths are read from the access list.
func TestReadersSkipWrites(t *testing.T) {
	t.Parallel()

	a := accounts.InternAddress(common.HexToAddress("0xba3"))
	bal := types.BlockAccessList{{Address: a.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(1)}}}}
	fields := &WriteSet{}
	fields.SetBalance(a, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: a, Path: BalancePath, Version: Version{TxIndex: 0}}})
	lifecycle := &WriteSet{}
	lifecycle.SetSelfDestruct(a, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: a, Path: SelfDestructPath, Version: Version{TxIndex: 0}}, Val: true})

	require.True(t, NewBALReadVersionMap(bal).ReadersSkipWrites(a, fields))
	require.False(t, NewBALReadVersionMap(bal).ReadersSkipWrites(a, lifecycle))
	require.False(t, NewVersionMap(nil).ReadersSkipWrites(a, fields), "a map without a BAL serves reads from cells")
}

// TestBALReadFieldMissReadsPreBlockState pins that a field the BAL does not change reads the
// pre-block value, not the in-block address record: a CREATE2 onto a funded address writes a
// record with the fields the account had at creation.
func TestBALReadFieldMissReadsPreBlockState(t *testing.T) {
	c := accounts.InternAddress(common.HexToAddress("0xc2ea7e"))
	funded := accounts.NewAccount()
	funded.Balance = *uint256.NewInt(100_000)
	reader := &staticAccountReader{addr: c, acc: &funded}
	vm := NewBALReadVersionMap(types.BlockAccessList{{
		Address:      c.Value(),
		NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}},
		CodeChanges:  []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60, 0x01}}},
	}})
	record := accounts.NewAccount()
	vm.WriteAddress(c, Version{TxIndex: 0}, &record, true)

	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	b, err := ibs.GetBalance(c)
	require.NoError(t, err)
	require.Equal(t, uint64(100_000), b.Uint64())
	n, err := ibs.GetNonce(c)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n)
}
