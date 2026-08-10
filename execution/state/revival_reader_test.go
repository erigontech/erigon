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
	"github.com/erigontech/erigon/execution/types/accounts"
)

// preBlockStateReader serves one slot and one code blob from before the block,
// so a test can assert the reader stops falling through to it.
type preBlockStateReader struct {
	*accountStateReader
	slot accounts.StorageKey
	val  uint256.Int
	code []byte
}

func (r *preBlockStateReader) ReadAccountStorage(_ accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if key == r.slot {
		return r.val, true, nil
	}
	return uint256.Int{}, false, nil
}

func (r *preBlockStateReader) ReadAccountCode(accounts.Address) ([]byte, error) { return r.code, nil }

func (r *preBlockStateReader) HasStorage(accounts.Address) (bool, error) { return true, nil }

// destructThenRevive seeds a version map with a value at tx0, a destruct at tx1
// and a revival at tx2 — the shape where the latest SelfDestruct entry is the
// revival and so cannot answer "was this wiped".
func destructThenRevive(vm *VersionMap, addr accounts.Address) {
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, false, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 2}, uint64(2), true)
}

// A value-transfer revival records Balance/Incarnation/SelfDestruct but no
// Nonce or CodeHash, so the account record still floors on the pre-destruct
// cells. The code hash has to be wiped — left alone it contradicts
// ReadAccountCode, which reports the account has none — but the nonce must not
// be, because that is the one the block goes on to commit.
func TestVersionedStateReader_AccountCodeHashWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(105)

	vm := NewVersionMap(nil)
	code := accounts.NewCode([]byte{0x60, 0x00})
	vm.WriteCode(addr, Version{TxIndex: 0}, code, true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 0}, code.Hash, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0}, uint64(7), true)
	destructThenRevive(vm, addr)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 2}, *uint256.NewInt(1000), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, acc.CodeHash, "code hash must agree with the code the reader reports")
	// Nonce is inherited, matching what the block commits — see
	// TestSelfDestructReceiveAccountRecord in execution/tests.
	require.Equal(t, uint64(7), acc.Nonce)
	require.Equal(t, uint64(1000), acc.Balance.Uint64(), "the revival's balance is the live value")
}

func TestVersionedStateReader_AccountFieldsSurviveWhenRevivalRewrote(t *testing.T) {
	t.Parallel()
	addr := getAddress(106)

	vm := NewVersionMap(nil)
	old := accounts.NewCode([]byte{0x60, 0x00})
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 0}, old.Hash, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0}, uint64(7), true)
	destructThenRevive(vm, addr)
	fresh := accounts.NewCode([]byte{0x60, 0x01})
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 2}, fresh.Hash, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 2}, uint64(1), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(1), acc.Nonce, "a write above the destruct is the live value")
	require.Equal(t, fresh.Hash, acc.CodeHash, "and so is the recreated contract's code hash")
}

func TestVersionedStateReader_StorageWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(101)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	destructThenRevive(vm, addr)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "the destruct wiped the slot; the revival did not restore it")
	require.False(t, found, "a wiped slot is absent, not a slot that happens to hold zero")
}

func TestVersionedStateReader_StorageSurvivesWhenRevivalRewrote(t *testing.T) {
	t.Parallel()
	addr := getAddress(102)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	destructThenRevive(vm, addr)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 2}, *uint256.NewInt(9), true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(9), val.Uint64(), "a write above the destruct is the live value")
	require.True(t, found, "and it is present, not absent")
}

func TestVersionedStateReader_CodeWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(103)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, CodePath, accounts.NilKey, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}), true)
	destructThenRevive(vm, addr)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	code, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "the revived account has no code until something writes it")

	size, err := vr.ReadAccountCodeSize(addr)
	require.NoError(t, err)
	require.Zero(t, size, "code size must agree with code")
}

// A destruct with no revival above it must not wipe a cell written after it.
// Storage cells arrive without a matching SelfDestruct=false — BAL
// pre-population writes value paths only — so the latest entry stays the
// destruct while the value above it is live.
func TestVersionedStateReader_StorageSurvivesDestructWithoutRevivalEntry(t *testing.T) {
	t.Parallel()
	addr := getAddress(107)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 4}, *uint256.NewInt(9), true)

	vr := NewVersionedStateReader(5, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(9), val.Uint64(), "the tx4 write sits above the destruct")
	require.True(t, found)
}

// With no cell of its own the reader would fall through to the pre-block state,
// which the destruct erased.
func TestVersionedStateReader_PreBlockValueWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(108)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	destructThenRevive(vm, addr)

	reader := &preBlockStateReader{
		accountStateReader: newAccountStateReader(addr),
		slot:               key,
		val:                *uint256.NewInt(5),
		code:               []byte{0x60, 0x00},
	}

	vr := NewVersionedStateReader(3, ReadSet{}, vm, reader)

	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "the pre-block slot must not survive the destruct")
	require.False(t, found)

	code, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "nor the pre-block code")

	has, err := vr.HasStorage(addr)
	require.NoError(t, err)
	require.False(t, has, "HasStorage must agree with ReadAccountStorage")
}

// An in-flight destruct still counts: MarkEstimate leaves the aborted
// incarnation's value behind, so a scan that only accepts Done cells would read
// the destruct as absent and serve the pre-destruct slot.
func TestVersionedStateReader_EstimateDestructStillWipes(t *testing.T) {
	t.Parallel()
	addr := getAddress(110)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	vm.MarkEstimate(addr, SelfDestructPath, accounts.NilKey, 1)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, found, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "a destruct being re-executed is not a destruct that did not happen")
	require.False(t, found)
}

// A same-tx destroy-and-recreate writes the new contract's code hash at the same
// TxIndex as the destruct, so that entry is already the post-destruct value and
// the scan must start above it — the convention read_paths.go applies to
// CodeHash and Balance but not to the other paths.
func TestVersionedStateReader_DestructOwnCodeHashSurvives(t *testing.T) {
	t.Parallel()
	addr := getAddress(109)
	recreated := accounts.NewCode([]byte{0x60, 0x01}).Hash

	vm := NewVersionMap(nil)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0}, uint64(7), true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}).Hash, true)

	// The recreate lands at the destruct's own TxIndex, AddressPath included —
	// that is what marks the account revived rather than gone.
	fresh := accounts.NewAccount()
	fresh.CodeHash = recreated
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, AddressPath, accounts.NilKey, Version{TxIndex: 1}, &fresh, true)
	writeFor(vm, addr, CodeHashPath, accounts.NilKey, Version{TxIndex: 1}, recreated, true)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, newAccountStateReader(addr))
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, recreated, acc.CodeHash, "the recreate's own code hash is not what the destruct erased")
}

// Same shape through IntraBlockState, which resolves code via versionedReadCore
// rather than these readers.
func TestGetCodeAfterDestructThenRevivalIsEmpty(t *testing.T) {
	t.Parallel()
	addr := getAddress(104)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, CodePath, accounts.NilKey, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}), true)
	destructThenRevive(vm, addr)

	reader := newAccountStateReader(addr)
	ibs := NewWithVersionMap(NewVersionedStateReader(3, ReadSet{}, vm, reader), vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 3)
	ibs.SetVersion(0)

	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "the destruct removed the code and the revival did not write any")

	size, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	require.Zero(t, size, "code size must agree with code")
}
