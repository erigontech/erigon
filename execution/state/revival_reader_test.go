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

// destructThenRevive seeds a version map with a value at tx0, a destruct at tx1
// and a revival at tx2 — the shape where the latest SelfDestruct entry is the
// revival and so cannot answer "was this wiped".
func destructThenRevive(vm *VersionMap, addr accounts.Address) {
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 1}, true, true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2}, false, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 2}, uint64(2), true)
}

func TestVersionedStateReader_StorageWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(101)
	key := accounts.InternKey(common.HexToHash("0x01"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 0}, *uint256.NewInt(5), true)
	destructThenRevive(vm, addr)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	val, _, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "the destruct wiped the slot; the revival did not restore it")
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
	val, _, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint64(9), val.Uint64(), "a write above the destruct is the live value")
}

func TestVersionedStateReader_CodeWipedByInRangeDestruct(t *testing.T) {
	t.Parallel()
	addr := getAddress(103)

	vm := NewVersionMap(nil)
	vm.WriteCode(addr, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}), true)
	destructThenRevive(vm, addr)

	vr := NewVersionedStateReader(3, ReadSet{}, vm, nil)
	code, err := vr.ReadAccountCode(addr)
	require.NoError(t, err)
	require.Empty(t, code, "the revived account has no code until something writes it")

	size, err := vr.ReadAccountCodeSize(addr)
	require.NoError(t, err)
	require.Zero(t, size, "code size must agree with code")
}

// The Done-branch guard for CodePath is latest-only, so a revival hides the
// destruct and the pre-destruct code cell wins. CodeSizePath has no guard there
// at all.
func TestGetCodeAfterDestructThenRevivalIsEmpty(t *testing.T) {
	t.Parallel()
	addr := getAddress(104)

	vm := NewVersionMap(nil)
	vm.WriteCode(addr, Version{TxIndex: 0}, accounts.NewCode([]byte{0x60, 0x00}), true)
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
