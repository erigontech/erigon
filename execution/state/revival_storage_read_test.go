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

// A storage slot written before an in-block SELFDESTRUCT, read after a later tx
// revived the account: the reader must return zero (the destruct wiped it) and
// the read it records must validate. The reader justifies the zero by scanning
// the range for the destruct, so it records that destruct's version rather than
// the latest SelfDestruct entry — and the validator has to judge it the same way,
// or the read can never be made valid and the tx exhausts its incarnations.
func TestStorageReadAcrossDestructThenRevivalValidates(t *testing.T) {
	t.Parallel()
	addr := getAddress(91)
	key := accounts.InternKey(common.HexToHash("0x33"))

	vm := NewVersionMap(nil)
	// Slot written, then destructed, then the account revived above the destruct.
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 16}, *uint256.NewInt(0xAA), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 41}, true, true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 42}, false, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 42}, uint64(3), true)

	reader := newAccountStateReader(addr)
	ibs := NewWithVersionMap(NewVersionedStateReader(43, ReadSet{}, vm, reader), vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 43)
	ibs.SetVersion(0)

	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.True(t, got.IsZero(), "the destruct wiped the slot, so the read is zero")

	io := NewVersionedIO(44)
	io.RecordReads(Version{TxIndex: 43}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(43, io, validateEqualVersion, false, ""),
		"the reader justified the zero by an in-range destruct; the validator must accept the same justification")
}

// The dep lives in its own map, so every read-set operation has to carry it like
// the per-path maps do — a merge that drops it silently retires the check.
func TestStorageReadRangeDepSurvivesMerge(t *testing.T) {
	t.Parallel()
	addr := getAddress(93)

	var src ReadSet
	src.SetSelfDestructInRange(addr, Version{TxIndex: 40, Incarnation: 1})
	require.Equal(t, 1, src.Len(), "the dep is an entry and must be counted")

	var dst ReadSet
	dst.MergeFrom(src)
	require.True(t, dst.hasAddr(addr))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 41}, true, true)

	io := NewVersionedIO(44)
	io.RecordReads(Version{TxIndex: 43}, dst)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(43, io, validateEqualVersion, false, ""),
		"the merged dep names a destruct at tx 40 that is not in the map")

	dst.Delete(addr)
	require.False(t, dst.hasAddr(addr))
}

// The guard the fix must not trade away: when the destruct the read depended on
// is gone from the map, the read is genuinely stale and must still invalidate.
func TestStorageReadInvalidatesWhenDependedDestructVanishes(t *testing.T) {
	t.Parallel()
	addr := getAddress(92)
	key := accounts.InternKey(common.HexToHash("0x34"))

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: 16}, *uint256.NewInt(0xAA), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 41}, true, true)

	io := NewVersionedIO(44)
	rs := ReadSet{}
	// A read that depended on a destruct at tx 40 — no such entry exists.
	rs.SetSelfDestructInRange(addr, Version{TxIndex: 40, Incarnation: 1})
	io.RecordReads(Version{TxIndex: 43}, rs)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(43, io, validateEqualVersion, false, ""),
		"a read pinned to a destruct that is not in the map must invalidate")
}
