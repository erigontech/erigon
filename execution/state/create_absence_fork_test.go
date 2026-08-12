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

// TestCreateOverAbsenceConsumedBeforeDestructFlush requires a direct dependency
// when a destruct published mid-attempt changes a consumed absence into a live
// EIP-8246 account. The collision-check variant also proves that a previously
// memoized destruct probe does not hide the conflict.
func TestCreateOverAbsenceConsumedBeforeDestructFlush(t *testing.T) {
	t.Parallel()
	for _, collisionCheck := range []bool{false, true} {
		name := "createOnly"
		if collisionCheck {
			name = "collisionCheckFirst"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ibs, vm, addr := newAbsenceForkState(t)
			empty, err := ibs.Empty(addr)
			require.NoError(t, err)
			require.True(t, empty)

			// Publish only after Empty has populated the attempt's destruct memo.
			destructVersion := Version{TxIndex: 0, Incarnation: 3}
			writeAbsenceForkDestruct(vm, addr, destructVersion, 1_000_000, 1)

			if collisionCheck {
				codeHash, err := ibs.GetCodeHash(addr)
				require.NoError(t, err)
				require.Equal(t, accounts.EmptyCodeHash, codeHash)
				nonce, err := ibs.GetNonce(addr)
				require.NoError(t, err)
				require.Zero(t, nonce)
			}

			require.PanicsWithValue(t, ErrDependency, func() {
				_ = ibs.CreateAccount(addr, true)
			})
			require.Equal(t, destructVersion.TxIndex, ibs.DepTxIndex())

			reads := ibs.VersionedReads()
			addressRead, ok := reads.GetAddress(addr)
			require.True(t, ok)
			require.True(t, addressRead.Val == nil || addressRead.Val.Account() == nil)

			destructRead, ok := reads.GetSelfDestruct(addr)
			require.True(t, ok)
			require.Equal(t, MapRead, destructRead.Source)
			require.Equal(t, destructVersion, destructRead.Version)
			require.True(t, destructRead.Val)
		})
	}
}

// TestCreateOverAbsenceConsumedBeforeEmptyDestructFlush verifies that no
// dependency is raised when reconstruction agrees with the consumed absence.
func TestCreateOverAbsenceConsumedBeforeEmptyDestructFlush(t *testing.T) {
	t.Parallel()
	ibs, vm, addr := newAbsenceForkState(t)
	empty, err := ibs.Empty(addr)
	require.NoError(t, err)
	require.True(t, empty)

	writeAbsenceForkDestruct(vm, addr, Version{TxIndex: 0, Incarnation: 3}, 0, 0)

	require.NoError(t, ibs.CreateAccount(addr, true))
	_, created := ibs.VersionedWrites().GetAddress(addr)
	require.True(t, created)

	io := NewVersionedIO(2)
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(1, io, validateEqualVersion, true, false, false, ""))
}

func newAbsenceForkState(t *testing.T) (*IntraBlockState, *VersionMap, accounts.Address) {
	t.Helper()
	_, tx, domains := NewTestRwTx(t)
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), vm)
	t.Cleanup(ibs.Close)
	ibs.SetTxContext(0, 1)
	ibs.SetNoMaterialize(true)
	ibs.SetVersion(0)
	ibs.eip8246 = true
	ibs.eip161 = true
	return ibs, vm, getAddress(8246)
}

func writeAbsenceForkDestruct(vm *VersionMap, addr accounts.Address, version Version, balance, nonce uint64) {
	writeFor(vm, addr, BalancePath, accounts.NilKey, version, *uint256.NewInt(balance), true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, version, nonce, true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, version, uint64(0), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, version, true, true)
	writeFor(vm, addr, CreateContractPath, accounts.NilKey, version, true, true)
}
