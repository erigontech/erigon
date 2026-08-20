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

// A cold storage read on the parallel path must seed the slot's committed value
// as its versionMap origin (originIndex), mirroring the account origin. Without
// it every reader (each tx, the apply, the commitment) re-resolves the slot base
// fresh from the execution store at a different point relative to the async fold
// — inconsistent across readers and recorded as a bare UnknownVersion read that
// validation checks by version only, never value. That is the non-deterministic
// wrong-root race. This test pins the seed: pre-fix the cold read leaves no
// origin cell and records UnknownVersion, so both assertions fail.
func TestStorageColdReadSeedsOrigin(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xA0, 0xb8})
	key := accounts.InternKey([32]byte{0x18, 0xea})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	committed := *uint256.NewInt(97458463514)

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: committed},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	v, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, committed, v)

	// The seed is visible to any later reader as an originIndex cell.
	got, res, ok := ibs.versionMap.ReadStorage(addr, key, 5)
	require.True(t, ok, "cold storage read must seed a versionMap origin cell")
	require.Equal(t, MVReadResultDone, res.Status())
	require.Equal(t, originIndex, res.DepIdx(), "slot origin must be seeded at originIndex (-2)")
	require.Equal(t, committed, got)

	// The read itself is recorded at originIndex (not UnknownVersion), so it is
	// value-validated against the shared origin rather than version-only.
	tr, recorded := ibs.versionedReads.GetStorage(addr, key)
	require.True(t, recorded)
	require.Equal(t, originIndex, tr.Version.TxIndex, "cold read must record the origin version")
}

// GetCommittedState (the EIP-2200/refund original-value path) must seed the slot
// origin too, so it agrees with the SLOAD path and never leaves a stray
// UnknownVersion read that would mismatch a later-seeded origin cell.
func TestStorageCommittedReadSeedsOrigin(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xA0, 0xb8})
	key := accounts.InternKey([32]byte{0x02})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	committed := *uint256.NewInt(7)

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: committed},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	v, err := ibs.GetCommittedState(addr, key)
	require.NoError(t, err)
	require.Equal(t, committed, v)

	got, res, ok := ibs.versionMap.ReadStorage(addr, key, 5)
	require.True(t, ok, "cold committed read must seed a versionMap origin cell")
	require.Equal(t, originIndex, res.DepIdx())
	require.Equal(t, committed, got)
}
