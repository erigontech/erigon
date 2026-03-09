// Copyright 2025 The Erigon Authors
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
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestAsBlockAccessList_SystemAddressExcludedWithoutChanges verifies that the
// system address (0xff...fe) is excluded from the BAL when it has no actual
// state changes and only revertable accesses (e.g. incidental gas-calculation
// reads during system calls).
func TestAsBlockAccessList_SystemAddressExcludedWithoutChanges(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress
	userAddr := accounts.InternAddress(common.HexToAddress("0x1111"))

	io := NewVersionedIO(1) // 2 tx slots: system call at -1, user tx at 0

	// System call (txIndex = -1): record system address as a revertable access.
	// This simulates EIP-4788 beacon root call where system address is msg.sender.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx (txIndex = 0): record a normal address with a balance write.
	readSets := ReadSet{}
	readSets.Set(VersionedRead{
		Address: userAddr,
		Path:    BalancePath,
		Val:     uint64(100),
	})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{
			Address: userAddr,
			Path:    BalancePath,
			Version: Version{TxIndex: 0},
			Val:     *uint256.NewInt(200),
		},
	})

	bal := io.AsBlockAccessList()

	// System address should be excluded (no state changes, only revertable access).
	for _, ac := range bal {
		require.NotEqual(t, sysAddr, ac.Address,
			"system address should be excluded from BAL when it has no state changes and only revertable accesses")
	}
	// User address should be present.
	found := false
	for _, ac := range bal {
		if ac.Address == userAddr {
			found = true
			break
		}
	}
	require.True(t, found, "user address should be present in BAL")
}

// TestAsBlockAccessList_SystemAddressIncludedWithNonRevertableAccess verifies
// that the system address is included in the BAL when a user tx performs a
// non-revertable access to it (e.g. BALANCE opcode, direct call, SELFDESTRUCT
// beneficiary), even without actual state changes.
func TestAsBlockAccessList_SystemAddressIncludedWithNonRevertableAccess(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(1) // system call at -1, user tx at 0

	// System call (txIndex = -1): revertable access (as usual).
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx (txIndex = 0): non-revertable access (e.g. BALANCE opcode on system address).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: false},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == sysAddr {
			found = true
			break
		}
	}
	require.True(t, found,
		"system address should be included in BAL when a user tx has non-revertable access")
}

// TestAsBlockAccessList_SystemAddressIncludedWithStateChanges verifies that the
// system address is kept in the BAL when it has actual state changes (e.g. a
// value transfer to the system address), regardless of access type.
func TestAsBlockAccessList_SystemAddressIncludedWithStateChanges(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(1)

	// System call (txIndex = -1): revertable access only.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx (txIndex = 0): revertable access BUT with a balance change
	// (e.g. ETH transferred to system address).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{
			Address: sysAddr,
			Path:    BalancePath,
			Version: Version{TxIndex: 0},
			Val:     *uint256.NewInt(42),
		},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == sysAddr {
			found = true
			break
		}
	}
	require.True(t, found,
		"system address should be included in BAL when it has actual state changes")
}

// TestAsBlockAccessList_SystemAddressRevertableFromSystemCallOnly verifies that
// a revertable access from a system call (txIndex = -1) does NOT set the
// nonRevertableUserAccess flag, so the system address is still excluded.
func TestAsBlockAccessList_SystemAddressRevertableFromSystemCallOnly(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress
	otherAddr := accounts.InternAddress(common.HexToAddress("0x2222"))

	io := NewVersionedIO(1)

	// System call (txIndex = -1): non-revertable access. Even though it's
	// non-revertable, it's from a system call (txIndex < 0) so it should
	// NOT mark the system address for inclusion.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: false},
	})

	// User tx (txIndex = 0): touches a different address to ensure there's
	// at least one user tx in the block.
	readSets := ReadSet{}
	readSets.Set(VersionedRead{
		Address: otherAddr,
		Path:    BalancePath,
		Val:     uint64(50),
	})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{
			Address: otherAddr,
			Path:    BalancePath,
			Version: Version{TxIndex: 0},
			Val:     *uint256.NewInt(100),
		},
	})

	bal := io.AsBlockAccessList()

	for _, ac := range bal {
		require.NotEqual(t, sysAddr, ac.Address,
			"system address should be excluded: non-revertable access from system call (txIndex < 0) should not trigger inclusion")
	}
}

// TestAsBlockAccessList_NonRevertableOverridesRevertable verifies that if the
// system address first gets a revertable access from a user tx, then a
// non-revertable access from another user tx, the non-revertable wins and the
// address is included.
func TestAsBlockAccessList_NonRevertableOverridesRevertable(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(2) // 3 slots: system call at -1, user tx 0, user tx 1

	// System call: revertable.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx 0: revertable access (e.g. gas calc).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx 1: non-revertable access (e.g. BALANCE opcode).
	io.RecordAccesses(Version{TxIndex: 1}, AccessSet{
		sysAddr: &accessOptions{revertable: false},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == sysAddr {
			found = true
			break
		}
	}
	require.True(t, found,
		"system address should be included: non-revertable user access overrides earlier revertable access")
}
