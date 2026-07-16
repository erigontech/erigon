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

package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// A storage row that propagates into an account cell must not overwrite that cell's
// hashedExtension: the account cell navigates by its keccak-derived account path, while
// the prepended extension is in storage nibble space. Writing the storage extension over
// it sends the next unfold to a prefix that owns no branch record ("empty branch data read
// during unfold" on the mount path).
//
// Only a cell carrying no plain key navigates by its extension, so only that cell may sync.
func TestFillFromLowerCell_StorageFoldKeepsAccountNavPath(t *testing.T) {
	t.Parallel()

	// Account cell holding a storage root, carrying the keccak-derived path it navigates by.
	accountCell := &cell{accountAddrLen: length.Addr, hashLen: 32}
	navPath := []byte{0x3, 0xc, 0x1, 0x9, 0xe}
	copy(accountCell.hashedExtension[:], navPath)
	accountCell.hashedExtLen = int16(len(navPath))

	// Storage branch one row below the account leaf (depth 65 > 64, no storage plain key).
	storageBranch := &cell{hashLen: 32}

	accountCell.fillFromLowerCell(storageBranch, 65, nil, 0x7)

	require.Equalf(t, navPath, accountCell.hashedExtension[:accountCell.hashedExtLen],
		"account cell must keep its account navigation path across a storage propagate fold; "+
			"got hashedExtLen=%d", accountCell.hashedExtLen)
	require.EqualValues(t, length.Addr, accountCell.accountAddrLen, "fold must not drop the account plain key")
	require.EqualValues(t, 1, accountCell.extLen, "storage extension still travels up in extension space")
}

// The mirror case: an account-space branch cell (no plain key) navigates by its extension, so a
// propagate fold must keep hashedExtension in sync or a restored extension-topped root demands a
// root branch record that a propagate fold never writes.
func TestFillFromLowerCell_AccountBranchSyncsNavPath(t *testing.T) {
	t.Parallel()

	branchCell := &cell{hashLen: 32}
	lowBranch := &cell{hashLen: 32, extLen: 2}
	lowBranch.extension[0] = 0xa
	lowBranch.extension[1] = 0xb

	branchCell.fillFromLowerCell(lowBranch, 3, []byte{0x1}, 0x2)

	// preExtension | nibble | lowCell.extension
	want := []byte{0x1, 0x2, 0xa, 0xb}
	require.Equal(t, want, branchCell.extension[:branchCell.extLen])
	require.Equalf(t, want, branchCell.hashedExtension[:branchCell.hashedExtLen],
		"a branch cell navigates by its extension, so hashedExtension must stay in sync; got hashedExtLen=%d",
		branchCell.hashedExtLen)
}

// A storage-internal branch carries no plain key, so it navigates by its extension and
// must sync like an account-space branch: the skip is keyed on the plain key, not on depth.
func TestFillFromLowerCell_StorageBranchSyncsNavPath(t *testing.T) {
	t.Parallel()

	branchCell := &cell{hashLen: 32}
	lowBranch := &cell{hashLen: 32, extLen: 1}
	lowBranch.extension[0] = 0xd

	branchCell.fillFromLowerCell(lowBranch, 70, nil, 0x5)

	require.Equal(t, []byte{0x5, 0xd}, branchCell.hashedExtension[:branchCell.hashedExtLen],
		"a keyless cell deep in storage still navigates by its extension")
}
