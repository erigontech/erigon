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

func TestFillFromLowerCell_StorageFoldKeepsAccountNavPath(t *testing.T) {
	t.Parallel()

	accountCell := &cell{accountAddrLen: length.Addr, hashLen: 32}
	navPath := []byte{0x3, 0xc, 0x1, 0x9, 0xe}
	copy(accountCell.hashedExtension[:], navPath)
	accountCell.hashedExtLen = int16(len(navPath))

	storageBranch := &cell{hashLen: 32}

	accountCell.fillFromLowerCell(storageBranch, 65, nil, 0x7)

	require.Equalf(t, navPath, accountCell.hashedExtension[:accountCell.hashedExtLen],
		"account cell must keep its account navigation path across a storage propagate fold; "+
			"got hashedExtLen=%d", accountCell.hashedExtLen)
	require.EqualValues(t, length.Addr, accountCell.accountAddrLen, "fold must not drop the account plain key")
	require.EqualValues(t, 1, accountCell.extLen, "storage extension still travels up in extension space")
}

func TestFillFromLowerCell_AccountBranchSyncsNavPath(t *testing.T) {
	t.Parallel()

	branchCell := &cell{hashLen: 32}
	lowBranch := &cell{hashLen: 32, extLen: 2}
	lowBranch.extension[0] = 0xa
	lowBranch.extension[1] = 0xb

	branchCell.fillFromLowerCell(lowBranch, 3, []byte{0x1}, 0x2)

	want := []byte{0x1, 0x2, 0xa, 0xb}
	require.Equal(t, want, branchCell.extension[:branchCell.extLen])
	require.Equalf(t, want, branchCell.hashedExtension[:branchCell.hashedExtLen],
		"a branch cell navigates by its extension, so hashedExtension must stay in sync; got hashedExtLen=%d",
		branchCell.hashedExtLen)
}

func TestFillFromLowerCell_StorageBranchSyncsNavPath(t *testing.T) {
	t.Parallel()

	branchCell := &cell{hashLen: 32}
	lowBranch := &cell{hashLen: 32, extLen: 1}
	lowBranch.extension[0] = 0xd

	branchCell.fillFromLowerCell(lowBranch, 70, nil, 0x5)

	require.Equal(t, []byte{0x5, 0xd}, branchCell.hashedExtension[:branchCell.hashedExtLen],
		"a keyless cell deep in storage still navigates by its extension")
}
