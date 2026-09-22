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

package v4

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertLeafAtEveryDepthInBothPlanes(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		t.Run(fmtPlane(plane), func(t *testing.T) {
			for depth := 0; depth <= 62; depth++ {
				prefix := repeatedPath(depth, 3)
				existing := appendPath(prefix, 4, bytes.Repeat([]byte{5}, 63-depth))
				incoming := appendPath(prefix, 4, bytes.Repeat([]byte{6}, 63-depth))
				n := fork(prefix)
				n.setLeaf(4, packPath(existing[depth+1:], nil), []byte{0x11})

				err := insert(n, incoming, []byte{0x22})
				require.NoError(t, err, "depth %d", depth)
				bit := uint16(1) << 4
				require.NotZero(t, n.childMask&bit, "depth %d", depth)
				require.Zero(t, n.leafMask&bit, "depth %d", depth)
				branch := n.children[4]
				require.NotNil(t, branch, "depth %d", depth)
				require.Equal(t, prefix, branch.path[:depth])
				require.Equal(t, byte(4), branch.path[depth])
				require.Equal(t, uint16(1<<5|1<<6), branch.childMask, "depth %d", depth)
				require.Equal(t, uint16(1<<5|1<<6), branch.leafMask, "depth %d", depth)
			}
		})
	}
}

func TestInsertCreatesExpectedExtensionLengths(t *testing.T) {
	for _, extensionLen := range []int{0, 1, 4} {
		t.Run(fmt.Sprintf("extension-%d", extensionLen), func(t *testing.T) {
			prefix := repeatedPath(7, 2)
			shared := repeatedPath(extensionLen, 8)
			oldTail := append(append([]byte(nil), shared...), bytes.Repeat([]byte{1}, 63-7-extensionLen)...)
			newTail := append(append([]byte(nil), shared...), byte(9))
			newTail = append(newTail, bytes.Repeat([]byte{3}, 63-7-extensionLen-1)...)
			oldPath := appendPath(prefix, 6, oldTail)
			newPath := appendPath(prefix, 6, newTail)
			n := fork(prefix)
			n.setLeaf(6, packPath(oldPath[8:], nil), []byte{1})

			require.NoError(t, insert(n, newPath, []byte{2}))
			branch := n.children[6]
			require.NotNil(t, branch)
			require.Equal(t, append(append([]byte(nil), prefix...), byte(6)), branch.path[:8])
			require.Len(t, branch.path, 8+extensionLen)
			expectedPath := append(append(append([]byte(nil), prefix...), byte(6)), shared...)
			require.Equal(t, expectedPath, branch.path)
		})
	}
}

func TestInsertRecomputesPushedLeafSuffixFromFullPath(t *testing.T) {
	prefix := repeatedPath(5, 1)
	oldPath := appendPath(prefix, 2, bytes.Repeat([]byte{3}, 58))
	newPath := appendPath(prefix, 2, append([]byte{3, 4}, bytes.Repeat([]byte{5}, 56)...))
	n := fork(prefix)
	n.setLeaf(2, packPath(oldPath[6:], nil), []byte{0xaa})

	require.NoError(t, insert(n, newPath, []byte{0xbb}))
	branch := n.children[2]
	require.NotNil(t, branch)
	require.Equal(t, packPath(oldPath[len(branch.path)+1:], nil), branch.leafSuffix[int(oldPath[len(branch.path)])])
	require.Equal(t, packPath(newPath[len(branch.path)+1:], nil), branch.leafSuffix[int(newPath[len(branch.path)])])
	require.Equal(t, []byte{0xaa}, branch.leafValue[int(oldPath[len(branch.path)])])
	require.Equal(t, []byte{0xbb}, branch.leafValue[int(newPath[len(branch.path)])])
}

func TestInsertUsesNoStateReads(t *testing.T) {
	ctx := newMockContext()
	n := fork(nil)
	oldPath := repeatedPath(64, 1)
	newPath := append(append([]byte(nil), oldPath[:2]...), bytes.Repeat([]byte{2}, 62)...)
	n.setLeaf(1, packPath(oldPath[1:], nil), []byte{1})
	require.NoError(t, insert(n, newPath, []byte{2}))
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)
	require.Empty(t, ctx.branchCalls)
}

func TestInsertRejectsStoredDescendant(t *testing.T) {
	n := fork(nil)
	n.setStoredChild(1, bytes.Repeat([]byte{0xab}, 32), []byte{2, 3})
	storedPath := append([]byte{1, 2, 3}, bytes.Repeat([]byte{4}, 61)...)
	require.ErrorIs(t, insert(n, storedPath, []byte{2}), ErrInsertStoredChild)
}

func repeatedPath(length int, nib byte) []byte {
	return bytes.Repeat([]byte{nib}, length)
}

func appendPath(prefix []byte, nib byte, suffix []byte) []byte {
	path := make([]byte, 0, len(prefix)+1+len(suffix))
	path = append(path, prefix...)
	path = append(path, nib)
	return append(path, suffix...)
}

func fmtPlane(plane byte) string {
	if plane == planeAccount {
		return "account"
	}
	return "storage"
}
