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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveClearsLeafAndMasks(t *testing.T) {
	n := fork([]byte{2})
	path := appendPath(n.path, 3, bytes.Repeat([]byte{4}, 62))
	n.setLeaf(3, packPath(path[2:], nil), []byte{1})
	other := appendPath(n.path, 5, bytes.Repeat([]byte{6}, 62))
	n.setLeaf(5, packPath(other[2:], nil), []byte{2})

	require.NoError(t, removeErr(n, path))
	require.Zero(t, n.childMask&(1<<3))
	require.Zero(t, n.leafMask&(1<<3))
	require.NotZero(t, n.childMask&(1<<5))
	require.NotZero(t, n.leafMask&(1<<5))

	require.ErrorIs(t, removeErr(n, path), ErrRemoveNotFound)
	require.NoError(t, removeErr(n, other))
	require.Zero(t, n.childMask)
	require.Zero(t, n.leafMask)
}

func TestRemovePromotesSoleLeafSurvivor(t *testing.T) {
	parent := fork([]byte{4})
	child := fork([]byte{4, 0, 1, 2})
	removed := appendPath(child.path, 3, bytes.Repeat([]byte{5}, 59))
	survivor := appendPath(child.path, 6, bytes.Repeat([]byte{7}, 59))
	child.setLeaf(3, packPath(removed[len(child.path)+1:], nil), []byte{1})
	child.setLeaf(6, packPath(survivor[len(child.path)+1:], nil), []byte{2})
	parent.setChild(0, child)

	require.NoError(t, removeErr(parent, removed))
	require.NotZero(t, parent.leafMask&(1<<0))
	require.Nil(t, parent.child(0))
	require.Equal(t, packPath(survivor[len(parent.path)+1:], nil), parent.leafSuffixAt(0))
	require.Equal(t, []byte{2}, parent.leafValueAt(0))
	full := append(append([]byte(nil), parent.path...), byte(0))
	full = append(full, unpackPath(parent.leafSuffixAt(0), 62, nil)...)
	require.Equal(t, survivor, full)
}

func TestRemoveD6CollapseKeepsPreExtensionHash(t *testing.T) {
	parent := fork([]byte{7})
	hash := bytes.Repeat([]byte{0xa5}, 32)
	parent.setStoredChild(0, hash, []byte{1, 2})
	removed := appendPath(parent.path, 1, bytes.Repeat([]byte{3}, 62))
	parent.setLeaf(1, packPath(removed[len(parent.path)+1:], nil), []byte{9})

	require.NoError(t, removeErr(parent, removed))
	require.Equal(t, uint16(1), parent.childMask)
	require.Zero(t, parent.leafMask)
	require.Equal(t, hash, parent.childHashAt(0))
	require.Equal(t, []byte{1, 2}, parent.childExtAt(0))
	path := append(append([]byte(nil), parent.path...), byte(0))
	path = append(path, parent.childExtAt(0)...)
	require.Equal(t, []byte{7, 0, 1, 2}, path)
}

func TestRemoveCollapseSoleBranchWithoutExtension(t *testing.T) {
	parent := fork([]byte{7})
	hash := bytes.Repeat([]byte{0x31}, 32)
	parent.setStoredChild(0, hash, nil)
	removed := appendPath(parent.path, 1, bytes.Repeat([]byte{3}, 62))
	parent.setLeaf(1, packPath(removed[len(parent.path)+1:], nil), []byte{9})

	require.NoError(t, removeErr(parent, removed))
	require.Equal(t, hash, parent.childHashAt(0))
	require.Empty(t, parent.childExtAt(0))
	require.Zero(t, parent.leafMask)
}

func TestRemoveConcatenatesNestedExtensions(t *testing.T) {
	parent := fork([]byte{7})
	child := fork([]byte{7, 0, 1, 2})
	hash := bytes.Repeat([]byte{0x4c}, 32)
	child.setStoredChild(3, hash, []byte{4, 5})
	removed := appendPath(child.path, 6, bytes.Repeat([]byte{8}, 59))
	child.setLeaf(6, packPath(removed[len(child.path)+1:], nil), []byte{9})
	parent.setChild(0, child)

	require.NoError(t, removeErr(parent, removed))
	require.Nil(t, parent.child(0))
	require.Equal(t, hash, parent.childHashAt(0))
	require.Equal(t, []byte{1, 2, 3, 4, 5}, parent.childExtAt(0))
	path := append(append([]byte(nil), parent.path...), byte(0))
	path = append(path, parent.childExtAt(0)...)
	require.Equal(t, []byte{7, 0, 1, 2, 3, 4, 5}, path)
}

func TestRemoveDoesNotReadBranchRecords(t *testing.T) {
	ctx := newMockContext()
	n := fork(nil)
	removed := appendPath(nil, 1, bytes.Repeat([]byte{2}, 63))
	survivor := appendPath(nil, 3, bytes.Repeat([]byte{4}, 63))
	n.setLeaf(1, packPath(removed[1:], nil), []byte{1})
	n.setLeaf(3, packPath(survivor[1:], nil), []byte{2})

	require.NoError(t, removeErr(n, removed))
	require.Empty(t, ctx.branchCalls)
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)
}

func TestRemoveTreatsDivergedPathsAsNotFound(t *testing.T) {
	ext := fork([]byte{1, 2, 3})
	kept := appendPath(ext.path, 4, bytes.Repeat([]byte{5}, 60))
	ext.setLeaf(4, packPath(kept[4:], nil), []byte{1})
	diverged := appendPath([]byte{1, 2, 9}, 4, bytes.Repeat([]byte{5}, 60))
	require.ErrorIs(t, removeErr(ext, diverged), ErrRemoveNotFound)

	root := fork(nil)
	branch := fork([]byte{7, 8, 8})
	branchLeaf := appendPath(branch.path, 1, bytes.Repeat([]byte{2}, 60))
	branch.setLeaf(1, packPath(branchLeaf[4:], nil), []byte{3})
	root.setChild(7, branch)
	pastBranch := appendPath([]byte{7, 8, 9}, 1, bytes.Repeat([]byte{2}, 60))
	require.ErrorIs(t, removeErr(root, pastBranch), ErrRemoveNotFound)
}

func TestRemoveBelowDivergedStoredChildIsNotFound(t *testing.T) {
	n := fork(nil)
	n.setStoredChild(1, bytes.Repeat([]byte{3}, 32), []byte{4, 5})

	diverged := appendPath(nil, 1, append([]byte{9, 9}, bytes.Repeat([]byte{2}, 61)...))
	require.ErrorIs(t, removeErr(n, diverged), ErrRemoveNotFound)

	under := appendPath(nil, 1, append([]byte{4, 5}, bytes.Repeat([]byte{2}, 61)...))
	require.ErrorIs(t, removeErr(n, under), ErrRemoveStoredChild)
}

func TestRemoveRejectsInvalidAndStoredPaths(t *testing.T) {
	n := fork(nil)
	path := appendPath(nil, 1, bytes.Repeat([]byte{2}, 63))
	n.setStoredChild(1, bytes.Repeat([]byte{3}, 32), []byte{2})

	require.ErrorIs(t, removeErr(n, []byte{1}), ErrRemovePath)
	require.ErrorIs(t, removeErr(n, path), ErrRemoveStoredChild)
}

func removeErr(n *node, path []byte) error {
	_, err := remove(n, path)
	return err
}
