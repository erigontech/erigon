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

func TestNodeMutationsKeepMasksConsistent(t *testing.T) {
	n := fork([]byte{1, 2})
	require.Equal(t, []byte{1, 2}, n.path)
	require.Zero(t, n.childMask)
	require.Zero(t, n.leafMask)

	n.setLeaf(3, []byte{4, 5}, []byte{6})
	assertNodeSlot(t, n, 3, true, true)
	require.Equal(t, []byte{4, 5}, n.leafSuffixAt(3))
	require.Equal(t, []byte{6}, n.leafValueAt(3))

	child := fork([]byte{1, 2, 3})
	n.setChild(3, child)
	assertNodeSlot(t, n, 3, true, false)
	require.Same(t, child, n.child(3))
	require.Nil(t, n.childHashAt(3))
	require.Nil(t, n.childExtAt(3))
	require.Nil(t, n.leafSuffixAt(3))
	require.Nil(t, n.leafValueAt(3))

	hash := bytes.Repeat([]byte{0xab}, 32)
	ext := []byte{7, 8}
	n.setStoredChild(3, hash, ext)
	assertNodeSlot(t, n, 3, true, false)
	require.Nil(t, n.child(3))
	require.Equal(t, hash, n.childHashAt(3))
	require.Equal(t, ext, n.childExtAt(3))

	n.setLeaf(3, []byte{9}, []byte{10, 11})
	assertNodeSlot(t, n, 3, true, true)
	require.Nil(t, n.child(3))
	require.Nil(t, n.childHashAt(3))
	require.Nil(t, n.childExtAt(3))
	require.Equal(t, []byte{9}, n.leafSuffixAt(3))
	require.Equal(t, []byte{10, 11}, n.leafValueAt(3))

	n.clear(3)
	assertNodeSlot(t, n, 3, false, false)
	require.Nil(t, n.child(3))
	require.Nil(t, n.childHashAt(3))
	require.Nil(t, n.childExtAt(3))
	require.Nil(t, n.leafSuffixAt(3))
	require.Nil(t, n.leafValueAt(3))
}

func TestNodeMutationsCoverEveryNibble(t *testing.T) {
	n := fork(nil)
	for nib := range 16 {
		n.setLeaf(nib, []byte{byte(nib)}, []byte{byte(nib + 1)})
	}
	require.Equal(t, uint16(0xffff), n.childMask)
	require.Equal(t, uint16(0xffff), n.leafMask)

	for nib := range 16 {
		n.setChild(nib, fork([]byte{byte(nib)}))
	}
	require.Equal(t, uint16(0xffff), n.childMask)
	require.Zero(t, n.leafMask)

	for nib := range 16 {
		n.clear(nib)
	}
	require.Zero(t, n.childMask)
	require.Zero(t, n.leafMask)
}

func TestNodeStoredChildAndJoinCopyReferences(t *testing.T) {
	n := fork(nil)
	hash := bytes.Repeat([]byte{1}, 32)
	ext := []byte{2, 3, 4}
	join(n, 5, hash)
	require.Equal(t, hash, n.childHashAt(5))
	require.Nil(t, n.childExtAt(5))

	n.setStoredChild(5, hash, ext)
	hash[0] = 9
	ext[0] = 8
	require.Equal(t, byte(1), n.childHashAt(5)[0])
	require.Equal(t, byte(2), n.childExtAt(5)[0])
}

func TestForkReturnsSharedNodePointer(t *testing.T) {
	parent := fork([]byte{1})
	forked := parent
	forked.setLeaf(2, []byte{3}, []byte{4})

	require.Same(t, parent, forked)
	require.Equal(t, uint16(1<<2), parent.childMask)
	require.Equal(t, uint16(1<<2), parent.leafMask)
}

func TestNodeRejectsInvalidReferences(t *testing.T) {
	n := fork(nil)
	require.Panics(t, func() { n.setLeaf(-1, nil, nil) })
	require.Panics(t, func() { n.setChild(16, nil) })
	require.Panics(t, func() { n.setStoredChild(0, nil, nil) })
	require.Panics(t, func() { n.setStoredChild(0, make([]byte, 31), nil) })
	require.Panics(t, func() { join(nil, 0, make([]byte, 32)) })
	require.Panics(t, func() { fork([]byte{16}) })
}

func assertNodeSlot(t *testing.T, n *node, nib int, child, leaf bool) {
	t.Helper()
	bit := uint16(1) << nib
	if child {
		require.NotZero(t, n.childMask&bit)
	} else {
		require.Zero(t, n.childMask&bit)
	}
	if leaf {
		require.NotZero(t, n.leafMask&bit)
	} else {
		require.Zero(t, n.leafMask&bit)
	}
	require.Zero(t, n.leafMask&^n.childMask)
}
