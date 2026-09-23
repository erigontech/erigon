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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyDeltasSkipsUnchangedRecords(t *testing.T) {
	var applied []recordDelta
	deltas := []recordDelta{
		{key: []byte{1}, data: []byte{2}, prev: []byte{2}},
		{key: []byte{3}, data: []byte{4}, prev: []byte{5}},
		{key: []byte{6}, data: nil, prev: nil},
	}
	err := applyDeltas(deltas, func(key, data, prev []byte) error {
		applied = append(applied, recordDelta{key: bytes.Clone(key), data: bytes.Clone(data), prev: bytes.Clone(prev)})
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []recordDelta{{key: []byte{3}, data: []byte{4}, prev: []byte{5}}}, applied)
}

func TestApplyDeltasReturnsPutError(t *testing.T) {
	wantErr := errors.New("put failed")
	err := applyDeltas([]recordDelta{{key: []byte{1}, data: []byte{2}, prev: []byte{3}}}, func(_, _, _ []byte) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestReadRecordDeltaCopiesBranchBuffer(t *testing.T) {
	ctx := newMockContext()
	key := []byte{0x40, 0}
	otherKey := []byte{0x40, 1, 1}
	ctx.branches[string(key)] = []byte{1, 2, 3}
	ctx.branches[string(otherKey)] = []byte{4, 5, 6}

	delta, err := readRecordDelta(ctx, key, []byte{7, 8})
	require.NoError(t, err)
	_, _, err = ctx.Branch(otherKey)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, delta.prev)
	require.Equal(t, key, delta.key)
	require.Equal(t, []byte{7, 8}, delta.data)
}

func TestRemovedRecordDeltaUsesNonNilTombstone(t *testing.T) {
	ctx := newMockContext()
	key := []byte{0x40, 0}
	ctx.branches[string(key)] = []byte{1, 2, 3}

	before := new(keySet)
	before.add(key)
	deltas, err := appendRemovedDeltas(ctx, nil, before, new(keySet))
	require.NoError(t, err)
	require.Len(t, deltas, 1)
	require.NotNil(t, deltas[0].data)
	require.Empty(t, deltas[0].data)
}

func TestFoldAndEncodeRecordKeepsFoldAndRecordInOneWalk(t *testing.T) {
	ctx := newMockContext()
	n := fork(nil)
	n.plane = planeStorage
	path := append([]byte{3}, bytes.Repeat([]byte{4}, 63)...)
	n.setLeaf(int(path[0]), packPath(path[1:], nil), []byte{9})

	hash, delta, err := foldAndEncodeRecord(ctx, n, 0, StorageRootKey([32]byte{}))
	require.NoError(t, err)
	require.Len(t, hash, 32)
	require.NotEmpty(t, delta.data)
	require.NoError(t, Validate(delta.data, 0))
	require.Empty(t, ctx.putCalls)
}

func TestPersistGraphRetainsOnlyFoldedDeltasAfterChildWalk(t *testing.T) {
	for _, count := range []int{1000, 100000} {
		t.Run(itoa(count), func(t *testing.T) {
			ctx := newMockContext()
			var addr [32]byte
			root := fork(nil)
			root.plane = planeStorage
			for i := range count {
				path := make([]byte, 64)
				path[0] = byte(i % 16)
				value := i
				for j := len(path) - 1; j >= 1 && value != 0; j-- {
					path[j] = byte(value & 0x0f)
					value >>= 4
				}
				if err := insert(root, path, []byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
			}
			g := storageGraph(addr[:])
			before := new(keySet)
			g.reachableRecordKeys(root, before)
			require.NoError(t, g.persistGraph(ctx, root, before))
			require.NotEmpty(t, ctx.branches)
			require.Equal(t, 1, linkedNodeCount(root))
			require.Empty(t, ctx.accountCalls)
			require.Empty(t, ctx.storageCalls)
		})
	}
}

func linkedNodeCount(n *node) int {
	if n == nil {
		return 0
	}
	count := 1
	for i := range n.slots {
		count += linkedNodeCount(n.slots[i].node)
	}
	return count
}

func TestPersistGraphKeepsRecordsThatOnlyMovedDeeper(t *testing.T) {
	ctx := newMockContext()
	var addr [32]byte
	addr[0] = 0x7e
	g := storageGraph(addr[:])

	deepPath := []byte{0x0c, 0x06}
	deepKey := StorageNodeKey(addr, deepPath, nil)
	deepData := []byte{0xde, 0xad, 0xbe, 0xef}
	ctx.branches[string(deepKey)] = deepData
	siblingKey := StorageNodeKey(addr, []byte{0x02}, nil)
	ctx.branches[string(siblingKey)] = []byte{0xca, 0xfe}

	root := fork(nil)
	root.plane = planeStorage
	root.storageRoot = true
	root.setStoredChild(0x0c, bytes.Repeat([]byte{0x11}, 32), []byte{0x06})
	root.setStoredChild(0x02, bytes.Repeat([]byte{0x22}, 32), nil)
	before := new(keySet)
	g.reachableRecordKeys(root, before)
	require.True(t, keySetHas(before, deepKey))

	diverging := append([]byte{0x0c, 0x07}, bytes.Repeat([]byte{0x05}, 62)...)
	require.NoError(t, insert(root, diverging, []byte{0x01}))
	require.NoError(t, g.persistGraph(ctx, root, before))

	require.Equal(t, deepData, ctx.branches[string(deepKey)], "record that only moved from depth 1 to depth 2 must not be tombstoned")
	require.Equal(t, []byte{0xca, 0xfe}, ctx.branches[string(siblingKey)])
}

func keySetHas(s *keySet, key []byte) bool {
	for i := range s.spans {
		if bytes.Equal(s.at(i), key) {
			return true
		}
	}
	return false
}
