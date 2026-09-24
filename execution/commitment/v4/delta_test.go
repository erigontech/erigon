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

func TestDeltaPartsDropUnchangedRecords(t *testing.T) {
	var parts deltaParts
	parts.add(recordDelta{Key: []byte{1}, Data: []byte{2}, Prev: []byte{2}})
	parts.add(recordDelta{Key: []byte{3}, Data: []byte{4}, Prev: []byte{5}})
	parts.add(recordDelta{Key: []byte{6}, Data: nil, Prev: nil})
	require.Equal(t, deltaParts{{{Key: []byte{3}, Data: []byte{4}, Prev: []byte{5}}}}, parts)
}

func TestApplyDeltasReturnsPutError(t *testing.T) {
	wantErr := errors.New("put failed")
	err := applyDeltas(deltaParts{{{Key: []byte{1}, Data: []byte{2}, Prev: []byte{3}}}}, func(_, _, _ []byte) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestMaterializeFoldsAndEncodesRecordInOneWalk(t *testing.T) {
	ctx := newMockContext()
	n := fork(nil)
	n.plane = planeStorage
	path := append([]byte{3}, bytes.Repeat([]byte{4}, 63)...)
	n.setLeaf(int(path[0]), packPath(path[1:], nil), []byte{9})

	var acc deltaParts
	hash, err := storageGraph(make([]byte, 32)).materialize(ctx, n, n, &acc)
	require.NoError(t, err)
	require.Len(t, hash, 32)
	require.Len(t, acc, 1)
	require.Len(t, acc[0], 1)
	delta := acc[0][0]
	require.Equal(t, StorageNodeKey([32]byte{}, nil, nil), delta.Key)
	require.NotEmpty(t, delta.Data)
	require.NoError(t, Validate(delta.Data, 0))
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
			parts, err := g.persistGraph(ctx, root, foldPlan{})
			require.NoError(t, err)
			require.NoError(t, applyDeltas(parts, ctx.PutBranch))
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
	root.setStoredChild(0x0c, bytes.Repeat([]byte{0x11}, 32), []byte{0x06})
	root.setStoredChild(0x02, bytes.Repeat([]byte{0x22}, 32), nil)

	diverging := append([]byte{0x0c, 0x07}, bytes.Repeat([]byte{0x05}, 62)...)
	require.NoError(t, insert(root, diverging, []byte{0x01}))
	parts, err := g.persistGraph(ctx, root, foldPlan{})
	require.NoError(t, err)
	require.NoError(t, applyDeltas(parts, ctx.PutBranch))

	require.Equal(t, deepData, ctx.branches[string(deepKey)], "record that only moved from depth 1 to depth 2 must not be tombstoned")
	require.Equal(t, []byte{0xca, 0xfe}, ctx.branches[string(siblingKey)])
}
