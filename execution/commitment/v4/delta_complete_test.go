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

	"github.com/erigontech/erigon/execution/commitment"
)

type deltaContext struct {
	*mockContext
	deltas []recordDelta
}

func newDeltaContext() *deltaContext {
	return &deltaContext{mockContext: newMockContext()}
}

func (m *deltaContext) PutBranch(key, data, prev []byte) error {
	m.deltas = append(m.deltas, recordDelta{key: bytes.Clone(key), data: bytes.Clone(data), prev: bytes.Clone(prev)})
	return m.mockContext.PutBranch(key, data, prev)
}

func (m *deltaContext) snapshot() map[string][]byte {
	result := make(map[string][]byte, len(m.branches))
	for key, data := range m.branches {
		result[key] = bytes.Clone(data)
	}
	return result
}

func requireTombstone(t *testing.T, deltas []recordDelta, key []byte) {
	t.Helper()
	for _, delta := range deltas {
		if bytes.Equal(delta.key, key) {
			require.Empty(t, delta.data)
			require.NotEmpty(t, delta.prev)
			return
		}
	}
	t.Fatalf("missing tombstone for %x", key)
}

func TestCompleteDeltasCarryEveryChangedRecordPreviousValue(t *testing.T) {
	ctx := newDeltaContext()
	address := [32]byte{1}
	pathA := append([]byte{2}, bytes.Repeat([]byte{3}, 63)...)
	pathB := append([]byte{9}, bytes.Repeat([]byte{4}, 63)...)
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathA, update: phaseAStorageUpdate([]byte{1})},
		{path: pathB, update: phaseAStorageUpdate([]byte{2})},
	}})
	require.NoError(t, err)
	previous := ctx.snapshot()
	ctx.deltas = nil

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{
		path:   pathA,
		update: phaseAStorageUpdate([]byte{3}),
	}}})
	require.NoError(t, err)
	require.NotEmpty(t, ctx.deltas)
	for _, delta := range ctx.deltas {
		require.Equal(t, previous[string(delta.key)], delta.prev, "previous value for %x", delta.key)
		require.NotEqual(t, delta.prev, delta.data, "unchanged record emitted for %x", delta.key)
	}
}

func TestCompleteDeltasTombstoneCollapseSurvivor(t *testing.T) {
	var address [32]byte
	address[0] = 0x61
	child := fork([]byte{2})
	child.plane = planeStorage
	pathA := append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...)
	pathB := append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)
	child.setLeaf(3, packPath(pathA[2:], nil), []byte{1})
	child.setLeaf(5, packPath(pathB[2:], nil), []byte{2})
	childHash, err := fold(child, 1)
	require.NoError(t, err)
	rootPath := append([]byte{9}, bytes.Repeat([]byte{8}, 63)...)
	root := fork(nil)
	root.plane = planeStorage
	root.setStoredChild(2, childHash[:], nil)
	root.setLeaf(9, packPath(rootPath[1:], nil), []byte{3})
	ctx := newDeltaContext()
	ctx.branches[string(StorageRootKey(address))] = encodeRecord(root, 0, nil)
	ctx.branches[string(StorageNodeKey(address, []byte{2}, nil))] = encodeRecord(child, 1, nil)
	childBefore := bytes.Clone(ctx.branches[string(StorageNodeKey(address, []byte{2}, nil))])

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathB, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
		{path: rootPath, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
	}})
	require.NoError(t, err)
	requireTombstone(t, ctx.deltas, StorageNodeKey(address, []byte{2}, nil))
	for _, delta := range ctx.deltas {
		if bytes.Equal(delta.key, StorageNodeKey(address, []byte{2}, nil)) {
			require.Equal(t, childBefore, delta.prev)
		}
	}
}

func TestCompleteDeltasTombstoneWipedStorageSubtree(t *testing.T) {
	var address [32]byte
	address[0] = 0x71
	pathA := append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...)
	pathB := append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)
	ctx := newDeltaContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathA, update: phaseAStorageUpdate([]byte{1})},
		{path: pathB, update: phaseAStorageUpdate([]byte{2})},
	}})
	require.NoError(t, err)
	previous := ctx.snapshot()
	ctx.deltas = nil
	_, err = runStorageTask(ctx, storageTask{addrHash: address, wipe: true})
	require.NoError(t, err)
	for key, data := range previous {
		if len(data) != 0 {
			requireTombstone(t, ctx.deltas, []byte(key))
		}
	}
	require.Len(t, ctx.deltas, len(previous))
}

func TestCompleteDeltasTombstoneDeletedAccountSubtree(t *testing.T) {
	pathA := accountDeltaPath(1, 2)
	pathB := accountDeltaPath(1, 3)
	pathC := accountDeltaPath(9, 4)
	ctx := newDeltaContext()
	_, err := runAccountTrie(ctx, []accountEntry{
		{hashedKey: pathA, update: accountDeltaUpdate(1)},
		{hashedKey: pathB, update: accountDeltaUpdate(2)},
		{hashedKey: pathC, update: accountDeltaUpdate(3)},
	}, nil)
	require.NoError(t, err)
	childKey := AccountNodeKey([]byte{1}, nil)
	childBefore := bytes.Clone(ctx.branches[string(childKey)])
	require.NotEmpty(t, childBefore)
	ctx.deltas = nil

	_, err = runAccountTrie(ctx, []accountEntry{
		{hashedKey: pathA, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
		{hashedKey: pathB, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
	}, nil)
	require.NoError(t, err)
	requireTombstone(t, ctx.deltas, childKey)
	for _, delta := range ctx.deltas {
		if bytes.Equal(delta.key, childKey) {
			require.Equal(t, childBefore, delta.prev)
		}
	}
}

func TestCompleteDeltasReplayAndReloadRecovery(t *testing.T) {
	var address [32]byte
	address[0] = 0x81
	pathA := append([]byte{1}, bytes.Repeat([]byte{2}, 63)...)
	pathB := append([]byte{8}, bytes.Repeat([]byte{3}, 63)...)
	pathC := append([]byte{13}, bytes.Repeat([]byte{4}, 63)...)
	ctx := newDeltaContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathA, update: phaseAStorageUpdate([]byte{1})},
		{path: pathB, update: phaseAStorageUpdate([]byte{2})},
	}})
	require.NoError(t, err)
	firstDeltas := append([]recordDelta(nil), ctx.deltas...)
	ctx.deltas = nil
	root, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathA, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
		{path: pathC, update: phaseAStorageUpdate([]byte{3})},
	}})
	require.NoError(t, err)
	secondDeltas := append([]recordDelta(nil), ctx.deltas...)
	final := ctx.snapshot()
	replayed := make(map[string][]byte)
	for _, delta := range append(firstDeltas, secondDeltas...) {
		replayed[string(delta.key)] = bytes.Clone(delta.data)
	}
	require.Equal(t, final, replayed)

	reloaded := newDeltaContext()
	for key, data := range final {
		reloaded.branches[key] = bytes.Clone(data)
	}
	n, err := unfold(reloaded, nil, planeStorage, address[:], &unfoldScratch{})
	require.NoError(t, err)
	require.NotNil(t, n)
	got, err := fold(n, 0)
	require.NoError(t, err)
	require.Equal(t, root, got)
}

func accountDeltaPath(first, second byte) []byte {
	path := make([]byte, 64)
	path[0] = first
	path[1] = second
	return path
}

func accountDeltaUpdate(nonce uint64) *commitment.Update {
	return &commitment.Update{Flags: commitment.NonceUpdate, Nonce: nonce}
}

var _ commitment.PatriciaContext = (*deltaContext)(nil)
