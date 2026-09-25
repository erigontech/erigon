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

package v3

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
	m.deltas = append(m.deltas, recordDelta{Key: bytes.Clone(key), Data: bytes.Clone(data), Prev: bytes.Clone(prev)})
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
		if bytes.Equal(delta.Key, key) {
			require.Empty(t, delta.Data)
			require.NotEmpty(t, delta.Prev)
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
		entryOf(pathA, phaseAStorageUpdate([]byte{1})),
		entryOf(pathB, phaseAStorageUpdate([]byte{2})),
	}})
	require.NoError(t, err)
	previous := ctx.snapshot()
	ctx.deltas = nil

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{entryOf(pathA, phaseAStorageUpdate([]byte{3}))}})
	require.NoError(t, err)
	require.NotEmpty(t, ctx.deltas)
	for _, delta := range ctx.deltas {
		require.Equal(t, previous[string(delta.Key)], delta.Prev, "previous value for %x", delta.Key)
		require.NotEqual(t, delta.Prev, delta.Data, "unchanged record emitted for %x", delta.Key)
	}
}

func TestCompleteDeltasTombstoneWipedStorageSubtree(t *testing.T) {
	var address [32]byte
	address[0] = 0x71
	pathA := append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...)
	pathB := append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)
	ctx := newDeltaContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		entryOf(pathA, phaseAStorageUpdate([]byte{1})),
		entryOf(pathB, phaseAStorageUpdate([]byte{2})),
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

func TestCompleteDeltasReplayAndReloadRecovery(t *testing.T) {
	var address [32]byte
	address[0] = 0x81
	pathA := append([]byte{1}, bytes.Repeat([]byte{2}, 63)...)
	pathB := append([]byte{8}, bytes.Repeat([]byte{3}, 63)...)
	pathC := append([]byte{13}, bytes.Repeat([]byte{4}, 63)...)
	ctx := newDeltaContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		entryOf(pathA, phaseAStorageUpdate([]byte{1})),
		entryOf(pathB, phaseAStorageUpdate([]byte{2})),
	}})
	require.NoError(t, err)
	firstDeltas := append([]recordDelta(nil), ctx.deltas...)
	ctx.deltas = nil
	root, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		entryOf(pathA, &commitment.Update{Flags: commitment.DeleteUpdate}),
		entryOf(pathC, phaseAStorageUpdate([]byte{3})),
	}})
	require.NoError(t, err)
	secondDeltas := append([]recordDelta(nil), ctx.deltas...)
	final := ctx.snapshot()
	replayed := make(map[string][]byte)
	for _, delta := range append(firstDeltas, secondDeltas...) {
		replayed[string(delta.Key)] = bytes.Clone(delta.Data)
	}
	require.Equal(t, final, replayed)

	reloaded := newDeltaContext()
	for key, data := range final {
		reloaded.branches[key] = bytes.Clone(data)
	}
	n, err := unfold(reloaded, nil, planeStorage, address[:])
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
