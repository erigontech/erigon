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
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestCollectDeferredUpdateV3KeepsParentRunPending(t *testing.T) {
	prefix := nibbles.HexToCompact([]byte{1, 2, 3})
	var cells [16]cellEncodeData
	cells[1] = recordTestData("account", nil)
	cells[9] = recordTestData("account", nil)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.setEdgeRecords(true)

	require.NoError(t, be.CollectDeferredUpdate(ctx, prefix, 1<<1, 1<<1, 1<<1, &cells, true))
	require.NoError(t, be.CollectDeferredUpdate(ctx, prefix, 1<<9, 1<<9, 1<<9, &cells, false))

	require.Len(t, be.deferred, 2)
	require.True(t, be.HasPendingPrefix(prefix))
	require.Empty(t, ctx.puts, "a parent run must remain deferred until a read or flush")

	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	require.Len(t, ctx.puts, 2)
	require.True(t, be.HasPendingPrefix(prefix))
	be.ClearDeferred()
	require.False(t, be.HasPendingPrefix(prefix))
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3([]byte{1, 2, 3}), 1), ctx.puts[0].prefix)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3([]byte{1, 2, 3}), 9), ctx.puts[1].prefix)
}

func TestApplyDeferredV3LastWriteWinsPerRecord(t *testing.T) {
	key := nibbles.ChildKeyV3(nibbles.EncodeKeyV3([]byte{4, 5}), 7)
	deferred := []*DeferredBranchUpdate{
		{prefix: key, raw: []byte{1, 2}, edgeRecord: true},
		{prefix: key, raw: []byte{3, 4, 5}, edgeRecord: true},
	}

	var got []struct {
		key []byte
		val []byte
	}
	written, err := ApplyDeferredBranchUpdates(deferred, 1, func(key, val, _ []byte) error {
		got = append(got, struct {
			key []byte
			val []byte
		}{bytes.Clone(key), bytes.Clone(val)})
		return nil
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, []byte{3, 4, 5}, got[0].val)
	require.Equal(t, key, got[0].key)
}

func TestCollectDeferredV3AutoFlushesAtDefaultLimit(t *testing.T) {
	var cells [16]cellEncodeData
	cells[0] = recordTestData("account", nil)
	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.setEdgeRecords(true)

	for i := range DefaultMaxDeferredUpdates {
		path := []byte{byte(i >> 12 & 0xf), byte(i >> 8 & 0xf), byte(i >> 4 & 0xf), byte(i & 0xf)}
		require.NoError(t, be.CollectDeferredUpdate(ctx, nibbles.HexToCompact(path), 1, 1, 1, &cells, true))
	}
	require.Len(t, be.deferred, DefaultMaxDeferredUpdates)
	require.Empty(t, ctx.puts)

	path := []byte{byte(DefaultMaxDeferredUpdates >> 12 & 0xf), byte(DefaultMaxDeferredUpdates >> 8 & 0xf), byte(DefaultMaxDeferredUpdates >> 4 & 0xf), byte(DefaultMaxDeferredUpdates & 0xf)}
	require.NoError(t, be.CollectDeferredUpdate(ctx, nibbles.HexToCompact(path), 1, 1, 1, &cells, true))
	require.Len(t, ctx.puts, DefaultMaxDeferredUpdates)
	require.Len(t, be.deferred, 1)
}

func TestApplyDeferredV3DisjointChildSetsCommute(t *testing.T) {
	prefix := nibbles.EncodeKeyV3([]byte{6, 7})
	updates := []*DeferredBranchUpdate{
		{prefix: nibbles.ChildKeyV3(prefix, 2), raw: []byte{2}, edgeRecord: true},
		{prefix: nibbles.ChildKeyV3(prefix, 11), raw: []byte{11}, edgeRecord: true},
	}

	apply := func(order []int) map[string][]byte {
		got := make(map[string][]byte)
		deferred := make([]*DeferredBranchUpdate, 0, len(order))
		for _, i := range order {
			deferred = append(deferred, updates[i])
		}
		written, err := ApplyDeferredBranchUpdates(deferred, 2, func(key, val, _ []byte) error {
			got[string(key)] = bytes.Clone(val)
			return nil
		}, nil)
		require.NoError(t, err)
		require.Equal(t, 2, written)
		return got
	}

	require.Equal(t, apply([]int{0, 1}), apply([]int{1, 0}))
}

func TestConcurrentWorkersV3WriteDisjointChildSetsUnderParent(t *testing.T) {
	parentPath := []byte{8, 9}
	parentPrefix := nibbles.HexToCompact(parentPath)
	workerNibbles := []byte{2, 13}
	deferred := make([]*DeferredBranchUpdate, len(workerNibbles))
	workerErrs := make([]error, len(workerNibbles))

	var wg sync.WaitGroup
	for i, nibble := range workerNibbles {
		wg.Go(func() {
			var cells [16]cellEncodeData
			cells[nibble] = recordTestData("account", nil)
			be := NewBranchEncoder(1024)
			be.setDeferUpdates(true)
			be.setEdgeRecords(true)
			workerErrs[i] = be.CollectDeferredUpdate(nil, parentPrefix, uint16(1)<<nibble, uint16(1)<<nibble, uint16(1)<<nibble, &cells, true)
			if workerErrs[i] != nil {
				return
			}
			deferred[i] = be.deferred[0]
		})
	}
	wg.Wait()
	for _, err := range workerErrs {
		require.NoError(t, err)
	}
	defer func() {
		for _, update := range deferred {
			putDeferredUpdate(update)
		}
	}()

	got := make(map[string][]byte)
	written, err := ApplyDeferredBranchUpdates(deferred, 2, func(key, value, _ []byte) error {
		got[string(key)] = bytes.Clone(value)
		return nil
	}, nil)
	require.NoError(t, err)
	require.Equal(t, len(workerNibbles), written)
	for _, nibble := range workerNibbles {
		key := nibbles.ChildKeyV3(nibbles.EncodeKeyV3(parentPath), nibble)
		require.Contains(t, got, string(key))
	}
}
