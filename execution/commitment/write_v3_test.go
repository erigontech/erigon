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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestCollectUpdateV3WritesOnlyChangedChildRecords(t *testing.T) {
	t.Parallel()

	path := []byte{1, 2, 3, 4}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[3] = recordTestData("account", []byte{5, 6})
	cells[7] = recordTestData("branch", []byte{7, 8})
	cells[7].branchMask = 0x0a0b

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)

	require.NoError(t, be.CollectUpdate(ctx, prefix, 1<<3, 1<<3|1<<7, 1<<3|1<<7, &cells, false))
	require.Zero(t, ctx.branchCalls, "v3 writes must not read the previous bundled row")
	require.Len(t, ctx.puts, 1, "one changed child must produce one record")

	wantKey := nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 3)
	require.Equal(t, wantKey, ctx.puts[0].prefix)
	require.Equal(t, EncodeLeafChild(&cells[3]), ctx.puts[0].data)
	require.Nil(t, ctx.puts[0].prev)

	require.NotEqual(t, nibbles.EncodeKeyV3(path), ctx.puts[0].prefix, "the node key must not be written as a separate record")
}

func TestCollectUpdateV3StoresBranchMaskOnParentEdge(t *testing.T) {
	t.Parallel()

	path := []byte{0xa, 0xb, 0xc}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[5] = recordTestData("branch", []byte{1, 2, 3})
	cells[5].branchMask = 0x1234

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)
	require.NoError(t, be.CollectUpdate(ctx, prefix, 1<<5, 1<<5, 1<<5, &cells, false))

	require.Len(t, ctx.puts, 1)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 5), ctx.puts[0].prefix)
	var decoded cell
	mask, err := DecodeRecordInto(ctx.puts[0].data, &decoded)
	require.NoError(t, err)
	require.Equal(t, uint16(0x1234), mask)
	require.Equal(t, int16(3), decoded.extLen)
	require.Equal(t, []byte{1, 2, 3}, decoded.extension[:decoded.extLen])

	for _, put := range ctx.puts {
		require.False(t, bytes.Equal(nibbles.EncodeKeyV3(path), put.prefix), "a node record must not carry the branch mask")
	}
}

func TestCollectDeferredUpdateV3WritesRecordsWithoutRowMerge(t *testing.T) {
	t.Parallel()

	path := []byte{2, 4, 6}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[1] = recordTestData("account", nil)
	cells[9] = recordTestData("storage", nil)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.setEdgeRecords(true)
	require.NoError(t, be.CollectDeferredUpdate(ctx, prefix, 1<<1|1<<9, 1<<1|1<<9, 1<<1|1<<9, &cells, false))
	require.Zero(t, ctx.branchCalls)
	require.Len(t, be.deferred, 2)
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	require.Len(t, ctx.puts, 2)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 1), ctx.puts[0].prefix)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 9), ctx.puts[1].prefix)
}

func TestV3RootMaskIsEncodedInStateBlob(t *testing.T) {
	t.Parallel()

	hph := newHexPatriciaHashed()
	hph.rootMask = 0x4321

	encoded, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)

	var decoded state
	require.NoError(t, decoded.Decode(encoded))
	require.Equal(t, uint16(0x4321), decoded.RootMask)
}

func TestHexPatriciaHashedV3WritesFoldedMasksToParentEdges(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.DeferBranchUpdates = false
	cfg.EdgeRecords = true
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, cfg)

	plainKeys, updates := fixtureBaseAccounts().Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	require.NotEmpty(t, updates)

	_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, ms.cm)

	var branchRecords, parentMaskRecords int
	for key, record := range ms.cm {
		v3Key := []byte(key)
		require.True(t, nibbles.IsChildKeyV3(v3Key), "commitment key %x must be a v3 child key", v3Key)
		var decoded cell
		mask, err := DecodeRecordInto(record, &decoded)
		require.NoError(t, err, "record %x", v3Key)
		if decoded.accountAddrLen == 0 && decoded.storageAddrLen == 0 {
			branchRecords++
			if mask != 0 {
				parentMaskRecords++
			}
		}
	}
	require.Positive(t, branchRecords)
	require.Equal(t, branchRecords, parentMaskRecords, "every persisted branch child must carry its child mask")

	stateBlob, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)
	var stateValue state
	require.NoError(t, stateValue.Decode(stateBlob))
	require.Equal(t, hph.rootMask, stateValue.RootMask)
	require.NotZero(t, stateValue.RootMask)
	for key := range ms.cm {
		require.NotEqual(t, nibbles.EncodeKeyV3(nil), []byte(key), "the root node key must not be persisted")
	}
}
