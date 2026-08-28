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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestCollectUpdateV3WritesZeroLengthTombstone(t *testing.T) {
	t.Parallel()

	path := []byte{1, 2, 3}
	prefix := nibbles.HexToCompact(path)
	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)

	require.NoError(t, be.CollectUpdate(ctx, prefix, 0, 1<<6, 0, nil, false))
	require.Len(t, ctx.puts, 1, "a deleted child must produce one edge-record update")
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 6), ctx.puts[0].prefix)
	require.NotNil(t, ctx.puts[0].data, "the tombstone must be a present zero-length value")
	require.Empty(t, ctx.puts[0].data)
	require.Nil(t, ctx.puts[0].prev)
}

func TestCollectUpdateV3WritesLiveAndDeletedChildren(t *testing.T) {
	t.Parallel()

	path := []byte{0xa, 0xb}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[2] = recordTestData("branch", []byte{1, 2})
	cells[2].branchMask = 0x0040

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)

	require.NoError(t, be.CollectUpdate(ctx, prefix, 1<<2, 1<<2|1<<9, 1<<2, &cells, false))
	require.Len(t, ctx.puts, 2)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 2), ctx.puts[0].prefix)
	require.Equal(t, EncodeBranchChild(cells[2].branchMask, &cells[2]), ctx.puts[0].data)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 9), ctx.puts[1].prefix)
	require.NotNil(t, ctx.puts[1].data)
	require.Empty(t, ctx.puts[1].data)
}

func TestV3DeletionClearsParentMaskAndTombstonesChild(t *testing.T) {
	t.Parallel()

	parentPath := []byte{3, 4}
	parentPrefix := nibbles.HexToCompact(parentPath)
	childPrefix := nibbles.HexToCompact(append(append([]byte{}, parentPath...), 5))
	var cells [16]cellEncodeData
	cells[5] = recordTestData("branch", nil)
	oldMask := uint16(1<<2 | 1<<7)
	cells[5].branchMask = oldMask &^ (1 << 7)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)
	require.NoError(t, be.CollectUpdate(ctx, parentPrefix, 1<<5, 1<<5, 1<<5, &cells, false))
	require.NoError(t, be.CollectUpdate(ctx, childPrefix, 0, 1<<7, 0, nil, false))

	require.Len(t, ctx.puts, 2)
	var decoded cell
	mask, err := DecodeRecordInto(ctx.puts[0].data, &decoded)
	require.NoError(t, err)
	require.Equal(t, oldMask&^(1<<7), mask, "the parent edge must carry the cleared child mask")
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(append(append([]byte{}, parentPath...), 5)), 7), ctx.puts[1].prefix)
	require.NotNil(t, ctx.puts[1].data)
	require.Empty(t, ctx.puts[1].data)
}

func TestCollectDeferredUpdateV3WritesZeroLengthTombstone(t *testing.T) {
	t.Parallel()

	path := []byte{4, 5, 6}
	prefix := nibbles.HexToCompact(path)
	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.setEdgeRecords(true)

	require.NoError(t, be.CollectDeferredUpdate(ctx, prefix, 0, 1<<4, 0, nil, false))
	require.Len(t, be.deferred, 1)
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	require.Len(t, ctx.puts, 1)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 4), ctx.puts[0].prefix)
	require.NotNil(t, ctx.puts[0].data)
	require.Empty(t, ctx.puts[0].data)
}

func TestCollectDeleteUpdateV3TombstonesVisitedChildren(t *testing.T) {
	t.Parallel()

	ctx := &recordingCtx{}
	hph := newHexPatriciaHashed()
	hph.ctx = ctx
	hph.cfg.EdgeRecords = true
	hph.branchEncoder.setEdgeRecords(true)
	hph.touchMap[3] = 1<<1 | 1<<12
	hph.afterMap[3] = 1 << 12

	path := nibbles.HexToCompact([]byte{7, 8, 9})
	require.NoError(t, hph.collectDeleteUpdate(path, 3))
	require.Len(t, ctx.puts, 2, "deleting a subtree must visit only its old child records")
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(nibbles.CompactToHex(path)), 1), ctx.puts[0].prefix)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(nibbles.CompactToHex(path)), 12), ctx.puts[1].prefix)
	for _, put := range ctx.puts {
		require.NotNil(t, put.data)
		require.Empty(t, put.data)
	}
}

func TestBranchDataTombstoneDistinguishesMissingValue(t *testing.T) {
	t.Parallel()

	require.False(t, BranchData(nil).IsTombstone(), "a missing record must not be treated as an empty branch")
	require.True(t, BranchData([]byte{}).IsTombstone())
}

func TestSynthesizeBranchRowTombstoneDoesNotResurrectLegacyRecord(t *testing.T) {
	t.Parallel()

	var cells [16]cellEncodeData
	cells[5] = recordTestData("account", nil)
	legacy, err := NewBranchEncoder(1024).EncodeBranch(1<<5, 1<<5, 1<<5, &cells)
	require.NoError(t, err)

	var records [16][]byte
	records[5] = []byte{}
	read, err := SynthesizeBranchRow(1<<5, true, records, 1<<5, legacy)
	require.NoError(t, err)
	require.Empty(t, read.Data, "the newest zero-length record must clear the stale legacy child")
}

func TestSynthesizeBranchRowMissingRecordUsesOrdinaryFold(t *testing.T) {
	t.Parallel()

	var cells [16]cellEncodeData
	cells[2] = recordTestData("account", []byte{1, 2})
	legacy, err := NewBranchEncoder(1024).EncodeBranch(1<<2, 1<<2, 1<<2, &cells)
	require.NoError(t, err)

	read, err := SynthesizeBranchRow(1<<2, true, [16][]byte{}, 0, legacy)
	require.NoError(t, err)
	require.NotEmpty(t, read.Data)
	var decoded [16]cell
	maps, err := DecodeBranchInto(read.Data[2:], false, &decoded)
	require.NoError(t, err)
	require.Equal(t, uint16(1<<2), maps.AfterMap)
	require.Equal(t, cells[2].accountAddr[:], decoded[2].accountAddr[:cells[2].accountAddrLen])
}

func TestBranchMergerTombstoneShadowsOlderRow(t *testing.T) {
	t.Parallel()

	var cells [16]cellEncodeData
	cells[1] = recordTestData("account", nil)
	row, err := NewBranchEncoder(1024).EncodeBranch(1<<1, 1<<1, 1<<1, &cells)
	require.NoError(t, err)

	merged, err := NewHexBranchMerger(1024).Merge(row, BranchData([]byte{}))
	require.NoError(t, err)
	require.True(t, bytes.Equal(merged, []byte{}))
	require.NotNil(t, merged)
}
