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

package block_collector

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

// makeBeaconBlock builds a Deneb BeaconBlock whose ExecutionPayload carries the
// given block number chained onto parent. forkTag seeds header.Extra so two blocks
// at the same number produce distinct SSZ roots — mimicking competing beacon variants.
func makeBeaconBlock(t *testing.T, number uint64, forkTag byte, parent common.Hash) *cltypes.BeaconBlock {
	t.Helper()
	var zero uint64
	// ParentBeaconBlockRoot must be non-nil: decodeBlock always reconstructs it
	// from the stored 32-byte parentRoot prefix, so leaving the source header's
	// field nil would produce a different header.Hash() and fail RlpHeader's
	// consistency check.
	zeroHash := common.Hash{}
	header := &types.Header{
		ParentHash:            parent,
		Number:                *uint256.NewInt(number),
		BaseFee:               uint256.NewInt(1),
		Extra:                 []byte{forkTag},
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &zeroHash,
	}
	block := types.NewBlock(header, nil, nil, nil, []*types.Withdrawal{})

	bb := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	bb.Body.ExecutionPayload = cltypes.NewEth1BlockFromHeaderAndBody(block.Header(), block.RawBody(), &clparams.MainnetBeaconConfig)
	return bb
}

// blockHash returns the execution BlockHash of a BeaconBlock (what becomes the
// parent-hash seed for the next block in a chain).
func blockHash(bb *cltypes.BeaconBlock) common.Hash {
	return bb.Body.ExecutionPayload.BlockHash
}

// flushTestHarness wires a PersistentBlockCollector to a gomock ExecutionEngine
// that records every batch passed to InsertBlocks (and every FCU call).
type flushTestHarness struct {
	collector *PersistentBlockCollector
	inserted  []*types.Block
	fcuHeads  []common.Hash
}

// insertedNumbers returns the block numbers of every inserted block in call order.
func (h *flushTestHarness) insertedNumbers() []uint64 {
	nums := make([]uint64, len(h.inserted))
	for i, b := range h.inserted {
		nums[i] = b.NumberU64()
	}
	return nums
}

func newFlushTestHarness(t *testing.T, frozen uint64) *flushTestHarness {
	t.Helper()
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)

	h := &flushTestHarness{}
	engine.EXPECT().FrozenBlocks(gomock.Any()).Return(frozen).AnyTimes()
	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, blocks []*types.Block, _ [][]byte) error {
			h.inserted = append(h.inserted, blocks...)
			return nil
		}).AnyTimes()
	engine.EXPECT().CurrentHeader(gomock.Any()).Return(nil, nil).AnyTimes()
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _, head common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			h.fcuHeads = append(h.fcuHeads, head)
			return nil, nil
		}).AnyTimes()

	persistDir := filepath.Join(t.TempDir(), "collector")
	c := NewPersistentBlockCollector(log.New(), engine, &clparams.MainnetBeaconConfig, persistDir)
	require.NotNil(t, c)
	t.Cleanup(func() { _ = c.Close() })

	h.collector = c
	return h
}

// countRowsAtOrAbove returns the number of rows whose 8-byte block-number prefix is >= minNumber.
func countRowsAtOrAbove(t *testing.T, db kv.RoDB, minNumber uint64) int {
	t.Helper()
	count := 0
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		defer cursor.Close()
		for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
			if err != nil {
				return err
			}
			if len(k) >= 8 && binary.BigEndian.Uint64(k[:8]) >= minNumber {
				count++
			}
		}
		return nil
	}))
	return count
}

func TestDecodeBlockRejectsShortPersistentValue(t *testing.T) {
	c := &PersistentBlockCollector{}
	for name, raw := range map[string][]byte{
		"empty decompressed value": nil,
		"missing parent root":      {byte(clparams.DenebVersion)},
		"missing requests hash":    append([]byte{byte(clparams.ElectraVersion)}, make([]byte, 32)...),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := c.decodeBlock(utils.CompressSnappy(raw))
			require.ErrorContains(t, err, "persistent block value too short")
		})
	}
}

func TestFlushSkipsDuplicateBlockNumbers(t *testing.T) {
	// payloadKey uses only block number, so adding two variants at the same
	// height causes the second to overwrite the first. The duplicate must not
	// be mis-classified as a gap; blocks 1 and 2 should be inserted.
	h := newFlushTestHarness(t, 0)

	b1a := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b1b := makeBeaconBlock(t, 1, 'b', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1a))
	require.NoError(t, h.collector.AddBlock(b1a))
	require.NoError(t, h.collector.AddBlock(b1b))
	require.NoError(t, h.collector.AddBlock(b2))

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	// payloadKey overwrites: b1b replaced b1a, so block 1 is b1b.
	require.Equal(t, blockHash(b1b), h.inserted[0].Hash())
	require.Equal(t, blockHash(b2), h.inserted[1].Hash())

	// Clean path drops the whole DB.
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushPicksCanonicalVariantRegardlessOfOrder(t *testing.T) {
	// Same setup as above but block 2 chains off variant 'b' this time. The
	// collector must pick 1b regardless of whether 1a or 1b came first in
	// cursor order.
	h := newFlushTestHarness(t, 0)

	b1a := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b1b := makeBeaconBlock(t, 1, 'b', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1b))
	require.NoError(t, h.collector.AddBlock(b1a))
	require.NoError(t, h.collector.AddBlock(b1b))
	require.NoError(t, h.collector.AddBlock(b2))

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	require.Equal(t, blockHash(b1b), h.inserted[0].Hash())
}

func TestFlushPreservesMultiVariantAtEndOfCursor(t *testing.T) {
	// payloadKey uses only block number, so b2b overwrites b2a. With only
	// one variant per height there is no ambiguity: both blocks are inserted
	// and the DB is fully cleaned.
	h := newFlushTestHarness(t, 0)

	b1 := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b2a := makeBeaconBlock(t, 2, 'a', blockHash(b1))
	b2b := makeBeaconBlock(t, 2, 'b', blockHash(b1))
	for _, bb := range []*cltypes.BeaconBlock{b1, b2a, b2b} {
		require.NoError(t, h.collector.AddBlock(bb))
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	// Both blocks inserted (b2b is the surviving variant at height 2).
	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	require.False(t, h.collector.HasBlock(1))
	require.False(t, h.collector.HasBlock(2))
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushStopsWhenForkHasNoMatchingParent(t *testing.T) {
	// payloadKey uses only block number, so b1b overwrites b1a. With one
	// variant per height, resolvePending always succeeds (len==1 returns
	// pending[0]). Both blocks are inserted and the DB is cleaned.
	h := newFlushTestHarness(t, 0)

	b1a := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b1b := makeBeaconBlock(t, 1, 'b', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', common.HexToHash("0xdeadbeef"))
	require.NoError(t, h.collector.AddBlock(b1a))
	require.NoError(t, h.collector.AddBlock(b1b))
	require.NoError(t, h.collector.AddBlock(b2))

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	require.False(t, h.collector.HasBlock(1))
	require.False(t, h.collector.HasBlock(2))
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushPreservesRowsPastGap(t *testing.T) {
	// Chain [1, 2] followed by a disjoint segment [4, 5] (simulating that
	// block 3 was never received). Post-gap rows must survive for the next
	// Flush once 3 is re-downloaded.
	h := newFlushTestHarness(t, 0)

	b1 := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1))
	// b4's parent is a placeholder for the missing b3 — it doesn't chain onto b2.
	b4 := makeBeaconBlock(t, 4, 'a', common.HexToHash("0xb3b3b3"))
	b5 := makeBeaconBlock(t, 5, 'a', blockHash(b4))
	for _, bb := range []*cltypes.BeaconBlock{b1, b2, b4, b5} {
		require.NoError(t, h.collector.AddBlock(bb))
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())

	require.False(t, h.collector.HasBlock(1))
	require.False(t, h.collector.HasBlock(2))
	require.True(t, h.collector.HasBlock(4))
	require.True(t, h.collector.HasBlock(5))
	require.Equal(t, 2, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushDupThenGapKeepsPostGapRows(t *testing.T) {
	// payloadKey uses only block number: b1b overwrites b1a. Block 2 chains
	// off b1a but that doesn't matter — with one variant per height,
	// resolvePending returns the sole variant. Gap before block 4 is still
	// detected and rows past the gap survive.
	h := newFlushTestHarness(t, 0)

	b1a := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b1b := makeBeaconBlock(t, 1, 'b', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1a))
	b4 := makeBeaconBlock(t, 4, 'a', common.HexToHash("0xb3b3b3"))
	for _, bb := range []*cltypes.BeaconBlock{b1a, b1b, b2, b4} {
		require.NoError(t, h.collector.AddBlock(bb))
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	// b1b overwrote b1a, so block 1 is b1b.
	require.Equal(t, blockHash(b1b), h.inserted[0].Hash())

	require.False(t, h.collector.HasBlock(1))
	require.False(t, h.collector.HasBlock(2))
	require.True(t, h.collector.HasBlock(4))
}

func TestFlushDropsRowsBelowFrozen(t *testing.T) {
	// Rows [2, 3] with minInsertableBlockNumber=3: block 2 is already frozen
	// and must be skipped, block 3 must be inserted, DB cleared cleanly.
	h := newFlushTestHarness(t, 3)

	// b2 is frozen and won't be read; b3 is the only block the collector sees
	// so its parent doesn't need to point at b2.
	b2 := makeBeaconBlock(t, 2, 'a', common.Hash{})
	b3 := makeBeaconBlock(t, 3, 'a', common.Hash{})
	require.NoError(t, h.collector.AddBlock(b2))
	require.NoError(t, h.collector.AddBlock(b3))

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{3}, h.insertedNumbers())
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
}

// TestFlushDrivesFCUPerBatch verifies the per-batch FCU pattern: when Flush()
// is called with more blocks than batchSize, doForkChoiceUpdate is invoked
// once per completed batch (so the engine can run execution + prune mid-flush
// and bound BlockTransaction growth), plus one final FCU after the loop.
func TestFlushDrivesFCUPerBatch(t *testing.T) {
	// Temporarily lower batchSize so the test stays fast.
	origBatchSize := batchSize
	batchSize = 3
	t.Cleanup(func() { batchSize = origBatchSize })

	h := newFlushTestHarness(t, 0)

	// 7 blocks: two full batches of 3 plus a tail of 1 → expect 3 FCUs
	// (two per-batch + one final after the tail insert).
	prev := common.Hash{}
	blocks := make([]*cltypes.BeaconBlock, 7)
	for i := 0; i < 7; i++ {
		blocks[i] = makeBeaconBlock(t, uint64(i+1), 'a', prev)
		require.NoError(t, h.collector.AddBlock(blocks[i]))
		prev = blockHash(blocks[i])
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, h.insertedNumbers())
	require.Len(t, h.fcuHeads, 3, "expected 2 per-batch FCUs + 1 final FCU")
	require.Equal(t, blockHash(blocks[2]), h.fcuHeads[0], "first FCU should target the last block of batch 1 (block 3)")
	require.Equal(t, blockHash(blocks[5]), h.fcuHeads[1], "second FCU should target the last block of batch 2 (block 6)")
	require.Equal(t, blockHash(blocks[6]), h.fcuHeads[2], "final FCU should target the last inserted block (block 7)")
}

// TestFlushSingleFCUWhenBelowBatchSize verifies the baseline: when total
// blocks fit in a single sub-batch (no per-batch FCU triggered), only the
// final after-loop FCU fires.
func TestFlushSingleFCUWhenBelowBatchSize(t *testing.T) {
	origBatchSize := batchSize
	batchSize = 100 // well above the 3 blocks we add
	t.Cleanup(func() { batchSize = origBatchSize })

	h := newFlushTestHarness(t, 0)

	prev := common.Hash{}
	var last *cltypes.BeaconBlock
	for i := 0; i < 3; i++ {
		b := makeBeaconBlock(t, uint64(i+1), 'a', prev)
		require.NoError(t, h.collector.AddBlock(b))
		prev = blockHash(b)
		last = b
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2, 3}, h.insertedNumbers())
	require.Len(t, h.fcuHeads, 1, "with all blocks in a single sub-batch, only the final FCU fires")
	require.Equal(t, blockHash(last), h.fcuHeads[0])
}
