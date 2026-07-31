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
	"os"
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

// makeTestHeader builds the minimal Deneb header that survives the collector's
// encode/decode round trip. ParentBeaconBlockRoot must be non-nil: decodeBlock
// always reconstructs it from the stored 32-byte parentRoot prefix, so leaving
// the source header's field nil would produce a different header.Hash() and
// fail RlpHeader's consistency check.
func makeTestHeader(number uint64, parent common.Hash, extra []byte) *types.Header {
	var zero uint64
	zeroHash := common.Hash{}
	return &types.Header{
		ParentHash:            parent,
		Number:                *uint256.NewInt(number),
		BaseFee:               uint256.NewInt(1),
		Extra:                 extra,
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &zeroHash,
	}
}

// makeBeaconBlock builds a Deneb BeaconBlock whose ExecutionPayload carries the
// given block number chained onto parent. forkTag seeds header.Extra so two blocks
// at the same number produce distinct SSZ roots — mimicking competing beacon variants.
func makeBeaconBlock(t *testing.T, number uint64, forkTag byte, parent common.Hash, txs ...types.Transaction) *cltypes.BeaconBlock {
	t.Helper()
	block := types.NewBlock(makeTestHeader(number, parent, []byte{forkTag}), txs, nil, nil, []*types.Withdrawal{})

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
	return newFlushTestHarnessWithElHead(t, frozen, nil)
}

// newFlushTestHarnessWithElHead lets tests control EL's reported head
// header. Pass nil for the legacy nil-header behaviour; pass a
// non-nil header to exercise the post-Mode-B catchup path where EL's
// chaindata head sits below frozen.
func newFlushTestHarnessWithElHead(t *testing.T, frozen uint64, elHead *types.Header) *flushTestHarness {
	t.Helper()
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)

	h := &flushTestHarness{}
	engine.EXPECT().FrozenBlocks(gomock.Any()).Return(frozen, nil).AnyTimes()
	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, blocks []*types.Block, _ [][]byte) error {
			h.inserted = append(h.inserted, blocks...)
			return nil
		}).AnyTimes()
	engine.EXPECT().CurrentHeader(gomock.Any()).Return(elHead, nil).AnyTimes()
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _, head common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			h.fcuHeads = append(h.fcuHeads, head)
			return nil, nil
		},
	).AnyTimes()

	persistDir := filepath.Join(t.TempDir(), "collector")
	c := NewPersistentBlockCollector(t.Context(), log.New(), engine, &clparams.MainnetBeaconConfig, persistDir)
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

// TestFlushFloorIsElHeadNotFrozenAfterModeBUnwind pins the
// post-Mode-B-recovery contract: when EL's chaindata head sits
// BELOW the snapshot tip (a deep Mode-B unwind to a target inside
// the snapshot range), Flush's floor must be elHead+1, NOT
// FrozenBlocks(). Otherwise it skips precisely the gap blocks the
// catchup needs to push and the chain wedges at the unwind target.
//
// Live-caught on hoodi soak v19 iter 4 (depth 60k, target=2,986,464,
// FrozenBlocks=3,042,999): every cached gap-block was skipped by
// Flush, the chain wedged at the unwind target for 1802s until
// soak driver timed out.
//
// Setup: elHead=2 (post-unwind head), FrozenBlocks=5 (snapshot tip
// untouched by unwind). Cached blocks: 3, 4, 5, 6. Under the
// fixed code path: floor = elHead+1 = 3, all four blocks insert.
// Under the buggy (pre-fix) code path: floor = FrozenBlocks = 5,
// blocks 3 and 4 are skipped and the chain can't bridge to block 5.
func TestFlushFloorIsElHeadNotFrozenAfterModeBUnwind(t *testing.T) {
	// elHead at block 2 — post-Mode-B unwind state.
	elHead := &types.Header{
		Number:  *uint256.NewInt(2),
		BaseFee: uint256.NewInt(1),
	}
	// FrozenBlocks=5 — snapshot tip well above elHead.
	h := newFlushTestHarnessWithElHead(t, 5, elHead)

	// Build a chain b3 → b4 → b5 → b6 that fills the gap from elHead
	// up past FrozenBlocks. Without the fix, blocks 3 and 4 would be
	// silently skipped (< FrozenBlocks=5) and the chain wedges; b5
	// would then fail to insert because EL has no parent for it.
	prev := common.Hash{}
	blocks := make([]*cltypes.BeaconBlock, 4)
	for i := range 4 {
		blocks[i] = makeBeaconBlock(t, uint64(i+3), 'a', prev)
		require.NoError(t, h.collector.AddBlock(blocks[i]))
		prev = blockHash(blocks[i])
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{3, 4, 5, 6}, h.insertedNumbers(),
		"all blocks past elHead must be inserted — including those below FrozenBlocks; "+
			"using FrozenBlocks as the floor wedges post-Mode-B recovery")
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

// plantMarker writes a sentinel file into the collector's persistDir; it
// survives Flush only if Flush did not RemoveAll the directory.
func plantMarker(t *testing.T, h *flushTestHarness) string {
	t.Helper()
	marker := filepath.Join(h.collector.persistDir, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("x"), 0o644))
	return marker
}

func TestFlushEmptyDBKeepsDirectory(t *testing.T) {
	// At chain-tip Flush runs on every block with an empty DB; it must not
	// drop and recreate the directory each time.
	h := newFlushTestHarness(t, 0)
	marker := plantMarker(t, h)

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Empty(t, h.inserted)
	require.FileExists(t, marker)
}

func TestFlushSmallDBClearsRowsInPlace(t *testing.T) {
	h := newFlushTestHarness(t, 0)

	b1 := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1))
	require.NoError(t, h.collector.AddBlock(b1))
	require.NoError(t, h.collector.AddBlock(b2))
	marker := plantMarker(t, h)

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
	require.FileExists(t, marker)
}

func TestFlushDropsDBOverSizeThreshold(t *testing.T) {
	origThreshold := dropDBSizeThreshold
	dropDBSizeThreshold = 0 // any non-empty database exceeds it
	t.Cleanup(func() { dropDBSizeThreshold = origThreshold })

	h := newFlushTestHarness(t, 0)

	b1 := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1))
	require.NoError(t, h.collector.AddBlock(b1))
	require.NoError(t, h.collector.AddBlock(b2))
	marker := plantMarker(t, h)

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	require.NoFileExists(t, marker)

	// The reopened database is functional.
	b3 := makeBeaconBlock(t, 3, 'a', blockHash(b2))
	require.NoError(t, h.collector.AddBlock(b3))
	require.True(t, h.collector.HasBlock(3))
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
	for i := range 7 {
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
	for i := range 3 {
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

// TestPruneSkipsWhenElHeadIsZero pins the wedge live-reproduced on
// hoodi 2026-06-09 first-launch: preverified bootstrap completes (EL
// snapshot files cover blocks 0 → 2,973,999) but no FCU has fired
// yet, so engine.CurrentHeader returns the genesis-block header with
// Number=0. Caplin's PersistentBlockCollector holds beacon blocks at
// numbers 2,974,000+ (its lowest cached row past the snapshot tip).
//
// Pre-fix behaviour: pruneStaleCachedBlocks computed elHead=0,
// firstPast=2,974,000, decided Case C (gap from elHead) and **wiped
// all 9,919 cached blocks**, leaving Caplin to re-download them via
// ForwardSync on a slow 60-slot/13-min cadence. End result: head
// stayed at 2,973,999 until a manual restart unstuck things.
//
// Post-fix: pruneStaleCachedBlocks returns early when elHead == 0,
// letting Flush call InsertBlocks normally. The EL's snapshot files
// resolve the parent (2,973,999) for block 2,974,000, the insert
// succeeds, and the chain progresses.
//
// Test setup: CurrentHeader returns a genesis-block header (Number=0).
// Add 3 cached blocks at high numbers (chained off a placeholder
// parent, since the test EL never actually verifies). Flush must
// insert all 3 — not wipe them via prune.
func TestPruneSkipsWhenElHeadIsZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)

	var inserted []*types.Block
	engine.EXPECT().FrozenBlocks(gomock.Any()).Return(uint64(0), nil).AnyTimes()
	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, blocks []*types.Block, _ [][]byte) error {
			inserted = append(inserted, blocks...)
			return nil
		}).AnyTimes()
	// The crux of the test: CurrentHeader returns a genesis header
	// (Number=0). Pre-fix this triggered the Case C prune-everything path.
	genesisHeader := &types.Header{Number: *uint256.NewInt(0)}
	engine.EXPECT().CurrentHeader(gomock.Any()).Return(genesisHeader, nil).AnyTimes()
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	persistDir := filepath.Join(t.TempDir(), "collector")
	c := NewPersistentBlockCollector(t.Context(), log.New(), engine, &clparams.MainnetBeaconConfig, persistDir)
	require.NotNil(t, c)
	t.Cleanup(func() { _ = c.Close() })

	// Cached blocks far above elHead=0 (the preverified-bootstrap shape).
	// The test EL stubs InsertBlocks so parent lookup never runs.
	b1 := makeBeaconBlock(t, 2_974_000, 'a', common.HexToHash("0xdeadbeef"))
	b2 := makeBeaconBlock(t, 2_974_001, 'a', blockHash(b1))
	b3 := makeBeaconBlock(t, 2_974_002, 'a', blockHash(b2))
	for _, bb := range []*cltypes.BeaconBlock{b1, b2, b3} {
		require.NoError(t, c.AddBlock(bb))
	}

	require.NoError(t, c.Flush(t.Context()))

	insertedNums := make([]uint64, len(inserted))
	for i, b := range inserted {
		insertedNums[i] = b.NumberU64()
	}
	require.Equal(t, []uint64{2_974_000, 2_974_001, 2_974_002}, insertedNums,
		"with elHead=0 (pre-FCU genesis), prune must not wipe blocks. A non-3-length result means Case C fired and deleted the cached payload, reproducing the first-launch wedge.")
}

// pruneCaseCTestHarness builds a flushTestHarness pointing at elHead =
// `elHead`. CurrentHeader and FrozenBlocks are stubbed so the prune
// path runs without triggering insertion side effects.
func pruneCaseCTestHarness(t *testing.T, elHead uint64) *flushTestHarness {
	t.Helper()
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)

	h := &flushTestHarness{}
	engine.EXPECT().FrozenBlocks(gomock.Any()).Return(uint64(0), nil).AnyTimes()
	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, blocks []*types.Block, _ [][]byte) error {
			h.inserted = append(h.inserted, blocks...)
			return nil
		}).AnyTimes()
	header := &types.Header{Number: *uint256.NewInt(elHead)}
	engine.EXPECT().CurrentHeader(gomock.Any()).Return(header, nil).AnyTimes()
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _, head common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			h.fcuHeads = append(h.fcuHeads, head)
			return nil, nil
		}).AnyTimes()

	persistDir := filepath.Join(t.TempDir(), "collector")
	c := NewPersistentBlockCollector(t.Context(), log.New(), engine, &clparams.MainnetBeaconConfig, persistDir)
	require.NotNil(t, c)
	t.Cleanup(func() { _ = c.Close() })
	h.collector = c
	return h
}

// TestPruneCaseC_FCUNudgePreservesCache pins the new gap-prune
// semantics: when there's a gap between elHead and the lowest cached
// block, prune fires ONE FCU at the lowest cached block's hash and
// KEEPS the cached payload. The previous behaviour (unconditional
// delete on Case C) stranded EL forever behind the cache during
// mode-B unwind recovery.
func TestPruneCaseC_FCUNudgePreservesCache(t *testing.T) {
	const elHead = uint64(100)
	h := pruneCaseCTestHarness(t, elHead)

	// Cached blocks at 200, 201, 202 — gap of 99 from elHead.
	b1 := makeBeaconBlock(t, 200, 'a', common.HexToHash("0xdead"))
	b2 := makeBeaconBlock(t, 201, 'a', blockHash(b1))
	b3 := makeBeaconBlock(t, 202, 'a', blockHash(b2))
	for _, bb := range []*cltypes.BeaconBlock{b1, b2, b3} {
		require.NoError(t, h.collector.AddBlock(bb))
	}

	require.NoError(t, h.collector.pruneStaleCachedBlocks(t.Context()))

	// Cache must survive — none of the three rows deleted.
	require.Equal(t, 3, countRowsAtOrAbove(t, h.collector.db, 200),
		"Case C must keep cached rows so EL can catch up to them via Execution stage; pre-fix this was 0 (unconditional delete)")

	// Exactly one FCU fired, targeting the lowest cached block.
	require.Len(t, h.fcuHeads, 1, "Case C must fire exactly one FCU nudge per prune cycle")
	require.Equal(t, blockHash(b1), h.fcuHeads[0],
		"FCU target must be the lowest cached block's hash so engineapi initialCycle drives Execution forward to it")
}

// TestPruneCaseC_TrimsCacheTailPastMaxAhead pins the upper-bound
// guard: if the cache grows past firstPast + caseCMaxCachedAhead (EL
// not catching up), the tail is trimmed to bound memory growth.
func TestPruneCaseC_TrimsCacheTailPastMaxAhead(t *testing.T) {
	const elHead = uint64(100)
	// Use a tiny cap for the test so we don't have to add 16k+ blocks.
	origMax := caseCMaxCachedAhead
	caseCMaxCachedAhead = 3
	t.Cleanup(func() { caseCMaxCachedAhead = origMax })

	h := pruneCaseCTestHarness(t, elHead)

	// 6 cached blocks at 200..205. With cap=3, blocks 200,201,202,203
	// are kept (firstPast=200, trimAt=200+3=203 inclusive on Seek);
	// blocks 204,205 trimmed.
	var prev common.Hash = common.HexToHash("0xdead")
	for n := uint64(200); n <= 205; n++ {
		bb := makeBeaconBlock(t, n, 'a', prev)
		require.NoError(t, h.collector.AddBlock(bb))
		prev = blockHash(bb)
	}

	require.NoError(t, h.collector.pruneStaleCachedBlocks(t.Context()))

	// firstPast + cap = 200 + 3 = 203 is the trim threshold; the Seek
	// hits 203 first and deletes everything from there onward.
	// Expected survivors: 200, 201, 202 (3 rows).
	require.Equal(t, 3, countRowsAtOrAbove(t, h.collector.db, 200),
		"trim cap must keep exactly cap blocks: 200..(200+cap-1)")
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 203),
		"rows past firstPast + cap must be trimmed (cap=3 means rows ≥ 203 deleted)")

	// FCU still fires at the lowest cached block.
	require.Len(t, h.fcuHeads, 1)
}
