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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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
// that records every batch passed to InsertBlocks.
type flushTestHarness struct {
	collector *PersistentBlockCollector
	inserted  []*types.Block
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
		func(_ context.Context, blocks []*types.Block, _ bool) error {
			h.inserted = append(h.inserted, blocks...)
			return nil
		}).AnyTimes()
	engine.EXPECT().CurrentHeader(gomock.Any()).Return(nil, nil).AnyTimes()
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	persistDir := filepath.Join(t.TempDir(), "collector")
	c := NewPersistentBlockCollector(log.New(), engine, &clparams.MainnetBeaconConfig, 1, persistDir)
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

func TestFlushSkipsDuplicateBlockNumbers(t *testing.T) {
	// Two beacon variants carry block 1 (different SSZ roots), then block 2
	// chains off variant 'a'. The duplicate must not be mis-classified as a
	// gap; blocks 1 and 2 (specifically variant 'a') should be inserted.
	h := newFlushTestHarness(t, 0)

	b1a := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b1b := makeBeaconBlock(t, 1, 'b', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', blockHash(b1a))
	require.NoError(t, h.collector.AddBlock(b1a))
	require.NoError(t, h.collector.AddBlock(b1b))
	require.NoError(t, h.collector.AddBlock(b2))

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Equal(t, []uint64{1, 2}, h.insertedNumbers())
	// Look-ahead must pick the canonical 1a (whose BlockHash == b2.ParentHash),
	// not 1b.
	require.Equal(t, blockHash(b1a), h.inserted[0].Hash())
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
	// Two variants at the final height with no successor to disambiguate.
	// Guessing would risk the clean-path DB wipe discarding the canonical
	// variant, so we leave the rows for the next Flush.
	h := newFlushTestHarness(t, 0)

	b1 := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b2a := makeBeaconBlock(t, 2, 'a', blockHash(b1))
	b2b := makeBeaconBlock(t, 2, 'b', blockHash(b1))
	for _, bb := range []*cltypes.BeaconBlock{b1, b2a, b2b} {
		require.NoError(t, h.collector.AddBlock(bb))
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	// Block 1 resolves against b2a's (or b2b's — same parent) ParentHash.
	// Block 2's variants stay unresolved.
	require.Equal(t, []uint64{1}, h.insertedNumbers())
	require.False(t, h.collector.HasBlock(1))
	require.True(t, h.collector.HasBlock(2))
	require.Equal(t, 2, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushStopsWhenForkHasNoMatchingParent(t *testing.T) {
	// Two variants at height 1, and block 2's parent matches neither. This is
	// a fork we can't resolve forward: abort iteration, leave all rows for a
	// future Flush (with more data) to retry.
	h := newFlushTestHarness(t, 0)

	b1a := makeBeaconBlock(t, 1, 'a', common.Hash{})
	b1b := makeBeaconBlock(t, 1, 'b', common.Hash{})
	b2 := makeBeaconBlock(t, 2, 'a', common.HexToHash("0xdeadbeef"))
	require.NoError(t, h.collector.AddBlock(b1a))
	require.NoError(t, h.collector.AddBlock(b1b))
	require.NoError(t, h.collector.AddBlock(b2))

	require.NoError(t, h.collector.Flush(t.Context()))

	require.Empty(t, h.inserted)
	require.True(t, h.collector.HasBlock(1))
	require.True(t, h.collector.HasBlock(2))
	// All three rows should remain.
	require.Equal(t, 3, countRowsAtOrAbove(t, h.collector.db, 0))
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
	// Bug reproducer: two variants of block 1, block 2 chaining off 1a, then
	// a gap before block 4. Dup must not crash iteration; rows past the gap
	// must survive.
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
	require.Equal(t, blockHash(b1a), h.inserted[0].Hash())

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
