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
// given block number. forkTag seeds header.Extra so that two blocks at the same
// number produce distinct SSZ roots — mimicking competing beacon variants.
func makeBeaconBlock(t *testing.T, number uint64, forkTag byte) *cltypes.BeaconBlock {
	t.Helper()
	var zero uint64
	// ParentBeaconBlockRoot must be non-nil: decodeBlock always reconstructs it
	// from the stored 32-byte parentRoot prefix, so leaving the source header's
	// field nil would produce a different header.Hash() and fail RlpHeader's
	// consistency check.
	zeroHash := common.Hash{}
	header := &types.Header{
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

// flushTestHarness wires a PersistentBlockCollector to a gomock ExecutionEngine
// that records every batch passed to InsertBlocks.
type flushTestHarness struct {
	collector *PersistentBlockCollector
	inserted  [][]uint64
}

func newFlushTestHarness(t *testing.T, frozen uint64) *flushTestHarness {
	t.Helper()
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)

	h := &flushTestHarness{}
	engine.EXPECT().FrozenBlocks(gomock.Any()).Return(frozen).AnyTimes()
	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, blocks []*types.Block, _ bool) error {
			nums := make([]uint64, len(blocks))
			for i, b := range blocks {
				nums[i] = b.NumberU64()
			}
			h.inserted = append(h.inserted, nums)
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
	// Two beacon variants carry block 1 (different SSZ roots), then block 2.
	// The duplicate must not be mis-classified as a gap; both block numbers
	// should be inserted and the DB cleared.
	h := newFlushTestHarness(t, 0)

	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 1, 'a')))
	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 1, 'b')))
	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 2, 'a')))

	require.NoError(t, h.collector.Flush(t.Context()))

	// One variant of 1 and one of 2 — no duplicate, no spurious gap.
	var all []uint64
	for _, batch := range h.inserted {
		all = append(all, batch...)
	}
	require.Equal(t, []uint64{1, 2}, all)

	// Clean path drops the whole DB.
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushPreservesRowsPastGap(t *testing.T) {
	// Rows [1, 2, 4, 5]: a gap at 3 breaks iteration after 2. Post-gap rows
	// (4, 5) must survive so the next Flush can pick up once 3 re-downloads.
	h := newFlushTestHarness(t, 0)

	for _, n := range []uint64{1, 2, 4, 5} {
		require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, n, 'a')))
	}

	require.NoError(t, h.collector.Flush(t.Context()))

	var all []uint64
	for _, batch := range h.inserted {
		all = append(all, batch...)
	}
	require.Equal(t, []uint64{1, 2}, all)

	require.False(t, h.collector.HasBlock(1))
	require.False(t, h.collector.HasBlock(2))
	require.True(t, h.collector.HasBlock(4))
	require.True(t, h.collector.HasBlock(5))
	// Exactly the two post-gap rows should remain.
	require.Equal(t, 2, countRowsAtOrAbove(t, h.collector.db, 0))
}

func TestFlushDupThenGapKeepsPostGapRows(t *testing.T) {
	// Bug reproducer: two variants of block 1 followed by a real gap.
	// The duplicate must not crash iteration, and rows past the real gap
	// must survive so recovery is possible.
	h := newFlushTestHarness(t, 0)

	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 1, 'a')))
	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 1, 'b')))
	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 2, 'a')))
	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 4, 'a')))

	require.NoError(t, h.collector.Flush(t.Context()))

	var all []uint64
	for _, batch := range h.inserted {
		all = append(all, batch...)
	}
	require.Equal(t, []uint64{1, 2}, all)

	require.False(t, h.collector.HasBlock(1))
	require.False(t, h.collector.HasBlock(2))
	require.True(t, h.collector.HasBlock(4))
}

func TestFlushDropsRowsBelowFrozen(t *testing.T) {
	// Rows [2, 3] with minInsertableBlockNumber=3: block 2 is already frozen
	// and must be skipped, block 3 must be inserted, DB cleared cleanly.
	h := newFlushTestHarness(t, 3)

	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 2, 'a')))
	require.NoError(t, h.collector.AddBlock(makeBeaconBlock(t, 3, 'a')))

	require.NoError(t, h.collector.Flush(t.Context()))

	var all []uint64
	for _, batch := range h.inserted {
		all = append(all, batch...)
	}
	require.Equal(t, []uint64{3}, all)
	require.Equal(t, 0, countRowsAtOrAbove(t, h.collector.db, 0))
}
