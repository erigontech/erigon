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

package das

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

// bodyReader serves blocks without their execution payload, by slot.
type bodyReader struct {
	freezeblocks.BeaconSnapshotReader
	blocks map[uint64]*cltypes.SignedBeaconBlock
}

func (r bodyReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return r.blocks[slot], nil
}

// backfillFixture is an archive node at slot 100 whose canonical chain holds the given blocks.
// complete holds the slots whose columns are already stored.
type backfillFixture struct {
	d     *peerdas
	roots map[uint64]common.Hash
}

func newBackfillFixture(t *testing.T, currentSlot uint64, blocks map[uint64]*cltypes.SignedBeaconBlock, complete map[uint64]bool) *backfillFixture {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	roots := map[uint64]common.Hash{}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for slot := range blocks {
			roots[slot] = common.Hash{byte(slot), 0xab}
			if err := beacon_indicies.MarkRootCanonical(tx, slot, roots[slot]); err != nil {
				return err
			}
		}
		return nil
	}))
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, slot uint64, _ common.Hash) ([]uint64, error) {
			if complete[slot] {
				return slices.Collect(func(yield func(uint64) bool) {
					for i := range cfg.NumberOfColumns {
						if !yield(i) {
							return
						}
					}
				}), nil
			}
			return nil, nil
		}).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	return &backfillFixture{d: &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
		indiciesDB:    db,
		blockReader:   bodyReader{blocks: blocks},
	}, roots: roots}
}

func (f *backfillFixture) queuedSlots() []uint64 {
	f.d.blocksToCheckSync.mu.Lock()
	defer f.d.blocksToCheckSync.mu.Unlock()
	var slots []uint64
	for _, e := range f.d.blocksToCheckSync.entries {
		slots = append(slots, e.block.GetSlot())
	}
	slices.Sort(slots)
	return slots
}

func blockWithBlobs(t *testing.T, slot uint64, blobs bool) *cltypes.SignedBeaconBlock {
	cfg := clparams.MainnetBeaconConfig
	block, _ := fuluColumnBlock(t, &cfg, slot)
	if !blobs {
		block.Block.Body.BlobKzgCommitments.Clear()
	}
	return block
}

// Forward sync imports blocks without fetching their columns. The backfill queues the blocks of
// the range that commit to blobs and miss columns, and nothing else.
func TestBackfillCustodyColumnsQueuesBlocksMissingColumns(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
		11: blockWithBlobs(t, 11, false),
		12: blockWithBlobs(t, 12, true),
		14: blockWithBlobs(t, 14, true),
		20: blockWithBlobs(t, 20, true), // above the range
	}, map[uint64]bool{12: true})

	f.d.backfillCustodyColumns(t.Context(), 10, 14)

	require.Equal(t, []uint64{10, 14}, f.queuedSlots())
	f.d.blocksToCheckSync.mu.Lock()
	entry := f.d.blocksToCheckSync.entries[f.roots[14]]
	f.d.blocksToCheckSync.mu.Unlock()
	require.NotNil(t, entry)
	root, err := entry.block.BlockHashSSZ()
	require.NoError(t, err)
	require.Equal(t, f.roots[14], common.Hash(root), "the entry carries the canonical root")
	require.Equal(t, 1, entry.block.GetBlobKzgCommitments().Len())
	require.False(t, entry.nextAttempt.After(time.Now()), "a backfilled root is due at once")
}

// The backfill keeps at most maxBackfillQueued roots in the queue and walks from the newest slot,
// so the most recent blocks are served first.
func TestBackfillCustodyColumnsQueuesNewestFirstWithinItsBound(t *testing.T) {
	blocks := map[uint64]*cltypes.SignedBeaconBlock{}
	for slot := uint64(10); slot < 10+2*maxBackfillQueued; slot++ {
		blocks[slot] = blockWithBlobs(t, slot, true)
	}
	f := newBackfillFixture(t, 100, blocks, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	f.d.backfillCustodyColumns(ctx, 10, 10+2*maxBackfillQueued-1)

	queued := f.queuedSlots()
	require.Len(t, queued, maxBackfillQueued)
	require.Equal(t, uint64(10+maxBackfillQueued), queued[0], "the newest slots are queued first")
}

// Blocks below the column serve range are no longer requested by peers, so they are skipped.
func TestBackfillCustodyColumnsSkipsSlotsBelowTheServeRange(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = 0 // as in the fixture
	currentSlot := (cfg.MinEpochsForDataColumnSidecarsRequests + 10) * cfg.SlotsPerEpoch
	serveStart := cfg.DataColumnSidecarServeRangeStartSlot(currentSlot)
	require.Positive(t, serveStart)
	f := newBackfillFixture(t, currentSlot, map[uint64]*cltypes.SignedBeaconBlock{
		serveStart - 1: blockWithBlobs(t, serveStart-1, true),
		serveStart:     blockWithBlobs(t, serveStart, true),
	}, nil)

	f.d.backfillCustodyColumns(t.Context(), serveStart-1, serveStart)

	require.Equal(t, []uint64{serveStart}, f.queuedSlots())
}

func TestBackfillCustodyColumnsRequestIsAsynchronous(t *testing.T) {
	d := &peerdas{backfills: make(chan slotRange, 1)}
	d.BackfillCustodyColumns(5, 9)
	d.BackfillCustodyColumns(10, 12) // the channel is full: dropped, not blocking
	d.BackfillCustodyColumns(9, 5)   // an empty range is ignored
	require.Equal(t, slotRange{from: 5, to: 9}, <-d.backfills)
	require.Empty(t, d.backfills)
}

// Forward sync stores its blocks before the fork choice stage marks them canonical, so the
// backfill waits until the canonical chain reaches the end of its range.
func TestBackfillCustodyColumnsWaitsForTheCanonicalChain(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
	}, nil)
	late := blockWithBlobs(t, 14, true)
	f.d.blockReader.(bodyReader).blocks[14] = late
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = f.d.indiciesDB.(kv.RwDB).Update(context.Background(), func(tx kv.RwTx) error {
			return beacon_indicies.MarkRootCanonical(tx, 14, common.Hash{14, 0xab})
		})
	}()

	start := time.Now()
	f.d.backfillCustodyColumns(t.Context(), 10, 14)

	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	require.Equal(t, []uint64{10, 14}, f.queuedSlots())
}
