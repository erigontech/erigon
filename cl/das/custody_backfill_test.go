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
	"bytes"
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/p2p/enode"
)

// bodyReader serves blocks without their execution payload, by slot, and records the slots read.
type bodyReader struct {
	freezeblocks.BeaconSnapshotReader
	blocks map[uint64]*cltypes.SignedBeaconBlock
	read   *[]uint64
	onRead func()
}

func (r bodyReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	if r.read != nil {
		*r.read = append(*r.read, slot)
	}
	if r.onRead != nil {
		r.onRead()
	}
	return r.blocks[slot], nil
}

type forkChoiceBlocks map[common.Hash]*cltypes.SignedBeaconBlock

func (f forkChoiceBlocks) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block, ok := f[root]
	return block, ok
}

func (forkChoiceBlocks) Slot() uint64 { return 0 }

// backfillFixture is an archive node at currentSlot whose canonical chain holds the given blocks.
// complete holds the slots whose columns are already stored. held holds the blocks fork choice
// still has.
type backfillFixture struct {
	d     *peerdas
	roots map[uint64]common.Hash
	held  forkChoiceBlocks
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
	columnStorage.EXPECT().ColumnSidecarExists(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, slot uint64, _ common.Hash, _ int64) (bool, error) {
			return complete[slot], nil
		}).AnyTimes()
	blobStorage := blob_storage_mock_services.NewMockBlobStorage(ctrl)
	blobStorage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(0), nil).AnyTimes()
	held := forkChoiceBlocks{}
	return &backfillFixture{d: &peerdas{
		beaconConfig:  &cfg,
		ethClock:      clock,
		caplinConfig:  &clparams.CaplinConfig{ArchiveBlobs: true},
		columnStorage: columnStorage,
		blobStorage:   blobStorage,
		indiciesDB:    db,
		blockReader:   bodyReader{blocks: blocks, read: &[]uint64{}},
		forkChoice:    held,
	}, roots: roots, held: held}
}

// storeSideBranchBlock stores block as an accepted, non-canonical block held by the fork choice. It
// returns the root of the block.
func (f *backfillFixture) storeSideBranchBlock(t *testing.T, block *cltypes.SignedBeaconBlock) common.Hash {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.NoError(t, f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, false)
	}))
	f.held[root] = block
	return root
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

func (f *backfillFixture) queuedRoots() []common.Hash {
	f.d.blocksToCheckSync.mu.Lock()
	defer f.d.blocksToCheckSync.mu.Unlock()
	return slices.Collect(maps.Keys(f.d.blocksToCheckSync.entries))
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

// Blocks below the column serve range, canonical or not, are no longer requested by peers, so they
// are skipped.
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
	side := blockWithBlobs(t, serveStart-1, true)
	side.Block.ProposerIndex = 1
	f.storeSideBranchBlock(t, side)

	f.d.backfillCustodyColumns(t.Context(), serveStart-1, serveStart)

	require.Equal(t, []uint64{serveStart}, f.queuedSlots())
}

// A request that comes while another one waits is merged into it, so no slot of a later forward
// sync range is lost while the worker walks an earlier one.
func TestBackfillCustodyColumnsMergesWaitingRequests(t *testing.T) {
	d := &peerdas{backfills: make(chan struct{}, 1)}
	d.BackfillCustodyColumns(1, 50)
	r, ok := d.takeBackfillRequest() // the worker starts the first request
	require.True(t, ok)
	require.Equal(t, slotRange{from: 1, to: 50}, r)

	d.BackfillCustodyColumns(30, 60)
	d.BackfillCustodyColumns(40, 70)
	d.BackfillCustodyColumns(9, 5) // an empty range is ignored

	r, ok = d.takeBackfillRequest()
	require.True(t, ok)
	require.Equal(t, slotRange{from: 30, to: 70}, r)
	_, ok = d.takeBackfillRequest()
	require.False(t, ok)
	require.Len(t, d.backfills, 1, "the worker is woken once, without blocking the callers")
}

// Forward sync stores its blocks before the fork choice stage marks them canonical, so the
// backfill waits until the canonical chain reaches the end of its range.
func TestBackfillCustodyColumnsWaitsForTheCanonicalChain(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
	}, nil)
	f.d.blockReader.(bodyReader).blocks[14] = blockWithBlobs(t, 14, true)
	marked := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		marked <- f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
			return beacon_indicies.MarkRootCanonical(tx, 14, common.Hash{14, 0xab})
		})
	}()

	start := time.Now()
	f.d.backfillCustodyColumns(t.Context(), 10, 14)

	require.NoError(t, <-marked)
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	require.Equal(t, []uint64{10, 14}, f.queuedSlots())
}

func gloasBlockWithBlobs(t *testing.T, slot uint64) *cltypes.SignedBeaconBlock {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	commitments := block.GetBlobKzgCommitments()
	require.NotNil(t, commitments)
	commitments.Append(&cltypes.KZGCommitment{})
	return block
}

// A Gloas block commits to blobs in its bid, but the columns exist only if the builder revealed
// the payload. Forward sync imports a block without an envelope when none was revealed, so the
// backfill queues only the blocks whose envelope was processed.
func TestBackfillCustodyColumnsSkipsGloasBlocksWithoutAnEnvelope(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: gloasBlockWithBlobs(t, 10),
		12: gloasBlockWithBlobs(t, 12),
	}, nil)
	require.NoError(t, f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionBlockHash(tx, f.roots[10], common.Hash{0x10})
	}))

	f.d.backfillCustodyColumns(t.Context(), 10, 12)

	require.Equal(t, []uint64{10}, f.queuedSlots())
}

// The walk reads a side-branch block from fork choice, so it skips a stored root that fork choice
// does not hold, such as a branch that was pruned.
func TestBackfillCustodyColumnsSkipsSideBranchBlocksForkChoiceDoesNotHold(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		12: blockWithBlobs(t, 12, true),
	}, nil)
	held := blockWithBlobs(t, 12, true)
	held.Block.ProposerIndex = 1
	heldRoot := f.storeSideBranchBlock(t, held)
	pruned := blockWithBlobs(t, 12, true)
	pruned.Block.ProposerIndex = 2
	delete(f.held, f.storeSideBranchBlock(t, pruned))

	f.d.backfillCustodyColumns(t.Context(), 12, 12)

	require.ElementsMatch(t, []common.Hash{f.roots[12], heldRoot}, f.queuedRoots())
}

func TestBackfillCustodyColumnsWithoutForkChoiceQueuesOnlyCanonicalBlocks(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		12: blockWithBlobs(t, 12, true),
	}, nil)
	side := blockWithBlobs(t, 12, true)
	side.Block.ProposerIndex = 1
	f.storeSideBranchBlock(t, side)
	f.d.forkChoice = nil

	f.d.backfillCustodyColumns(t.Context(), 12, 12)

	require.Equal(t, []common.Hash{f.roots[12]}, f.queuedRoots())
}

// As for canonical blocks, the side-branch blocks queued are those that commit to blobs and miss
// columns, and nothing else.
func TestBackfillCustodyColumnsQueuesSideBranchBlocksMissingColumns(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		14: blockWithBlobs(t, 14, true),
	}, map[uint64]bool{12: true})
	missingRoot := f.storeSideBranchBlock(t, blockWithBlobs(t, 10, true))
	f.storeSideBranchBlock(t, blockWithBlobs(t, 11, false))
	f.storeSideBranchBlock(t, blockWithBlobs(t, 12, true))

	f.d.backfillCustodyColumns(t.Context(), 10, 14)

	require.ElementsMatch(t, []common.Hash{missingRoot, f.roots[14]}, f.queuedRoots())
}

// A side-branch Gloas block, like a canonical one, has columns only once its payload was revealed.
func TestBackfillCustodyColumnsQueuesSideBranchGloasBlocksOnlyWithAnEnvelope(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: gloasBlockWithBlobs(t, 10),
	}, nil)
	revealed := gloasBlockWithBlobs(t, 10)
	revealed.Block.ProposerIndex = 1
	revealedRoot := f.storeSideBranchBlock(t, revealed)
	withheld := gloasBlockWithBlobs(t, 10)
	withheld.Block.ProposerIndex = 2
	f.storeSideBranchBlock(t, withheld)
	require.NoError(t, f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionBlockHash(tx, revealedRoot, common.Hash{0x10})
	}))

	f.d.backfillCustodyColumns(t.Context(), 10, 10)

	require.Equal(t, []common.Hash{revealedRoot}, f.queuedRoots())
	f.d.blocksToCheckSync.mu.Lock()
	entry := f.d.blocksToCheckSync.entries[revealedRoot]
	f.d.blocksToCheckSync.mu.Unlock()
	require.Equal(t, clparams.GloasVersion, entry.block.Version())
}

// A side-branch root is queued like a canonical one: compact, and given up at the maximum backoff.
func TestBackfillCustodyColumnsQueuesSideBranchBlocksAsBackfillRoots(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		12: blockWithBlobs(t, 12, true),
	}, nil)
	side := blockWithBlobs(t, 12, true)
	side.Block.ProposerIndex = 1
	sideRoot := f.storeSideBranchBlock(t, side)

	f.d.backfillCustodyColumns(t.Context(), 12, 12)

	f.d.blocksToCheckSync.mu.Lock()
	entry := f.d.blocksToCheckSync.entries[sideRoot]
	f.d.blocksToCheckSync.mu.Unlock()
	require.NotNil(t, entry)
	queuedBlock, ok := entry.block.(*backfillColumnBlock)
	require.True(t, ok, "the entry is a compact backfill block")
	require.Equal(t, sideRoot, queuedBlock.root)
	require.Equal(t, uint64(12), queuedBlock.GetSlot())
	require.Equal(t, 1, queuedBlock.GetBlobKzgCommitments().Len())
	require.True(t, f.d.blocksToCheckSync.isBackfill(sideRoot))
	require.False(t, entry.nextAttempt.After(time.Now()), "a backfilled root is due at once")
}

// Fork choice holds the canonical block too, but the canonical walk owns that root: a root that is
// canonical when the side pass reads its slot is not queued by it.
func TestBackfillCustodyColumnsSideBranchPassLeavesTheCanonicalRoot(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{}, nil)
	block := blockWithBlobs(t, 12, true)
	root := f.storeSideBranchBlock(t, block)
	require.NoError(t, f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, 12, root)
	}))

	f.d.backfillCustodyColumns(t.Context(), 12, 12) // the body reader holds no block for the canonical walk

	require.Empty(t, f.queuedRoots())
}

// A root that is canonical when the first side pass reads its slot and is reorged out while the
// walk waits for the canonical chain is queued by the second side pass.
func TestBackfillCustodyColumnsQueuesTheRootReorgedOutWhileWaitingForTheCanonicalChain(t *testing.T) {
	newCanonical := blockWithBlobs(t, 12, true)
	newCanonical.Block.ProposerIndex = 2
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{12: newCanonical}, nil)
	reorgedOut := blockWithBlobs(t, 12, true)
	reorgedOut.Block.ProposerIndex = 1
	reorgedOutRoot := f.storeSideBranchBlock(t, reorgedOut)
	newCanonicalRoot := f.storeSideBranchBlock(t, newCanonical)
	markCanonical := func(root common.Hash) {
		require.NoError(t, f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
			return beacon_indicies.MarkRootCanonical(tx, 12, root)
		}))
	}
	markCanonical(reorgedOutRoot)
	views := &countingDB{RwDB: f.d.indiciesDB}
	f.d.indiciesDB = views
	views.onView = func() {
		if views.views == 2 { // the side pass read slot 12 in the first view, the wait starts in the second
			markCanonical(newCanonicalRoot)
		}
	}

	f.d.backfillCustodyColumns(t.Context(), 12, 12)

	require.ElementsMatch(t, []common.Hash{reorgedOutRoot, newCanonicalRoot}, f.queuedRoots())
}

// Once the side pass has its quota it ends, instead of walking the older slots of the range.
func TestBackfillCustodyColumnsSideBranchPassEndsOnceItsQuotaIsReached(t *testing.T) {
	const toSlot = 1 << 30
	f := newBackfillFixture(t, 100, nil, nil)
	for i := range maxBackfillQueued / 2 {
		f.storeSideBranchBlock(t, blockWithBlobs(t, toSlot-uint64(i), true))
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	f.d.backfillSideBranchBlocks(ctx, 0, toSlot)

	require.NoError(t, ctx.Err(), "the pass ended on its own")
	require.Len(t, f.queuedRoots(), maxBackfillQueued/2)
}

// The walk down to slot 0 ends there instead of wrapping around.
func TestBackfillCustodyColumnsEndsAtSlotZero(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		0: blockWithBlobs(t, 0, true),
		2: blockWithBlobs(t, 2, true),
	}, nil)
	side := blockWithBlobs(t, 0, true)
	side.Block.ProposerIndex = 1
	sideRoot := f.storeSideBranchBlock(t, side)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	f.d.backfillCustodyColumns(ctx, 0, 2)

	require.NoError(t, ctx.Err(), "the walk ended on its own")
	require.ElementsMatch(t, []common.Hash{f.roots[0], f.roots[2], sideRoot}, f.queuedRoots())
}

// Right after a partition heals, a side branch can end above the canonical head. Its roots are
// queued at once, not once the canonical head has reached the end of the range.
func TestBackfillCustodyColumnsQueuesSideBranchBlocksWithoutWaitingForTheCanonicalHead(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
	}, nil)
	side := blockWithBlobs(t, 12, true)
	sideRoot := f.storeSideBranchBlock(t, side)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	f.d.backfillCustodyColumns(ctx, 10, 12)

	require.Equal(t, []common.Hash{sideRoot}, f.queuedRoots())
}

// Side-branch roots are best effort: the side pass takes at most half of the queue, newest roots
// first, and never waits for room, so roots that peers cannot serve do not stall the canonical
// blocks.
func TestBackfillCustodyColumnsSideBranchBlocksDoNotStallCanonicalBlocks(t *testing.T) {
	const slots, sideBranches = 50, 3
	blocks := map[uint64]*cltypes.SignedBeaconBlock{}
	for slot := uint64(10); slot < 10+slots; slot++ {
		blocks[slot] = blockWithBlobs(t, slot, true)
	}
	f := newBackfillFixture(t, 100, blocks, nil)
	sideSlots := map[common.Hash]uint64{}
	for slot := uint64(10); slot < 10+slots; slot++ {
		for proposer := uint64(1); proposer <= sideBranches; proposer++ {
			side := blockWithBlobs(t, slot, true)
			side.Block.ProposerIndex = proposer
			sideSlots[f.storeSideBranchBlock(t, side)] = slot
		}
	}
	served := map[common.Hash]bool{}
	serveCanonicalRoots := func() {
		for _, root := range f.queuedRoots() {
			if _, isSide := sideSlots[root]; !isSide {
				served[root] = true
				f.d.blocksToCheckSync.forget(root)
			}
		}
	}
	reader := f.d.blockReader.(bodyReader)
	reader.onRead = serveCanonicalRoots
	f.d.blockReader = reader
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	f.d.backfillCustodyColumns(ctx, 10, 10+slots-1)
	serveCanonicalRoots()

	require.Len(t, served, slots)
	var queuedSideSlots []uint64
	for _, root := range f.queuedRoots() {
		queuedSideSlots = append(queuedSideSlots, sideSlots[root])
	}
	slices.Sort(queuedSideSlots)
	newestFirst := make([]uint64, maxBackfillQueued/2)
	for i := range newestFirst {
		newestFirst[i] = 10 + slots - 1 - uint64(i/sideBranches)
	}
	slices.Sort(newestFirst)
	require.Equal(t, newestFirst, queuedSideSlots,
		"only side-branch roots are left: half of the bound, from the newest slots")
}

func TestBackfillCustodyColumnsTakesTheSideBranchSignatureFromTheStoredHeader(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		12: blockWithBlobs(t, 12, true),
	}, nil)
	side := blockWithBlobs(t, 12, true)
	side.Block.ProposerIndex = 1
	stored := common.Bytes96{1, 2, 3}
	side.Signature = stored
	sideRoot := f.storeSideBranchBlock(t, side)
	side.Signature = common.Bytes96{9, 9, 9} // the fork graph holds a copy that was not checked

	f.d.backfillCustodyColumns(t.Context(), 12, 12)

	f.d.blocksToCheckSync.mu.Lock()
	entry := f.d.blocksToCheckSync.entries[sideRoot]
	f.d.blocksToCheckSync.mu.Unlock()
	require.NotNil(t, entry)
	require.Equal(t, stored, entry.block.BlockSignature())
}

// An error on one side-branch root skips that root only.
func TestBackfillCustodyColumnsSkipsOnlyTheSideBranchBlockThatCannotBeRead(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		12: blockWithBlobs(t, 12, true),
	}, nil)
	var sideRoots []common.Hash
	for proposer := uint64(1); proposer <= 2; proposer++ {
		side := blockWithBlobs(t, 12, true)
		side.Block.ProposerIndex = proposer
		sideRoots = append(sideRoots, f.storeSideBranchBlock(t, side))
	}
	slices.SortFunc(sideRoots, func(a, b common.Hash) int { return bytes.Compare(a[:], b[:]) })
	unreadable, readable := sideRoots[0], sideRoots[1] // the walk reads roots in key order
	require.NoError(t, f.d.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.BeaconBlockHeaders, unreadable[:], []byte{1})
	}))

	f.d.backfillCustodyColumns(t.Context(), 12, 12)

	require.ElementsMatch(t, []common.Hash{f.roots[12], readable}, f.queuedRoots())
}

// Once the queue holds its half of the room, the side pass reads no slot.
func TestBackfillCustodyColumnsSideBranchPassReadsNothingWhenTheQueueIsHalfFull(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{}, nil)
	f.storeSideBranchBlock(t, blockWithBlobs(t, 12, true))
	cfg := clparams.MainnetBeaconConfig
	for i := range maxBackfillQueued / 2 {
		queuedBlock, root := fuluColumnBlock(t, &cfg, uint64(200+i))
		f.d.blocksToCheckSync.add(root, queuedBlock, time.Now())
	}
	views := &countingDB{RwDB: f.d.indiciesDB}
	f.d.indiciesDB = views

	f.d.backfillSideBranchBlocks(t.Context(), 10, 14)

	require.Zero(t, views.views)
}

// A root at the maximum backoff is retried once an epoch and may never complete. Such roots do not
// count toward the backfill's bound, so they cannot stall the walk.
func TestBackfillCustodyColumnsIsNotStalledByRootsAtTheMaximumBackoff(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
	}, nil)
	cfg := clparams.MainnetBeaconConfig
	for i := range maxBackfillQueued {
		stuck, root := fuluColumnBlock(t, &cfg, uint64(200+i))
		f.d.blocksToCheckSync.add(root, stuck, time.Time{})
		for range cfg.SlotsPerEpoch {
			f.d.blocksToCheckSync.failed(root, time.Now(), time.Second, cfg.SlotsPerEpoch)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	f.d.backfillCustodyColumns(ctx, 10, 10)

	require.Contains(t, f.queuedSlots(), uint64(10))
}

// Before Fulu no block has columns, so the walk reads nothing.
func TestBackfillCustodyColumnsDoesNothingBeforeFulu(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		50: blockWithBlobs(t, 50, true),
	}, nil)
	f.d.beaconConfig.FuluForkEpoch = 10 // slot 320, after the fixture's current slot
	f.d.beaconConfig.InitializeForkSchedule()

	f.d.backfillCustodyColumns(t.Context(), 10, 50)

	require.Empty(t, *f.d.blockReader.(bodyReader).read)
}

// A cancelled walk stops at once instead of trying every remaining slot, in the side pass as well
// as in the canonical walk.
func TestBackfillCustodyColumnsStopsWhenCancelled(t *testing.T) {
	for _, tc := range []struct {
		name      string
		inWalk    bool
		wantReads []uint64
	}{
		{name: "side pass"},
		{name: "canonical walk", inWalk: true, wantReads: []uint64{50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
				50: blockWithBlobs(t, 50, true),
			}, nil)
			ctx, cancel := context.WithCancel(t.Context())
			views := &countingDB{RwDB: f.d.indiciesDB}
			f.d.indiciesDB = views
			reader := f.d.blockReader.(bodyReader)
			if tc.inWalk {
				f.d.forkChoice = nil
				reader.onRead = cancel
				f.d.blockReader = reader
			} else {
				views.onView = cancel
			}

			f.d.backfillCustodyColumns(ctx, 10, 50)

			require.ElementsMatch(t, tc.wantReads, *reader.read)
			require.LessOrEqual(t, views.views, 2, "at most two views: the cancel stops the walk")
		})
	}
}

type countingDB struct {
	kv.RwDB
	views  int
	onView func()
}

func (c *countingDB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	c.views++
	if c.onView != nil {
		c.onView()
	}
	return c.RwDB.View(ctx, f)
}

// A root the backfill queued is given up at the maximum backoff: on a range peers cannot serve,
// such roots would otherwise stay for the serve range and be retried every epoch. A canonical root
// queued by the live path stays.
func TestColumnSyncGivesUpBackfillRootsAtTheMaximumBackoff(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
		12: blockWithBlobs(t, 12, true),
	}, nil)
	f.d.backfillCustodyColumns(t.Context(), 10, 10)
	live, _ := fuluColumnBlock(t, f.d.beaconConfig, 12)
	f.d.blocksToCheckSync.add(f.roots[12], live, time.Time{})
	require.Equal(t, []uint64{10, 12}, f.queuedSlots())

	fail := func() {
		f.d.columnSyncRoundFailed(t.Context(), f.roots[10], 10, time.Now())
		f.d.columnSyncRoundFailed(t.Context(), f.roots[12], 12, time.Now())
	}
	for range f.d.beaconConfig.SlotsPerEpoch - 1 {
		fail()
	}
	require.Equal(t, []uint64{10, 12}, f.queuedSlots(), "a backfill root is kept below the maximum backoff")

	fail()
	require.Equal(t, []uint64{12}, f.queuedSlots())
}

// On a custody node, not in archive mode, a block is complete once its custody columns are stored.
func TestBackfillCustodyColumnsUsesTheCustodyColumnsOfACustodyNode(t *testing.T) {
	f := newBackfillFixture(t, 100, map[uint64]*cltypes.SignedBeaconBlock{
		10: blockWithBlobs(t, 10, true),
		12: blockWithBlobs(t, 12, true),
	}, map[uint64]bool{12: true})
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	initTestBeaconConfig(f.d.beaconConfig)
	custody := peerdasstate.NewPeerDasState(f.d.beaconConfig, &clparams.NetworkConfig{})
	custody.SetLocalNodeID(enode.NewLocalNode(nodeDB, key))
	f.d.state = custody
	f.d.caplinConfig = &clparams.CaplinConfig{}
	require.False(t, f.d.IsArchivedMode())

	f.d.backfillCustodyColumns(t.Context(), 10, 12)

	require.Equal(t, []uint64{10}, f.queuedSlots())
}
