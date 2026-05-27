package stages

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

// TestUpdateCanonicalChainReorgEvent verifies that the chain_reorg SSE event
// emitted by updateCanonicalChainInTheDatabase has the correct Depth and
// OldHeadBlock fields.
//
// Scenario (2-slot reorg, same length):
//
//	slot 100 (root_100) -- slot 101a (root_101a) -- slot 102a (root_102a)  <- old canonical
//	                   \-- slot 101b (root_101b) -- slot 102b (root_102b)  <- new head
//
// Expected reorg event:
//
//	Depth        = 2   (old head 102a minus fork point 100)
//	OldHeadBlock = root_102a  (the previous canonical tip, NOT the common ancestor)
func TestUpdateCanonicalChainReorgEvent(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state101a := common.Hash{0xa1}
	state102a := common.Hash{0xa2}
	state101b := common.Hash{0xb1}
	state102b := common.Hash{0xb2}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(ctx, tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(ctx, tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, state100, 100, true)
	writeBlock(root101a, root100, state101a, 101, true)
	writeBlock(root102a, root101a, state102a, 102, true)

	writeBlock(root101b, root100, state101b, 101, false)
	writeBlock(root102b, root101b, state102b, 102, false)

	reorg := drainReorgEvent(t, ctx, tx, 102, root102b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(102), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(2), reorg.Depth, "reorg Depth should be oldHeadSlot - forkPointSlot")
	require.Equal(t, root102a, reorg.OldHeadBlock, "OldHeadBlock should be the previous canonical tip")
	require.Equal(t, root102b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state102a, reorg.OldHeadState, "OldHeadState should match old canonical tip's state root")
	require.Equal(t, state102b, reorg.NewHeadState, "NewHeadState")
}

// TestUpdateCanonicalChainReorgShorterFork verifies the reorg event when a
// shorter fork wins — the old canonical chain extends past the new head slot.
//
// Scenario:
//
//	slot 100 (root_100) -- slot 101a -- slot 102a -- slot 103a  <- old canonical (tip at 103)
//	                   \-- slot 101b -- slot 102b               <- new head (tip at 102)
//
// Expected reorg event:
//
//	OldHeadBlock = root_103a  (the actual old tip, NOT root_102a)
//	Depth        = 3          (old head 103 minus fork point 100)
func TestUpdateCanonicalChainReorgShorterFork(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root103a := common.Hash{0x13, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}

	state100 := common.Hash{0xa0}
	state103a := common.Hash{0xa3}
	state102b := common.Hash{0xb2}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(ctx, tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(ctx, tx, slot, root))
		}
	}

	// Old canonical chain: 100 → 101a → 102a → 103a
	writeBlock(root100, common.Hash{0x99}, state100, 100, true)
	writeBlock(root101a, root100, common.Hash{0xa1}, 101, true)
	writeBlock(root102a, root101a, common.Hash{0xa2}, 102, true)
	writeBlock(root103a, root102a, state103a, 103, true)

	// New fork (shorter): 100 → 101b → 102b
	writeBlock(root101b, root100, common.Hash{0xb1}, 101, false)
	writeBlock(root102b, root101b, state102b, 102, false)

	reorg := drainReorgEvent(t, ctx, tx, 102, root102b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(102), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(3), reorg.Depth, "reorg Depth: old tip 103 - fork point 100 = 3")
	require.Equal(t, root103a, reorg.OldHeadBlock, "OldHeadBlock must be the actual old tip at slot 103, not 102a")
	require.Equal(t, root102b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state103a, reorg.OldHeadState, "OldHeadState must match old tip's state root")
	require.Equal(t, state102b, reorg.NewHeadState, "NewHeadState")
}

// TestUpdateCanonicalChainReorgLongerFork verifies the reorg event when the
// new fork extends past the old canonical tip.
//
// Scenario:
//
//	slot 100 (root_100) -- slot 101a -- slot 102a               <- old canonical (tip at 102)
//	                   \-- slot 101b -- slot 102b -- slot 103b  <- new head (tip at 103)
//
// Expected reorg event:
//
//	OldHeadBlock = root_102a  (the old canonical tip)
//	Depth        = 2          (old head 102 minus fork point 100)
func TestUpdateCanonicalChainReorgLongerFork(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root102a := common.Hash{0x12, 0xaa}
	root101b := common.Hash{0x11, 0xbb}
	root102b := common.Hash{0x12, 0xbb}
	root103b := common.Hash{0x13, 0xbb}

	state102a := common.Hash{0xa2}
	state103b := common.Hash{0xb3}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(ctx, tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(ctx, tx, slot, root))
		}
	}

	// Old canonical chain: 100 -> 101a -> 102a
	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101a, root100, common.Hash{0xa1}, 101, true)
	writeBlock(root102a, root101a, state102a, 102, true)

	// New fork (longer): 100 -> 101b -> 102b -> 103b
	writeBlock(root101b, root100, common.Hash{0xb1}, 101, false)
	writeBlock(root102b, root101b, common.Hash{0xb2}, 102, false)
	writeBlock(root103b, root102b, state103b, 103, false)

	reorg := drainReorgEvent(t, ctx, tx, 103, root103b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(103), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(2), reorg.Depth, "reorg Depth: old tip 102 - fork point 100 = 2")
	require.Equal(t, root102a, reorg.OldHeadBlock, "OldHeadBlock must be the old tip at slot 102")
	require.Equal(t, root103b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state102a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state103b, reorg.NewHeadState, "NewHeadState")
}

// TestUpdateCanonicalChainNoReorg verifies that extending the canonical chain
// (new head is a child of the current tip) does NOT emit a chain_reorg event.
//
// Scenario:
//
//	slot 100 -- slot 101 -- slot 102 (new head, child of 101)
func TestUpdateCanonicalChainNoReorg(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101 := common.Hash{0x11}
	root102 := common.Hash{0x12}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(ctx, tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(ctx, tx, slot, root))
		}
	}

	// Existing canonical chain: 100 -> 101
	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101, root100, common.Hash{0xa1}, 101, true)

	// New block extends the chain: 101 -> 102
	writeBlock(root102, root101, common.Hash{0xa2}, 102, false)

	reorg := drainReorgEvent(t, ctx, tx, 102, root102)
	require.Nil(t, reorg, "chain extension should NOT emit a chain_reorg event")
}

// TestUpdateCanonicalChainReorgOneSlot verifies a minimal 1-slot reorg.
//
// Scenario:
//
//	slot 100 -- slot 101a  <- old canonical
//	        \-- slot 101b  <- new head
//
// Expected: Depth = 1, OldHeadBlock = root_101a
func TestUpdateCanonicalChainReorgOneSlot(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	root100 := common.Hash{0x10}
	root101a := common.Hash{0x11, 0xaa}
	root101b := common.Hash{0x11, 0xbb}

	state101a := common.Hash{0xa1}
	state101b := common.Hash{0xb1}

	writeBlock := func(root, parentRoot, stateRoot common.Hash, slot uint64, canonical bool) {
		t.Helper()
		require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, root, slot))
		require.NoError(t, beacon_indicies.WriteParentBlockRoot(ctx, tx, root, parentRoot))
		require.NoError(t, beacon_indicies.WriteStateRoot(tx, root, stateRoot))
		if canonical {
			require.NoError(t, beacon_indicies.MarkRootCanonical(ctx, tx, slot, root))
		}
	}

	writeBlock(root100, common.Hash{0x99}, common.Hash{0xa0}, 100, true)
	writeBlock(root101a, root100, state101a, 101, true)
	writeBlock(root101b, root100, state101b, 101, false)

	reorg := drainReorgEvent(t, ctx, tx, 101, root101b)
	require.NotNil(t, reorg, "expected a chain_reorg event to be emitted")
	require.Equal(t, uint64(101), reorg.Slot, "reorg Slot")
	require.Equal(t, uint64(1), reorg.Depth, "reorg Depth: old tip 101 - fork point 100 = 1")
	require.Equal(t, root101a, reorg.OldHeadBlock, "OldHeadBlock must be root_101a")
	require.Equal(t, root101b, reorg.NewHeadBlock, "NewHeadBlock")
	require.Equal(t, state101a, reorg.OldHeadState, "OldHeadState")
	require.Equal(t, state101b, reorg.NewHeadState, "NewHeadState")
}

// drainReorgEvent calls updateCanonicalChainInTheDatabase and returns the
// first ChainReorgData event emitted, or nil if none arrives within 1 second.
func drainReorgEvent(t *testing.T, ctx context.Context, tx kv.RwTx, headSlot uint64, headRoot common.Hash) *beaconevents.ChainReorgData {
	t.Helper()
	emitter := beaconevents.NewEventEmitter()
	ch := make(chan *beaconevents.EventStream, 16)
	sub := emitter.State().Subscribe(ch)
	defer sub.Unsubscribe()

	cfg := &Cfg{
		emitter:   emitter,
		beaconCfg: &clparams.MainnetBeaconConfig,
	}

	err := updateCanonicalChainInTheDatabase(ctx, tx, headSlot, headRoot, cfg)
	require.NoError(t, err)

	timeout := time.After(time.Second)
	for {
		select {
		case evt := <-ch:
			if evt.Event == beaconevents.StateChainReorg {
				return evt.Data.(*beaconevents.ChainReorgData)
			}
		case <-timeout:
			return nil
		}
	}
}
