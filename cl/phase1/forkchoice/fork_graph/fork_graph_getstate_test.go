package fork_graph

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// TestGetState_InfiniteLoopOnMissingStateFile reproduces the infinite loop in
// getState when a header exists at a dump slot but the corresponding state file
// is missing from disk and the block is not in the blocks map.
//
// Before the fix, getState spins forever because the !isSegmentPresent branch
// sets copyReferencedState = nil on readBeaconStateFromDisk failure and then
// continues the loop without advancing currentIteratorRoot.
func TestGetState_InfiniteLoopOnMissingStateFile(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))

	fg, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := fg.(*forkGraphDisk)

	// Craft a fake root and header at a dump slot (slot % dumpSlotFrequency == 0).
	fakeRoot := common.Hash{0xde, 0xad}
	fakeHeader := &cltypes.BeaconBlockHeader{
		Slot: dumpSlotFrequency * 100, // guaranteed to be a dump slot
	}

	// Store the header but NOT the block, and don't write a state file.
	// This creates the exact condition: header exists, block absent, state file missing.
	graph.headers.Store(fakeRoot, fakeHeader)

	done := make(chan struct{})
	var gotState *state.CachingBeaconState
	var gotErr error
	go func() {
		defer close(done)
		// getState should return (nil, nil), not loop forever.
		gotState, gotErr = graph.getState(fakeRoot, false, false)
	}()

	select {
	case <-done:
		require.Nil(t, gotState)
		require.NoError(t, gotErr)
	case <-time.After(3 * time.Second):
		t.Fatal("getState did not return within 3s — infinite loop detected")
	}
}

// TestGetState_AnchorOffDumpSlot pins that states descending from an anchor
// whose slot is not a multiple of dumpSlotFrequency stay reachable once
// currentState has moved past the anchor.
func TestGetState_AnchorOffDumpSlot(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	blockA := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blockB := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(blockA, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockB, block2, int(clparams.Phase0Version)))

	fg, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := fg.(*forkGraphDisk)

	// The fixture anchor sits at slot 0; move its header off a dump slot.
	anchorHeader, ok := graph.GetHeader(graph.anchorRoot)
	require.True(t, ok)
	offSlotHeader := *anchorHeader
	offSlotHeader.Slot = dumpSlotFrequency*100 + 3
	graph.headers.Store(graph.anchorRoot, &offSlotHeader)

	_, status, err := graph.AddChainSegment(blockA, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)
	_, status, err = graph.AddChainSegment(blockB, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)

	// currentState is now blockB, so both lookups must go through the anchor's state file.
	anchorCopy, err := graph.GetState(graph.anchorRoot, true)
	require.NoError(t, err)
	require.NotNil(t, anchorCopy)

	blockARoot, err := blockA.Block.HashSSZ()
	require.NoError(t, err)
	stateA, err := graph.GetState(blockARoot, true)
	require.NoError(t, err)
	require.NotNil(t, stateA)
	require.Equal(t, blockA.Block.Slot, stateA.Slot())
}
