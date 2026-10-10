package fork_graph

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
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

// offDumpSlotAnchor returns blockB and blockA's post-state, optionally
// advanced through empty slots, as an anchor whose block is off the dump grid.
func offDumpSlotAnchor(t *testing.T, advanceTo uint64) (*cltypes.SignedBeaconBlock, *state.CachingBeaconState) {
	t.Helper()
	blockA := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blockB := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(blockA, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockB, block2, int(clparams.Phase0Version)))
	genesis := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(genesis, anchor, int(clparams.Phase0Version)))

	g0, err := NewForkGraphDisk(genesis, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	postA, status, err := g0.AddChainSegment(blockA, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)
	anchorState, err := postA.Copy()
	require.NoError(t, err)
	if advanceTo > anchorState.Slot() {
		require.NoError(t, transition.DefaultMachine.ProcessSlots(anchorState, advanceTo))
	}
	require.NotZero(t, anchorState.LatestBlockHeader().Slot%dumpSlotFrequency)
	return blockB, anchorState
}

// TestGetState_AnchorOffDumpSlot pins that the anchor state is reloaded from
// disk when its block is off the dump grid and currentState has moved on.
func TestGetState_AnchorOffDumpSlot(t *testing.T) {
	for _, tc := range []struct {
		name      string
		advanceTo uint64
	}{
		{name: "post-block state", advanceTo: 0},
		{name: "state advanced through empty slots", advanceTo: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockB, anchorState := offDumpSlotAnchor(t, tc.advanceTo)
			wantRoot, err := anchorState.HashSSZ()
			require.NoError(t, err)
			wantSlot := anchorState.Slot()

			fg, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
			require.NoError(t, err)
			graph := fg.(*forkGraphDisk)

			_, status, err := graph.AddChainSegment(blockB, true)
			require.NoError(t, err)
			require.Equal(t, Success, status)

			got, err := graph.GetState(graph.anchorRoot, true)
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, wantSlot, got.Slot())
			gotRoot, err := got.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, common.Hash(wantRoot), common.Hash(gotRoot))
		})
	}
}
