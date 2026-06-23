package fork_graph

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
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

	emitter := beaconevents.NewEventEmitter()
	fg := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}, emitter)
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
	go func() {
		defer close(done)
		// getState should return (nil, nil), not loop forever.
		graph.getState(fakeRoot, false, false)
	}()

	select {
	case <-done:
		// getState returned — no infinite loop.
	case <-time.After(3 * time.Second):
		t.Fatal("getState did not return within 3s — infinite loop detected")
	}
}
