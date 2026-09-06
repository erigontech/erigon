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

	fg := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
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

// TestGetState_AnchorAtNonDumpSlotIsReadable pins that the anchor's state is
// retrievable regardless of its slot.
//
// NewForkGraphDisk dumps the anchor with forced=true, deliberately bypassing
// the slot%dumpSlotFrequency sampling gate, because the anchor is the one node
// whose state must always be available: it is the only header-without-block
// entry in the graph, so every walk-back that reaches it has nowhere further to
// go. Re-applying the sampling gate on the read path made that forced dump
// unreadable whenever the anchor's slot was not a multiple of dumpSlotFrequency.
//
// The finalized checkpoint's block slot is a multiple of 4 only when the epoch
// boundary slot was actually proposed. When that proposal is missed — ordinary
// network behaviour — the anchor lands on a non-dump slot, getState returns
// (nil, nil), and fork choice fails with "baseState not found in graph" every
// slot with no recovery, because the code that would refresh the anchor runs
// downstream of the failure.
func TestGetState_AnchorAtNonDumpSlotIsReadable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset uint64
	}{
		{"dump slot", 0},
		{"one past a dump slot", 1},
		{"two past a dump slot", 2},
		{"three past a dump slot", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anchorState := state.New(&clparams.MainnetBeaconConfig)
			require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))

			// The read gate tests the anchor HEADER's slot, so move that —
			// the state's own slot is not what getState looks at.
			hdr := anchorState.LatestBlockHeader()
			base := (hdr.Slot / dumpSlotFrequency) * dumpSlotFrequency
			hdr.Slot = base + tc.offset
			anchorState.SetLatestBlockHeader(&hdr)
			anchorState.SetSlot(hdr.Slot)

			fg := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
			graph := fg.(*forkGraphDisk)

			// Drop the in-memory fast path so the lookup has to go to disk,
			// which is the post-restart condition this reproduces.
			graph.currentState = nil
			graph.currentStateBlockRoot = common.Hash{}

			got, err := graph.getState(graph.anchorRoot, true, false)
			require.NoError(t, err)
			require.NotNil(t, got,
				"anchor state must be readable at slot %d (mod %d = %d) — it was written with forced=true",
				graph.anchorSlot, dumpSlotFrequency, graph.anchorSlot%dumpSlotFrequency)
		})
	}
}
