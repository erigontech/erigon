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

package forkchoice

import (
	"context"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

type recordingNextEpochForkGraph struct {
	fork_graph.ForkGraph
	state         *state.CachingBeaconState
	beforePublish func()
}

func (g *recordingNextEpochForkGraph) SetNextEpochState(ctx context.Context, parentRoot common.Hash, next *state.CachingBeaconState) {
	if g.beforePublish != nil {
		g.beforePublish()
	}
	g.ForkGraph.SetNextEpochState(ctx, parentRoot, next)
	g.state = next
}

func forkChoiceEpochBoundaryBlock(t *testing.T) (*state.CachingBeaconState, *state.CachingBeaconState, []*cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock) {
	t.Helper()
	blocks, anchorState, _ := tests.GetBellatrixRandom()
	parentState, err := anchorState.Copy()
	require.NoError(t, err)
	for i, block := range blocks {
		if block.Block.Slot/parentState.BeaconConfig().SlotsPerEpoch > parentState.Slot()/parentState.BeaconConfig().SlotsPerEpoch {
			return anchorState, parentState, blocks[:i], block
		}
		require.NoError(t, transition.TransitionState(parentState, block, nil, false))
	}
	t.Fatal("Bellatrix fixture has no epoch-boundary block")
	return nil, nil, nil, nil
}

type epochPrecomputeTest struct {
	store          *ForkChoiceStore
	graph          *recordingNextEpochForkGraph
	realGraph      fork_graph.ForkGraph
	manager        *synced_data.SyncedDataManager
	parent         *state.CachingBeaconState
	block          *cltypes.SignedBeaconBlock
	nextEpochStart uint64
}

func newEpochPrecomputeTest(t *testing.T) *epochPrecomputeTest {
	t.Helper()
	anchor, parent, priorBlocks, block := forkChoiceEpochBoundaryBlock(t)
	manager := synced_data.NewSyncedDataManager(parent.BeaconConfig(), true)
	realGraph, err := fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := &recordingNextEpochForkGraph{ForkGraph: realGraph}
	clock := eth_clock.NewEthereumClock(parent.GenesisTime(), parent.GenesisValidatorsRoot(), parent.BeaconConfig())
	store, err := NewForkChoiceStore(
		clock,
		anchor,
		nil,
		pool.NewOperationsPool(parent.BeaconConfig()),
		graph,
		beaconevents.NewEventEmitter(),
		manager,
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		nil,
		false,
		nil,
	)
	require.NoError(t, err)
	store.SetSynced(true)
	store.OnTick((parent.GenesisTime() + (block.Block.Slot+1)*parent.BeaconConfig().SecondsPerSlot) * 1000)
	for _, priorBlock := range priorBlocks {
		require.NoError(t, store.OnBlock(context.Background(), priorBlock, false, true, false))
	}
	return &epochPrecomputeTest{
		store:          store,
		graph:          graph,
		realGraph:      realGraph,
		manager:        manager,
		parent:         parent,
		block:          block,
		nextEpochStart: parent.BeaconConfig().RoundSlotToEpoch(block.Block.Slot),
	}
}

func TestPrecomputeNextEpochStateIsUsedByOnBlock(t *testing.T) {
	test := newEpochPrecomputeTest(t)
	require.NoError(t, test.store.precomputeNextEpochState(t.Context(), test.nextEpochStart))
	require.Nil(t, test.graph.state)
	require.NoError(t, test.manager.OnHeadStateWithBlockRoot(test.parent, test.block.Block.ParentRoot))
	require.NoError(t, test.store.precomputeNextEpochState(t.Context(), test.nextEpochStart))
	require.NotNil(t, test.graph.state)

	normalState, err := test.parent.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(normalState, test.block, nil, true))
	require.NoError(t, test.store.OnBlock(t.Context(), test.block, false, true, false))

	blockRoot, err := test.block.Block.HashSSZ()
	require.NoError(t, err)
	imported := test.realGraph.CurrentStateLocked(common.Hash(blockRoot))
	require.Same(t, test.graph.state, imported)
	normalBytes, err := normalState.EncodeSSZ(nil)
	require.NoError(t, err)
	importedBytes, err := imported.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, normalBytes, importedBytes)
}

func TestPrecomputeNextEpochStateDoesNotPublishAfterCancellation(t *testing.T) {
	test := newEpochPrecomputeTest(t)
	require.NoError(t, test.manager.OnHeadStateWithBlockRoot(test.parent, test.block.Block.ParentRoot))

	cancelledCtx, cancel := context.WithCancel(t.Context())
	test.graph.beforePublish = cancel
	require.NoError(t, test.store.precomputeNextEpochState(cancelledCtx, test.nextEpochStart))
	require.NotNil(t, test.graph.state)

	normalState, err := test.parent.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(normalState, test.block, nil, true))
	require.NoError(t, test.store.OnBlock(t.Context(), test.block, false, true, false))

	blockRoot, err := test.block.Block.HashSSZ()
	require.NoError(t, err)
	imported := test.realGraph.CurrentStateLocked(common.Hash(blockRoot))
	require.NotSame(t, test.graph.state, imported)
	normalBytes, err := normalState.EncodeSSZ(nil)
	require.NoError(t, err)
	importedBytes, err := imported.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, normalBytes, importedBytes)
}

func TestNextEpochToPrecompute(t *testing.T) {
	tests := []struct {
		currentSlot uint64
		lastEpoch   uint64
		want        uint64
	}{
		{currentSlot: 95, want: 3},
		{currentSlot: 95, lastEpoch: 3, want: 4},
		{currentSlot: 63, lastEpoch: 3, want: 4},
		{currentSlot: 351, lastEpoch: 3, want: 11},
	}
	for _, test := range tests {
		require.Equal(t, test.want, nextEpochToPrecompute(test.currentSlot, test.lastEpoch, 32))
	}
}
