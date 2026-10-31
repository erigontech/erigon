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

package fork_graph

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func firstEpochBoundaryBlock(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock) {
	t.Helper()
	blocks, parentState, _ := tests.GetBellatrixRandom()
	for _, block := range blocks {
		if block.Block.Slot/parentState.BeaconConfig().SlotsPerEpoch > parentState.Slot()/parentState.BeaconConfig().SlotsPerEpoch {
			return parentState, block
		}
		require.NoError(t, transition.TransitionState(parentState, block, nil, false))
	}
	t.Fatal("Bellatrix fixture has no epoch-boundary block")
	return nil, nil
}

func precomputedEpochState(t *testing.T, parent *state.CachingBeaconState, block *cltypes.SignedBeaconBlock) *state.CachingBeaconState {
	t.Helper()
	advanced, err := parent.Copy()
	require.NoError(t, err)
	epochStart := parent.BeaconConfig().RoundSlotToEpoch(block.Block.Slot)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, epochStart))
	_, err = advanced.HashSSZ()
	require.NoError(t, err)
	return advanced
}

func newEpochBoundaryGraph(t *testing.T) (*forkGraphDisk, *state.CachingBeaconState, *cltypes.SignedBeaconBlock) {
	t.Helper()
	parent, block := firstEpochBoundaryBlock(t)
	graph, err := NewForkGraphDisk(parent, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	return graph.(*forkGraphDisk), parent, block
}

func TestAddChainSegmentUsesMatchingPrecomputedEpochState(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	advanced := precomputedEpochState(t, parent, block)
	graph.SetNextEpochState(t.Context(), block.Block.ParentRoot, advanced)
	beforeTarget := *block.Block
	beforeTarget.Slot = advanced.Slot() - 1
	require.Nil(t, graph.takeNextEpochStateLocked(&beforeTarget, true))
	require.Same(t, advanced, graph.nextEpochState)
	require.Equal(t, block.Block.ParentRoot, graph.nextEpochParentRoot)

	postState, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.Same(t, advanced, postState)
	require.Same(t, advanced, graph.CurrentStateLocked(common.Hash(mustBlockRoot(t, block))))
	require.Nil(t, graph.nextEpochState)

	root, err := postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(block.Block.StateRoot), common.Hash(root))
}

func TestAddChainSegmentDropsPrecomputedEpochStateForWrongParent(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	advanced := precomputedEpochState(t, parent, block)
	graph.SetNextEpochState(t.Context(), common.Hash{0xff}, advanced)

	postState, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.NotSame(t, advanced, postState)
	require.Nil(t, graph.nextEpochState)

	root, err := postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(block.Block.StateRoot), common.Hash(root))
}

func TestAddChainSegmentFallsBackFromCorruptedPrecomputedEpochState(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	normalState, err := parent.Copy()
	require.NoError(t, err)
	wantRewards := &eth2.BlockRewardsCollector{}
	require.NoError(t, transition.TransitionState(normalState, block, wantRewards, true))
	require.NotEqual(t, &eth2.BlockRewardsCollector{}, wantRewards)
	advanced := precomputedEpochState(t, parent, block)
	balance, err := advanced.ValidatorBalance(0)
	require.NoError(t, err)
	require.NoError(t, advanced.SetValidatorBalance(0, balance+1))
	graph.SetNextEpochState(t.Context(), block.Block.ParentRoot, advanced)
	logs := captureLogs(t)

	postState, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.NotSame(t, advanced, postState)
	require.Nil(t, graph.nextEpochState)

	root, err := postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(block.Block.StateRoot), common.Hash(root))
	blockRoot := common.Hash(mustBlockRoot(t, block))
	gotRewards, ok := graph.GetBlockRewards(blockRoot)
	require.True(t, ok)
	require.Equal(t, wantRewards, gotRewards)
	require.Equal(t, 1, strings.Count(logs.String(), "Precomputed epoch state diverged"))
	require.NotContains(t, logs.String(), "Invalid beacon block")
}

func TestAddChainSegmentDropsPrecomputedEpochStateWithoutFullValidation(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	advanced := precomputedEpochState(t, parent, block)
	graph.SetNextEpochState(t.Context(), block.Block.ParentRoot, advanced)

	postState, result, err := graph.AddChainSegment(block, false)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.NotSame(t, advanced, postState)
	require.Nil(t, graph.nextEpochState)

	root, err := postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(block.Block.StateRoot), common.Hash(root))
}

func TestAddChainSegmentRejectsInvalidBlockWithPrecomputedEpochState(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	advanced := precomputedEpochState(t, parent, block)
	graph.SetNextEpochState(t.Context(), block.Block.ParentRoot, advanced)
	block.Block.StateRoot[0] ^= 0xff
	blockRoot := common.Hash(mustBlockRoot(t, block))
	logs := captureLogs(t)

	postState, result, err := graph.AddChainSegment(block, true)
	require.Error(t, err)
	require.Equal(t, InvalidBlock, result)
	require.Nil(t, postState)
	_, isBad := graph.badBlocks.Load(blockRoot)
	require.True(t, isBad)
	require.Nil(t, graph.nextEpochState)
	require.Equal(t, 1, strings.Count(logs.String(), "Invalid beacon block"))
	require.Equal(t, 1, strings.Count(logs.String(), "lvl=warn"))
	require.NotContains(t, logs.String(), "Precomputed epoch state diverged")
}

func TestSetNextEpochStateRejectsLatePublication(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	advanced := precomputedEpochState(t, parent, block)

	_, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	graph.SetNextEpochState(t.Context(), block.Block.ParentRoot, advanced)
	require.Nil(t, graph.nextEpochState)
}

func TestSetNextEpochStateRejectsCancelledPublication(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t)
	advanced := precomputedEpochState(t, parent, block)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	graph.SetNextEpochState(ctx, block.Block.ParentRoot, advanced)
	require.Nil(t, graph.nextEpochState)
}

func mustBlockRoot(t *testing.T, block *cltypes.SignedBeaconBlock) [32]byte {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return root
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	oldHandler := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.SyncHandler(log.StreamHandler(&logs, log.LogfmtFormat())))
	t.Cleanup(func() { log.Root().SetHandler(oldHandler) })
	return &logs
}
