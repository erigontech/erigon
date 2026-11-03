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
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

type importArtifacts struct {
	result        ChainSegmentInsertionResult
	failed        bool
	stateRoot     common.Hash
	isCurrent     bool
	header        *cltypes.BeaconBlockHeader
	hasBlock      bool
	rewards       *eth2.BlockRewardsCollector
	justified     solid.Checkpoint
	finalized     solid.Checkpoint
	bootstrap     common.Hash
	lcUpdate      common.Hash
	participation []byte
	bad           bool
	isChild       bool
}

func readImportArtifacts(t *testing.T, g *forkGraphDisk, block *cltypes.SignedBeaconBlock, post *state.CachingBeaconState, result ChainSegmentInsertionResult, err error) importArtifacts {
	t.Helper()
	root := common.Hash(mustBlockRoot(t, block))
	a := importArtifacts{
		result:  result,
		failed:  err != nil,
		bad:     g.IsBlockInvalid(root),
		isChild: g.HasBlockChildAtOrAfter(block.Block.ParentRoot, block.Block.Slot),
	}
	if post != nil {
		stateRoot, hashErr := post.HashSSZ()
		require.NoError(t, hashErr)
		a.stateRoot = stateRoot
		a.isCurrent = g.CurrentStateLocked(root) == post
	}
	a.header, _ = g.GetHeader(root)
	_, a.hasBlock = g.GetBlock(root)
	a.rewards, _ = g.GetBlockRewards(root)
	a.justified, _ = g.GetCurrentJustifiedCheckpoint(root)
	a.finalized, _ = g.GetFinalizedCheckpoint(root)
	if bootstrap, ok := g.GetLightClientBootstrap(root); ok {
		bootstrapRoot, hashErr := bootstrap.HashSSZ()
		require.NoError(t, hashErr)
		a.bootstrap = bootstrapRoot
	}
	if update := g.NewestLightClientUpdate(); update != nil {
		updateRoot, hashErr := update.HashSSZ()
		require.NoError(t, hashErr)
		a.lcUpdate = updateRoot
	}
	participation, partErr := g.GetCurrentParticipationIndicies(block.Block.Slot / g.beaconCfg.SlotsPerEpoch)
	require.NoError(t, partErr)
	if participation != nil {
		a.participation = participation.Bytes()
	}
	return a
}

// newLightClientGraph serves light-client data and knows the fixture's finalized block
// (a genesis-slot block with the zero root), so the second fixture block builds a
// light-client update.
func newLightClientGraph(t *testing.T) (*forkGraphDisk, []*cltypes.SignedBeaconBlock) {
	t.Helper()
	blocks, anchorState, _ := tests.GetElectraRandom()
	require.Zero(t, anchorState.FinalizedCheckpoint().Root)
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{Beacon: true})
	require.NoError(t, err)
	g := graph.(*forkGraphDisk)
	genesisBlock := cltypes.NewSignedBeaconBlock(g.beaconCfg, anchorState.Version())
	g.blocks.Store(common.Hash{}, genesisBlock)
	g.lowestAvailableBlock.Store(0)
	return g, blocks
}

func TestAddChainSegmentPublishesImportArtifacts(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	for i, block := range blocks {
		post, result, err := g.AddChainSegment(block, true)
		require.NoError(t, err)
		got := readImportArtifacts(t, g, block, post, result, err)

		require.Equal(t, Success, got.result)
		require.Equal(t, block.Block.StateRoot, got.stateRoot)
		require.True(t, got.isCurrent)
		bodyRoot, err := block.Block.Body.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, &cltypes.BeaconBlockHeader{
			Slot:          block.Block.Slot,
			ProposerIndex: block.Block.ProposerIndex,
			ParentRoot:    block.Block.ParentRoot,
			Root:          block.Block.StateRoot,
			BodyRoot:      bodyRoot,
		}, got.header)
		require.True(t, got.hasBlock)
		require.NotNil(t, got.rewards)
		require.Equal(t, post.CurrentJustifiedCheckpoint(), got.justified)
		require.Equal(t, post.FinalizedCheckpoint(), got.finalized)
		require.NotZero(t, got.bootstrap)
		require.Equal(t, post.RawCurrentEpochParticipation(), got.participation)
		_, _, hasSyncCommittees := g.GetSyncCommittees(g.beaconCfg.SyncCommitteePeriod(block.Block.Slot))
		require.True(t, hasSyncCommittees)
		require.False(t, got.bad)
		require.True(t, got.isChild)
		require.Equal(t, i > 0, got.lcUpdate != common.Hash{}, "only a block whose parent block is known builds a light-client update")
	}
}

func TestAddChainSegmentInvalidBlockKeepsOnlyLightClientUpdate(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	_, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	parentRoot := common.Hash(mustBlockRoot(t, blocks[0]))

	invalid := blocks[1]
	invalid.Block.StateRoot[0] ^= 0xff
	before := readImportArtifacts(t, g, invalid, nil, InvalidBlock, nil)
	post, result, err := g.AddChainSegment(invalid, true)
	require.Error(t, err)
	require.Nil(t, post)
	got := readImportArtifacts(t, g, invalid, post, result, err)

	require.Equal(t, importArtifacts{
		result:        InvalidBlock,
		failed:        true,
		bad:           true,
		lcUpdate:      got.lcUpdate,
		participation: before.participation,
	}, got)
	require.Zero(t, before.lcUpdate)
	require.NotZero(t, got.lcUpdate, "the light-client update is built from the parent state before the transition")
	require.Nil(t, g.CurrentStateLocked(parentRoot))
	require.Nil(t, g.currentState)
	require.Equal(t, invalid.Block.Slot, g.highestTransitionSlot)
}

func TestAddChainSegmentMissingSegmentLeavesGraphUnchanged(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	current := g.currentState
	orphanBlock := *blocks[0].Block
	orphanBlock.ParentRoot = common.Hash{0xee}
	orphan := &cltypes.SignedBeaconBlock{Block: &orphanBlock, Signature: blocks[0].Signature}

	post, result, err := g.AddChainSegment(orphan, true)
	require.NoError(t, err)
	require.Nil(t, post)
	require.Equal(t, importArtifacts{result: MissingSegment}, readImportArtifacts(t, g, orphan, post, result, err))
	require.Same(t, current, g.CurrentStateLocked(g.anchorRoot))
	require.Zero(t, g.highestTransitionSlot)
}

func TestAddChainSegmentReplaysParentThatIsNotCurrent(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	_, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	g.currentState = nil
	g.currentStateBlockRoot = common.Hash{}

	post, result, err := g.AddChainSegment(blocks[1], true)
	require.NoError(t, err)
	got := readImportArtifacts(t, g, blocks[1], post, result, err)
	require.Equal(t, Success, got.result)
	require.Equal(t, blocks[1].Block.StateRoot, got.stateRoot)
	require.True(t, got.isCurrent)
	require.NotNil(t, got.header)
}
