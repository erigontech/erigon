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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/lightclient_utils"
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

// unknownProposerIndex makes a block invalid without a signature error, which would not mark its root bad.
const unknownProposerIndex = 1 << 30

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

// An invalid block leaves no light-client update behind: its sync aggregate was never verified.
func TestAddChainSegmentInvalidBlockLeavesNoLightClientUpdate(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	_, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	parentRoot := common.Hash(mustBlockRoot(t, blocks[0]))

	invalid := blocks[1]
	invalid.Block.ProposerIndex = unknownProposerIndex
	before := readImportArtifacts(t, g, invalid, nil, InvalidBlock, nil)
	post, result, err := g.AddChainSegment(invalid, true)
	require.Error(t, err)
	require.Nil(t, post)
	got := readImportArtifacts(t, g, invalid, post, result, err)

	require.Equal(t, importArtifacts{
		result:        InvalidBlock,
		failed:        true,
		bad:           true,
		participation: before.participation,
	}, got)
	require.Zero(t, before.lcUpdate)
	require.Zero(t, got.lcUpdate)
	require.Empty(t, g.lightClientUpdateCandidates)
	require.Nil(t, g.CurrentStateLocked(parentRoot))
	require.Nil(t, g.currentState)
	require.Equal(t, invalid.Block.Slot, g.highestTransitionSlot)
}

// A block's light-client update reaches its period store only once the block is finalized.
func TestAddChainSegmentLightClientUpdateReachesPeriodStoreAtFinalization(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	for _, block := range blocks {
		_, result, err := g.AddChainSegment(block, true)
		require.NoError(t, err)
		require.Equal(t, Success, result)
	}
	update := g.NewestLightClientUpdate()
	require.NotNil(t, update)
	period := g.beaconCfg.SyncCommitteePeriod(update.AttestedHeader.Beacon.Slot)
	head := common.Hash(mustBlockRoot(t, blocks[len(blocks)-1]))
	require.Nil(t, g.GetLightClientUpdates(period, period+1, common.Hash{})[0], "served before finalization")
	require.Same(t, update, g.GetLightClientUpdates(period, period+1, head)[0])

	g.KeepFinalizedLightClientUpdates(head)

	require.Same(t, update, g.GetLightClientUpdates(period, period+1, common.Hash{})[0])
}

func TestCommitChainSegmentLogisticErrorStoresNoLightClientUpdate(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	_, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)

	seg := beginStartedSegment(t, g, blocks[1])
	seg.Transition()
	require.NotNil(t, seg.lcUpdate)
	prepareErr := errors.New("prepare failed")
	seg.err = prepareErr

	post, result, err := g.CommitChainSegment(seg)
	require.ErrorIs(t, err, prepareErr)
	require.Nil(t, post)
	require.Equal(t, LogisticError, result)
	require.Nil(t, g.NewestLightClientUpdate())
	require.Empty(t, g.lightClientUpdateCandidates)
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

func beginStartedSegment(t *testing.T, g *forkGraphDisk, block *cltypes.SignedBeaconBlock) *ChainSegment {
	t.Helper()
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	seg, result, err := g.BeginChainSegment(block, blockRoot, true)
	require.NoError(t, err)
	require.Equal(t, Started, result)
	require.NotNil(t, seg)
	return seg
}

func TestChainSegmentPhasesMatchAddChainSegment(t *testing.T) {
	for _, invalidSecond := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalidSecond=%v", invalidSecond), func(t *testing.T) {
			inline, inlineBlocks := newLightClientGraph(t)
			phased, phasedBlocks := newLightClientGraph(t)
			if invalidSecond {
				inlineBlocks[1].Block.StateRoot[0] ^= 0xff
				phasedBlocks[1].Block.StateRoot[0] ^= 0xff
			}
			for i := range inlineBlocks {
				post, result, err := inline.AddChainSegment(inlineBlocks[i], true)
				want := readImportArtifacts(t, inline, inlineBlocks[i], post, result, err)

				seg := beginStartedSegment(t, phased, phasedBlocks[i])
				seg.Transition()
				post, result, err = phased.CommitChainSegment(seg)
				got := readImportArtifacts(t, phased, phasedBlocks[i], post, result, err)

				require.Equal(t, want, got)
			}
		})
	}
}

func TestChainSegmentSecondTransitionDoesNotRerun(t *testing.T) {
	once, onceBlocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, once, onceBlocks[0])
	seg.Transition()
	post, result, err := once.CommitChainSegment(seg)
	want := readImportArtifacts(t, once, onceBlocks[0], post, result, err)

	twice, twiceBlocks := newLightClientGraph(t)
	seg = beginStartedSegment(t, twice, twiceBlocks[0])
	seg.Transition()
	seg.Transition()
	post, result, err = twice.CommitChainSegment(seg)
	require.NoError(t, err)
	got := readImportArtifacts(t, twice, twiceBlocks[0], post, result, err)

	require.Equal(t, Success, got.result)
	require.False(t, got.bad)
	require.Equal(t, want, got)
}

func TestChainSegmentConcurrentTransitionsRunOnce(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(seg.Transition)
	}
	wg.Wait()

	post, result, err := g.CommitChainSegment(seg)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.False(t, g.IsBlockInvalid(seg.blockRoot))
	stateRoot, err := post.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, blocks[0].Block.StateRoot, common.Hash(stateRoot))
}

func TestCommitChainSegmentWhileTransitionRuns(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	done := make(chan struct{})
	go func() {
		defer close(done)
		seg.Transition()
	}()

	post, result, err := g.CommitChainSegment(seg)
	<-done
	if result == Success {
		require.NoError(t, err)
		return
	}
	require.ErrorContains(t, err, "before its transition")
	require.Equal(t, importArtifacts{result: LogisticError, failed: true}, readImportArtifacts(t, g, blocks[0], post, result, err))
	require.Nil(t, g.currentState)

	post, result, err = g.CommitChainSegment(seg)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.Same(t, post, g.CurrentStateLocked(seg.blockRoot))
}

func TestDroppedChainSegmentStoresNothing(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	anchorRoot := g.anchorRoot
	seg := beginStartedSegment(t, g, blocks[0])
	seg.Transition()

	require.Equal(t, importArtifacts{result: Success}, readImportArtifacts(t, g, blocks[0], nil, Success, nil))
	require.Nil(t, g.currentState)
	require.Nil(t, g.CurrentStateLocked(anchorRoot), "a dropped segment does not return its parent state")

	post, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.Same(t, post, g.CurrentStateLocked(common.Hash(mustBlockRoot(t, blocks[0]))))
}

func TestCommitChainSegmentReturnsPreValidatedWhenHeaderPublished(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	seg.Transition()
	inlinePost, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)

	post, result, err := g.CommitChainSegment(seg)
	require.NoError(t, err)
	require.Equal(t, PreValidated, result)
	require.Nil(t, post)
	require.Same(t, inlinePost, g.CurrentStateLocked(common.Hash(mustBlockRoot(t, blocks[0]))))
}

func TestCommitChainSegmentRejectsBlockBelowPrunedBoundary(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	seg.Transition()
	g.lowestAvailableBlock.Store(blocks[0].Block.Slot + 2)

	post, result, err := g.CommitChainSegment(seg)
	require.NoError(t, err)
	require.Equal(t, BelowAnchor, result)
	require.Nil(t, post)
	require.True(t, g.IsBlockInvalid(seg.blockRoot))
	_, hasBlock := g.blocks.Load(seg.blockRoot)
	require.False(t, hasBlock)
}

func TestCommitChainSegmentWithoutTransitionStoresNothing(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])

	post, result, err := g.CommitChainSegment(seg)
	require.Error(t, err)
	require.Equal(t, LogisticError, result)
	require.Nil(t, post)
	got := readImportArtifacts(t, g, blocks[0], post, result, err)
	require.Equal(t, importArtifacts{result: LogisticError, failed: true}, got)
	require.Nil(t, g.currentState)
}

// A prepare failure drops the block that AddChainSegment stored before the transition, and does not mark it bad.
func TestCommitChainSegmentPrepareErrorDeletesBlock(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	g.blocks.Store(seg.blockRoot, blocks[0])
	seg.Transition()
	prepareErr := errors.New("prepare failed")
	seg.err = prepareErr

	post, result, err := g.CommitChainSegment(seg)
	require.ErrorIs(t, err, prepareErr)
	require.Equal(t, LogisticError, result)
	require.Nil(t, post)
	require.Equal(t, importArtifacts{result: LogisticError, failed: true}, readImportArtifacts(t, g, blocks[0], post, result, err))
	require.Nil(t, g.currentState)
}

func TestCommitChainSegmentMarksInvalidBlockBad(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	blocks[0].Block.ProposerIndex = unknownProposerIndex
	seg := beginStartedSegment(t, g, blocks[0])
	seg.Transition()

	post, result, err := g.CommitChainSegment(seg)
	require.Error(t, err)
	require.Equal(t, InvalidBlock, result)
	require.Nil(t, post)
	got := readImportArtifacts(t, g, blocks[0], post, result, err)
	require.Equal(t, importArtifacts{result: InvalidBlock, failed: true, bad: true}, got)
}

func TestCommitChainSegmentKeepsRootValidForBadProposerSignature(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	genuine := blocks[0]
	forged := *genuine
	forged.Signature[5] ^= 0xff
	seg := beginStartedSegment(t, g, &forged)
	seg.Transition()

	post, result, err := g.CommitChainSegment(seg)
	require.ErrorIs(t, err, eth2.ErrInvalidBlockSignature)
	require.Equal(t, importArtifacts{result: InvalidBlock, failed: true}, readImportArtifacts(t, g, genuine, post, result, err))

	post, result, err = g.AddChainSegment(genuine, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.Same(t, post, g.CurrentStateLocked(seg.blockRoot))
}

func TestChainSegmentPostStateRequiresCompletedValidTransition(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	require.Nil(t, seg.PostState())
	seg.Transition()
	require.NotNil(t, seg.PostState())

	g, blocks = newLightClientGraph(t)
	blocks[0].Block.StateRoot[0] ^= 0xff
	seg = beginStartedSegment(t, g, blocks[0])
	seg.Transition()
	require.Nil(t, seg.PostState())
}

func TestChainSegmentTransitionPanicPropagates(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	seg := beginStartedSegment(t, g, blocks[0])
	body := blocks[0].Block.Body
	blocks[0].Block.Body = nil
	require.Panics(t, seg.Transition)
	blocks[0].Block.Body = body

	post, result, err := g.CommitChainSegment(seg)
	require.ErrorContains(t, err, "before its transition")
	require.Equal(t, LogisticError, result)
	require.Nil(t, post)
	require.Equal(t, importArtifacts{result: LogisticError, failed: true}, readImportArtifacts(t, g, blocks[0], post, result, err))
}

func TestAddChainSegmentTransitionPanicPropagates(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	_, result, err := g.AddChainSegment(blocks[0], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	parentRoot := common.Hash(mustBlockRoot(t, blocks[0]))
	blockRoot := common.Hash(mustBlockRoot(t, blocks[1]))
	// A nil body on the imported block would panic in Begin, which hashes it, so the
	// parent block loses its body instead: the light-client update reads it in Transition.
	g.blocks.Store(parentRoot, &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: blocks[0].Block.Slot}})

	require.Panics(t, func() { _, _, _ = g.AddChainSegment(blocks[1], true) })
	_, stored := g.blocks.Load(blockRoot)
	require.True(t, stored, "the block is stored before its transition runs")

	g.blocks.Store(parentRoot, blocks[0])
	post, result, err := g.AddChainSegment(blocks[1], true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	require.Same(t, post, g.CurrentStateLocked(blockRoot))
}

func TestBeginChainSegmentWithoutReplayRequiresCurrentParent(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	current := g.currentState
	blockRoot, err := blocks[1].Block.HashSSZ()
	require.NoError(t, err)

	seg, result, err := g.BeginChainSegment(blocks[1], blockRoot, true)
	require.NoError(t, err)
	require.Equal(t, ReplayRequired, result)
	require.Nil(t, seg)
	require.Same(t, current, g.CurrentStateLocked(g.anchorRoot))
	require.Zero(t, g.highestTransitionSlot)
	require.Equal(t, importArtifacts{result: ReplayRequired}, readImportArtifacts(t, g, blocks[1], nil, result, err))
}

func TestBeginChainSegmentUsesSuppliedRoot(t *testing.T) {
	t.Run("prevalidated", func(t *testing.T) {
		g, blocks := newLightClientGraph(t)

		seg, result, err := g.BeginChainSegment(blocks[0], g.anchorRoot, true)
		require.NoError(t, err)
		require.Equal(t, PreValidated, result)
		require.Nil(t, seg)
	})
	t.Run("started", func(t *testing.T) {
		g, blocks := newLightClientGraph(t)
		computedRoot, err := blocks[0].Block.HashSSZ()
		require.NoError(t, err)
		suppliedRoot := common.Hash(computedRoot)
		suppliedRoot[0] ^= 0xff

		seg, result, err := g.BeginChainSegment(blocks[0], suppliedRoot, true)
		require.NoError(t, err)
		require.Equal(t, Started, result)
		require.Equal(t, suppliedRoot, seg.blockRoot)

		seg.Transition()
		_, result, err = g.CommitChainSegment(seg)
		require.NoError(t, err)
		require.Equal(t, Success, result)
		_, hasHeader := g.GetHeader(suppliedRoot)
		require.True(t, hasHeader)
		storedBlock, hasBlock := g.GetBlock(suppliedRoot)
		require.True(t, hasBlock)
		require.Same(t, blocks[0], storedBlock)
		_, hasComputedHeader := g.GetHeader(computedRoot)
		require.False(t, hasComputedHeader)
	})
}

func TestBeginChainSegmentKeepsPrecomputedEpochState(t *testing.T) {
	graph, parent, block := newEpochBoundaryGraph(t, tests.GetBellatrixRandom)
	advanced := precomputedEpochState(t, parent, block)
	graph.SetNextEpochState(block.Block.ParentRoot, advanced)
	parentState := graph.CurrentStateLocked(block.Block.ParentRoot)
	require.NotNil(t, parentState)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	seg, result, err := graph.BeginChainSegment(block, blockRoot, true)
	require.NoError(t, err)
	require.Equal(t, PrecomputedEpochStateAvailable, result)
	require.Nil(t, seg)
	require.Same(t, parentState, graph.currentState)
	require.Same(t, advanced, graph.nextEpochState)
	require.Zero(t, graph.highestTransitionSlot)
}

// A block whose parent state's finalized block is unavailable still records its light client update, without finality.
func TestAddChainSegmentRecordsAnUpdateWithoutTheFinalizedBlock(t *testing.T) {
	g, blocks := newLightClientGraph(t)
	g.blocks.Delete(common.Hash{}) // the anchor's finalized block
	for _, block := range blocks {
		_, result, err := g.AddChainSegment(block, true)
		require.NoError(t, err)
		require.Equal(t, Success, result)
	}
	update := g.NewestLightClientUpdate()
	require.NotNil(t, update)
	require.False(t, lightclient_utils.IsFinalityUpdate(update))
	require.Nil(t, g.NewestLightClientFinalityUpdate())
}
