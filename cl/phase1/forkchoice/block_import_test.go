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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
)

func TestApplyBlockImportUsesCapturedStoreTime(t *testing.T) {
	blocks, anchor, _ := tests.GetBellatrixRandom()
	store := newBlockTestStore(t, anchor)

	first := blocks[0]
	firstTimeMs := (anchor.GenesisTime()+first.Block.Slot*anchor.BeaconConfig().SecondsPerSlot)*1000 + 1
	store.OnTick(firstTimeMs)
	require.NoError(t, store.OnBlock(t.Context(), first, false, true, false))

	block := blocks[1]
	blockTimeMs := (anchor.GenesisTime()+block.Block.Slot*anchor.BeaconConfig().SecondsPerSlot)*1000 + 1
	store.OnTick(blockTimeMs)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	func() {
		store.mu.Lock()
		defer store.mu.Unlock()

		postState, status, err := store.forkGraph.AddChainSegment(block, true)
		require.NoError(t, err)
		require.Equal(t, fork_graph.Success, status)
		storeTimeMs := store.timeMs.Load()
		prepared := preparePostState(postState, block.Block)
		require.True(t, store.timeMs.CompareAndSwap(storeTimeMs, storeTimeMs+store.beaconCfg.SecondsPerSlot*1000))

		_, err = store.applyBlockImportLocked(block.Block, blockRoot, common.Hash{}, storeTimeMs, postState, prepared)
		require.NoError(t, err)
	}()

	timeliness, ok := store.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.True(t, timeliness[clparams.AttestationTimelinessIndex])
	require.Equal(t, common.Hash(blockRoot), store.ProposerBoostRoot())
}

// update_proposer_boost_root compares the block with the head right before the block is added. The
// import releases f.mu during the EL call, so a head taken before that call can be stale.
func TestProposerBoostUsesTheHeadWhenTheBlockIsApplied(t *testing.T) {
	const blockIndex = 84
	for _, test := range []struct {
		name      string
		duringEL  func(f *ForkChoiceStore)
		wantBoost bool
	}{
		{name: "head unchanged", duringEL: func(*ForkChoiceStore) {}, wantBoost: true},
		{name: "head moved to another dependent root", duringEL: func(f *ForkChoiceStore) {
			// A justified root the store does not hold makes the anchor the head.
			f.justifiedCheckpoint.Store(solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch, Root: common.Hash{0xee}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			blocks, anchor, _ := tests.GetBellatrixRandom()
			f := newBlockTestStore(t, anchor)
			slotMs := func(slot uint64) uint64 { return (f.genesisTime + slot*f.beaconCfg.SecondsPerSlot) * 1000 }
			for _, block := range blocks[:blockIndex] {
				f.OnTick(slotMs(block.Block.Slot) + 1)
				require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
			}
			block := blocks[blockIndex]
			f.OnTick(slotMs(block.Block.Slot) + 100)
			engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
			f.engine = engine

			onBlockDone := make(chan error, 1)
			go func() { onBlockDone <- f.OnBlock(context.Background(), block, true, true, false) }()
			awaitSignal(t, elEntered, "NewPayload to start")
			test.duringEL(f)
			close(releaseEL)
			require.NoError(t, awaitOnBlock(t, onBlockDone))

			if test.wantBoost {
				require.Equal(t, mustRoot(t, block), f.ProposerBoostRoot())
			} else {
				require.Zero(t, f.ProposerBoostRoot())
			}
		})
	}
}

// Before each block, the head that decides the boost must have the shuffling dependent root of the head
// GetHead returns, whether the justified block stands in or the votes are counted.
func TestHeadBeforeBlockMatchesGetHead(t *testing.T) {
	for _, fixture := range []struct {
		name string
		load func() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState)
	}{
		{name: "bellatrix", load: tests.GetBellatrixRandom},
		{name: "capella", load: tests.GetCapellaRandom},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			blocks, anchor, _ := fixture.load()
			f := newBlockTestStore(t, anchor)
			votesCounted := 0
			for i, block := range blocks {
				slot := block.Block.Slot
				f.OnTick((f.genesisTime+slot*f.beaconCfg.SecondsPerSlot)*1000 + 1)
				head, _, err := f.GetHead(nil)
				require.NoError(t, err)
				func() {
					f.mu.Lock()
					defer f.mu.Unlock()
					if _, standsIn := f.preGloasHeadStandInLocked(f.JustifiedCheckpoint(), slot); !standsIn {
						votesCounted++
					}
					before, err := f.headBeforeBlockPreGloasLocked(slot)
					require.NoError(t, err)
					require.Equal(t, f.getDependentRoot(head, slot), f.getDependentRoot(before, slot), "block %d", i)
				}()
				require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
			}
			require.NotZero(t, votesCounted, "the fixture must count the votes before some blocks")
			t.Log("votes counted before", votesCounted, "of", len(blocks), "blocks")
		})
	}
}

// When the head before a block needs the votes, the import builds the justified checkpoint state while
// f.mu is released for the EL call, not under f.mu.
func TestImportBuildsJustifiedStateWhileLockIsReleased(t *testing.T) {
	f, block := importUntilHeadCountsVotes(t)
	justified := f.JustifiedCheckpoint()
	f.checkpointStates.Delete(justified)
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	f.engine = engine

	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- f.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")
	// A reader holding f.mu keeps the import from taking it back after the EL call.
	f.mu.RLock()
	close(releaseEL)
	built := assert.Eventually(t, func() bool {
		_, ok := f.checkpointStates.Load(justified)
		return ok
	}, lockScopeTimeout, time.Millisecond, "the justified state must be built before the import takes f.mu")
	f.mu.RUnlock()
	require.NoError(t, awaitOnBlock(t, onBlockDone))
	require.True(t, built)
	require.Equal(t, mustRoot(t, block), f.ProposerBoostRoot())
}

// importUntilHeadCountsVotes imports the Bellatrix fixture up to the first block whose head before it needs
// the votes, and moves the time 100 ms into that block's slot.
func importUntilHeadCountsVotes(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock) {
	t.Helper()
	blocks, anchor, _ := tests.GetBellatrixRandom()
	f := newBlockTestStore(t, anchor)
	for _, candidate := range blocks {
		f.OnTick(slotStartTimeMs(f, candidate.Block.Slot) + 100)
		if _, standsIn := f.preGloasHeadStandInLocked(f.JustifiedCheckpoint(), candidate.Block.Slot); !standsIn {
			return f, candidate
		}
		require.NoError(t, f.OnBlock(t.Context(), candidate, false, true, false))
	}
	t.Fatal("the fixture must count the votes before some block")
	return nil, nil
}

// A block already past the attestation deadline gets no head before it, so its import does not build the
// justified state for one.
func TestLateBlockDoesNotWarmTheJustifiedState(t *testing.T) {
	f, block := importUntilHeadCountsVotes(t)
	f.checkpointStates.Delete(f.JustifiedCheckpoint())
	slot := block.Block.Slot
	deadlineMs := slotStartTimeMs(f, slot) + f.getAttestationDueMs(f.computeEpochAtSlot(slot))

	_, warm := f.headCheckpointToWarmLocked(slot)
	require.True(t, warm)
	f.timeMs.Store(deadlineMs)
	_, warm = f.headCheckpointToWarmLocked(slot)
	require.False(t, warm)
}

// epochStartBlock returns the anchor and a block of the epoch's first slot that advances the
// unrealized justification.
func epochStartBlock(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock) {
	t.Helper()
	blocks, anchor, _ := tests.GetCapellaRandom()
	require.Zero(t, blocks[0].Block.Slot%anchor.BeaconConfig().SlotsPerEpoch)
	return anchor, blocks[0]
}

func slotStartTimeMs(f *ForkChoiceStore, slot uint64) uint64 {
	return (f.genesisTime + slot*f.beaconCfg.SecondsPerSlot) * 1000
}

func requireSameCheckpoints(t *testing.T, want, got *ForkChoiceStore) {
	t.Helper()
	require.Equal(t, want.justifiedCheckpoint.Load().(solid.Checkpoint), got.justifiedCheckpoint.Load().(solid.Checkpoint))
	require.Equal(t, want.finalizedCheckpoint.Load().(solid.Checkpoint), got.finalizedCheckpoint.Load().(solid.Checkpoint))
	require.Equal(t, want.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint), got.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint))
	require.Equal(t, want.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint), got.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint))
}

// A tick advances the store time before it takes f.mu for the slot-start work. A block of the
// epoch's first slot applied in between must leave the checkpoints as on_tick followed by on_block
// does: the block's own advance stays unrealized.
func TestBlockAppliedBeforeEpochStartWorkKeepsItsCheckpointsUnrealized(t *testing.T) {
	anchor, block := epochStartBlock(t)

	serial := newBlockTestStore(t, anchor)
	tickMs := slotStartTimeMs(serial, block.Block.Slot) + 100
	serial.OnTick(tickMs)
	require.NoError(t, serial.OnBlock(t.Context(), block, false, true, false))
	require.Greater(t, serial.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint).Epoch,
		serial.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch, "the fixture block must advance unrealized justification")

	f := newBlockTestStore(t, anchor)
	f.timeMs.Store(tickMs)
	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
	// The epoch tick's slot-start work takes f.mu only now.
	f.timeMs.Store(tickMs - 200)
	f.onTickPerSlot(tickMs)

	requireSameCheckpoints(t, serial, f)
}

// Spec test fixtures can move the store time back to an earlier epoch, whose next epoch start must
// still promote the unrealized checkpoints.
func TestSpecTestTimeMovedBackKeepsEpochStartPromotion(t *testing.T) {
	anchor, block := epochStartBlock(t)
	nextEpochSlot := block.Block.Slot + anchor.BeaconConfig().SlotsPerEpoch

	serial := newBlockTestStore(t, anchor)
	serial.OnTick(slotStartTimeMs(serial, block.Block.Slot) + 100)
	require.NoError(t, serial.OnBlock(t.Context(), block, false, true, false))
	serial.OnTick(slotStartTimeMs(serial, nextEpochSlot) + 100)

	f := newBlockTestStore(t, anchor)
	f.OnTick(slotStartTimeMs(f, nextEpochSlot+anchor.BeaconConfig().SlotsPerEpoch) + 100)
	f.SetTimeForSpecTest(slotStartTimeMs(f, block.Block.Slot) + 100)
	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
	f.OnTick(slotStartTimeMs(f, nextEpochSlot) + 100)

	requireSameCheckpoints(t, serial, f)
}

// The epoch-start promotion is skipped while the highest seen block lags more than two epochs
// behind the epoch start.
func TestEpochStartSkipsPromotionWhileHighestSeenLags(t *testing.T) {
	anchor, block := epochStartBlock(t)
	slotsPerEpoch := anchor.BeaconConfig().SlotsPerEpoch
	f := newBlockTestStore(t, anchor)
	f.OnTick(slotStartTimeMs(f, block.Block.Slot) + 100)
	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	require.Greater(t, f.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint).Epoch, justified.Epoch)

	f.highestSeen.Store(block.Block.Slot - 2*slotsPerEpoch)
	f.OnTick(slotStartTimeMs(f, block.Block.Slot+slotsPerEpoch) + 100)

	require.Equal(t, justified, f.justifiedCheckpoint.Load().(solid.Checkpoint))
}

// importBellatrixBeforeJustifyingBlock imports the Bellatrix fixture blocks before the one that advances
// unrealized justification and moves the time into that block's slot.
func importBellatrixBeforeJustifyingBlock(t *testing.T) (*ForkChoiceStore, *state.CachingBeaconState, *cltypes.SignedBeaconBlock) {
	t.Helper()
	const justifyingIndex = 84
	blocks, anchor, _ := tests.GetBellatrixRandom()
	f := newBlockTestStore(t, anchor)
	for _, block := range blocks[:justifyingIndex] {
		f.OnTick(slotStartTimeMs(f, block.Block.Slot) + 1)
		require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
	}
	block := blocks[justifyingIndex]
	f.OnTick(slotStartTimeMs(f, block.Block.Slot) + 1)
	return f, anchor, block
}

// The epoch tick can advance the store time after the import captured it and before the block is
// applied. The block must leave the checkpoints as on_block at the captured time followed by that
// tick does: the tick promotes the block's advance.
func TestEpochTickBetweenTimeCaptureAndApplyPromotesTheBlock(t *testing.T) {
	importUpTo := func(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock, uint64) {
		f, _, block := importBellatrixBeforeJustifyingBlock(t)
		slotsPerEpoch := f.beaconCfg.SlotsPerEpoch
		require.NotZero(t, block.Block.Slot%slotsPerEpoch)
		nextEpochMs := slotStartTimeMs(f, (block.Block.Slot/slotsPerEpoch+1)*slotsPerEpoch) + 100
		return f, block, nextEpochMs
	}

	serial, serialBlock, nextEpochMs := importUpTo(t)
	justifiedBefore := serial.justifiedCheckpoint.Load().(solid.Checkpoint)
	require.NoError(t, serial.OnBlock(t.Context(), serialBlock, false, true, false))
	serial.OnTick(nextEpochMs)
	require.Greater(t, serial.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch, justifiedBefore.Epoch,
		"the fixture block must advance justification")

	f, block, _ := importUpTo(t)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		postState, status, err := f.forkGraph.AddChainSegment(block, true)
		require.NoError(t, err)
		require.Equal(t, fork_graph.Success, status)
		storeTimeMs := f.timeMs.Load()
		prepared := preparePostState(postState, block.Block)
		// The epoch tick advances the time; its slot-start work waits for f.mu.
		require.True(t, f.timeMs.CompareAndSwap(storeTimeMs, nextEpochMs))
		_, err = f.applyBlockImportLocked(block.Block, blockRoot, common.Hash{}, storeTimeMs, postState, prepared)
		require.NoError(t, err)
	}()
	f.timeMs.Store(nextEpochMs - 200)
	f.onTickPerSlot(nextEpochMs)

	requireSameCheckpoints(t, serial, f)
}

// A tick that advanced the time into an epoch start but still waits for f.mu runs its promotion
// before on_block in the spec. A block at the slot of the finalized checkpoint that promotion sets
// is at the finalized horizon and must not be imported.
func TestBlockOvertakenByPendingPromotedFinalityIsNotImported(t *testing.T) {
	anchor, block := epochStartBlock(t)
	f := newBlockTestStore(t, anchor)
	epoch := block.Block.Slot / f.beaconCfg.SlotsPerEpoch
	f.unrealizedFinalizedCheckpoint.Store(solid.Checkpoint{Epoch: epoch, Root: block.Block.ParentRoot})
	f.timeMs.Store(slotStartTimeMs(f, block.Block.Slot) + 100)
	f.headHash = block.Block.ParentRoot // a head cached before the promotion

	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))

	require.Zero(t, f.headHash, "the promotion must drop the cached head")

	require.Equal(t, epoch, f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch)
	requireBlockNotAdded(t, f, block)
}

// A block that does not descend from the finalized checkpoint a pending epoch-start promotion
// sets must be rejected, also when the tick advances the time while the EL validates the payload.
func TestBlockConflictingWithPendingPromotedFinalityIsRejected(t *testing.T) {
	for _, duringEL := range []bool{false, true} {
		name := "tick before the call"
		if duringEL {
			name = "tick during the EL call"
		}
		t.Run(name, func(t *testing.T) {
			f, anchor, block := importBellatrixBeforeJustifyingBlock(t)
			slotsPerEpoch := f.beaconCfg.SlotsPerEpoch
			epoch := block.Block.Slot / slotsPerEpoch
			anchorRoot, err := anchor.BlockRoot()
			require.NoError(t, err)
			// A checkpoint of the block's epoch whose root is not the block's ancestor at that epoch start.
			conflicting := solid.Checkpoint{Epoch: epoch, Root: anchorRoot}
			require.NotEqual(t, conflicting.Root, f.getAncestor(f.getNodeForRoot(block.Block.ParentRoot), epoch*slotsPerEpoch).Root)
			f.unrealizedFinalizedCheckpoint.Store(conflicting)
			// The epoch tick advances the time; its slot-start work waits for f.mu.
			pendingTick := func() { f.timeMs.Store(slotStartTimeMs(f, (epoch+1)*slotsPerEpoch) + 100) }

			if !duringEL {
				pendingTick()
				f.headHash = block.Block.ParentRoot // a head cached before the promotion
				err = f.OnBlock(t.Context(), block, false, true, false)
				require.Zero(t, f.headHash, "the promotion must drop the cached head")
			} else {
				engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
				f.engine = engine
				onBlockDone := make(chan error, 1)
				go func() { onBlockDone <- f.OnBlock(context.Background(), block, true, true, false) }()
				awaitSignal(t, elEntered, "NewPayload to start")
				pendingTick()
				close(releaseEL)
				err = <-onBlockDone
			}

			require.ErrorIs(t, err, ErrNotFinalizedDescendant)
			requireBlockNotAdded(t, f, block)
		})
	}
}

// equivocationCheckHookForkGraph runs hook once at the first equivocation check, which admission
// runs before its slot check.
type equivocationCheckHookForkGraph struct {
	productionTestForkGraph
	hook func()
}

func (g *equivocationCheckHookForkGraph) HasBlockEquivocation(slot, proposerIndex uint64, exceptRoot common.Hash) bool {
	if hook := g.hook; hook != nil {
		g.hook = nil
		hook()
	}
	return g.productionTestForkGraph.HasBlockEquivocation(slot, proposerIndex, exceptRoot)
}

// The epoch tick can advance the time into the block's slot after admission ran the pending
// promotion for the previous epoch and before its slot check. The block must still be admitted
// only after the promotion of its own epoch.
func TestEpochTickDuringAdmissionRunsThePromotionFirst(t *testing.T) {
	anchor, block := epochStartBlock(t)
	tickMs := func(f *ForkChoiceStore) uint64 { return slotStartTimeMs(f, block.Block.Slot) + 100 }

	serial := newBlockTestStore(t, anchor)
	serial.OnTick(tickMs(serial))
	require.NoError(t, serial.OnBlock(t.Context(), block, false, true, false))

	f := newBlockTestStore(t, anchor)
	f.OnTick(tickMs(f) - f.beaconCfg.SecondsPerSlot*1000)
	f.forkGraph = &equivocationCheckHookForkGraph{
		productionTestForkGraph: f.forkGraph.(productionTestForkGraph),
		hook:                    func() { f.timeMs.Store(tickMs(f)) }, // the epoch tick's slot-start work waits for f.mu
	}
	require.NoError(t, f.OnBlockWithEquivocationCheck(t.Context(), block, false, true, false))
	f.timeMs.Store(tickMs(f) - 200)
	f.onTickPerSlot(tickMs(f))

	requireSameCheckpoints(t, serial, f)
}

func TestGloasBlockImportSeedsExecutionHashFromOwnBid(t *testing.T) {
	store, _, _ := newForkNodesTestStore(t)
	store.beaconCfg.GloasForkEpoch = 0
	store.beaconCfg.InitializeForkSchedule()
	parentRoot, childRoot := common.Hash{1}, common.Hash{2}
	parentPayloadHash, emptyParentHash := common.Hash{0xa1}, common.Hash{0xa2}
	store.eth2Roots.Add(parentRoot, parentPayloadHash)

	child := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
	child.Block.ParentRoot = parentRoot
	child.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = emptyParentHash

	store.mu.Lock()
	store.applyExecutionHashLocked(child.Block, childRoot, clparams.GloasVersion)
	store.mu.Unlock()

	require.Equal(t, emptyParentHash, store.GetEth1Hash(childRoot))
}

func TestGloasEmptyBlockInvalidationFollowsOwnBidParentHash(t *testing.T) {
	store, _, _ := newForkNodesTestStore(t)
	store.beaconCfg.GloasForkEpoch = 0
	store.beaconCfg.InitializeForkSchedule()
	parentRoot, childRoot := common.Hash{1}, common.Hash{2}
	parentPayloadHash, emptyParentHash := common.Hash{0xa1}, common.Hash{0xa2}
	store.eth2Roots.Add(parentRoot, parentPayloadHash)
	child := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
	child.Block.ParentRoot = parentRoot
	child.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = emptyParentHash
	store.mu.Lock()
	store.applyExecutionHashLocked(child.Block, childRoot, clparams.GloasVersion)
	store.mu.Unlock()
	store.invalidatedExecutionPayloads = &sync.Map{}

	store.invalidatedExecutionPayloads.Store(parentPayloadHash, struct{}{})
	require.False(t, store.payloadExecutionHashInvalidated(childRoot))

	store.invalidatedExecutionPayloads.Store(emptyParentHash, struct{}{})
	status, ok := store.GetRecentExecutionPayloadStatusByRoot(childRoot)
	require.True(t, ok)
	require.True(t, status == execution_client.PayloadStatusInvalidated)
}

func TestGloasBlockWithoutBidKeepsParentExecutionHash(t *testing.T) {
	bids := map[string]*cltypes.SignedExecutionPayloadBid{
		"nil bid":         nil,
		"bid without msg": {},
	}
	for name, bid := range bids {
		t.Run(name, func(t *testing.T) {
			store, _, _ := newForkNodesTestStore(t)
			store.beaconCfg.GloasForkEpoch = 0
			store.beaconCfg.InitializeForkSchedule()
			parentRoot, childRoot := common.Hash{1}, common.Hash{2}
			parentPayloadHash := common.Hash{0xa1}
			store.eth2Roots.Add(parentRoot, parentPayloadHash)

			child := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
			child.Block.ParentRoot = parentRoot
			child.Block.Body.SignedExecutionPayloadBid = bid

			store.mu.Lock()
			store.applyExecutionHashLocked(child.Block, childRoot, clparams.GloasVersion)
			store.mu.Unlock()

			require.Equal(t, parentPayloadHash, store.GetEth1Hash(childRoot))
		})
	}
}
