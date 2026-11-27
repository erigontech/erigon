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
	blocks, anchor, _ := tests.GetBellatrixRandom()
	f := newBlockTestStore(t, anchor)
	slotMs := func(slot uint64) uint64 { return (f.genesisTime + slot*f.beaconCfg.SecondsPerSlot) * 1000 }
	var block *cltypes.SignedBeaconBlock
	for _, candidate := range blocks {
		f.OnTick(slotMs(candidate.Block.Slot) + 100)
		if _, standsIn := f.preGloasHeadStandInLocked(f.JustifiedCheckpoint(), candidate.Block.Slot); !standsIn {
			block = candidate
			break
		}
		require.NoError(t, f.OnBlock(t.Context(), candidate, false, true, false))
	}
	require.NotNil(t, block, "the fixture must count the votes before some block")
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
