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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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

// A block called in one epoch and committed after the next epoch's tick must leave the store
// checkpoints as on_block followed by that tick does: the tick has already promoted the unrealized
// checkpoints, so the block's own advance has to be pulled up when it is applied.
func TestBlockImportedAcrossEpochTickPromotesItsCheckpoints(t *testing.T) {
	const targetIndex = 84 // advances unrealized justification in the fixture
	importUpTo := func(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock, uint64) {
		blocks, anchor, _ := tests.GetBellatrixRandom()
		f := newBlockTestStore(t, anchor)
		for _, block := range blocks[:targetIndex] {
			f.OnTick(slotTimeMs(f, block.Block.Slot) + 1)
			require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
		}
		block := blocks[targetIndex]
		f.OnTick(slotTimeMs(f, block.Block.Slot) + 1)
		nextEpochMs := slotTimeMs(f, (block.Block.Slot/f.beaconCfg.SlotsPerEpoch+1)*f.beaconCfg.SlotsPerEpoch) + 100
		return f, block, nextEpochMs
	}

	serial, serialBlock, nextEpochMs := importUpTo(t)
	justifiedBefore := serial.justifiedCheckpoint.Load().(solid.Checkpoint)
	require.NoError(t, serial.OnBlock(t.Context(), serialBlock, false, true, false))
	serial.OnTick(nextEpochMs)

	f, block, _ := importUpTo(t)
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	f.engine = engine
	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- f.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")
	f.OnTick(nextEpochMs)
	close(releaseEL)
	require.NoError(t, <-onBlockDone)

	want := serial.justifiedCheckpoint.Load().(solid.Checkpoint)
	require.Greater(t, want.Epoch, justifiedBefore.Epoch, "the fixture block must advance justification")
	require.Equal(t, want, f.justifiedCheckpoint.Load().(solid.Checkpoint))
	require.Equal(t, serial.finalizedCheckpoint.Load().(solid.Checkpoint), f.finalizedCheckpoint.Load().(solid.Checkpoint))
}
