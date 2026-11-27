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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
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
