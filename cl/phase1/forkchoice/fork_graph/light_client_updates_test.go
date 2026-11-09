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

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// A period serves its best light client update, not its first one, and an update signed in the next period is not a
// candidate for the period of its attested header.
func TestStoreLightClientUpdateKeepsTheBestOfAPeriod(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	update := func(participants int, attestedSlot uint64) *cltypes.LightClientUpdate {
		u := cltypes.NewLightClientUpdate(clparams.DenebVersion, cfg)
		for i := range participants {
			u.SyncAggregate.SyncCommiteeBits[i/8] |= 1 << (i % 8)
		}
		u.AttestedHeader.Beacon.Slot = attestedSlot
		u.SignatureSlot = attestedSlot + 1
		return u
	}

	f.storeLightClientUpdate(update(10, periodStart+1))
	best := update(400, periodStart+5)
	f.storeLightClientUpdate(best)
	f.storeLightClientUpdate(update(300, periodStart+7))
	f.storeLightClientUpdate(update(int(cfg.SyncCommitteeSize), 2*periodStart-1))

	got, ok := f.GetLightClientUpdate(1)
	require.True(t, ok)
	require.Same(t, best, got)
}

// A block that fails the state transition must leave no light client update behind: its sync aggregate is unverified.
func TestAddChainSegmentKeepsNoLightClientUpdateOfAnInvalidBlock(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	const parentSlot = uint64(100)

	finalized := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	finalized.Block.Slot = 1
	finalizedRoot, err := finalized.Block.HashSSZ()
	require.NoError(t, err)
	parent := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	parent.Block.Slot = parentSlot
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)

	parentState := state.New(cfg)
	parentState.SetVersion(clparams.AltairVersion)
	require.NoError(t, parentState.SetSlot(parentSlot))
	parentState.SetLatestBlockHeader(parent.SignedBeaconBlockHeader().Header)
	require.NoError(t, parentState.SetCurrentSyncCommittee(solid.NewSyncCommittee()))
	require.NoError(t, parentState.SetNextSyncCommittee(solid.NewSyncCommittee()))
	parentState.SetFinalizedCheckpoint(solid.Checkpoint{Root: finalizedRoot})

	f := &forkGraphDisk{
		fs:                    afero.NewMemMapFs(),
		beaconCfg:             cfg,
		rcfg:                  beacon_router_configuration.RouterConfiguration{Beacon: true},
		children:              make(map[common.Hash]*validatedChildren),
		currentState:          parentState,
		currentStateBlockRoot: common.Hash(parentRoot),
	}
	f.blocks.Store(common.Hash(parentRoot), parent)
	f.blocks.Store(common.Hash(finalizedRoot), finalized)

	block := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	block.Block.Slot = parentSlot + 1
	block.Block.ParentRoot = parentRoot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	for i := range block.Block.Body.SyncAggregate.SyncCommiteeBits {
		block.Block.Body.SyncAggregate.SyncCommiteeBits[i] = 0xff
	}

	_, result, err := f.AddChainSegment(block, true)
	require.Error(t, err)
	require.Equal(t, InvalidBlock, result)
	require.Nil(t, f.NewestLightClientUpdate())
	_, ok := f.GetLightClientUpdate(cfg.SyncCommitteePeriod(parentSlot))
	require.False(t, ok)
}
