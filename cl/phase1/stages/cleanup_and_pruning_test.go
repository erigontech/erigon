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

package stages

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func pruningCfg(t *testing.T, ctrl *gomock.Controller, beaconCfg *clparams.BeaconChainConfig, caplinCfg clparams.CaplinConfig, currentSlot uint64) (*Cfg, *blob_mock_services.MockBlobStorage, *das_mock_services.MockPeerDas) {
	t.Helper()
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	return &Cfg{
		indiciesDB:   memdb.NewTestDB(t, dbcfg.ChainDB),
		beaconCfg:    beaconCfg,
		ethClock:     clock,
		blobStore:    blobStore,
		peerDas:      peerDas,
		caplinConfig: caplinCfg,
	}, blobStore, peerDas
}

// An unset --caplin.columns-keep-slots must resolve to the chain's own window rather than a
// fixed slot count, so a chain with shorter slots stops retaining twice what it needs.
func TestCleanupAndPruningDerivesColumnRetentionFromTheChainConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 16
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 4096

	const currentSlot = 20_000*16 + 5
	cfg, blobStore, peerDas := pruningCfg(t, ctrl, &beaconCfg, clparams.CaplinConfig{}, currentSlot)
	blobStore.EXPECT().PruneBelow(uint64(currentSlot - 128600)).Return(nil)
	peerDas.EXPECT().PruneBelow(uint64((20_000 - 4096) * 16)).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

// An explicit value is a slot count and must reach the pruner as that distance below the head.
func TestCleanupAndPruningKeepsAnExplicitColumnSlotCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	beaconCfg := clparams.MainnetBeaconConfig

	const currentSlot = 500_000
	cfg, blobStore, peerDas := pruningCfg(t, ctrl, &beaconCfg, clparams.CaplinConfig{ColumnKeepSlots: 4_242}, currentSlot)
	blobStore.EXPECT().PruneBelow(uint64(currentSlot - 128600)).Return(nil)
	peerDas.EXPECT().PruneBelow(uint64(currentSlot - 4_242)).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

// Archive and pruning-disabled nodes must keep every blob, so the floor stays at zero.
func TestCleanupAndPruningKeepsAllBlobsWhenPruningIsOff(t *testing.T) {
	for name, caplinCfg := range map[string]clparams.CaplinConfig{
		"archive blobs":         {ArchiveBlobs: true},
		"blob pruning disabled": {BlobPruningDisabled: true},
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			beaconCfg := clparams.MainnetBeaconConfig

			cfg, blobStore, peerDas := pruningCfg(t, ctrl, &beaconCfg, caplinCfg, 500_000)
			blobStore.EXPECT().PruneBelow(uint64(0)).Return(nil)
			peerDas.EXPECT().PruneBelow(gomock.Any()).Return(nil)

			require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
		})
	}
}

// The serving window starts at the first slot of current_epoch - MIN_EPOCHS, and the floor also
// sets the earliest slot we advertise as servable, so it must never cut above that boundary for
// any head position inside an epoch.
func TestSpecColumnFloorNeverCutsAboveTheEpochBoundary(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 16
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 4096

	const epoch = 5000
	required := (epoch - beaconCfg.MinEpochsForDataColumnSidecarsRequests) * beaconCfg.SlotsPerEpoch
	for offset := uint64(0); offset < beaconCfg.SlotsPerEpoch; offset++ {
		head := epoch*beaconCfg.SlotsPerEpoch + offset
		require.LessOrEqual(t, specColumnFloor(head, &beaconCfg), required,
			"head %d cuts above the first slot the node must still serve", head)
	}
}

// Both inputs arrive as plain uint64 from --caplin.custom-config, so the derived floor must not
// wrap. A floor at the head is the dangerous outcome: it deletes every bucket below the head and
// advertises nothing as available, so anything unrepresentable has to fall back to retaining.
func TestSpecColumnFloorSaturatesInsteadOfWrapping(t *testing.T) {
	for _, test := range []struct {
		name          string
		minEpochs     uint64
		slotsPerEpoch uint64
	}{
		{name: "epoch count at the limit", minEpochs: math.MaxUint64, slotsPerEpoch: 32},
		{name: "product overflows", minEpochs: math.MaxUint64 / 32, slotsPerEpoch: 32},
		{name: "no slots per epoch", minEpochs: 4096, slotsPerEpoch: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			beaconCfg := clparams.MainnetBeaconConfig
			beaconCfg.MinEpochsForDataColumnSidecarsRequests = test.minEpochs
			beaconCfg.SlotsPerEpoch = test.slotsPerEpoch

			require.Zero(t, specColumnFloor(500_000, &beaconCfg),
				"an unrepresentable window must retain everything, never prune everything")
		})
	}
}

// The floor also sets the earliest slot we advertise as servable, so no config may push it past
// the start of the current epoch.
func TestSpecColumnFloorStaysAtOrBelowTheCurrentEpoch(t *testing.T) {
	const head = 5_000_000
	for _, minEpochs := range []uint64{0, 1, 4096, math.MaxUint64 - 1, math.MaxUint64} {
		for _, slotsPerEpoch := range []uint64{0, 1, 12, 16, 32, math.MaxUint64} {
			beaconCfg := clparams.MainnetBeaconConfig
			beaconCfg.MinEpochsForDataColumnSidecarsRequests = minEpochs
			beaconCfg.SlotsPerEpoch = slotsPerEpoch

			epochStart := uint64(0)
			if slotsPerEpoch != 0 {
				epochStart = head / slotsPerEpoch * slotsPerEpoch
			}
			require.LessOrEqual(t, specColumnFloor(head, &beaconCfg), epochStart,
				"MIN_EPOCHS=%d SLOTS_PER_EPOCH=%d resolved to a destructive floor",
				minEpochs, slotsPerEpoch)
		}
	}
}
