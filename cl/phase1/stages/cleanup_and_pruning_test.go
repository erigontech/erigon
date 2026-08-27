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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func pruningCfg(t *testing.T, ctrl *gomock.Controller, beaconCfg *clparams.BeaconChainConfig, caplinCfg clparams.CaplinConfig) (*Cfg, *das_mock_services.MockPeerDas) {
	t.Helper()
	blobStore := blob_mock_services.NewMockBlobStorage(ctrl)
	blobStore.EXPECT().Prune().Return(nil)
	peerDas := das_mock_services.NewMockPeerDas(ctrl)
	return &Cfg{
		indiciesDB:   memdb.NewTestDB(t, dbcfg.ChainDB),
		beaconCfg:    beaconCfg,
		blobStore:    blobStore,
		peerDas:      peerDas,
		caplinConfig: caplinCfg,
	}, peerDas
}

// An unset --caplin.columns-keep-slots must resolve to the chain's own window rather than a
// fixed slot count, so a chain with shorter slots stops retaining twice what it needs.
func TestCleanupAndPruningDerivesColumnRetentionFromTheChainConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 16
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 4096

	cfg, peerDas := pruningCfg(t, ctrl, &beaconCfg, clparams.CaplinConfig{})
	peerDas.EXPECT().Prune(uint64(65_552)).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

// An explicit value is a slot count and must reach the pruner unchanged.
func TestCleanupAndPruningKeepsAnExplicitColumnSlotCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	beaconCfg := clparams.MainnetBeaconConfig

	cfg, peerDas := pruningCfg(t, ctrl, &beaconCfg, clparams.CaplinConfig{ColumnKeepSlots: 4_242})
	peerDas.EXPECT().Prune(uint64(4_242)).Return(nil)

	require.NoError(t, cleanupAndPruning(t.Context(), log.New(), cfg, Args{}))
}

// The serving window starts at the first slot of current_epoch - MIN_EPOCHS, and the distance
// also sets the earliest slot we advertise as servable, so it must never cut above that
// boundary for any head position inside an epoch.
func TestSpecColumnKeepSlotsNeverCutsAboveTheEpochBoundary(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 16
	beaconCfg.MinEpochsForDataColumnSidecarsRequests = 4096
	keep := specColumnKeepSlots(&beaconCfg)

	const epoch = 5000
	for offset := uint64(0); offset < beaconCfg.SlotsPerEpoch; offset++ {
		head := epoch*beaconCfg.SlotsPerEpoch + offset
		required := (epoch - beaconCfg.MinEpochsForDataColumnSidecarsRequests) * beaconCfg.SlotsPerEpoch
		require.LessOrEqual(t, head-keep, required,
			"head %d cuts above the first slot the node must still serve", head)
	}
}
