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
	"context"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestGenesisStartNotifiesAntiquaryBackfilled(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	genesisState := state.New(&config)
	bodyRoot, err := cltypes.NewSignedBeaconBlock(&config, genesisState.Version()).Block.Body.HashSSZ()
	require.NoError(t, err)
	genesisState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{BodyRoot: bodyRoot})
	syncedData := synced_data.NewSyncedDataManager(&config, true)
	graph, err := fork_graph.NewForkGraphDisk(genesisState, syncedData, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	clock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), common.Hash{}, &config)
	store, err := forkchoice.NewForkChoiceStore(
		clock, genesisState, nil, pool.NewOperationsPool(&config),
		graph, beaconevents.NewEventEmitter(), syncedData, nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil,
	)
	require.NoError(t, err)

	antiq := antiquary.NewAntiquary(context.Background(), nil, nil, nil, &config, datadir.Dirs{}, nil, nil, nil, nil, nil, nil, log.New(), true, true, false, false, nil)
	cfg := &Cfg{
		ethClock:     clock,
		beaconCfg:    &config,
		dirs:         datadir.New(t.TempDir()),
		state:        genesisState,
		forkChoice:   store,
		indiciesDB:   db,
		antiquary:    antiq,
		syncedData:   syncedData,
		caplinConfig: clparams.CaplinConfig{ArchiveStates: true, ArchiveBlocks: true},
	}

	stage := ConsensusClStages().Stages[DownloadHistoricalBlocks]
	require.NoError(t, stage.ActionFunc(context.Background(), log.New(), cfg, Args{}))
	require.True(t, cfg.hasDownloaded)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		require.NoError(t, err)
		require.NotEqual(t, common.Hash{}, root)
		return nil
	}))

	require.True(t, antiq.Backfilled(), "a genesis start must mark the antiquary backfilled, otherwise its state worker never runs")
}
