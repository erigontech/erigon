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

package caplin1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/devgenesis"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestWriteDevGenesisBeaconBlockStoresTheGenesisRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch = 0, 0, 0, 0, 0
	cfg.InitializeForkSchedule()
	genesisState, _, err := devgenesis.BuildGenesisState("test", 4, &cfg, 1_700_000_000, common.HexToHash("0x1234"))
	require.NoError(t, err)
	genesisRoot, err := genesisState.BlockRoot()
	require.NoError(t, err)
	db := memdb.NewTestDB(t, dbcfg.ChainDB)

	require.NoError(t, writeDevGenesisBeaconBlock(context.Background(), genesisState, &cfg, db))

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, genesisRoot)
	require.NoError(t, err)
	require.NotNil(t, slot, "the genesis block is not stored under the genesis state's block root")
}
