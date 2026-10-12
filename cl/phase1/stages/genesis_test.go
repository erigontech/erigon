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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestWriteGenesisBeaconBlockGloas(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	genesisState := state.New(&config)
	genesisState.SetVersion(clparams.GloasVersion)
	genesisHash := common.Hash{0x11}
	genesisState.SetLatestBlockHash(genesisHash)
	bid := genesisState.GetLatestExecutionPayloadBid()
	bid.ParentBlockHash = genesisHash
	genesisState.SetLatestExecutionPayloadBid(bid)

	body := cltypes.NewBeaconBody(&config, clparams.GloasVersion)
	body.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(config.SyncCommitteeSize) / 8)
	body.SignedExecutionPayloadBid.Message = bid
	bodyRoot, err := body.HashSSZ()
	require.NoError(t, err)
	genesisState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{BodyRoot: bodyRoot})
	genesisRoot, err := genesisState.BlockRoot()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	require.NoError(t, writeGenesisBeaconBlock(t.Context(), &Cfg{
		state: genesisState, beaconCfg: &config, indiciesDB: db,
	}))

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	header, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(t.Context(), tx, genesisRoot)
	require.NoError(t, err)
	require.NotNil(t, header, "the genesis body must match the state's bid and be persisted")
	require.Equal(t, common.Hash(bodyRoot), header.Header.BodyRoot)
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, config.GenesisSlot)
	require.NoError(t, err)
	require.Equal(t, common.Hash(genesisRoot), canonicalRoot)
}
