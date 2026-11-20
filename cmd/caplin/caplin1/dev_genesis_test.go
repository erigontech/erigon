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
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	require.NoError(t, writeDevGenesisBeaconBlock(context.Background(), genesisState, &cfg, db))

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, genesisRoot)
	require.NoError(t, err)
	require.NotNil(t, slot, "the genesis block is not stored under the genesis state's block root")
}

func TestDevGenesisBeaconBodyUsesGloasStateBid(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	genesisState := state.New(&cfg)
	genesisState.SetVersion(clparams.GloasVersion)

	bid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       common.HexToHash("0x01"),
		BlockHash:             common.HexToHash("0x02"),
		GasLimit:              30_000_000,
		BlobKzgCommitments:    *solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		ExecutionRequestsRoot: common.HexToHash("0x03"),
	}
	genesisState.SetLatestExecutionPayloadBid(bid)

	body := devGenesisBeaconBody(genesisState, &cfg)
	require.Equal(t, bid, body.SignedExecutionPayloadBid.Message)

	expected := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	expected.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	expected.SignedExecutionPayloadBid.Message = bid
	expectedRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	bodyRoot, err := body.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, bodyRoot)
}

func TestDevGenesisBeaconBodyLeavesPreGloasPayloadEmpty(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	genesisState := state.New(&cfg)
	genesisState.SetVersion(clparams.FuluVersion)
	header := cltypes.NewEth1Header(clparams.FuluVersion)
	header.ParentHash = common.HexToHash("0x01")
	header.StateRoot = common.HexToHash("0x02")
	header.BlockHash = common.HexToHash("0x03")
	header.GasLimit = 30_000_000
	genesisState.SetLatestExecutionPayloadHeader(header)

	body := devGenesisBeaconBody(genesisState, &cfg)
	require.Equal(t, common.Hash{}, body.ExecutionPayload.ParentHash)
	require.Equal(t, common.Hash{}, body.ExecutionPayload.StateRoot)
	require.Equal(t, common.Hash{}, body.ExecutionPayload.BlockHash)
	require.Zero(t, body.ExecutionPayload.GasLimit)
	require.Nil(t, body.SignedExecutionPayloadBid)
}

func TestDevGenesisBeaconBodyToleratesMissingGloasBid(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	genesisState := state.New(&cfg)
	genesisState.SetVersion(clparams.GloasVersion)

	body := devGenesisBeaconBody(genesisState, &cfg)
	require.NotNil(t, body.SignedExecutionPayloadBid.Message)
	_, err := body.HashSSZ()
	require.NoError(t, err)
}

func TestDevGenesisBeaconBodyUsesGloasStateBidWhenDefaultBodyMatchesHeader(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	genesisState := state.New(&cfg)
	genesisState.SetVersion(clparams.GloasVersion)
	bid := &cltypes.ExecutionPayloadBid{
		BlockHash:          common.HexToHash("0x01"),
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}
	genesisState.SetLatestExecutionPayloadBid(bid)

	expected := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	expected.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	expectedRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	genesisState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{BodyRoot: expectedRoot})

	body := devGenesisBeaconBody(genesisState, &cfg)
	bodyRoot, err := body.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, bid, body.SignedExecutionPayloadBid.Message)
	require.NotEqual(t, expectedRoot, bodyRoot)
}
