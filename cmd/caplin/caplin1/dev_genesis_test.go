package caplin1

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

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

func TestDevGenesisBeaconBodyUsesPreGloasPayloadHeader(t *testing.T) {
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
	require.Equal(t, header.ParentHash, body.ExecutionPayload.ParentHash)
	require.Equal(t, header.StateRoot, body.ExecutionPayload.StateRoot)
	require.Equal(t, header.BlockHash, body.ExecutionPayload.BlockHash)
	require.Equal(t, header.GasLimit, body.ExecutionPayload.GasLimit)
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

func TestDevGenesisBeaconBodyPreservesMatchingDefaultGloasBody(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	genesisState := state.New(&cfg)
	genesisState.SetVersion(clparams.GloasVersion)
	genesisState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		BlockHash:          common.HexToHash("0x01"),
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	})

	expected := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	expected.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	expectedRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	genesisState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{BodyRoot: expectedRoot})

	body := devGenesisBeaconBody(genesisState, &cfg)
	bodyRoot, err := body.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, bodyRoot)
	require.Equal(t, common.Hash{}, body.SignedExecutionPayloadBid.Message.BlockHash)
}
