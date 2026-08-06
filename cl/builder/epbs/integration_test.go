package epbs

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type testImportedBlockReader struct {
	block *cltypes.SignedBeaconBlock
}

func (r testImportedBlockReader) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return r.block, r.block != nil
}

func TestInitBuilderService_Disabled(t *testing.T) {
	cfg := epbscfg.Config{Enabled: false}
	svc, err := InitBuilderService(cfg, BuilderDeps{})
	require.NoError(t, err)
	require.Nil(t, svc)
}

func TestInitBuilderService_RejectsInvalidConfig(t *testing.T) {
	_, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: "key", BidMargin: -1}, BuilderDeps{})
	require.ErrorContains(t, err, "bid margin")

	_, err = InitBuilderService(epbscfg.Config{Enabled: true, BidMargin: 0.5}, BuilderDeps{})
	require.ErrorContains(t, err, "key path")
}

func TestInitBuilderService_RejectsMissingDependencies(t *testing.T) {
	_, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: "key", BidMargin: 0.5}, BuilderDeps{})
	require.ErrorContains(t, err, "context is required")
}

func TestBuilderService_Shutdown_Nil(t *testing.T) {
	// Shutdown on nil should not panic.
	var svc *BuilderService
	svc.Shutdown()
}

func TestBalanceStatus_Zero(t *testing.T) {
	status := BalanceStatus{}
	require.False(t, status.Active)
	require.Equal(t, uint64(0), status.Balance)
}

func TestHandleImportedBlock_RevealsExactWinningBid(t *testing.T) {
	cfg := testBeaconCfg()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 100
	block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		Slot:            100,
		BuilderIndex:    42,
		ParentBlockHash: common.HexToHash("0x1111"),
		ParentBlockRoot: common.HexToHash("0x2222"),
		BlockHash:       common.HexToHash("0x3333"),
	}}
	blockRoot := common.HexToHash("0x4444")
	var called bool
	err := handleImportedBlock(context.Background(), &beaconevents.BlockData{Slot: 100, Block: blockRoot}, testImportedBlockReader{block: block}, func(_ context.Context, slot, builderIndex uint64, parentHash, parentRoot, blockHash, beaconRoot common.Hash) error {
		called = true
		require.Equal(t, uint64(100), slot)
		require.Equal(t, uint64(42), builderIndex)
		require.Equal(t, common.HexToHash("0x1111"), parentHash)
		require.Equal(t, common.HexToHash("0x2222"), parentRoot)
		require.Equal(t, common.HexToHash("0x3333"), blockHash)
		require.Equal(t, blockRoot, beaconRoot)
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}
