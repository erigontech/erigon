package block_collector_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/block_collector"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBlockCollectorAccumulateAndFlush(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	engine := execution_client.NewMockExecutionEngine(ctrl)
	blocks, _, _ := tests.GetBellatrixRandom()

	blocksLeft := make(map[uint64]struct{})
	for _, block := range blocks {
		blocksLeft[block.Block.Body.ExecutionPayload.BlockNumber] = struct{}{}
	}

	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes().DoAndReturn(func(ctx context.Context, blocks []*types.Block, wait bool) error {
		for _, block := range blocks {
			delete(blocksLeft, block.NumberU64())
		}
		return nil
	})
	bc := block_collector.NewBlockCollector(log.Root(), engine, &clparams.MainnetBeaconConfig, ".")
	for _, block := range blocks {
		err := bc.AddBlock(block.Block)
		if err != nil {
			t.Fatal(err)
		}
	}
	require.NoError(t, bc.Flush(context.Background()))
	require.Equal(t, len(blocksLeft), 0)
}
