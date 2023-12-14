package stagedsync_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/stagedsync/test"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func TestBorHeimdallForwardPersistsSpans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := testlog.Logger(t, log.LvlInfo)
	numBlocks := 6640
	testHarness := test.InitHarness(ctx, t, logger, test.HarnessCfg{
		ChainConfig:            test.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDb(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 3)
	require.Equal(t, uint64(0), spans[0].ID)
	require.Equal(t, uint64(0), spans[0].StartBlock)
	require.Equal(t, uint64(255), spans[0].EndBlock)
	require.Equal(t, uint64(1), spans[1].ID)
	require.Equal(t, uint64(256), spans[1].StartBlock)
	require.Equal(t, uint64(6655), spans[1].EndBlock)
	require.Equal(t, uint64(2), spans[2].ID)
	require.Equal(t, uint64(6656), spans[2].StartBlock)
	require.Equal(t, uint64(13055), spans[2].EndBlock)
}
