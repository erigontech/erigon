//go:build notzkevm
// +build notzkevm

package stagedsync_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stagedsynctest"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

func TestMiningBorHeimdallForwardPersistsSpans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 6640
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
	})
	// pretend-update previous stage progress
	testHarness.SetMiningBlockEmptyHeader(ctx, t, uint64(numBlocks))
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunMiningStageForward(ctx, t, stages.MiningBorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 3)
	require.Equal(t, heimdall.SpanId(0), spans[0].Id)
	require.Equal(t, uint64(0), spans[0].StartBlock)
	require.Equal(t, uint64(255), spans[0].EndBlock)
	require.Equal(t, heimdall.SpanId(1), spans[1].Id)
	require.Equal(t, uint64(256), spans[1].StartBlock)
	require.Equal(t, uint64(6655), spans[1].EndBlock)
	require.Equal(t, heimdall.SpanId(2), spans[2].Id)
	require.Equal(t, uint64(6656), spans[2].StartBlock)
	require.Equal(t, uint64(13055), spans[2].EndBlock)
}

func TestMiningBorHeimdallForwardPersistsStateSyncEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 15
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
	})
	// pretend-update previous stage progress
	testHarness.SetMiningBlockEmptyHeader(ctx, t, uint64(numBlocks))
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunMiningStageForward(ctx, t, stages.MiningBorHeimdall)

	// asserts
	// 1 event per sprint expected
	events, err := testHarness.ReadStateSyncEventsFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, events, 1)

	firstEventNumPerBlock, err := testHarness.ReadFirstStateSyncEventNumPerBlockFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, firstEventNumPerBlock, 1)
	require.Equal(t, uint64(1), firstEventNumPerBlock[16])
}
