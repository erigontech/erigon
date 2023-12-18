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

func TestBorHeimdallForwardPersistsStateSyncEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := testlog.Logger(t, log.LvlInfo)
	numBlocks := 96
	testHarness := test.InitHarness(ctx, t, logger, test.HarnessCfg{
		ChainConfig:            test.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)

	// asserts
	// 1 event per sprint expected
	events, err := testHarness.ReadStateSyncEventsFromDb(ctx)
	require.NoError(t, err)
	require.Len(t, events, 6)

	firstEventNumPerBlock, err := testHarness.ReadFirstStateSyncEventNumPerBlockFromDb(ctx)
	require.NoError(t, err)
	require.Len(t, firstEventNumPerBlock, 6)
	require.Equal(t, uint64(1), firstEventNumPerBlock[16])
	require.Equal(t, uint64(2), firstEventNumPerBlock[32])
	require.Equal(t, uint64(3), firstEventNumPerBlock[48])
	require.Equal(t, uint64(4), firstEventNumPerBlock[64])
	require.Equal(t, uint64(5), firstEventNumPerBlock[80])
	require.Equal(t, uint64(6), firstEventNumPerBlock[96])
}

//
// TODO - PR 1 after this is to add mining sync and RunMiningStageForward to test harness and 2 tests for spans & bor events persistence in mining=true
//      - PR 2 after this is to tidy up the code in bor heimdall stage (refactor out funcs, style, warnings, etc.)
//      - PR 3 is a change in logic + test to fetch next span at beginning of last sprint in current span
//

func TestBorHeimdallForwardErrInvalidSpanValidatorsDueToLenMismatch(t *testing.T) {
	//
	// TODO - for this we can simply mock the heimdall client to return a different validator set and proposer at end of sprint
	//

	t.Parallel()

	ctx := context.Background()
	logger := testlog.Logger(t, log.LvlInfo)
	numBlocks := 271
	testHarness := test.InitHarness(ctx, t, logger, test.HarnessCfg{
		ChainConfig:            test.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)
}

func TestBorHeimdallForwardErrInvalidSpanValidatorsDueToDiffValidatorSet(t *testing.T) {
	//
	// TODO - for this we can simply mock the heimdall client to return a different validator set and proposer at end of sprint
	//
}

func TestBorHeimdallForwardUnauthorizedSignerErrorDueToSnapshotValidatorSetDiff(t *testing.T) {
	//
	// TODO - for this persist a new sealed header but with different signer
	//
}
