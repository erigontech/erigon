package stagedsync_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stagedsynctest"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
)

func TestBorHeimdallForwardPersistsSpans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 4000
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 2)
	require.Equal(t, uint64(0), spans[0].ID)
	require.Equal(t, uint64(0), spans[0].StartBlock)
	require.Equal(t, uint64(255), spans[0].EndBlock)
	require.Equal(t, uint64(1), spans[1].ID)
	require.Equal(t, uint64(256), spans[1].StartBlock)
	require.Equal(t, uint64(6655), spans[1].EndBlock)
}

func TestBorHeimdallForwardFetchesNextSpanDuringLastSprintOfCurrentSpan(t *testing.T) {
	// heimdall prepares the next span a number of sprints before the end of the current one
	// we should be fetching the next span once we reach the last sprint of the current span
	// this mimics the behaviour in bor
	t.Parallel()

	ctx := context.Background()
	numBlocks := 6640
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDB(ctx)
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
	numBlocks := 96
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)

	// asserts
	// 1 event per sprint expected
	events, err := testHarness.ReadStateSyncEventsFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, events, 6)

	firstEventNumPerBlock, err := testHarness.ReadFirstStateSyncEventNumPerBlockFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, firstEventNumPerBlock, 6)
	require.Equal(t, uint64(1), firstEventNumPerBlock[16])
	require.Equal(t, uint64(2), firstEventNumPerBlock[32])
	require.Equal(t, uint64(3), firstEventNumPerBlock[48])
	require.Equal(t, uint64(4), firstEventNumPerBlock[64])
	require.Equal(t, uint64(5), firstEventNumPerBlock[80])
	require.Equal(t, uint64(6), firstEventNumPerBlock[96])
}

func TestBorHeimdallForwardErrHeaderValidatorsLengthMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 271
	validatorKey1, err := crypto.GenerateKey()
	require.NoError(t, err)
	validatorKey2, err := crypto.GenerateKey()
	require.NoError(t, err)
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
		HeimdallProducersOverride: map[uint64][]valset.Validator{
			1: {
				*valset.NewValidator(crypto.PubkeyToAddress(validatorKey1.PublicKey), 1),
				*valset.NewValidator(crypto.PubkeyToAddress(validatorKey2.PublicKey), 1),
			},
		},
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForwardWithErrorIs(t, stages.BorHeimdall, stagedsync.ErrHeaderValidatorsLengthMismatch)
}

func TestBorHeimdallForwardErrHeaderValidatorsBytesMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 271
	validatorKey1, err := crypto.GenerateKey()
	require.NoError(t, err)
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
		HeimdallProducersOverride: map[uint64][]valset.Validator{
			1: {
				*valset.NewValidator(crypto.PubkeyToAddress(validatorKey1.PublicKey), 1),
			},
		},
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStageForwardWithErrorIs(t, stages.BorHeimdall, stagedsync.ErrHeaderValidatorsBytesMismatch)
}

func TestBorHeimdallForwardDetectsUnauthorizedSignerError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 312
	chainConfig := stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays()
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            chainConfig,
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlInfo,
	})

	// prepare invalid header and insert it in the db
	latestHeader, err := testHarness.ReadHeaderByNumber(ctx, uint64(numBlocks))
	require.NoError(t, err)
	gasLimit := uint64(15500)
	invalidHeader := core.MakeEmptyHeader(latestHeader, chainConfig, uint64(time.Now().Unix()), &gasLimit)
	invalidHeader.Number = new(big.Int).Add(latestHeader.Number, big.NewInt(1))
	invalidHeader.Extra = bytes.Repeat([]byte{0x00}, types.ExtraVanityLength+types.ExtraSealLength)
	validatorKey1, err := crypto.GenerateKey()
	require.NoError(t, err)
	sighash, err := crypto.Sign(crypto.Keccak256(bor.BorRLP(invalidHeader, testHarness.BorConfig())), validatorKey1)
	require.NoError(t, err)
	copy(invalidHeader.Extra[len(invalidHeader.Extra)-types.ExtraSealLength:], sighash)
	testHarness.SaveHeader(ctx, t, invalidHeader)
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, invalidHeader.Number.Uint64())
	require.Equal(t, uint64(numBlocks+1), testHarness.GetStageProgress(ctx, t, stages.Headers))
	require.Equal(t, uint64(0), testHarness.GetStageProgress(ctx, t, stages.BorHeimdall))

	// run stage under test
	testHarness.RunStageForward(t, stages.BorHeimdall)

	// asserts
	require.Equal(t, uint64(numBlocks+1), testHarness.GetStageProgress(ctx, t, stages.BorHeimdall))
	require.Equal(t, invalidHeader.Number.Uint64()-1, testHarness.StateSyncUnwindPoint())
	unwindReason := testHarness.StateSyncUnwindReason()
	require.Equal(t, invalidHeader.Hash(), *unwindReason.Block)
	var unauthorizedSignerErr *valset.UnauthorizedSignerError
	ok := errors.As(unwindReason.Err, &unauthorizedSignerErr)
	require.True(t, ok)
	require.Equal(t, invalidHeader.Number.Uint64(), unauthorizedSignerErr.Number)
	require.Equal(t, crypto.PubkeyToAddress(validatorKey1.PublicKey).Bytes(), unauthorizedSignerErr.Signer)
}
