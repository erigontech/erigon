// Copyright 2024 The Erigon Authors
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

package stagedsync_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stagedsynctest"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func TestBorHeimdallForwardPersistsSpans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 4000
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlError,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStateSyncStageForward(t, stages.BorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 2)
	require.Equal(t, heimdall.SpanId(0), spans[0].Id)
	require.Equal(t, uint64(0), spans[0].StartBlock)
	require.Equal(t, uint64(255), spans[0].EndBlock)
	require.Equal(t, heimdall.SpanId(1), spans[1].Id)
	require.Equal(t, uint64(256), spans[1].StartBlock)
	require.Equal(t, uint64(6655), spans[1].EndBlock)
}

func TestBorHeimdallForwardFetchesFirstSpanDuringSecondSprintStart(t *testing.T) {
	// span 0 and 1 are required in the start of second sprint (of 0th span) to commit
	// in genesis contracts. we need span 1 at that time to mimic behaviour in bor.
	t.Parallel()

	ctx := context.Background()
	numBlocks := 16 // Start of 2nd sprint of 0th span
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlError,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStateSyncStageForward(t, stages.BorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 2)
	require.Equal(t, heimdall.SpanId(0), spans[0].Id)
	require.Equal(t, uint64(0), spans[0].StartBlock)
	require.Equal(t, uint64(255), spans[0].EndBlock)
	require.Equal(t, heimdall.SpanId(1), spans[1].Id)
	require.Equal(t, uint64(256), spans[1].StartBlock)
	require.Equal(t, uint64(6655), spans[1].EndBlock)
}

func TestBorHeimdallForwardFetchesFirstSpanAfterSecondSprintStart(t *testing.T) {
	// Note this test differs from TestBorHeimdallForwardFetchesFirstSpanDuringSecondSprintStart
	// since we should be able to handle both scenarios:
	//   - calling the stage with toBlockNum=16
	//   - calling the stage with toBlockNum=20 (some block num after second sprint start)
	//
	// span 0 and 1 are required at and after the start of second sprint (of 0th span) to commit
	// in genesis contracts. we need span 1 at that time to mimic behaviour in bor.
	t.Parallel()

	ctx := context.Background()
	numBlocks := 20 // After the start of 2nd sprint of 0th span
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlError,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStateSyncStageForward(t, stages.BorHeimdall)

	// asserts
	spans, err := testHarness.ReadSpansFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, spans, 2)
	require.Equal(t, heimdall.SpanId(0), spans[0].Id)
	require.Equal(t, uint64(0), spans[0].StartBlock)
	require.Equal(t, uint64(255), spans[0].EndBlock)
	require.Equal(t, heimdall.SpanId(1), spans[1].Id)
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
		LogLvl:                 log.LvlError,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStateSyncStageForward(t, stages.BorHeimdall)

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

func TestBorHeimdallForwardPersistsStateSyncEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 96
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlError,
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStateSyncStageForward(t, stages.BorHeimdall)

	// asserts
	// 1 event per sprint expected
	events, err := testHarness.ReadStateSyncEventsFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, events, 6)

	lastEventNumPerBlock, err := testHarness.ReadLastStateSyncEventNumPerBlockFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, lastEventNumPerBlock, 6)
	require.Equal(t, uint64(1), lastEventNumPerBlock[16])
	require.Equal(t, uint64(2), lastEventNumPerBlock[32])
	require.Equal(t, uint64(3), lastEventNumPerBlock[48])
	require.Equal(t, uint64(4), lastEventNumPerBlock[64])
	require.Equal(t, uint64(5), lastEventNumPerBlock[80])
	require.Equal(t, uint64(6), lastEventNumPerBlock[96])
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
		LogLvl:                 log.LvlError,
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
	testHarness.RunStateSyncStageForwardWithErrorIs(t, stages.BorHeimdall, stagedsync.ErrHeaderValidatorsLengthMismatch)
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
		LogLvl:                 log.LvlError,
		HeimdallProducersOverride: map[uint64][]valset.Validator{
			1: {
				*valset.NewValidator(crypto.PubkeyToAddress(validatorKey1.PublicKey), 1),
			},
		},
	})
	// pretend-update previous stage progress
	testHarness.SaveStageProgress(ctx, t, stages.Headers, uint64(numBlocks))

	// run stage under test
	testHarness.RunStateSyncStageForwardWithErrorIs(t, stages.BorHeimdall, stagedsync.ErrHeaderValidatorsBytesMismatch)
}

func TestBorHeimdallForwardDetectsUnauthorizedSignerError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 312
	chainConfig := stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays()
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            chainConfig,
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlError,
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
	testHarness.RunStateSyncStageForward(t, stages.BorHeimdall)

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
