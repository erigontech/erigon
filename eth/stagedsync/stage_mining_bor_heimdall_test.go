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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/eth/stagedsync/stagedsynctest"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func TestMiningBorHeimdallForwardPersistsSpans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	numBlocks := 6640
	testHarness := stagedsynctest.InitHarness(ctx, t, stagedsynctest.HarnessCfg{
		ChainConfig:            stagedsynctest.BorDevnetChainConfigWithNoBlockSealDelays(),
		GenerateChainNumBlocks: numBlocks,
		LogLvl:                 log.LvlError,
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
		LogLvl:                 log.LvlError,
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

	lastEventNumPerBlock, err := testHarness.ReadLastStateSyncEventNumPerBlockFromDB(ctx)
	require.NoError(t, err)
	require.Len(t, lastEventNumPerBlock, 1)
	require.Equal(t, uint64(1), lastEventNumPerBlock[16])
}
