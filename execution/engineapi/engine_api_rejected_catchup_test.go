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

package engineapi_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestEngineApiRejectedCatchupRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	args := engineapitester.EngineApiTesterInitArgs{
		Logger: testlog.Logger(t, log.LvlError), DataDir: newSmallStepDataDir(t),
		Genesis: genesis, CoinbaseKey: key, NoEmptyBlock1: true,
		EthConfigTweaker: configureCatchupCrashRecovery,
	}
	ref, err := engineapitester.InitialiseEngineApiTester(ctx, args)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ref.Close()) })
	empty, err := ref.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	payloads, _, _, _ := buildChurnChain(ctx, t, ref, 30, func(k int) int64 { return int64(k) })
	payloads = append([]*engineapitester.MockClPayload{empty}, payloads...)
	blocks, err := decodeCrashRecoveryBlocks(readCrashRecoveryBlocks(t, ref.ChainDB, payloads))
	require.NoError(t, err)
	want := readCrashRecoveryState(t, ref.ChainDB)
	clState := ref.MockCl.State()
	require.NoError(t, ref.Close())

	args.DataDir, args.NoEmptyBlock1 = newSmallStepDataDir(t), false
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, args)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	insertCrashRecoveryPayloads(ctx, t, eat, payloads[:2])
	require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[1]))
	eat.ExecutionModule.Drain()

	header := types.CopyHeader(blocks[len(blocks)-1].HeaderNoCopy())
	header.Root[0] ^= 1
	badTip := types.NewBlockFromNetwork(header, blocks[len(blocks)-1].Body(), blocks[len(blocks)-1].BlockAccessListSidecar())
	downloaded := append([]*types.Block(nil), blocks[2:]...)
	downloaded[len(downloaded)-1] = badTip
	status, err := eat.ExecutionModule.InsertBlocks(ctx, downloaded)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := eat.ExecutionModule.UpdateForkChoice(ctx, badTip.Hash(), common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.Status)
	eat.ExecutionModule.Drain()

	var committed uint64
	require.NoError(t, eat.ChainDB.View(ctx, func(tx kv.Tx) error {
		var err error
		committed, err = stages.GetStageProgress(tx, stages.Finish)
		require.NoError(t, err)
		require.Greater(t, committed, uint64(2), "catch-up must commit before reaching the invalid block")
		require.Less(t, committed, badTip.NumberU64())
		head := blocks[committed-1].Hash()
		require.Equal(t, head, rawdb.ReadHeadBlockHash(tx))
		require.Equal(t, head, rawdb.ReadHeadHeaderHash(tx))
		require.Equal(t, head, result.LatestValidHash)
		for _, stage := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders} {
			progress, err := stages.GetStageProgress(tx, stage)
			require.NoError(t, err)
			require.Equal(t, committed, progress, "%s", stage)
		}
		canonical, err := rawdb.ReadCanonicalHash(tx, badTip.NumberU64())
		require.NoError(t, err)
		require.Zero(t, canonical, "rejected blocks must not be replayed at startup")
		lastBlock, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.Equal(t, committed, lastBlock)
		return nil
	}))

	args.NoEmptyBlock1, args.MockClState = true, clState
	started := make(chan struct{}, 1)
	args.StateTransitionObserver = func(_ context.Context, point execmodule.StateTransitionPoint) {
		if point == execmodule.StateTransitionStartupCommitComplete {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	}
	require.NoError(t, eat.Close())
	eat, err = engineapitester.InitialiseEngineApiTester(ctx, args)
	require.NoError(t, err)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	eat.ExecutionModule.Drain()
	require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[committed-1]))

	// Previously downloaded valid blocks remain available after rejecting the tip.
	status, err = eat.ExecutionModule.InsertBlocks(ctx, blocks[len(blocks)-1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[len(payloads)-1]))
	eat.ExecutionModule.Drain()
	assertCrashRecoveryState(t, want, readCrashRecoveryState(t, eat.ChainDB))
}
