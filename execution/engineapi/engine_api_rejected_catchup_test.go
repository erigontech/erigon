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

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/rlp"
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
	encoded := readCrashRecoveryBlocks(t, ref.ChainDB, payloads)
	blocks, err := decodeCrashRecoveryBlocks(encoded)
	require.NoError(t, err)
	want := readCrashRecoveryState(t, ref.ChainDB)
	clState := *ref.MockCl.State()
	require.NoError(t, ref.Close())

	args.DataDir, args.NoEmptyBlock1 = newSmallStepDataDir(t), false
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, args)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	acceptedTip, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.Equal(t, blocks[1].ParentHash(), acceptedTip.ExecutionPayload.ParentHash)
	require.NotEqual(t, blocks[1].Hash(), acceptedTip.ExecutionPayload.BlockHash, "catch-up must cross onto a different branch")
	eat.ExecutionModule.Drain()
	lastAccepted, err := eat.ExecutionModule.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, acceptedTip.ExecutionPayload.BlockHash, lastAccepted.HeadHash)

	rejectTip := func(t *testing.T, node engineapitester.EngineApiTester, chain []*types.Block) (*types.Block, *enginetypes.PayloadStatus) {
		t.Helper()
		last := chain[len(chain)-1]
		header := types.CopyHeader(last.HeaderNoCopy())
		header.Root[0] ^= 1
		bad := types.NewBlockFromNetwork(header, last.Body(), last.BlockAccessListSidecar())
		downloaded := append([]*types.Block(nil), chain[1:]...)
		downloaded[len(downloaded)-1] = bad
		status, err := node.ExecutionModule.InsertBlocks(ctx, downloaded)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, status)
		result, err := node.EngineApiClient.ForkchoiceUpdatedV4(ctx, &enginetypes.ForkChoiceState{
			HeadHash: bad.Hash(), SafeBlockHash: chain[2].Hash(), FinalizedBlockHash: chain[1].Hash(),
		}, nil, nil)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, result.PayloadStatus.Status)
		node.ExecutionModule.Drain()
		return bad, result.PayloadStatus
	}
	badTip, result := rejectTip(t, eat, blocks)
	require.Nil(t, result.LatestValidHash)

	assertCheckpoint := func(t *testing.T, node engineapitester.EngineApiTester) uint64 {
		t.Helper()
		var committed uint64
		require.NoError(t, node.ChainDB.View(ctx, func(tx kv.Tx) error {
			var err error
			committed, err = stages.GetStageProgress(tx, stages.Finish)
			require.NoError(t, err)
			require.Greater(t, committed, uint64(2), "catch-up must commit before reaching the invalid block")
			require.Less(t, committed, badTip.NumberU64())
			checkpoint := blocks[committed-1].Hash()
			require.Equal(t, checkpoint, rawdb.ReadHeadBlockHash(tx))
			require.Equal(t, checkpoint, rawdb.ReadHeadHeaderHash(tx))
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
		forkchoice, err := node.ExecutionModule.GetForkChoice(ctx)
		require.NoError(t, err)
		require.Equal(t, lastAccepted, forkchoice, "rejecting a forkchoice must preserve the last accepted head, safe and finalized blocks")
		return committed
	}
	committed := assertCheckpoint(t, eat)
	require.Less(t, committed, badTip.NumberU64()-1, "the durable checkpoint must precede the valid parent")

	require.NoError(t, eat.Close())
	restart := func(t *testing.T, dir string) engineapitester.EngineApiTester {
		t.Helper()
		restartArgs := args
		state := clState
		restartArgs.DataDir, restartArgs.NoEmptyBlock1, restartArgs.MockClState = dir, true, &state
		started := make(chan struct{}, 1)
		restartArgs.StateTransitionObserver = func(_ context.Context, point execmodule.StateTransitionPoint) {
			if point == execmodule.StateTransitionStartupCommitComplete {
				select {
				case started <- struct{}{}:
				default:
				}
			}
		}
		node, err := engineapitester.InitialiseEngineApiTester(ctx, restartArgs)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, node.Close()) })
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		node.ExecutionModule.Drain()
		return node
	}
	resumeValidChain := func(t *testing.T, node engineapitester.EngineApiTester) {
		t.Helper()
		// The earlier valid blocks are still available by hash. Only the replacement tip needs importing.
		status, err := node.ExecutionModule.InsertBlocks(ctx, blocks[len(blocks)-1:])
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, status)
		require.NoError(t, node.MockCl.UpdateForkChoice(ctx, payloads[len(payloads)-1]))
		node.ExecutionModule.Drain()
		assertCrashRecoveryState(t, want, readCrashRecoveryState(t, node.ChainDB))
		built, err := node.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.Equal(t, blocks[len(blocks)-1].Hash(), built.ExecutionPayload.ParentHash)
	}

	t.Run("restart_after_rejection", func(t *testing.T) {
		node := restart(t, args.DataDir)
		require.Equal(t, committed, assertCheckpoint(t, node))
		require.NoError(t, node.MockCl.UpdateForkChoice(ctx, payloads[committed-1]))
		resumeValidChain(t, node)
	})

	t.Run("checkpoint_is_parent", func(t *testing.T) {
		freshArgs := args
		freshArgs.DataDir = newSmallStepDataDir(t)
		node, err := engineapitester.InitialiseEngineApiTester(ctx, freshArgs)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, node.Close()) })
		_, err = node.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		// End the same fork one block after the measured commit boundary.
		bad, status := rejectTip(t, node, blocks[:committed+1])
		require.NotNil(t, status.LatestValidHash)
		require.Equal(t, bad.ParentHash(), *status.LatestValidHash)
	})

	t.Run("interrupted_before_rejection", func(t *testing.T) {
		if testing.Short() {
			t.Skip("subprocess crash recovery")
		}
		dir := newSmallStepDataDir(t)
		downloaded := append([]crashRecoveryBlock(nil), encoded[1:]...)
		badRLP, err := rlp.EncodeToBytes(badTip)
		require.NoError(t, err)
		downloaded[len(downloaded)-1].RLP = badRLP
		killAtUnwindBoundary(t, crashRecoveryRequest{
			Genesis: genesis, CoinbaseKey: crypto.FromECDSA(key), DataDir: dir,
			Point: execmodule.StateTransitionFCUCatchupCommitComplete, CatchupCycle: 1,
			Canonical: []*engineapitester.MockClPayload{empty, acceptedTip}, Downloaded: downloaded,
		})
		var interruptedAt uint64
		inspectCrashRecoveryDataDir(t, dir, func(db kv.TemporalRoDB) {
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				canonical, err := rawdb.ReadCanonicalHash(tx, badTip.NumberU64())
				require.NoError(t, err)
				require.Equal(t, badTip.Hash(), canonical)
				interruptedAt, err = stages.GetStageProgress(tx, stages.Finish)
				return err
			}))
		})
		node := restart(t, dir)
		require.Greater(t, assertCheckpoint(t, node), interruptedAt, "startup must keep the cycles it committed before rejecting the tip")
		resumeValidChain(t, node)
	})
}
