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

package execmoduletester

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestCatchupRetriesAfterMissingBlockResult(t *testing.T) {
	for _, tc := range []struct {
		name              string
		blocks, failCycle int
	}{
		{"cycle_1", 32, 1},
		{"cycle_2", 32, 2},
		{"at_tip", 4, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := New(t, WithStepSize(16), WithLoopBlockLimit(8))
			chain, err := m.GenerateChain(tc.blocks, nil)
			require.NoError(t, err)
			status, err := m.InsertBlocks(t.Context(), chain.Blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)

			// Inject the executor's missing-result error just before a batch would commit.
			missingResult := fmt.Errorf("%w: block result missing", rules.ErrInvalidBlock)
			finishCfg := stagedsync.StageFinishCfg()
			cycle := 0
			m.posStagedSync.MockExecFunc(stages.Finish, func(_ bool, s *stagedsync.StageState, _ stagedsync.Unwinder, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
				cycle++
				if cycle == tc.failCycle {
					return missingResult
				}
				return stagedsync.FinishForward(s, tx, finishCfg)
			})

			head := chain.TopBlock
			result, err := m.UpdateForkChoice(t.Context(), head.Header())
			m.ExecModule.WaitIdle(t.Context())
			require.ErrorIs(t, err, missingResult, "forkchoice returned %s", result.Status)
			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				hash, err := rawdb.ReadCanonicalHash(tx, head.NumberU64())
				if err != nil {
					return err
				}
				if tc.failCycle == 1 {
					require.Zero(t, hash, "the first cycle has not committed the proposal")
				} else {
					require.Equal(t, head.Hash(), hash, "the committed proposal must remain available for retry")
				}
				return nil
			}))

			result, err = m.UpdateForkChoice(t.Context(), head.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			require.Equal(t, head.Hash(), result.LatestValidHash)
		})
	}
}

func TestRejectedCatchupRetryRestoresCheckpoint(t *testing.T) {
	for _, accepted := range []int{0, 2} {
		t.Run(fmt.Sprintf("accepted_blocks_%d", accepted), func(t *testing.T) {
			testRejectedCatchupRetry(t, accepted)
		})
	}
}

func testRejectedCatchupRetry(t *testing.T, accepted int) {
	ctx := t.Context()
	m := New(t, WithStepSize(16), WithLoopBlockLimit(8))
	chain, err := m.GenerateChain(19, nil)
	require.NoError(t, err)
	var acceptedHead common.Hash
	if accepted > 0 {
		require.NoError(t, m.InsertValidateAndUfc1By1(ctx, chain.Blocks[:accepted]))
		m.ExecModule.WaitIdle(ctx)
		acceptedHead = chain.Blocks[accepted-1].Hash()
	} else {
		// The tester selects genesis during setup. Start this case before any accepted forkchoice.
		m.ExecModule.WaitIdle(ctx)
		require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error { return tx.ClearTable(kv.LastForkchoice) }))
	}

	withWrongGasUsed := func(block *types.Block) *types.Block {
		header := block.Header()
		header.GasUsed++
		return types.NewBlockFromNetwork(header, block.Body(), block.BlockAccessListSidecar())
	}
	badTip := withWrongGasUsed(chain.TopBlock)
	proposal := append([]*types.Block(nil), chain.Blocks[accepted:]...)
	proposal[len(proposal)-1] = badTip
	status, err := m.InsertBlocks(ctx, proposal)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	// Interrupt the second cycle before execution can reject the tip. The first
	// cycle has already made the whole proposal canonical on disk.
	interrupted := errors.New("interrupted catch-up")
	cycle := 0
	m.posStagedSync.MockExecFunc(stages.BlockHashes, func(_ bool, s *stagedsync.StageState, _ stagedsync.Unwinder, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
		cycle++
		if cycle == 2 {
			return interrupted
		}
		return stagedsync.SpawnBlockHashStage(s, tx)
	})
	_, err = m.UpdateForkChoice(ctx, badTip.Header())
	m.ExecModule.WaitIdle(ctx)
	require.ErrorIs(t, err, interrupted)

	pendingCheckpoint := func() uint64 {
		t.Helper()
		var finish uint64
		require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
			hash, err := rawdb.ReadCanonicalHash(tx, badTip.NumberU64())
			require.NoError(t, err)
			require.Equal(t, badTip.Hash(), hash)
			finish, err = stages.GetStageProgress(tx, stages.Finish)
			return err
		}))
		return finish
	}
	finish := pendingCheckpoint()
	require.Greater(t, finish, uint64(accepted))
	require.Less(t, finish, badTip.NumberU64())

	checkpoint := chain.Blocks[finish-1].Hash()
	// Rejecting a different child of the checkpoint must leave the pending proposal available for retry.
	sibling := withWrongGasUsed(chain.Blocks[finish])
	status, err = m.InsertBlocks(ctx, []*types.Block{sibling})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(ctx, sibling.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.Status)
	require.Equal(t, checkpoint, result.LatestValidHash)
	m.ExecModule.WaitIdle(ctx)
	require.Equal(t, finish, pendingCheckpoint())

	result, err = m.UpdateForkChoice(ctx, badTip.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.Status)
	m.ExecModule.WaitIdle(ctx)
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		hash, err := rawdb.ReadCanonicalHash(tx, badTip.NumberU64())
		require.NoError(t, err)
		require.Zero(t, hash, "the rejected tip must no longer be canonical")
		last, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.Equal(t, finish, last)
		progress, aligned, err := stages.GetStageProgressIfAllEqual(tx, stages.Finish, stages.Execution, stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders)
		require.NoError(t, err)
		require.True(t, aligned)
		require.Equal(t, finish, progress)
		require.Equal(t, checkpoint, rawdb.ReadHeadBlockHash(tx))
		require.Equal(t, checkpoint, rawdb.ReadHeadHeaderHash(tx))
		require.Equal(t, acceptedHead, rawdb.ReadForkchoiceHead(tx))
		return nil
	}))
	require.Equal(t, checkpoint, result.LatestValidHash)

	insertAndSwitchTo(t, m, []*types.Block{chain.TopBlock})
}
