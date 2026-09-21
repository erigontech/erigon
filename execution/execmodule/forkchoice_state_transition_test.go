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

package execmodule_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func TestRepeatedForkchoicePersistsFinality(t *testing.T) {
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(4, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks))
	m.ExecModule.Drain()
	head, safe, finalized := chain.TopBlock.Hash(), chain.Blocks[2].Hash(), chain.Blocks[1].Hash()
	result, err := m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
	result, err = m.ExecModule.UpdateForkChoice(t.Context(), head, common.Hash{0xff}, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
	m.ExecModule.Drain()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx), "rejected finality must not be persisted")
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
	result, err = m.ExecModule.UpdateForkChoice(t.Context(), chain.Blocks[0].Hash(), common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadHeadBlockHash(tx), "an FCU below finality must not move the executed head")
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
}

func TestRepeatedForkchoiceDoesNotWaitForWriter(t *testing.T) {
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(4, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks))
	m.ExecModule.Drain()
	head, safe, finalized := chain.TopBlock.Hash(), chain.Blocks[2].Hash(), chain.Blocks[1].Hash()
	result, err := m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()
	for _, zeroFinality := range []bool{false, true} {
		t.Run(fmt.Sprintf("zero_finality_%t", zeroFinality), func(t *testing.T) {
			defer m.ExecModule.Drain()
			writer, err := m.DB.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer writer.Rollback()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			requestSafe, requestFinalized := safe, finalized
			if zeroFinality {
				requestSafe, requestFinalized = common.Hash{}, common.Hash{}
			}
			result, err := m.ExecModule.UpdateForkChoice(ctx, head, requestSafe, requestFinalized)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, "unchanged markers must not need the MDBX writer lock")
		})
	}
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
}

func TestRepeatedForkchoicePersistsAfterCallerTimeout(t *testing.T) {
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(4, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks))
	m.ExecModule.Drain()
	head, safe, finalized := chain.TopBlock.Hash(), chain.Blocks[2].Hash(), chain.Blocks[1].Hash()

	// Release the writer before draining, including on assertion failures.
	defer m.ExecModule.Drain()
	writer, err := m.DB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer writer.Rollback()
	previousSafe, previousFinalized := rawdb.ReadForkchoiceSafe(writer), rawdb.ReadForkchoiceFinalized(writer)
	require.NotEqual(t, safe, previousSafe)
	require.NotEqual(t, finalized, previousFinalized)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result, err := m.ExecModule.UpdateForkChoice(ctx, head, safe, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, result.Status, "changed markers cannot succeed before the durable write")
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		ready, err := m.ExecModule.Ready(t.Context())
		require.NoError(collect, err)
		require.False(collect, ready, "the pending marker update must hold the execution semaphore")
	}, time.Minute, 10*time.Millisecond)
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, previousSafe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, previousFinalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))

	writer.Rollback()
	idleCtx, idleCancel := context.WithTimeout(t.Context(), time.Minute)
	defer idleCancel()
	m.ExecModule.WaitIdle(idleCtx)
	require.NoError(t, idleCtx.Err(), "the marker update must finish after the writer is released")
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadHeadBlockHash(tx))
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
	result, err = m.ExecModule.UpdateForkChoice(idleCtx, head, safe, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, "later FCUs must be able to acquire the execution semaphore")
}

func TestRepeatedForkchoiceWithLaggingFinish(t *testing.T) {
	for _, tc := range []struct {
		name               string
		requestedHeadIndex int
		ignored            bool
	}{
		{name: "below_finality", requestedHeadIndex: 0, ignored: true},
		{name: "at_finality", requestedHeadIndex: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := execmoduletester.New(t)
			chain, err := m.GenerateChain(4, nil)
			require.NoError(t, err)
			require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks))
			m.ExecModule.Drain()
			head, safe, finalized := chain.TopBlock.Hash(), chain.Blocks[2].Hash(), chain.Blocks[1].Hash()
			result, err := m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			m.ExecModule.Drain()

			requestedHead := chain.Blocks[tc.requestedHeadIndex]
			// A matching Finish height must not override stored finality, even when
			// stage progress and forkchoice markers disagree.
			require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
				return stages.SaveStageProgress(tx, stages.Finish, requestedHead.NumberU64())
			}))
			result, err = m.ExecModule.UpdateForkChoice(t.Context(), requestedHead.Hash(), requestedHead.Hash(), requestedHead.Hash())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			require.Equal(t, requestedHead.Hash(), result.LatestValidHash)
			m.ExecModule.Drain()

			wantHead, wantSafe, wantFinalized := head, safe, finalized
			if !tc.ignored {
				wantHead, wantSafe, wantFinalized = requestedHead.Hash(), requestedHead.Hash(), requestedHead.Hash()
			}
			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				require.Equal(t, wantHead, rawdb.ReadHeadBlockHash(tx), "head block marker")
				require.Equal(t, wantHead, rawdb.ReadForkchoiceHead(tx), "forkchoice head marker")
				require.Equal(t, wantSafe, rawdb.ReadForkchoiceSafe(tx), "safe marker")
				require.Equal(t, wantFinalized, rawdb.ReadForkchoiceFinalized(tx), "finalized marker")
				return nil
			}))
		})
	}
}

func TestCatchupCommitObservations(t *testing.T) {
	var mu sync.Mutex
	var observed []execmodule.StateTransitionPoint
	m := execmoduletester.New(t, execmoduletester.WithStateTransitionObserver(func(_ context.Context, point execmodule.StateTransitionPoint) {
		mu.Lock()
		defer mu.Unlock()
		observed = append(observed, point)
	}))
	m.ExecModule.Drain()
	mu.Lock()
	observed = nil
	mu.Unlock()
	chain, err := m.GenerateChain(20, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(t.Context(), chain.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), chain.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []execmodule.StateTransitionPoint{
		execmodule.StateTransitionFCUCatchupCommitReady,
		execmodule.StateTransitionFCUCatchupCommitComplete,
		execmodule.StateTransitionOverlayPublished,
		execmodule.StateTransitionCommitReady,
		execmodule.StateTransitionCommitComplete,
		execmodule.StateTransitionOverlayCleared,
	}, observed)
}
