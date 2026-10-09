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
	for _, tc := range []struct {
		name                        string
		storedFinality              bool
		updateSafe, updateFinalized bool
	}{
		{name: "initialize_both", updateSafe: true, updateFinalized: true},
		{name: "update_both", storedFinality: true, updateSafe: true, updateFinalized: true},
		{name: "update_safe_only", storedFinality: true, updateSafe: true},
		{name: "update_finalized_only", storedFinality: true, updateFinalized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := execmoduletester.New(t)
			chain, err := m.GenerateChain(4, nil)
			require.NoError(t, err)
			var previousSafe, previousFinalized common.Hash
			fcuOpts := make([][]execmoduletester.UFCOpt, len(chain.Blocks))
			if tc.storedFinality {
				previousSafe, previousFinalized = chain.Blocks[2].Hash(), chain.Blocks[1].Hash()
				fcuOpts[len(fcuOpts)-1] = []execmoduletester.UFCOpt{
					execmoduletester.WithSafeHash(previousSafe),
					execmoduletester.WithFinalisedHash(previousFinalized),
				}
			}
			require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks, execmoduletester.WithFcuOptSeq(fcuOpts)))
			m.ExecModule.Drain()

			head := chain.TopBlock.Hash()
			var safe, finalized common.Hash
			wantSafe, wantFinalized := previousSafe, previousFinalized
			if tc.updateSafe {
				safe = head
				wantSafe = safe
			}
			if tc.updateFinalized {
				finalized = chain.Blocks[2].Hash()
				wantFinalized = finalized
			}
			result, err := m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			m.ExecModule.Drain()
			assertPersistedForkchoice(t, m.DB, head, wantSafe, wantFinalized)

			result, err = m.ExecModule.UpdateForkChoice(t.Context(), head, common.Hash{0xff}, wantFinalized)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
			m.ExecModule.Drain()
			assertPersistedForkchoice(t, m.DB, head, wantSafe, wantFinalized)

			result, err = m.ExecModule.UpdateForkChoice(t.Context(), chain.Blocks[0].Hash(), common.Hash{}, common.Hash{})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			m.ExecModule.Drain()
			assertPersistedForkchoice(t, m.DB, head, wantSafe, wantFinalized)
		})
	}
}

func TestRepeatedForkchoiceRepairsHeadHeader(t *testing.T) {
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
	assertPersistedForkchoice(t, m.DB, head, safe, finalized)

	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteHeadHeaderHash(tx, chain.Blocks[0].Hash())
	}))
	result, err = m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()
	assertPersistedForkchoice(t, m.DB, head, safe, finalized)
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

	for _, tc := range []struct {
		name            string
		safe, finalized common.Hash
	}{
		{name: "unchanged_hashes", safe: safe, finalized: finalized},
		{name: "zero_safe_and_finalized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer m.ExecModule.Drain()
			writer, err := m.DB.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer writer.Rollback()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			result, err := m.ExecModule.UpdateForkChoice(ctx, head, tc.safe, tc.finalized)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, "unchanged markers must not need the MDBX writer lock")
		})
	}

	assertPersistedForkchoice(t, m.DB, head, safe, finalized)
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
	assertPersistedForkchoice(t, m.DB, head, previousSafe, previousFinalized)

	writer.Rollback()
	idleCtx, idleCancel := context.WithTimeout(t.Context(), time.Minute)
	defer idleCancel()
	m.ExecModule.WaitIdle(idleCtx)
	require.NoError(t, idleCtx.Err(), "the marker update must finish after the writer is released")
	assertPersistedForkchoice(t, m.DB, head, safe, finalized)
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
			assertPersistedForkchoice(t, m.DB, wantHead, wantSafe, wantFinalized)
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

func assertPersistedForkchoice(t *testing.T, db kv.RoDB, head, safe, finalized common.Hash) {
	t.Helper()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadHeadBlockHash(tx), "head block marker")
		require.Equal(t, head, rawdb.ReadHeadHeaderHash(tx), "head header marker")
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx), "forkchoice head marker")
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx), "safe marker")
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx), "finalized marker")
		return nil
	}))
}
