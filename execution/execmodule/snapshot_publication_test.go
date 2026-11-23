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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func newSnapshotBoundaryChain(t *testing.T, opts ...execmoduletester.Option) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack, uint64) {
	t.Helper()
	const stepSize = 16
	ctx := t.Context()
	opts = append(opts,
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithMaxReorgDepth(96),
		execmoduletester.WithAlwaysGenerateChangesets(false),
	)
	m, chainPack := newExecutedTransferChain(t, opts...)
	m.ExecModule.Drain()
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	agg.WaitForFiles()
	var lastFileBlock uint64
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		block, found, err := m.BlockReader.TxnumReader().FindBlockNum(ctx, tx, stepSize-1)
		require.NoError(t, err)
		require.True(t, found)
		require.Positive(t, block)
		lastFileBlock = block
		return nil
	}))
	return m, chainPack, lastFileBlock
}

func holdStateFilePublication(t *testing.T) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack, uint64, func()) {
	t.Helper()
	m, chainPack, lastFileBlock := newSnapshotBoundaryChain(t)
	ctx := t.Context()
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	built, publish := make(chan struct{}), make(chan struct{})
	allowPublication := sync.OnceFunc(func() {
		agg.SetMaintenanceObserver(nil)
		close(publish)
	})
	agg.SetMaintenanceObserver(func(ctx context.Context, event dbstate.MaintenanceEvent) {
		if event.Point == dbstate.MaintenanceFilesBuilt {
			close(built)
			select {
			case <-publish:
			case <-ctx.Done():
			}
		}
	})
	t.Cleanup(allowPublication)

	// Finalize the block containing the step's last transaction so the step can be built.
	// Pruning keeps that block's changeset, so the old snapshot view still allows a rewind to its parent.
	finalizedHash := chainPack.Blocks[lastFileBlock-1].Hash()
	fcu, err := m.UpdateForkChoice(ctx, chainPack.TopBlock.Header(),
		execmoduletester.WithSafeHash(finalizedHash),
		execmoduletester.WithFinalisedHash(finalizedHash),
	)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	finished, err := m.ExecModule.Prune(ctx)
	require.NoError(t, err)
	select {
	case <-built:
	case <-finished:
		t.Fatal("the build finished without producing the first snapshot step")
	}
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		require.Zero(t, tx.FreezeInfo().ValueFilesEndTxNum())
		oldestChangeset, err := changeset.ReadLowestUnwindableBlock(tx)
		require.NoError(t, err)
		require.Equal(t, lastFileBlock, oldestChangeset, "the finalized block must have the oldest remaining changeset")
		return nil
	}))
	return m, chainPack, lastFileBlock, func() {
		allowPublication()
		<-finished
	}
}

func TestSetHeadWaitsForStateFilePublication(t *testing.T) {
	m, chainPack, lastFileBlock, publish := holdStateFilePublication(t)
	ctx := t.Context()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- m.ExecModule.SetHead(ctx, lastFileBlock-1) }()
		synctest.Wait()
		writer, err := m.DB.(*temporal.DB).BeginTemporalRwTry(t.Context())
		require.NoError(t, err, "SetHead must wait before opening the transaction that pins the files")
		writer.Rollback()
		cancel()
		require.ErrorIs(t, <-result, context.Canceled, "SetHead must wait while the snapshot is being built")
	})

	// A short read-ahead wait leaves only the rest of the budget for publication.
	// A longer one must time out while read-ahead is still held.
	for _, readAheadDelay := range []time.Duration{2 * time.Second, 10 * time.Second} {
		synctest.Test(t, func(t *testing.T) {
			callerCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			resume, err := m.ReadAheader.SuspendWarmup(callerCtx)
			require.NoError(t, err)
			defer resume()
			timer := time.AfterFunc(readAheadDelay, resume)
			defer timer.Stop()

			start := time.Now()
			err = m.ExecModule.SetHead(callerCtx, lastFileBlock-1)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 5*time.Second, time.Since(start), "read-ahead held for %s", readAheadDelay)
		})
	}
	head, finalized := chainPack.TopBlock.Hash(), chainPack.Blocks[lastFileBlock-1].Hash()
	assertPersistedForkchoice(t, m.DB, head, finalized, finalized)
	fcuCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	fcu, err := m.ExecModule.UpdateForkChoice(fcuCtx, head, finalized, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status, "execution must be available while publication is still held")

	publish()
	require.ErrorContains(t, m.ExecModule.SetHead(ctx, lastFileBlock-1), fmt.Sprintf("minimum unwindable block is %d", lastFileBlock))
	assertPersistedForkchoice(t, m.DB, head, finalized, finalized)
	require.NoError(t, m.ExecModule.SetHead(ctx, lastFileBlock))
}

func TestForkchoiceLoweringFinalityWaitsForStateFilePublication(t *testing.T) {
	for _, name := range []string{"same_head", "advance_head", "rewind_at_boundary"} {
		t.Run(name, func(t *testing.T) {
			m, chain, lastFileBlock, publish := holdStateFilePublication(t)
			ctx := t.Context()
			finalized := chain.Blocks[lastFileBlock-1]
			parentHash := finalized.ParentHash()
			extension, err := m.GenerateChainFrom(chain.TopBlock, 2, nil)
			require.NoError(t, err)
			status, err := m.InsertBlocks(ctx, extension.Blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			currentHead := extension.Blocks[0].Hash()
			currentFinalized := finalized.Hash()
			head := currentHead
			switch name {
			case "advance_head":
				currentFinalized = chain.Blocks[lastFileBlock].Hash()
				head = extension.TopBlock.Hash()
			case "rewind_at_boundary":
				// Forkchoice first unwinds to this older head's parent.
				head = finalized.Hash()
			}
			// An extension can keep or advance finality during the build.
			advanceCtx, cancelAdvance := context.WithTimeout(ctx, time.Minute)
			defer cancelAdvance()
			result, err := m.ExecModule.UpdateForkChoice(advanceCtx, currentHead, currentFinalized, currentFinalized)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			m.ExecModule.Drain()

			// Exercise both the module's limit on the pause and a shorter caller deadline.
			wait := time.Second
			if name == "same_head" {
				wait = time.Minute
			}
			requestCtx, cancel := context.WithTimeout(ctx, wait)
			defer cancel()
			result, err = m.ExecModule.UpdateForkChoice(requestCtx, head, parentHash, parentHash)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusBusy, result.Status, "lowering finality must wait for publication")
			require.False(t, result.UpdateInProgress, "the caller must retry an update that could not pause publication")
			if name == "same_head" {
				require.NoError(t, requestCtx.Err(), "the module must bound the wait even when the caller can wait longer")
			}
			idleCtx, cancelIdle := context.WithTimeout(ctx, time.Minute)
			defer cancelIdle()
			m.ExecModule.WaitIdle(idleCtx)
			require.NoError(t, idleCtx.Err(), "a held build must release execution for other requests")
			assertPersistedForkchoice(t, m.DB, currentHead, currentFinalized, currentFinalized)
			publish()
			result = updateForkChoice(t, m, head, parentHash, parentHash)
			if name == "rewind_at_boundary" {
				assertPersistedForkchoice(t, m.DB, currentHead, finalized.Hash(), finalized.Hash())
				require.Equal(t, execmodule.ExecutionStatusReorgTooDeep, result.Status)
				return
			}
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			assertPersistedForkchoice(t, m.DB, head, parentHash, parentHash)

			// The parent is now finalized, but its child is already in an immutable file.
			header := finalized.Header()
			header.Extra = []byte("sibling")
			sibling := finalized.WithSeal(header)
			status, err = m.InsertBlocks(ctx, []*types.Block{sibling})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			result = updateForkChoice(t, m, sibling.Hash(), parentHash, parentHash)
			require.Equal(t, execmodule.ExecutionStatusReorgTooDeep, result.Status)
			assertPersistedForkchoice(t, m.DB, head, parentHash, parentHash)
			require.Equal(t, finalized.Hash(), canonicalHashAt(t, m, lastFileBlock))
		})
	}
}

func TestQueuedSnapshotBuildAcrossFinalityChange(t *testing.T) {
	for _, when := range []string{"queued_before", "queued_after"} {
		t.Run(when, func(t *testing.T) {
			m, chain, fileBlock := newSnapshotBoundaryChain(t)
			ctx := t.Context()
			agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
			shared := semaphore.NewWeighted(1)
			require.NoError(t, shared.Acquire(ctx, 1))
			release := sync.OnceFunc(func() { shared.Release(1) })
			t.Cleanup(release)
			agg.SetSnapshotBuildSema(shared)

			head := chain.TopBlock.Hash()
			finalized := chain.Blocks[fileBlock-1]
			parent := finalized.ParentHash()
			require.Equal(t, execmodule.ExecutionStatusSuccess, updateForkChoice(t, m, head, finalized.Hash(), finalized.Hash()).Status)
			prune := func() <-chan struct{} {
				finished, err := m.ExecModule.Prune(ctx)
				require.NoError(t, err)
				return finished
			}
			var finished <-chan struct{}
			if when == "queued_before" {
				finished = prune()
			}
			waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			result, err := m.ExecModule.UpdateForkChoice(waitCtx, head, parent, parent)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, "a queued state build must not block forkchoice")
			m.ExecModule.Drain()
			assertPersistedForkchoice(t, m.DB, head, parent, parent)
			if when == "queued_before" {
				select {
				case <-finished:
				case <-waitCtx.Done():
					t.Fatal("the obsolete build is still waiting for the shared snapshot worker")
				}
			} else {
				finished = prune()
			}
			release()
			<-finished
			require.Zero(t, agg.EndTxNumMinimax(), "the first step ends above the new finalized block")
			require.NoError(t, m.ExecModule.SetHead(ctx, fileBlock-1))
		})
	}
}

func TestForkchoiceKeepsStateFilesPausedThroughCommit(t *testing.T) {
	var tryBuild func(context.Context)
	m, chain, fileBlock := newSnapshotBoundaryChain(t, execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
		if point == execmodule.StateTransitionCommitReady && tryBuild != nil {
			tryBuild(ctx)
		}
	}))
	ctx := t.Context()
	// Lower finality by one block while keeping the first snapshot step eligible.
	finalized := chain.Blocks[fileBlock]
	head := chain.TopBlock
	require.Equal(t, execmodule.ExecutionStatusSuccess, updateForkChoice(t, m, head.Hash(), finalized.Hash(), finalized.Hash()).Status)
	extension, err := m.GenerateChainFrom(head, 1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, extension.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	// Until the FCU commits, a reader still sees the checkpoint that allows this step.
	oldFinality := execfinality.NewContext(head.NumberU64(), finalized.NumberU64(), 96, false, m.BlockReader.TxnumReader())
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	var endsAtCommit []uint64
	var buildErr error
	tryBuild = func(ctx context.Context) {
		buildErr = m.DB.BuildFiles2(ctx, 0, 1, oldFinality, false)
		agg.WaitForFiles()
		endsAtCommit = append(endsAtCommit, agg.EndTxNumMinimax())
	}
	parent := finalized.ParentHash()
	result := updateForkChoice(t, m, extension.TopBlock.Hash(), parent, parent)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	require.NoError(t, buildErr)
	require.Equal(t, []uint64{0}, endsAtCommit)
	agg.WaitForFiles()
	require.Equal(t, m.DB.StepSize(), agg.EndTxNumMinimax(), "the prune after commit must be able to build the eligible step")
}
