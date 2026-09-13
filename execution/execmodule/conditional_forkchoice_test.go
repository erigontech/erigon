// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package execmodule_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func newValidatedChild(t *testing.T, extra ...execmoduletester.Option) (*execmoduletester.ExecModuleTester, *types.Block) {
	t.Helper()
	ctx := t.Context()
	opts := append([]execmoduletester.Option{execmoduletester.WithChainConfig(chain.AllProtocolChanges)}, extra...)
	m := execmoduletester.New(t, opts...)
	genesisHash := m.Genesis.Hash()
	initial, err := m.UpdateForkChoice(
		ctx,
		m.Genesis.Header(),
		execmoduletester.WithSafeHash(genesisHash),
		execmoduletester.WithFinalisedHash(genesisHash),
	)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, chainPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	return m, chainPack.TopBlock
}

func TestUpdateForkChoiceIfHeadWaitsForExecutionReadiness(t *testing.T) {
	ctx := t.Context()
	pruneStarted := make(chan struct{})
	releasePrune := make(chan struct{})
	var armed atomic.Bool
	m, child := newValidatedChild(t,
		execmoduletester.WithFcuBackgroundPrune(),
		execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
			if point != execmodule.StateTransitionPostForkchoiceStarted || !armed.CompareAndSwap(true, false) {
				return
			}
			close(pruneStarted)
			select {
			case <-releasePrune:
			case <-ctx.Done():
			}
		}),
	)
	armed.Store(true)
	type outcome struct {
		result execmodule.ForkChoiceResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
		done <- outcome{result: result, err: err}
	}()

	select {
	case <-pruneStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case result := <-done:
		close(releasePrune)
		t.Fatalf("conditional forkchoice returned before execution readiness: result=%+v err=%v", result.result, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePrune)
	result := <-done
	require.NoError(t, result.err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.result.Status)
}

func TestUpdateForkChoiceIfHeadAlreadyCurrentWaitsForExecutionReadiness(t *testing.T) {
	ctx := t.Context()
	resultSent := make(chan struct{})
	releaseResult := make(chan struct{})
	var armed atomic.Bool
	m, child := newValidatedChild(t,
		execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
			if point != execmodule.StateTransitionConditionalResultSent || !armed.CompareAndSwap(true, false) {
				return
			}
			close(resultSent)
			select {
			case <-releaseResult:
			case <-ctx.Done():
			}
		}),
	)
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)

	armed.Store(true)
	type outcome struct {
		result execmodule.ForkChoiceResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
		done <- outcome{result: result, err: err}
	}()
	select {
	case <-resultSent:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case result := <-done:
		close(releaseResult)
		t.Fatalf("already-current forkchoice returned before execution readiness: result=%+v err=%v", result.result, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseResult)
	result = (<-done).result
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
}

func TestAssembleBlockRejectsParentSupersededAfterConditionalForkchoice(t *testing.T) {
	ctx := t.Context()
	m, first := newValidatedChild(t)
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), first.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)

	secondPack, err := m.GenerateChainFrom(first, 1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, secondPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, secondPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	result, err = m.ExecModule.UpdateForkChoiceIfHead(ctx, first.Hash(), secondPack.TopBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)

	assembled, err := m.ExecModule.AssembleBlock(ctx, &builder.Parameters{
		ParentHash:  first.Hash(),
		Timestamp:   secondPack.TopBlock.Time() + 1,
		Withdrawals: []*types.Withdrawal{},
	})
	require.NoError(t, err)
	require.True(t, assembled.Busy)
}

func TestAssembleBlockRejectsInconsistentHeadMarkers(t *testing.T) {
	for _, marker := range []struct {
		name  string
		write func(kv.Putter, common.Hash)
	}{
		{name: "forkchoice", write: rawdb.WriteForkchoiceHead},
		{name: "block", write: rawdb.WriteHeadBlockHash},
	} {
		t.Run(marker.name, func(t *testing.T) {
			ctx := t.Context()
			m, child := newValidatedChild(t)
			result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
				marker.write(tx, m.Genesis.Hash())
				return nil
			}))

			assembled, err := m.ExecModule.AssembleBlock(ctx, &builder.Parameters{
				ParentHash:  child.Hash(),
				Timestamp:   child.Time() + 1,
				Withdrawals: []*types.Withdrawal{},
			})
			require.NoError(t, err)
			require.True(t, assembled.Busy)
		})
	}
}

func assertHead(t *testing.T, m *execmoduletester.ExecModuleTester, want common.Hash) {
	t.Helper()
	forkchoice, err := m.ExecModule.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.Equal(t, want, forkchoice.HeadHash)
}

func TestUpdateForkChoiceIfHeadAdvancesValidatedChildAndPreservesCheckpoints(t *testing.T) {
	ctx := t.Context()
	m, child := newValidatedChild(t)
	genesisHash := m.Genesis.Hash()
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	forkchoice, err := m.ExecModule.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, child.Hash(), forkchoice.HeadHash)
	require.Equal(t, genesisHash, forkchoice.SafeHash)
	require.Equal(t, genesisHash, forkchoice.FinalizedHash)
	alreadyCurrent, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, alreadyCurrent.Status)
	assertHead(t, m, child.Hash())
}

func TestUpdateForkChoiceIfHeadPreservesZeroCheckpoints(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	genesisHash := m.Genesis.Hash()
	initial, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		rawdb.WriteForkchoiceSafe(tx, common.Hash{})
		rawdb.WriteForkchoiceFinalized(tx, common.Hash{})
		return nil
	}))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, chainPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)

	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, chainPack.TopBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	forkchoice, err := m.ExecModule.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, common.Hash{}, forkchoice.SafeHash)
	require.Equal(t, common.Hash{}, forkchoice.FinalizedHash)
}

func TestUpdateForkChoiceIfHeadRejectsWithoutConsumingValidatedChild(t *testing.T) {
	ctx := t.Context()
	m, child := newValidatedChild(t)
	wantHash, wantNumber, wantState := m.ExecModule.ForkValidator().ExtendingFork()
	require.Equal(t, child.Hash(), wantHash)
	require.NotNil(t, wantState)
	for _, test := range []struct {
		name     string
		expected common.Hash
		target   common.Hash
	}{
		{name: "zero expected", target: child.Hash()},
		{name: "zero target", expected: m.Genesis.Hash()},
		{name: "mismatched expected", expected: common.Hash{0xff}, target: child.Hash()},
		{name: "missing target", expected: m.Genesis.Hash(), target: common.Hash{0xfe}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, test.expected, test.target)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusBusy, result.Status)
			assertHead(t, m, m.Genesis.Hash())
			gotHash, gotNumber, gotState := m.ExecModule.ForkValidator().ExtendingFork()
			require.Equal(t, wantHash, gotHash)
			require.Equal(t, wantNumber, gotNumber)
			require.Same(t, wantState, gotState)
		})
	}
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
}

func TestUpdateForkChoiceIfHeadRejectsUnvalidatedTarget(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	genesisHash := m.Genesis.Hash()
	initial, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, genesisHash, genesisHash)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, chainPack.TopBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, result.Status)
	assertHead(t, m, genesisHash)
}

func TestUpdateForkChoiceIfHeadRejectsValidatedNonChildWithoutCleanup(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	genesisHash := m.Genesis.Hash()
	initial, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, genesisHash, genesisHash)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
	chainPack, err := m.GenerateChain(2, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, chainPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	wantHash, wantNumber, wantState := m.ExecModule.ForkValidator().ExtendingFork()
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, chainPack.TopBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, result.Status)
	assertHead(t, m, genesisHash)
	gotHash, gotNumber, gotState := m.ExecModule.ForkValidator().ExtendingFork()
	require.Equal(t, wantHash, gotHash)
	require.Equal(t, wantNumber, gotNumber)
	require.Same(t, wantState, gotState)
}

func TestUpdateForkChoiceIfHeadRejectsMissingTargetBodyWithoutCleanup(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	genesisHash := m.Genesis.Hash()
	initial, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, genesisHash, genesisHash)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
	chainPack, err := m.GenerateChain(17, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	target := chainPack.Blocks[0]
	validation, err := m.ValidateChain(ctx, target.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	wantHash, wantNumber, wantState := m.ExecModule.ForkValidator().ExtendingFork()
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		rawdb.DeleteBody(tx, target.Hash(), target.NumberU64())
		return nil
	}))

	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, target.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, result.Status)
	assertHead(t, m, genesisHash)
	gotHash, gotNumber, gotState := m.ExecModule.ForkValidator().ExtendingFork()
	require.Equal(t, wantHash, gotHash)
	require.Equal(t, wantNumber, gotNumber)
	require.Same(t, wantState, gotState)
}

func TestUpdateForkChoiceIfHeadRejectsAlreadyCurrentTargetWithoutBody(t *testing.T) {
	ctx := t.Context()
	m, child := newValidatedChild(t)
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		rawdb.DeleteBody(tx, child.Hash(), child.NumberU64())
		return nil
	}))

	result, err = m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, result.Status)
}

func TestUpdateForkChoiceIfHeadRejectsInvalidCheckpointWithoutCleanup(t *testing.T) {
	for _, checkpoint := range []struct {
		name    string
		write   func(kv.Putter, common.Hash)
		missing bool
	}{
		{name: "non-ancestor safe", write: rawdb.WriteForkchoiceSafe},
		{name: "non-ancestor finalized", write: rawdb.WriteForkchoiceFinalized},
		{name: "missing safe", write: rawdb.WriteForkchoiceSafe, missing: true},
		{name: "missing finalized", write: rawdb.WriteForkchoiceFinalized, missing: true},
	} {
		t.Run(checkpoint.name, func(t *testing.T) {
			ctx := t.Context()
			m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
			genesisHash := m.Genesis.Hash()
			initial, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, genesisHash, genesisHash)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
			targetPack, err := m.GenerateChainFrom(m.Genesis, 1, func(_ int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(common.Address{1})
			})
			require.NoError(t, err)
			siblingPack, err := m.GenerateChainFrom(m.Genesis, 1, func(_ int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(common.Address{2})
			})
			require.NoError(t, err)
			target := targetPack.TopBlock
			sibling := siblingPack.TopBlock
			require.NotEqual(t, target.Hash(), sibling.Hash())
			status, err := m.InsertBlocks(ctx, []*types.Block{target, sibling})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			validation, err := m.ValidateChain(ctx, target.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
			wantHash, wantNumber, wantState := m.ExecModule.ForkValidator().ExtendingFork()
			checkpointHash := sibling.Hash()
			if checkpoint.missing {
				checkpointHash = common.Hash{0xff}
			}
			require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
				checkpoint.write(tx, checkpointHash)
				return nil
			}))

			result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, target.Hash())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusBusy, result.Status)
			assertHead(t, m, genesisHash)
			gotHash, gotNumber, gotState := m.ExecModule.ForkValidator().ExtendingFork()
			require.Equal(t, wantHash, gotHash)
			require.Equal(t, wantNumber, gotNumber)
			require.Same(t, wantState, gotState)
		})
	}
}

func TestUpdateForkChoiceIfHeadPreMergeFailureAllowsValidationRetry(t *testing.T) {
	expectedErr := errors.New("conditional pre-merge failure")
	var failOnce atomic.Bool
	failOnce.Store(true)
	m, child := newValidatedChild(t, execmoduletester.WithConditionalForkChoiceReadyHook(func() error {
		if failOnce.CompareAndSwap(true, false) {
			return expectedErr
		}
		return nil
	}))

	_, err := m.ExecModule.UpdateForkChoiceIfHead(t.Context(), m.Genesis.Hash(), child.Hash())
	require.ErrorIs(t, err, expectedErr)
	assertHead(t, m, m.Genesis.Hash())
	validation, err := m.ValidateChain(t.Context(), child.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	result, err := m.ExecModule.UpdateForkChoiceIfHead(t.Context(), m.Genesis.Hash(), child.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
}

func TestUpdateForkChoiceIfHeadDoesNotStartWithCanceledContext(t *testing.T) {
	m, child := newValidatedChild(t)
	wantHash, wantNumber, wantState := m.ExecModule.ForkValidator().ExtendingFork()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), child.Hash())
	require.ErrorIs(t, err, context.Canceled)
	assertHead(t, m, m.Genesis.Hash())
	gotHash, gotNumber, gotState := m.ExecModule.ForkValidator().ExtendingFork()
	require.Equal(t, wantHash, gotHash)
	require.Equal(t, wantNumber, gotNumber)
	require.Same(t, wantState, gotState)
}

func TestUpdateForkChoiceIfHeadDoesNotAdvanceFromStaleExpectedHead(t *testing.T) {
	ctx := t.Context()
	m, first := newValidatedChild(t)
	result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), first.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	secondPack, err := m.GenerateChainFrom(first, 1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, secondPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, secondPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	wantHash, wantNumber, wantState := m.ExecModule.ForkValidator().ExtendingFork()

	stale, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, m.Genesis.Hash(), secondPack.TopBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, stale.Status)
	assertHead(t, m, first.Hash())
	gotHash, gotNumber, gotState := m.ExecModule.ForkValidator().ExtendingFork()
	require.Equal(t, wantHash, gotHash)
	require.Equal(t, wantNumber, gotNumber)
	require.Same(t, wantState, gotState)

	result, err = m.ExecModule.UpdateForkChoiceIfHead(ctx, first.Hash(), secondPack.TopBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	assertHead(t, m, secondPack.TopBlock.Hash())
}

func TestUpdateForkChoiceIfHeadExcludesConcurrentForkchoice(t *testing.T) {
	ctx := t.Context()
	reached := make(chan struct{})
	release := make(chan struct{})
	var armed atomic.Bool
	m := execmoduletester.New(
		t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithStateTransitionObserver(func(_ context.Context, point execmodule.StateTransitionPoint) {
			if point == execmodule.StateTransitionOverlayPublished && armed.CompareAndSwap(true, false) {
				close(reached)
				<-release
			}
		}),
	)
	genesisHash := m.Genesis.Hash()
	initial, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, genesisHash, genesisHash)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, initial.Status)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, chainPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)

	armed.Store(true)
	conditionalDone := make(chan execmodule.ForkChoiceResult, 1)
	conditionalErr := make(chan error, 1)
	go func() {
		result, err := m.ExecModule.UpdateForkChoiceIfHead(ctx, genesisHash, chainPack.TopBlock.Hash())
		conditionalDone <- result
		conditionalErr <- err
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	concurrent, err := m.ExecModule.UpdateForkChoice(ctx, genesisHash, genesisHash, genesisHash)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, concurrent.Status)
	close(release)
	require.NoError(t, <-conditionalErr)
	require.Equal(t, execmodule.ExecutionStatusSuccess, (<-conditionalDone).Status)
	assertHead(t, m, chainPack.TopBlock.Hash())
}
