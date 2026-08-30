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

package payloadoptimizer_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

func TestCanceledQueuedApplyNeverCallsBackend(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("unexpected collection")
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	firstDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		firstDone <- applyErr
	}()
	<-firstStarted
	queuedCtx, cancel := context.WithCancel(t.Context())
	queuedDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(queuedCtx, update)
		queuedDone <- applyErr
	}()
	cancel()

	select {
	case err := <-queuedDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled queued apply did not return")
	}
	require.Equal(t, uint64(1), calls.Load())
	close(releaseFirst)
	require.ErrorIs(t, <-firstDone, payloadoptimizer.ErrBackendBusy)
}

func TestSessionRetriesBusyCollectionAndDiscardsPayload(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var gets atomic.Uint64
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 91}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			if gets.Add(1) < 3 {
				return execmodule.AssembledBlockResult{Busy: true}, nil
			}
			return validColdResult(params, requests, 1), nil
		},
		discard: func(id uint64) {
			require.Equal(t, uint64(91), id)
			discards.Add(1)
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	candidate, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Equal(t, uint64(3), gets.Load())
	require.Equal(t, uint64(1), discards.Load())
}

func TestSessionDiscardsAnIdReturnedWithBusy(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 7, Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("unexpected collection")
			return execmodule.AssembledBlockResult{}, nil
		},
		discard: func(id uint64) {
			require.Equal(t, uint64(7), id)
			discards.Add(1)
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
	require.Equal(t, uint64(1), discards.Load())
}

func TestSessionDiscardsAnIdReturnedWithAssemblyError(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	backendErr := errors.New("assembly failed after allocating an id")
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 9}, backendErr
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("unexpected collection")
			return execmodule.AssembledBlockResult{}, nil
		},
		discard: func(id uint64) {
			require.Equal(t, uint64(9), id)
			discards.Add(1)
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, backendErr)
	require.Equal(t, uint64(1), discards.Load())
}

func TestSessionBusyRetryStopsOnCancellationAndDiscards(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	var gets atomic.Uint64
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 8}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			gets.Add(1)
			return execmodule.AssembledBlockResult{Busy: true}, nil
		},
		discard: func(uint64) { discards.Add(1) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	applyCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err = session.Apply(applyCtx, update)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Positive(t, gets.Load())
	require.Equal(t, uint64(1), discards.Load())
}

func TestCloseWaitsForActiveApplyCleanup(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	started := make(chan struct{})
	discarded := make(chan struct{})
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			close(started)
			<-ctx.Done()
			return execmodule.AssembleBlockResult{PayloadID: 12}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("collection must not start after close")
			return execmodule.AssembledBlockResult{}, nil
		},
		discard: func(uint64) { close(discarded) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	applyDone := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		applyDone <- applyErr
	}()
	<-started

	require.NoError(t, session.Close())
	require.ErrorIs(t, <-applyDone, context.Canceled)
	select {
	case <-discarded:
	default:
		t.Fatal("Close returned before payload cleanup")
	}
}

func TestOrderflowProviderHonorsAmountAcrossCalls(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	txs := types.Transactions{
		&types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21_000}},
		&types.LegacyTx{CommonTx: types.CommonTx{Nonce: 2, GasLimit: 21_000}},
	}
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			first, err := params.CustomTxnProvider.ProvideTxns(ctx, txnprovider.WithAmount(1))
			require.NoError(t, err)
			second, err := params.CustomTxnProvider.ProvideTxns(ctx, txnprovider.WithAmount(1))
			require.NoError(t, err)
			exhausted, err := params.CustomTxnProvider.ProvideTxns(ctx, txnprovider.WithAmount(1))
			require.NoError(t, err)
			require.Equal(t, uint64(1), first[0].GetNonce())
			require.Equal(t, uint64(2), second[0].GetNonce())
			require.Empty(t, exhausted)
			return execmodule.AssembleBlockResult{Busy: true}, nil
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(txs)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrBackendBusy)
}

func TestCloseCancelsActiveApplyAndPreventsLaterUse(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	started := make(chan struct{})
	backend := &optimizerBackend{
		assemble: func(ctx context.Context, _ *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			close(started)
			<-ctx.Done()
			return execmodule.AssembleBlockResult{}, ctx.Err()
		},
		get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
			t.Fatal("GetAssembledBlock called after canceled assembly")
			return execmodule.AssembledBlockResult{}, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		done <- applyErr
	}()
	<-started

	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
	require.ErrorIs(t, <-done, context.Canceled)
	_, ok := session.Best()
	require.False(t, ok)
	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrSessionClosed)
}

func TestSessionKeepsOnlyStrictlyImprovedCandidates(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	values := []uint64{100, 90, 110}
	var next atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: next.Add(1)}, nil
		},
		get: func(_ context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
			result := validColdResult(params, requests, values[payloadID-1])
			result.Block.Block.HeaderNoCopy().Root[0] = byte(payloadID)
			return result, nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	first, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, first)
	second, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.Nil(t, second)
	best, ok := session.Best()
	require.True(t, ok)
	require.Equal(t, uint64(100), best.Value().Uint64())
	third, err := session.Apply(t.Context(), update)
	require.NoError(t, err)
	require.NotNil(t, third)
	best, ok = session.Best()
	require.True(t, ok)
	require.Equal(t, uint64(110), best.Value().Uint64())
}

func TestSessionReportsColdBackendStates(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	backendErr := errors.New("backend failed")
	tests := map[string]struct {
		assemble execmodule.AssembleBlockResult
		get      execmodule.AssembledBlockResult
		errAt    string
		want     error
	}{
		"assemble busy":  {assemble: execmodule.AssembleBlockResult{Busy: true}, want: payloadoptimizer.ErrBackendBusy},
		"unknown":        {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Unknown: true}, want: payloadoptimizer.ErrUnknownPayload},
		"not ready":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, want: payloadoptimizer.ErrPayloadNotReady},
		"nil block":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{}}, want: payloadoptimizer.ErrPayloadNotReady},
		"nil header":     {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{Block: new(types.Block)}}, want: payloadoptimizer.ErrPayloadNotReady},
		"assemble error": {errAt: "assemble", want: backendErr},
		"get error":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, errAt: "get", want: backendErr},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var discards atomic.Uint64
			backend := &optimizerBackend{
				assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
					if test.errAt == "assemble" {
						return execmodule.AssembleBlockResult{}, backendErr
					}
					return test.assemble, nil
				},
				get: func(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
					if test.errAt == "get" {
						return execmodule.AssembledBlockResult{}, backendErr
					}
					return test.get, nil
				},
				discard: func(uint64) { discards.Add(1) },
			}
			session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
			require.NoError(t, err)
			update, err := payloadoptimizer.NewOrderflowUpdate(nil)
			require.NoError(t, err)

			_, err = session.Apply(t.Context(), update)
			require.ErrorIs(t, err, test.want)
			if test.assemble.PayloadID == 0 {
				require.Zero(t, discards.Load())
			} else {
				require.Equal(t, uint64(1), discards.Load())
			}
		})
	}
}

func TestSessionDiscardsAContextMismatchedCandidate(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	result := validColdResult(params, requests, 1)
	result.Block.Block.HeaderNoCopy().ParentHash[0]++
	var discards atomic.Uint64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			return execmodule.AssembleBlockResult{PayloadID: 10}, nil
		},
		get:     func(context.Context, uint64) (execmodule.AssembledBlockResult, error) { return result, nil },
		discard: func(uint64) { discards.Add(1) },
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)

	_, err = session.Apply(t.Context(), update)
	require.ErrorIs(t, err, payloadoptimizer.ErrCandidateContextMismatch)
	require.Equal(t, uint64(1), discards.Load())
}

func TestOrderflowUpdateOwnsTransactions(t *testing.T) {
	tx := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21_000, Data: []byte{0x01}}}
	wantSender := accounts.InternAddress(common.Address{0x02})
	tx.SetSender(wantSender)
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
	require.NoError(t, err)
	tx.Data[0] = 0xff
	tx.SetSender(accounts.InternAddress(common.Address{0xff}))

	first := update.Transactions()
	require.Equal(t, []byte{0x01}, first[0].GetData())
	gotSender, ok := first[0].GetSender()
	require.True(t, ok)
	require.Equal(t, wantSender, gotSender)
	first[0].(*types.LegacyTx).Data[0] = 0xee
	first[0].SetSender(accounts.InternAddress(common.Address{0xee}))
	require.Equal(t, []byte{0x01}, update.Transactions()[0].GetData())
	gotSender, ok = update.Transactions()[0].GetSender()
	require.True(t, ok)
	require.Equal(t, wantSender, gotSender)
}

func TestSessionSerializesConcurrentApply(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests, baseParentGasLimit)
	require.NoError(t, err)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Uint64
	var active atomic.Int64
	var maxActive atomic.Int64
	backend := &optimizerBackend{
		assemble: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			current := active.Add(1)
			for previous := maxActive.Load(); current > previous && !maxActive.CompareAndSwap(previous, current); previous = maxActive.Load() {
			}
			call := calls.Add(1)
			if call == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			active.Add(-1)
			return execmodule.AssembleBlockResult{PayloadID: call}, nil
		},
		get: func(_ context.Context, payloadID uint64) (execmodule.AssembledBlockResult, error) {
			return validColdResult(params, requests, payloadID), nil
		},
	}
	session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
	require.NoError(t, err)
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	done := make(chan error, 2)
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		done <- applyErr
	}()
	<-firstStarted
	go func() {
		_, applyErr := session.Apply(t.Context(), update)
		done <- applyErr
	}()
	require.Never(t, func() bool { return maxActive.Load() > 1 }, 50*time.Millisecond, time.Millisecond)
	close(releaseFirst)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.Equal(t, int64(1), maxActive.Load())
}
