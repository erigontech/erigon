package payloadoptimizer_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/types"
)

func TestCloseCancelsActiveApplyAndPreventsLaterUse(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
	require.NoError(t, err)
	backendErr := errors.New("backend failed")
	tests := map[string]struct {
		assemble execmodule.AssembleBlockResult
		get      execmodule.AssembledBlockResult
		errAt    string
		want     error
	}{
		"assemble busy":  {assemble: execmodule.AssembleBlockResult{Busy: true}, want: payloadoptimizer.ErrBackendBusy},
		"get busy":       {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Busy: true}, want: payloadoptimizer.ErrBackendBusy},
		"unknown":        {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Unknown: true}, want: payloadoptimizer.ErrUnknownPayload},
		"not ready":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, want: payloadoptimizer.ErrPayloadNotReady},
		"nil block":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{}}, want: payloadoptimizer.ErrPayloadNotReady},
		"nil header":     {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, get: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{Block: new(types.Block)}}, want: payloadoptimizer.ErrPayloadNotReady},
		"assemble error": {errAt: "assemble", want: backendErr},
		"get error":      {assemble: execmodule.AssembleBlockResult{PayloadID: 1}, errAt: "get", want: backendErr},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
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
			}
			session, err := payloadoptimizer.New(backend).Open(t.Context(), buildCtx)
			require.NoError(t, err)
			update, err := payloadoptimizer.NewOrderflowUpdate(nil)
			require.NoError(t, err)

			_, err = session.Apply(t.Context(), update)
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestOrderflowUpdateOwnsTransactions(t *testing.T) {
	tx := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21_000, Data: []byte{0x01}}}
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{tx})
	require.NoError(t, err)
	tx.Data[0] = 0xff

	first := update.Transactions()
	require.Equal(t, []byte{0x01}, first[0].GetData())
	first[0].(*types.LegacyTx).Data[0] = 0xee
	require.Equal(t, []byte{0x01}, update.Transactions()[0].GetData())
}

func TestSessionSerializesConcurrentApply(t *testing.T) {
	params, fork, requests := baseBuildContextInput()
	buildCtx, err := payloadoptimizer.NewBuildContext(params, fork, requests)
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
