package execution_client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func TestPayloadValidationCoordinatorLeaderCancellationDoesNotPoisonWaiter(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockExecutionEngine(ctrl)
	started := make(chan struct{})
	var calls atomic.Int32
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(ctx context.Context, _ *cltypes.Eth1Block, _ *common.Hash, _ []common.Hash, _ []hexutil.Bytes) (PayloadStatus, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return PayloadStatusNone, ctx.Err()
			}
			return PayloadStatusValidated, nil
		})

	coordinator := NewPayloadValidationCoordinator(engine)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := coordinator.NewPayload(leaderCtx, common.Hash{1}, nil, nil, nil, nil)
		leaderDone <- err
	}()
	<-started

	waiterDone := make(chan payloadValidationContextResult, 1)
	go func() {
		status, err := coordinator.NewPayload(context.Background(), common.Hash{1}, nil, nil, nil, nil)
		waiterDone <- payloadValidationContextResult{status: status, err: err}
	}()
	cancelLeader()

	require.ErrorIs(t, <-leaderDone, context.Canceled)
	waiter := <-waiterDone
	require.NoError(t, waiter.err)
	require.EqualValues(t, PayloadStatusValidated, waiter.status)
	require.Equal(t, int32(2), calls.Load())
}

type payloadValidationContextResult struct {
	status PayloadStatus
	err    error
}

type payloadValidationObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newPayloadValidationObservedContext(parent context.Context) *payloadValidationObservedContext {
	return &payloadValidationObservedContext{Context: parent, observed: make(chan struct{})}
}

func (c *payloadValidationObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestPayloadValidationCoordinatorWaiterCancellationIsLocal(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockExecutionEngine(ctrl)
	started := make(chan struct{})
	release := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (PayloadStatus, error) {
			close(started)
			<-release
			return PayloadStatusValidated, nil
		})

	coordinator := NewPayloadValidationCoordinator(engine)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := coordinator.NewPayload(context.Background(), common.Hash{1}, nil, nil, nil, nil)
		leaderDone <- err
	}()
	<-started
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	cancelWaiter()
	_, err := coordinator.NewPayload(waiterCtx, common.Hash{1}, nil, nil, nil, nil)
	require.True(t, errors.Is(err, context.Canceled))
	close(release)
	require.NoError(t, <-leaderDone)
}

func TestPayloadValidationCoordinatorBoundsDistinctCalls(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockExecutionEngine(ctrl)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (PayloadStatus, error) {
			current := active.Add(1)
			for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return PayloadStatusValidated, nil
		})

	coordinator := NewPayloadValidationCoordinator(engine)
	done := make(chan struct{}, 3)
	for i := range 3 {
		go func(key byte) {
			_, _ = coordinator.NewPayload(context.Background(), common.Hash{key}, nil, nil, nil, nil)
			done <- struct{}{}
		}(byte(i + 1))
	}
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than two NewPayload calls ran concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-started
	for range 3 {
		<-done
	}
	require.Equal(t, int32(2), maximum.Load())
}

func TestPayloadValidationCoordinatorReportsLeaderPanicToWaiter(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockExecutionEngine(ctrl)
	started := make(chan struct{})
	release := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (PayloadStatus, error) {
			close(started)
			<-release
			panic("engine panic")
		})

	coordinator := NewPayloadValidationCoordinator(engine)
	key := common.Hash{1}
	leaderPanic := make(chan any, 1)
	go func() {
		defer func() { leaderPanic <- recover() }()
		_, _ = coordinator.NewPayload(context.Background(), key, nil, nil, nil, nil)
	}()
	<-started
	waiterDone := make(chan error, 1)
	waiterCtx := newPayloadValidationObservedContext(context.Background())
	go func() {
		_, err := coordinator.NewPayload(waiterCtx, key, nil, nil, nil, nil)
		waiterDone <- err
	}()
	select {
	case <-waiterCtx.observed:
	case <-time.After(time.Second):
		t.Fatal("payload validation waiter did not reach its wait point")
	}
	close(release)
	require.Equal(t, "engine panic", <-leaderPanic)
	require.Error(t, <-waiterDone)
}
