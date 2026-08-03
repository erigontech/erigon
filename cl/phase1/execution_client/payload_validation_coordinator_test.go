package execution_client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

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
	go func() {
		_, err := coordinator.NewPayload(context.Background(), key, nil, nil, nil, nil)
		waiterDone <- err
	}()
	time.Sleep(10 * time.Millisecond)
	close(release)
	require.Equal(t, "engine panic", <-leaderPanic)
	require.Error(t, <-waiterDone)
}
