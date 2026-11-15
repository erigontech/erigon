// Copyright 2025 The Erigon Authors
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

package shutter_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
)

func TestBlockTracker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testlog.Logger(t, log.LvlTrace)
	recvC := make(chan *remoteproto.StateChangeBatch)
	bl := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(ctx, recvC))
	btInitialisationWg := &sync.WaitGroup{}
	btInitialisationWg.Add(1)
	readCommittedState := func(ctx context.Context) (shutter.BlockState, error) {
		btInitialisationWg.Done()
		return shutter.BlockState{BlockNum: 10}, nil
	}
	bt := shutter.NewBlockTracker(logger, bl, readCommittedState)
	eg, egCtx := errgroup.WithContext(ctx)
	defer func(eg *errgroup.Group) {
		cancel()
		err := eg.Wait()
		require.ErrorIs(t, err, context.Canceled)
	}(eg)
	eg.Go(func() error { return bl.Run(egCtx) })
	eg.Go(func() error { return bt.Run(egCtx) })
	btInitialisationWg.Wait() // This ensures the block tracker has been initialised i.e. it observes the block listener

	waitCtx1, waitCtxCancel1 := context.WithTimeout(ctx, 50*time.Millisecond)
	defer waitCtxCancel1()
	err := bt.Wait(waitCtx1, 15)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	recvC <- &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 15}}}
	waitCtx2, waitCtxCancel2 := context.WithTimeout(ctx, time.Minute)
	defer waitCtxCancel2()
	err = bt.Wait(waitCtx2, 15)
	require.NoError(t, err)
}

func TestBlockTrackerInitialHead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		replay    shutter.BlockState
		committed shutter.BlockState
		unwind    bool
		wantHead  uint64
	}{
		{
			name:      "database ahead of replay",
			replay:    shutter.BlockState{BlockNum: 42, StateVersionID: 1},
			committed: shutter.BlockState{BlockNum: 43, StateVersionID: 2},
			wantHead:  43,
		},
		{
			name:      "database ahead of replayed unwind",
			replay:    shutter.BlockState{BlockNum: 42, StateVersionID: 1},
			committed: shutter.BlockState{BlockNum: 43, StateVersionID: 2},
			unwind:    true,
			wantHead:  43,
		},
		{
			name:      "replay ahead of database",
			replay:    shutter.BlockState{BlockNum: 44, StateVersionID: 2},
			committed: shutter.BlockState{BlockNum: 43, StateVersionID: 1},
			wantHead:  44,
		},
		{
			name:      "database rewind after replay",
			replay:    shutter.BlockState{BlockNum: 42, StateVersionID: 1},
			committed: shutter.BlockState{BlockNum: 40, StateVersionID: 2},
			wantHead:  40,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				eg, egCtx := errgroup.WithContext(ctx)
				defer func() {
					cancel()
					require.ErrorIs(t, eg.Wait(), context.Canceled)
				}()
				logger := testlog.Logger(t, log.LvlTrace)
				batches := make(chan *remoteproto.StateChangeBatch)
				listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(egCtx, batches))
				eg.Go(func() error { return listener.Run(egCtx) })
				direction := remoteproto.Direction_FORWARD
				if tc.unwind {
					direction = remoteproto.Direction_UNWIND
				}
				batches <- &remoteproto.StateChangeBatch{StateVersionId: tc.replay.StateVersionID, ChangeBatch: []*remoteproto.StateChange{{
					BlockHeight: tc.replay.BlockNum, Direction: direction,
				}}}
				synctest.Wait()
				tracker := shutter.NewBlockTracker(logger, listener, func(context.Context) (shutter.BlockState, error) {
					return tc.committed, nil
				})
				eg.Go(func() error { return tracker.Run(egCtx) })
				synctest.Wait()

				waitCtx, cancelWait := context.WithTimeout(egCtx, time.Second)
				defer cancelWait()
				require.NoError(t, tracker.Wait(waitCtx, tc.wantHead))
				require.ErrorIs(t, tracker.Wait(waitCtx, tc.wantHead+1), context.DeadlineExceeded)
			})
		})
	}
}

func TestBlockTrackerIgnoresEventsBeforeSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		eg, egCtx := errgroup.WithContext(ctx)
		defer func() {
			cancel()
			require.ErrorIs(t, eg.Wait(), context.Canceled)
		}()
		logger := testlog.Logger(t, log.LvlTrace)
		batches := make(chan *remoteproto.StateChangeBatch)
		listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(egCtx, batches))
		eg.Go(func() error { return listener.Run(egCtx) })

		readStarted, finishRead := make(chan struct{}), make(chan struct{})
		tracker := shutter.NewBlockTracker(logger, listener, func(context.Context) (shutter.BlockState, error) {
			close(readStarted)
			<-finishRead
			return shutter.BlockState{BlockNum: 40, StateVersionID: 2}, nil
		})
		eg.Go(func() error { return tracker.Run(egCtx) })
		<-readStarted
		batches <- &remoteproto.StateChangeBatch{StateVersionId: 1, ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 42}}}
		synctest.Wait()
		close(finishRead)
		synctest.Wait()

		waitCtx, cancelWait := context.WithTimeout(egCtx, time.Second)
		defer cancelWait()
		require.NoError(t, tracker.Wait(waitCtx, 40))
		require.ErrorIs(t, tracker.Wait(waitCtx, 41), context.DeadlineExceeded)
	})
}

func TestBlockTrackerAppliesUnwindDuringInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		eg, egCtx := errgroup.WithContext(ctx)
		defer func() {
			cancel()
			require.ErrorIs(t, eg.Wait(), context.Canceled)
		}()
		logger := testlog.Logger(t, log.LvlTrace)
		batches := make(chan *remoteproto.StateChangeBatch)
		listener := shutter.NewBlockListener(logger, testhelpers.NewStateChangesClientMock(egCtx, batches))
		eg.Go(func() error { return listener.Run(egCtx) })
		batches <- &remoteproto.StateChangeBatch{StateVersionId: 1, ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 42}}}
		synctest.Wait()

		readStarted, finishRead := make(chan struct{}), make(chan struct{})
		tracker := shutter.NewBlockTracker(logger, listener, func(ctx context.Context) (shutter.BlockState, error) {
			close(readStarted)
			select {
			case <-finishRead:
				return shutter.BlockState{BlockNum: 43, StateVersionID: 2}, nil
			case <-ctx.Done():
				return shutter.BlockState{}, ctx.Err()
			}
		})
		eg.Go(func() error { return tracker.Run(egCtx) })
		<-readStarted
		batches <- &remoteproto.StateChangeBatch{StateVersionId: 3, ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 40, Direction: remoteproto.Direction_UNWIND,
		}}}
		synctest.Wait()
		close(finishRead)
		synctest.Wait()

		waitCtx, cancelWait := context.WithTimeout(egCtx, time.Second)
		defer cancelWait()
		require.NoError(t, tracker.Wait(waitCtx, 40))
		require.ErrorIs(t, tracker.Wait(waitCtx, 41), context.DeadlineExceeded)
	})
}

// settlingContext lets every other goroutine in the bubble block or exit before it reports done.
type settlingContext struct{ context.Context }

func (c settlingContext) Done() <-chan struct{} {
	synctest.Wait()
	return c.Context.Done()
}

func TestBlockTrackerWaitReportsCancellationForAnUnseenBlock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		bt := shutter.NewBlockTracker(testlog.Logger(t, log.LvlTrace), nil, nil)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for range 100 {
			require.ErrorIs(t, bt.Wait(settlingContext{ctx}, 15), context.Canceled)
		}
	})
}

func TestBlockTrackerWaitReportsCancellationAfterTheTrackerStops(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	logger := testlog.Logger(t, log.LvlTrace)
	bl := shutter.NewBlockListener(logger, nil)
	bt := shutter.NewBlockTracker(logger, bl, func(ctx context.Context) (shutter.BlockState, error) { return shutter.BlockState{}, ctx.Err() })
	require.ErrorIs(t, bt.Run(ctx), context.Canceled)

	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	require.ErrorIs(t, bt.Wait(waitCtx, 15), context.Canceled)
}

// cancelledAfterCheckContext is cancelled by its first Err call, which still reports it as live, so the
// cancellation lands between the waiter's check and its Cond.Wait.
type cancelledAfterCheckContext struct {
	context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelledAfterCheckContext) Err() error {
	first := false
	c.once.Do(func() { first = true })
	if !first {
		return c.Context.Err()
	}
	c.cancel()
	time.Sleep(10 * time.Millisecond)
	return nil
}

func TestBlockTrackerWaitReturnsWhenCancelledRightAfterItsWaiterChecks(t *testing.T) {
	t.Parallel()
	bt := shutter.NewBlockTracker(testlog.Logger(t, log.LvlTrace), nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	returned := make(chan error, 1)
	go func() { returned <- bt.Wait(&cancelledAfterCheckContext{Context: ctx, cancel: cancel}, 15) }()
	select {
	case err := <-returned:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait missed a cancellation")
	}
}
