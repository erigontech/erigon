package epbs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestRevealSchedulerDoesNotStarveIndependentWinner(t *testing.T) {
	scheduler := newRevealScheduler(t.Context(), 1, 4)
	rootA := common.HexToHash("0xa1")
	rootB := common.HexToHash("0xb2")
	aAttempted := make(chan struct{}, 1)
	bSucceeded := make(chan struct{})
	deadline := time.Now().Add(time.Second)
	require.True(t, scheduler.Enqueue(revealTask{
		root: rootA, deadline: deadline,
		reveal: func(context.Context) error {
			select {
			case aAttempted <- struct{}{}:
			default:
			}
			return errors.New("permanent failure")
		},
	}))
	<-aAttempted
	require.True(t, scheduler.Enqueue(revealTask{
		root: rootB, deadline: deadline,
		reveal: func(context.Context) error {
			close(bSucceeded)
			return nil
		},
	}))
	select {
	case <-bSucceeded:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("independent reveal was starved")
	}
}

func TestRevealWinningBidUntilRejectsExpiredDeadlineBeforeAttempt(t *testing.T) {
	attempts := 0
	err := revealWinningBidUntil(t.Context(), time.Now().Add(-time.Second), func(context.Context) error {
		attempts++
		return nil
	})
	require.ErrorIs(t, err, ErrRevealExpired)
	require.Zero(t, attempts)
}

func TestRevealSchedulerPassesDeadlineToBlockedAttempt(t *testing.T) {
	scheduler := newRevealScheduler(t.Context(), 1, 1)
	done := make(chan error, 1)
	require.True(t, scheduler.Enqueue(revealTask{
		root: common.HexToHash("0xa1"), deadline: time.Now().Add(20 * time.Millisecond),
		reveal: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		terminal: func(err error) { done <- err },
	}))
	require.ErrorIs(t, <-done, ErrRevealExpired)
}

func TestRevealSchedulerDoesNotAttemptAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	scheduler := newRevealScheduler(ctx, 1, 1)
	attempts := 0
	done := make(chan error, 1)
	require.True(t, scheduler.Enqueue(revealTask{
		root: common.HexToHash("0xa1"), deadline: time.Now().Add(time.Second),
		reveal: func(context.Context) error {
			attempts++
			return nil
		},
		terminal: func(err error) { done <- err },
	}))
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, attempts)
}
