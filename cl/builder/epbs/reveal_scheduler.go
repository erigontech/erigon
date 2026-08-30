package epbs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
)

var ErrRevealExpired = errors.New("payload reveal deadline expired")

const (
	revealRetryDelay         = 100 * time.Millisecond
	lateRevealAttemptTimeout = time.Second
)

type revealTask struct {
	root     common.Hash
	deadline time.Time
	reveal   func(context.Context) error
	terminal func(error)
}

type revealScheduler struct {
	ctx       context.Context
	sem       chan struct{}
	maxQueued int
	mu        sync.Mutex
	queued    map[common.Hash]struct{}
	wg        sync.WaitGroup
}

func newRevealScheduler(ctx context.Context, maxConcurrent, maxQueued int) *revealScheduler {
	return &revealScheduler{
		ctx:       ctx,
		sem:       make(chan struct{}, maxConcurrent),
		maxQueued: maxQueued,
		queued:    make(map[common.Hash]struct{}),
	}
}

func (s *revealScheduler) Enqueue(task revealTask) bool {
	s.mu.Lock()
	if _, exists := s.queued[task.root]; exists || len(s.queued) >= s.maxQueued {
		s.mu.Unlock()
		return false
	}
	s.queued[task.root] = struct{}{}
	s.wg.Go(func() {
		err := revealWinningBidUntil(s.ctx, task.deadline, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.sem <- struct{}{}:
			}
			defer func() { <-s.sem }()
			if !time.Now().Before(task.deadline) {
				attemptCtx, cancel := context.WithTimeout(ctx, lateRevealAttemptTimeout)
				defer cancel()
				return task.reveal(attemptCtx)
			}
			return task.reveal(ctx)
		})
		s.mu.Lock()
		delete(s.queued, task.root)
		s.mu.Unlock()
		if err != nil && task.terminal != nil {
			task.terminal(err)
		}
	})
	s.mu.Unlock()
	return true
}

func (s *revealScheduler) Wait() {
	s.wg.Wait()
}

func revealWinningBidUntil(ctx context.Context, deadline time.Time, reveal func(context.Context) error) error {
	if !time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := reveal(ctx); err != nil {
			return revealExpiredError(err)
		}
		return nil
	}
	deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var err error
	for {
		if cause := deadlineCtx.Err(); cause != nil {
			if errors.Is(cause, context.DeadlineExceeded) {
				return revealExpiredError(err)
			}
			return cause
		}
		if !time.Now().Before(deadline) {
			return ErrRevealExpired
		}
		if err = reveal(deadlineCtx); err == nil {
			return nil
		}
		timer := time.NewTimer(revealRetryDelay)
		select {
		case <-deadlineCtx.Done():
			timer.Stop()
			if errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
				return revealExpiredError(err)
			}
			return deadlineCtx.Err()
		case <-timer.C:
		}
	}
}

func revealExpiredError(lastAttempt error) error {
	if lastAttempt == nil {
		return ErrRevealExpired
	}
	return fmt.Errorf("%w: %w", ErrRevealExpired, lastAttempt)
}
