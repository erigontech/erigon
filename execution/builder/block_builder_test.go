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

package builder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestBlockBuilderRunningHasNotFailed(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b := NewBlockBuilder(t.Context(), func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		<-release
		return nil, errors.New("builder stopped")
	}, &Parameters{}, time.Minute, time.Minute)

	require.Never(t, b.Failed, 50*time.Millisecond, 5*time.Millisecond)
}

func TestBlockBuilderStoppedForItsPayloadHasNotFailed(t *testing.T) {
	t.Parallel()

	b := NewBlockBuilder(t.Context(), func(_ context.Context, _ *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	}, &Parameters{}, time.Minute, time.Minute)

	_, err := b.Stop(t.Context())
	require.NoError(t, err)

	// Collecting the payload is what a proposal does. Reading that as failure would make a repeated
	// request rebuild from scratch instead of being handed the block that was just built.
	require.False(t, b.Failed())
}

func TestBlockBuilderHasFailedOnceItErrors(t *testing.T) {
	t.Parallel()

	b := NewBlockBuilder(t.Context(), func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, errors.New("build failed")
	}, &Parameters{}, time.Minute, time.Minute)

	require.Eventually(t, b.Failed, time.Second, time.Millisecond)
}

func TestBlockBuilderStaysReusableOnceItFillsTheBlock(t *testing.T) {
	t.Parallel()

	built := make(chan struct{})
	b := NewBlockBuilder(t.Context(), func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		defer close(built)
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	}, &Parameters{}, time.Minute, time.Minute)

	<-built
	// A builder that ran out of room holds a complete payload, so its id is still worth reusing.
	require.Never(t, b.Failed, 50*time.Millisecond, 5*time.Millisecond)
}

func TestBlockBuilderReleasesABuildThatIgnoresTheDeadline(t *testing.T) {
	t.Parallel()

	// A build parked in something that never reads the interrupt flag - a transaction provider
	// waiting on a block, say - would hold its read view until the builder count forced it out.
	released := make(chan error, 1)
	b := NewBlockBuilder(t.Context(), func(ctx context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		select {
		case <-ctx.Done():
			released <- ctx.Err()
		case <-time.After(time.Minute):
			released <- errors.New("build was never released")
		}
		return nil, errors.New("builder stopped")
	}, &Parameters{}, time.Millisecond, 10*time.Millisecond)

	select {
	case err := <-released:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("build outlived its budget without being released")
	}
	require.Eventually(t, b.Failed, 5*time.Second, time.Millisecond)
}

func TestBlockBuilderStillHandsOverAPayloadWhenItsBudgetRunsOut(t *testing.T) {
	t.Parallel()

	// Reaching the budget asks for the block it has, which is what the budget is for. Only a build
	// that will not answer is discarded.
	b := NewBlockBuilder(t.Context(), func(_ context.Context, _ *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	}, &Parameters{}, time.Millisecond, time.Minute)

	require.Eventually(t, func() bool { return b.Block() != nil }, 5*time.Second, time.Millisecond)
	require.False(t, b.Failed())
}

func TestBlockBuilderKeepsAHealthyBuildThatIsSlowToObserveTheStop(t *testing.T) {
	t.Parallel()

	// A single heavy transaction has no interrupt boundary, so a healthy build can be slow to
	// answer the stop without being unresponsive. Discarding it destroys a payload a proposal may
	// still collect.
	b := NewBlockBuilder(t.Context(), func(ctx context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
		}
	}, &Parameters{}, time.Millisecond, 5*time.Second)

	result, err := b.Stop(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestBlockBuilderReleasesABuildThatWedgesAfterObservingTheStop(t *testing.T) {
	t.Parallel()

	// Observing the stop earns the build its grace, not an unbounded stay. Discard must still cancel
	// context-aware work that blocks later in the build.
	released := make(chan error, 1)
	NewBlockBuilder(t.Context(), func(ctx context.Context, _ *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		select {
		case <-ctx.Done():
			released <- ctx.Err()
		case <-time.After(time.Minute):
			released <- errors.New("build was never released")
		}
		return nil, errors.New("builder stopped")
	}, &Parameters{}, time.Millisecond, 50*time.Millisecond)

	select {
	case err := <-released:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("wedged build outlived its grace without being released")
	}
}

func TestBlockBuilderReplaysPayloadCompletedAfterDiscard(t *testing.T) {
	t.Parallel()

	want := &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}
	b := NewBlockBuilder(t.Context(), func(ctx context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		<-ctx.Done()
		return want, nil
	}, &Parameters{}, time.Minute, time.Minute)

	b.Discard()
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Fatal("discarded builder did not finish")
	}

	require.True(t, b.Discarded())
	for range 2 {
		got, err := b.Stop(t.Context())
		require.NoError(t, err)
		require.Same(t, want, got)
	}
}

func TestBlockBuilderStopPrefersCompletedOutcomeOverCanceledCaller(t *testing.T) {
	t.Parallel()

	wantBlock := &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}
	wantErr := errors.New("build failed")
	tests := []struct {
		name   string
		result *types.BlockWithReceipts
		err    error
	}{
		{name: "payload", result: wantBlock},
		{name: "error", err: wantErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBlockBuilder(t.Context(), func(context.Context, *Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
				return tc.result, tc.err
			}, &Parameters{}, time.Minute, time.Minute)
			select {
			case <-b.done:
			case <-time.After(5 * time.Second):
				t.Fatal("builder did not finish")
			}

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			for range 100 {
				got, err := b.Stop(ctx)
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
					continue
				}
				require.NoError(t, err)
				require.Same(t, tc.result, got)
			}
		})
	}
}

func TestBlockBuilderStopReturnsImmediatelyAfterDiscard(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b := NewBlockBuilder(t.Context(), func(context.Context, *Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		<-release
		return nil, errors.New("builder stopped")
	}, &Parameters{}, time.Minute, time.Minute)
	b.Discard()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := b.Stop(ctx)
	require.ErrorIs(t, err, ErrDiscarded)
}
