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

package kv

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrefetcherBatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	const workers = 3
	started := make(chan [][2][]byte, workers)
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var active, peak atomic.Int32
	p := newPrefetcher(workers, func(ctx context.Context, pairs [][2][]byte) error {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); old < n && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		started <- pairs
		select {
		case <-release[pairs[0][1][0]]:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	defer p.Close()
	defer cancel()
	inputs := [2][][2][]byte{}
	for batch := range inputs {
		for key := range 7 {
			inputs[batch] = append(inputs[batch], [2][]byte{{byte(key)}, {byte(batch)}})
		}
	}

	first := p.Prefetch(ctx, inputs[0])
	for batch := range inputs {
		var got [][2][]byte
		for range workers {
			select {
			case pairs := <-started:
				for i := 1; i < len(pairs); i++ {
					require.Equal(t, pairs[i-1][0][0]+1, pairs[i][0][0])
				}
				got = append(got, pairs...)
			case <-ctx.Done():
				t.Fatal("prefetch workers did not start", ctx.Err())
			}
		}
		require.ElementsMatch(t, inputs[batch], got)
		var next *PrefetchBatch
		if batch == 0 {
			next = p.Prefetch(ctx, inputs[1])
		}
		done := make(chan error, 1)
		go func() { done <- first.Wait() }()
		select {
		case err := <-done:
			t.Fatalf("batch completed before its reads finished: %v", err)
		default:
		}
		close(release[batch])
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("batch did not finish independently", ctx.Err())
		}
		require.NoError(t, first.Wait())
		first = next
	}
	require.EqualValues(t, workers, peak.Load())
	require.Zero(t, active.Load())
	require.NoError(t, p.Prefetch(ctx, nil).Wait())
}

func TestPrefetcherClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{}, 2)
	p := newPrefetcher(1, func(ctx context.Context, _ [][2][]byte) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	pairs := [][2][]byte{{[]byte("key"), []byte("value")}}
	active := p.Prefetch(ctx, pairs)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		p.Close()
		t.Fatal("prefetch did not start")
	}
	queued := p.Prefetch(ctx, pairs)
	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()
	defer func() {
		cancel()
		<-closed
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel active prefetch reads")
	}
	require.ErrorIs(t, active.Wait(), context.Canceled)
	require.ErrorIs(t, queued.Wait(), context.Canceled)
	require.Empty(t, started, "queued work must not start after Close")
	require.ErrorIs(t, p.Prefetch(ctx, pairs).Wait(), context.Canceled)
	require.ErrorIs(t, p.Prefetch(ctx, nil).Wait(), context.Canceled)
	p.Close()
}

func TestPrefetcherCancelSubmission(t *testing.T) {
	for _, closePool := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := newPrefetcher(1, func(ctx context.Context, _ [][2][]byte) error {
				<-ctx.Done()
				return ctx.Err()
			})
			defer p.Close()
			pairs := [][2][]byte{{[]byte("key"), []byte("value")}}
			active := p.Prefetch(ctx, pairs)
			synctest.Wait()
			queued := p.Prefetch(ctx, pairs)
			submitted := make(chan *PrefetchBatch, 1)
			go func() { submitted <- p.Prefetch(ctx, pairs) }()
			synctest.Wait()
			require.Empty(t, submitted, "submission must wait when the queue is full")
			if closePool {
				p.Close()
			} else {
				cancel()
			}
			synctest.Wait()
			require.Len(t, submitted, 1)
			require.ErrorIs(t, (<-submitted).Wait(), context.Canceled)
			require.ErrorIs(t, active.Wait(), context.Canceled)
			require.ErrorIs(t, queued.Wait(), context.Canceled)
		})
	}
}

func TestPrefetcherErrorWaitsForReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		wantErr := errors.New("read failed")
		release := make(chan struct{})
		p := newPrefetcher(2, func(ctx context.Context, pairs [][2][]byte) error {
			switch pairs[0][0][0] {
			case 0:
				return wantErr
			case 1:
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		defer p.Close()
		batch := p.Prefetch(ctx, [][2][]byte{{{0}, nil}, {{1}, nil}})
		done := make(chan error, 1)
		go func() { done <- batch.Wait() }()
		synctest.Wait()
		require.Empty(t, done, "an error must not release buffers still used by another reader")
		close(release)
		synctest.Wait()
		require.Len(t, done, 1)
		require.ErrorIs(t, <-done, wantErr)
		require.NoError(t, p.Prefetch(ctx, [][2][]byte{{{2}, nil}}).Wait())
	})
}

func TestPrefetcherBatchCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		p := newPrefetcher(1, func(ctx context.Context, pairs [][2][]byte) error {
			if pairs[0][0][0] == 0 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})
		defer p.Close()
		batch := p.Prefetch(ctx, [][2][]byte{{{0}, nil}})
		synctest.Wait()
		cancel()
		require.ErrorIs(t, batch.Wait(), context.Canceled)
		require.NoError(t, p.Prefetch(t.Context(), [][2][]byte{{{1}, nil}}).Wait())
	})
}
