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
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestInvertedIndexPrefetcherBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 3
		started := make(chan [][2][]byte, workers)
		release, releaseReads := context.WithCancel(t.Context())
		defer releaseReads()
		p := &InvertedIndexPrefetcher{workers: workers, fetch: func(ctx context.Context, pairs [][2][]byte) error {
			started <- pairs
			<-release.Done()
			return nil
		}}
		var pairs [][2][]byte
		for key := range 7 {
			pairs = append(pairs, [2][]byte{{byte(key)}, nil})
		}
		done := make(chan error, 1)
		go func() { done <- p.Prefetch(t.Context(), pairs) }()
		synctest.Wait()
		require.Len(t, started, workers)
		require.Empty(t, done, "Prefetch must wait for its readers")
		var got [][2][]byte
		for range workers {
			batch := <-started
			for i := 1; i < len(batch); i++ {
				require.Equal(t, batch[i-1][0][0]+1, batch[i][0][0])
			}
			got = append(got, batch...)
		}
		require.ElementsMatch(t, pairs, got)
		releaseReads()
		synctest.Wait()
		require.Len(t, done, 1)
		require.NoError(t, <-done)

		require.NoError(t, p.Prefetch(t.Context(), pairs[:2]))
		require.Len(t, started, 2, "worker count must not exceed pair count")
		require.Len(t, <-started, 1)
		require.Len(t, <-started, 1)
		require.NoError(t, p.Prefetch(t.Context(), nil))
		require.Empty(t, started)
	})
}

func TestInvertedIndexPrefetcherErrorWaitsForReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		wantErr := errors.New("read failed")
		release, releaseReads := context.WithCancel(ctx)
		defer releaseReads()
		p := &InvertedIndexPrefetcher{workers: 2, fetch: func(ctx context.Context, pairs [][2][]byte) error {
			switch pairs[0][0][0] {
			case 0:
				return wantErr
			case 1:
				<-release.Done()
			}
			return nil
		}}
		done := make(chan error, 1)
		go func() { done <- p.Prefetch(ctx, [][2][]byte{{{0}, nil}, {{1}, nil}}) }()
		synctest.Wait()
		require.Empty(t, done, "an error must not release buffers still used by another reader")
		releaseReads()
		synctest.Wait()
		require.Len(t, done, 1)
		require.ErrorIs(t, <-done, wantErr)
		require.NoError(t, p.Prefetch(ctx, [][2][]byte{{{2}, nil}}))
	})
}

func TestInvertedIndexPrefetcherCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		p := &InvertedIndexPrefetcher{workers: 1, fetch: func(ctx context.Context, pairs [][2][]byte) error {
			if pairs[0][0][0] == 0 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}}
		done := make(chan error, 1)
		go func() { done <- p.Prefetch(ctx, [][2][]byte{{{0}, nil}}) }()
		synctest.Wait()
		require.Empty(t, done)
		cancel()
		synctest.Wait()
		require.Len(t, done, 1)
		require.ErrorIs(t, <-done, context.Canceled)
		require.ErrorIs(t, p.Prefetch(ctx, nil), context.Canceled)
		require.NoError(t, p.Prefetch(t.Context(), [][2][]byte{{{1}, nil}}))
	})
}
