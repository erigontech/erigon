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

package prove

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// attempts answers the n-th attempt with script(n, ctx), counting them.
type attempts struct {
	n      int
	script func(n int, ctx context.Context) ([]byte, error)
}

func (a *attempts) run(ctx context.Context) ([]byte, error) {
	a.n++
	return a.script(a.n, ctx)
}

// stall answers when the attempt's context ends.
func stall(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestRetry(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "down")
	proof := []byte{1}
	t.Run("transient failure", func(t *testing.T) {
		a := &attempts{script: func(n int, _ context.Context) ([]byte, error) {
			if n == 1 {
				return nil, unavailable
			}
			return proof, nil
		}}
		got, err := retry(t.Context(), time.Now().Add(time.Second), 0, a.run)
		require.NoError(t, err)
		require.Equal(t, proof, got)
		require.Equal(t, 2, a.n)
	})
	t.Run("lasting failure", func(t *testing.T) {
		a := &attempts{script: func(int, context.Context) ([]byte, error) { return nil, status.Error(codes.Internal, "bad window") }}
		_, err := retry(t.Context(), time.Now().Add(time.Second), 0, a.run)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, 1, a.n)
	})
	t.Run("start passed", func(t *testing.T) {
		a := &attempts{script: func(int, context.Context) ([]byte, error) { return proof, nil }}
		_, err := retry(t.Context(), time.Now().Add(-time.Millisecond), time.Second, a.run)
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
		require.Zero(t, a.n)
	})
	t.Run("silent prover", func(t *testing.T) {
		a := &attempts{script: func(_ int, ctx context.Context) ([]byte, error) { return stall(ctx) }}
		begin := time.Now()
		_, err := retry(t.Context(), begin.Add(300*time.Millisecond), 0, a.run)
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
		require.Equal(t, 1, a.n)
		require.GreaterOrEqual(t, time.Since(begin), 300*time.Millisecond)
	})
	t.Run("an attempt takes what is left", func(t *testing.T) {
		a := &attempts{script: func(n int, ctx context.Context) ([]byte, error) {
			if n == 1 {
				time.Sleep(300 * time.Millisecond)
				return nil, unavailable
			}
			return stall(ctx)
		}}
		begin := time.Now()
		_, err := retry(t.Context(), begin.Add(time.Second), 0, a.run)
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
		require.Equal(t, 2, a.n)
		require.InDelta(t, time.Second, time.Since(begin), float64(400*time.Millisecond))
	})
	t.Run("budget spent", func(t *testing.T) {
		a := &attempts{script: func(int, context.Context) ([]byte, error) { return nil, unavailable }}
		begin := time.Now()
		_, err := retry(t.Context(), begin.Add(2*time.Second), 0, a.run)
		require.Equal(t, codes.Unavailable, status.Code(err))
		// 0, 0.1, 0.3, 0.7 and 1.5 s, ±20%: the waits double.
		require.Greater(t, a.n, 3)
		require.LessOrEqual(t, a.n, 6)
		elapsed := time.Since(begin)
		require.GreaterOrEqual(t, elapsed, 80*time.Millisecond)
		require.Less(t, elapsed, 2*time.Second)
	})
	t.Run("per bounds each attempt", func(t *testing.T) {
		a := &attempts{script: func(_ int, ctx context.Context) ([]byte, error) { return stall(ctx) }}
		begin := time.Now()
		_, err := retry(t.Context(), begin.Add(time.Second), 100*time.Millisecond, a.run)
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
		require.Greater(t, a.n, 1)
		require.Less(t, time.Since(begin), time.Second+400*time.Millisecond)
	})
	t.Run("caller leaves", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		a := &attempts{script: func(int, context.Context) ([]byte, error) {
			cancel()
			return nil, unavailable
		}}
		_, err := retry(ctx, time.Now().Add(time.Second), 0, a.run)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, a.n)
	})
}

func TestBackoff(t *testing.T) {
	require.Equal(t, 80*time.Millisecond, backoff(1, 0))
	require.Equal(t, 120*time.Millisecond, backoff(1, 40))
	require.Equal(t, 800*time.Millisecond, backoff(5, 0))
	for attempt := 1; attempt <= 40; attempt++ {
		base := min(100*time.Millisecond<<min(attempt-1, 31), time.Second)
		for _, seed := range []uint64{0, 1, math.MaxUint64 / 2, math.MaxUint64} {
			d := backoff(attempt, seed)
			require.GreaterOrEqual(t, d, base*8/10, "attempt %d seed %d", attempt, seed)
			require.LessOrEqual(t, d, base*12/10, "attempt %d seed %d", attempt, seed)
		}
	}
}
