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

package concurrent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func produced[T any](value T) func() (T, bool, error) {
	return func() (T, bool, error) { return value, true, nil }
}

func TestCachedValueReportsNothingBeforeTheFirstPass(t *testing.T) {
	t.Parallel()

	value, observed, fresh := new(CachedValue[uint64]).Load()
	require.Zero(t, value)
	require.False(t, observed, "the zero value is a default, not an observation")
	require.False(t, fresh)
}

func TestCachedValueKeepsWhatWasProduced(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	value, ran, err := cached.Produce(t.Context(), produced(uint64(42)))
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, uint64(42), value)

	value, observed, fresh := cached.Load()
	require.Equal(t, uint64(42), value)
	require.True(t, observed)
	require.True(t, fresh)
}

func TestCachedValueStopsBeingFreshAfterTheTTL(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	_, _, err := cached.Produce(t.Context(), produced(uint64(42)))
	require.NoError(t, err)
	cached.SetTTL(0)

	value, observed, fresh := cached.Load()
	require.Equal(t, uint64(42), value, "a stale value is still the one that was observed")
	require.True(t, observed)
	require.False(t, fresh)
}

// TestCachedValueRecordsAFailedAttempt pins that a failure is remembered as an attempt
// without becoming an observation: a producer that is down must cost one attempt per TTL,
// and a caller must still be able to tell that nothing was ever produced.
func TestCachedValueRecordsAFailedAttempt(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	_, _, err := cached.Produce(t.Context(), func() (uint64, bool, error) {
		return 0, false, errors.New("down")
	})
	require.Error(t, err)

	value, observed, fresh := cached.Load()
	require.Zero(t, value)
	require.False(t, observed)
	require.True(t, fresh, "a recent failure stands in for the attempts that would follow it")
}

// TestCachedValueKeepsAnUndecidedProducerOutOfTheValue pins the store=false contract: the
// answer reaches the caller that asked for it, but a producer with nothing worth
// remembering leaves the stored value and observed untouched.
func TestCachedValueKeepsAnUndecidedProducerOutOfTheValue(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	value, ran, err := cached.Produce(t.Context(), func() (uint64, bool, error) { return 7, false, nil })
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, uint64(7), value, "the caller that asked gets the answer")

	stored, observed, _ := cached.Load()
	require.Zero(t, stored)
	require.False(t, observed)
}

// TestCachedValueProduceRunsThePassToTheEnd pins what a producer bound to the caller's
// transaction depends on: the caller that runs the pass does not return, or honour its own
// cancellation, before the producer is done with what it borrowed.
func TestCachedValueProduceRunsThePassToTheEnd(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var finished atomic.Bool
	_, ran, err := cached.Produce(ctx, func() (uint64, bool, error) {
		time.Sleep(10 * time.Millisecond)
		finished.Store(true)
		return 42, true, nil
	})
	require.NoError(t, err, "a cancelled context does not abandon a pass the caller runs itself")
	require.True(t, ran)
	require.True(t, finished.Load())
}

// TestCachedValueReleasesAWaiterWithItsOwnContext pins the other half: a caller that only
// waits owns nothing the pass borrowed, so its own cancellation releases it.
func TestCachedValueReleasesAWaiterWithItsOwnContext(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	go func() {
		_, _, _ = cached.Produce(t.Context(), func() (uint64, bool, error) {
			close(entered)
			<-release
			return 42, true, nil
		})
	}()
	<-entered
	defer close(release)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, ran, err := cached.Produce(ctx, produced(uint64(1)))
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ran, "the waiter must not run a producer of its own")
}

// TestCachedValueRunsOneProducerForConcurrentCallers pins the dedup: the pass exists to
// keep one slow producer from being asked once per caller.
func TestCachedValueRunsOneProducerForConcurrentCallers(t *testing.T) {
	t.Parallel()

	const waiters = 8
	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	entered := make(chan struct{}, waiters+1)
	release := make(chan struct{})

	var producers atomic.Int64
	produce := func() (uint64, bool, error) {
		producers.Add(1)
		entered <- struct{}{}
		<-release
		return 42, true, nil
	}

	answers := make(chan uint64, waiters+1)
	for range waiters + 1 {
		go func() {
			value, _, _ := cached.Produce(t.Context(), produce)
			answers <- value
		}()
	}
	<-entered
	select {
	case <-entered:
		close(release)
		t.Fatal("a caller arriving while the producer ran asked for one of its own")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)

	for range waiters + 1 {
		require.Equal(t, uint64(42), <-answers, "the pass answers every caller waiting on it")
	}
	require.EqualValues(t, 1, producers.Load())
}

// TestCachedValueProducePropagatesAProducerPanic pins that a panic reaches the caller that
// caused it, having released the pass: swallowing it would hand that caller a zero value
// it cannot tell from an answer, and holding the pass would strand the next caller.
func TestCachedValueProducePropagatesAProducerPanic(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(0)
	require.Panics(t, func() {
		_, _, _ = cached.Produce(t.Context(), func() (uint64, bool, error) { panic("producer blew up") })
	})
	_, observed, _ := cached.Load()
	require.False(t, observed, "a producer that did not finish observed nothing")

	value, ran, err := cached.Produce(t.Context(), produced(uint64(42)))
	require.NoError(t, err)
	require.True(t, ran, "the pass a panic ended is over, so the next caller runs its own")
	require.Equal(t, uint64(42), value)
}

// TestCachedValueGoContainsAProducerPanic pins that a panic on the goroutine the pass owns
// does not escape it: there is no caller there to recover it, so it would end the process.
func TestCachedValueGoContainsAProducerPanic(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	<-cached.Go(func() (uint64, bool, error) { panic("producer blew up") })

	_, observed, fresh := cached.Load()
	require.False(t, observed)
	require.True(t, fresh, "a pass that panicked still counts as an attempt")
}

// TestCachedValueGoDoesNotHoldTheCaller pins the refresh-behind form: the caller gets the
// channel and moves on while the producer is still running.
func TestCachedValueGoDoesNotHoldTheCaller(t *testing.T) {
	t.Parallel()

	cached := new(CachedValue[uint64])
	cached.SetTTL(time.Hour)
	release := make(chan struct{})
	done := cached.Go(func() (uint64, bool, error) {
		<-release
		return 42, true, nil
	})

	_, observed, _ := cached.Load()
	require.False(t, observed, "Go returned before the producer did")
	close(release)
	<-done
	value, observed, _ := cached.Load()
	require.Equal(t, uint64(42), value)
	require.True(t, observed)
}
