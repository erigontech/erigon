package log

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamHandlerNoContention(t *testing.T) {
	// Each Write waits for the other one, so both return promptly only if StreamHandler lets writes overlap.
	both := make(chan struct{})
	var entered atomic.Int32
	lg := New()
	lg.SetHandler(StreamHandler(writeFunc(func(p []byte) (int, error) {
		if entered.Add(1) == 2 {
			close(both)
		}
		select {
		case <-both:
		case <-time.After(10 * time.Second):
			t.Error("StreamHandler serializes writes")
		}
		return len(p), nil
	}), TerminalFormatNoColor()))
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { lg.Info("msg") })
	}
	wg.Wait()
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }

func TestStreamHandlerAllocsUpperBound(t *testing.T) {
	lg := New()
	lg.SetHandler(StreamHandler(io.Discard, TerminalFormatNoColor()))

	allocs := testing.AllocsPerRun(100, func() {
		lg.Info("test message", "key", "value")
	})
	// Formatting allocates (bytes.Buffer, fmt.Fprintf, etc.), so this
	// is not expected to be literally zero. Keep a generous upper bound
	// so the test remains stable across Go versions while still catching
	// meaningful regressions in per-call allocation behavior.
	const maxAllocsPerOp = 16
	if allocs > maxAllocsPerOp {
		t.Fatalf("StreamHandler allocs/op too high: got %.0f, want <= %d", allocs, maxAllocsPerOp)
	}
	t.Logf("StreamHandler allocs/op: %.0f", allocs)
}

func TestStreamHandlerNoConcurrencyOverhead(t *testing.T) {
	lg := New()
	lg.SetHandler(StreamHandler(io.Discard, TerminalFormatNoColor()))

	// Measure single-goroutine allocs as baseline.
	baseline := testing.AllocsPerRun(100, func() {
		lg.Info("msg", "k", "v")
	})

	// Pre-spawn workers so AllocsPerRun measures logging under concurrency
	// rather than goroutine creation/teardown or per-run WaitGroup setup.
	const goroutines = 64
	start := make(chan struct{})
	done := make(chan struct{}, goroutines)

	var workers sync.WaitGroup
	workers.Add(goroutines)
	for range goroutines {
		go func() {
			defer workers.Done()
			for range start {
				lg.Info("msg", "k", "v")
				done <- struct{}{}
			}
		}()
	}

	concurrent := testing.AllocsPerRun(100, func() {
		for range goroutines {
			start <- struct{}{}
		}
		for range goroutines {
			<-done
		}
	})

	close(start)
	workers.Wait()

	perCall := concurrent / goroutines

	// Concurrent allocs per call should stay close to baseline. Allow a
	// small slack for channel/scheduler overhead in the fan-out harness
	// itself (amortized across `goroutines` calls) — the purpose of the
	// test is to catch a mutex/sync.Pool regression that would add O(1)
	// extra allocs per call, not sub-alloc harness noise.
	const slack = 2.0
	if perCall > baseline+slack {
		t.Fatalf("concurrent allocs/op (%.1f) exceed baseline+slack (%.1f) — likely mutex or sync overhead", perCall, baseline+slack)
	}
	t.Logf("baseline=%.0f  concurrent_per_call=%.1f", baseline, perCall)
}
