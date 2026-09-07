package log

import (
	"io"
	"sync"
	"testing"
	"time"
)

func TestStreamHandlerNoContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wall-clock contention test in short mode")
	}

	const (
		goroutines = 100
		writeDelay = 2 * time.Millisecond
	)

	run := func(handler Handler, wr *slowWriter) (time.Duration, int64) {
		lg := New()
		lg.SetHandler(handler)

		start := time.Now()
		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				lg.Info("msg")
			})
		}
		wg.Wait()

		return time.Since(start), wr.lines.Load()
	}

	parallelWr := &slowWriter{delay: writeDelay}
	parallelElapsed, parallelLines := run(
		StreamHandler(parallelWr, TerminalFormatNoColor()),
		parallelWr,
	)
	if parallelLines != goroutines {
		t.Fatalf("parallel StreamHandler run: expected %d log lines, got %d", goroutines, parallelLines)
	}

	serialWr := &slowWriter{delay: writeDelay}
	serialElapsed, serialLines := run(
		SyncHandler(StreamHandler(serialWr, TerminalFormatNoColor())),
		serialWr,
	)
	if serialLines != goroutines {
		t.Fatalf("sync-wrapped StreamHandler run: expected %d log lines, got %d", goroutines, serialLines)
	}

	// Compare relative behavior within the same test run instead of asserting a
	// fixed wall-clock threshold. The sync-wrapped handler should be
	// meaningfully slower because it serializes all writes through one mutex.
	if serialElapsed <= parallelElapsed {
		t.Fatalf("expected SyncHandler(StreamHandler(...)) to be slower than StreamHandler(...): parallel=%v serial=%v", parallelElapsed, serialElapsed)
	}
	if serialElapsed < 2*parallelElapsed {
		t.Fatalf("expected SyncHandler(StreamHandler(...)) to be at least 2x slower: parallel=%v serial=%v", parallelElapsed, serialElapsed)
	}

	t.Logf("parallel=%v serial=%v lines=%d", parallelElapsed, serialElapsed, parallelLines)
}

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
