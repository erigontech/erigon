package log

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func BenchmarkStreamNoCtx(b *testing.B) {
	lg := New()

	buf := bytes.Buffer{}
	lg.SetHandler(StreamHandler(&buf, LogfmtFormat()))

	for b.Loop() {
		lg.Info("test message")
		buf.Reset()
	}
}

func BenchmarkDiscard(b *testing.B) {
	lg := New()
	lg.SetHandler(DiscardHandler())

	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkCallerFileHandler(b *testing.B) {
	lg := New()
	lg.SetHandler(CallerFileHandler(DiscardHandler()))

	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkCallerFuncHandler(b *testing.B) {
	lg := New()
	lg.SetHandler(CallerFuncHandler(DiscardHandler()))

	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkLogfmtNoCtx(b *testing.B) {
	r := Record{
		Time: time.Now(),
		Lvl:  LvlInfo,
		Msg:  "test message",
		Ctx:  []any{},
	}

	logfmt := LogfmtFormat()
	for b.Loop() {
		logfmt.Format(&r)
	}
}

func BenchmarkJsonNoCtx(b *testing.B) {
	r := Record{
		Time: time.Now(),
		Lvl:  LvlInfo,
		Msg:  "test message",
		Ctx:  []any{},
	}

	jsonfmt := JsonFormat()
	for b.Loop() {
		jsonfmt.Format(&r)
	}
}

func BenchmarkMultiLevelFilter(b *testing.B) {
	handler := MultiHandler(
		LvlFilterHandler(LvlDebug, DiscardHandler()),
		LvlFilterHandler(LvlError, DiscardHandler()),
	)

	lg := New()
	lg.SetHandler(handler)
	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkDescendant1(b *testing.B) {
	lg := New()
	lg.SetHandler(DiscardHandler())
	lg = lg.New()
	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkDescendant2(b *testing.B) {
	lg := New()
	lg.SetHandler(DiscardHandler())
	for i := 0; i < 2; i++ {
		lg = lg.New()
	}
	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkDescendant4(b *testing.B) {
	lg := New()
	lg.SetHandler(DiscardHandler())
	for i := 0; i < 4; i++ {
		lg = lg.New()
	}
	for b.Loop() {
		lg.Info("test message")
	}
}

func BenchmarkDescendant8(b *testing.B) {
	lg := New()
	lg.SetHandler(DiscardHandler())
	for i := 0; i < 8; i++ {
		lg = lg.New()
	}
	for b.Loop() {
		lg.Info("test message")
	}
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
	for i := 0; i < goroutines; i++ {
		go func() {
			defer workers.Done()
			for range start {
				lg.Info("msg", "k", "v")
				done <- struct{}{}
			}
		}()
	}

	concurrent := testing.AllocsPerRun(100, func() {
		for i := 0; i < goroutines; i++ {
			start <- struct{}{}
		}
		for i := 0; i < goroutines; i++ {
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

// Copied from https://github.com/uber-go/zap/blob/master/benchmarks/log15_bench_test.go
// (MIT License)
func newLog15() Logger {
	logger := New()
	logger.SetHandler(StreamHandler(io.Discard, JsonFormat()))
	return logger
}

var errExample = errors.New("fail")

type user struct {
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

var _jane = user{
	Name:      "Jane Doe",
	Email:     "jane@test.com",
	CreatedAt: time.Date(1980, 1, 1, 12, 0, 0, 0, time.UTC),
}

func BenchmarkLog15AddingFields(b *testing.B) {
	logger := newLog15()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("Go fast.",
				"int", 1,
				"int64", int64(1),
				"float", 3.0,
				"string", "four!",
				"bool", true,
				"time", time.Unix(0, 0),
				"error", errExample.Error(),
				"duration", time.Second,
				"user-defined type", _jane,
				"another string", "done!",
			)
		}
	})
}

func BenchmarkLog15WithAccumulatedContext(b *testing.B) {
	logger := newLog15().New(
		"int", 1,
		"int64", int64(1),
		"float", 3.0,
		"string", "four!",
		"bool", true,
		"time", time.Unix(0, 0),
		"error", errExample.Error(),
		"duration", time.Second,
		"user-defined type", _jane,
		"another string", "done!",
	)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("Go really fast.")
		}
	})
}

func BenchmarkLog15WithoutFields(b *testing.B) {
	logger := newLog15()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("Go fast.")
		}
	})
}
