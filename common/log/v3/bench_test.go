package log

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
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

type slowWriter struct {
	delay time.Duration
	lines atomic.Int64
}

func (w *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	w.lines.Add(int64(bytes.Count(p, []byte{'\n'})))
	return len(p), nil
}

func TestStreamHandlerNoContention(t *testing.T) {
	const (
		goroutines = 500
		writeDelay = 1 * time.Nanosecond
	)

	wr := &slowWriter{delay: writeDelay}
	lg := New()
	lg.SetHandler(AsyncStreamHandler(wr, TerminalFormatNoColor()))

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			lg.Info("msg")
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Without mutex: ~1ms (parallel writes).
	// With SyncHandler mutex: ~50ms (serialized writes).
	// Use 15ms as threshold — well above parallel, well below serialized.
	if lines := wr.lines.Load(); lines != goroutines {
		t.Fatalf("expected %d log lines, got %d — messages lost", goroutines, lines)
	}
	limit := time.Duration(goroutines/3) * writeDelay
	if elapsed > limit {
		t.Fatalf("logging took %v with %d goroutines (limit %v) — likely mutex contention", elapsed, goroutines, limit)
	}
	t.Logf("elapsed=%v (limit=%v) lines=%d", elapsed, limit, wr.lines.Load())
}
