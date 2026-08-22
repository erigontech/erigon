package onefile

import (
	"runtime"
	"runtime/metrics"
	"testing"
	"time"
)

const size = 256 << 20

// maxStopTheWorld is how long the runtime had to wait for every goroutine to
// stop, worst case so far. This is the number a long assembly call inflates.
func maxStopTheWorld() time.Duration {
	s := []metrics.Sample{{Name: "/sched/pauses/stopping/gc:seconds"}}
	metrics.Read(s)
	h := s[0].Value.Float64Histogram()
	var max float64
	for i, count := range h.Counts {
		if count > 0 {
			max = h.Buckets[i+1]
		}
	}
	return time.Duration(max * float64(time.Second))
}

func measure(t *testing.T, work func()) {
	stop := make(chan struct{})
	go func() { // keep asking for a stop-the-world
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.GC()
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(200 * time.Millisecond)

	for range 5 {
		work()
	}
	time.Sleep(300 * time.Millisecond) // let a blocked stop-the-world finish
	t.Log("max stop-the-world wait:", maxStopTheWorld())
	close(stop)
}

// copy() is runtime.memmove: assembly, which the runtime can never int
func TestOneBigCopy(t *testing.T) {
	dst, src := make([]byte, size), make([]byte, size)
	measure(t, func() { copy(dst, src) })
}

// Same bytes, 4096 small copies -- and still nothing to interrupt, bec
// loop calls nothing but assembly.
func TestManySmallCopies(t *testing.T) {
	dst, src := make([]byte, size), make([]byte, size)
	measure(t, func() {
		for off := 0; off < size; off += 64 << 10 {
			copy(dst[off:off+64<<10], src[off:off+64<<10])
		}
	})
}

//go:noinline
func copyChunk(dst, src []byte) { copy(dst, src) }

// Identical work, but every chunk now enters a Go function, whose prol
// carries the stack-growth check. Poisoning that check is how the runtime asks
// a goroutine to stop.
func TestManySmallCopiesViaGoCall(t *testing.T) {
	dst, src := make([]byte, size), make([]byte, size)
	measure(t, func() {
		for off := 0; off < size; off += 64 << 10 {
			copyChunk(dst[off:off+64<<10], src[off:off+64<<10])
		}
	})
}
