// Package npprof is a measurement probe: it profiles each engine newPayload call and serves the last profile.
package npprof

import (
	"bytes"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/felixge/fgprof"

	"github.com/erigontech/erigon/common/log/v3"
)

var (
	mode = os.Getenv("NP_PROF")
	mu   sync.Mutex
	last = map[string][]byte{}
)

// Begin starts the profile NP_PROF names (cpu or fgprof); the returned func stops it and keeps it.
func Begin() func() {
	f := faults()
	defer func() { lastFaults = f }()
	var buf bytes.Buffer
	switch mode {
	case "cpu":
		runtime.SetCPUProfileRate(500)
		if pprof.StartCPUProfile(&buf) != nil {
			return func() {}
		}
		return func() { pprof.StopCPUProfile(); keep("npcpu", buf.Bytes()) }
	case "fgprof":
		stop := fgprof.Start(&buf, fgprof.FormatPprof)
		return func() {
			defer func() { _ = recover() }() // fgprof divides by zero on a call without samples
			_ = stop()
			keep("npfg", buf.Bytes())
		}
	}
	return func() {}
}

var lastFaults [2]int64

// faults returns the process major and minor page faults so far.
func faults() [2]int64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return [2]int64{ru.Majflt, ru.Minflt}
}

// End logs the page faults since Begin.
func End(t0 time.Time) {
	f := faults()
	log.Info("[npprof] faults", "majflt", f[0]-lastFaults[0], "minflt", f[1]-lastFaults[1], "wall", time.Since(t0))
}

func keep(kind string, b []byte) { mu.Lock(); last[kind] = b; mu.Unlock() }

// Handler waits 3 s, so the block the caller is waiting for has finished, then serves the last profile of kind.
func Handler(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(3 * time.Second)
		mu.Lock()
		b := last[kind]
		mu.Unlock()
		_, _ = w.Write(b)
	}
}
