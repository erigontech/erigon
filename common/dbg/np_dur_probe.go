package dbg

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// [np-dur] probe counters.
var (
	NpSenderRecovers atomic.Int64
	NpAuthRecovers   atomic.Int64
	NpRwBegins       atomic.Int64
	NpRwCommits      atomic.Int64
	NpRwCommitNs     atomic.Int64
	NpWindow         atomic.Bool

	npCallersMu sync.Mutex
	npCallers   []string
)

func NpRwCommitDone(start time.Time) {
	NpRwCommits.Add(1)
	NpRwCommitNs.Add(int64(time.Since(start)))
	if !NpWindow.Load() {
		return
	}
	pcs := make([]uintptr, 12)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var b strings.Builder
	for {
		f, more := frames.Next()
		fn := f.Function[strings.LastIndex(f.Function, "/")+1:]
		fmt.Fprintf(&b, "%s<", fn)
		if !more {
			break
		}
	}
	npCallersMu.Lock()
	npCallers = append(npCallers, b.String())
	npCallersMu.Unlock()
}

func NpTakeCallers() []string {
	npCallersMu.Lock()
	defer npCallersMu.Unlock()
	c := npCallers
	npCallers = nil
	return c
}
