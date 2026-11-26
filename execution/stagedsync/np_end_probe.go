package stagedsync

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime/trace"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

var (
	npTraceDir    = dbg.EnvString("NP_TRACE_DIR", "")
	npTraceMinGas = uint64(dbg.EnvInt("NP_TRACE_MIN_GAS", 100_000_000))
)

type npUsage struct {
	cpu                            time.Duration
	majflt, minflt, inblock, nvcsw int64
	nivcsw                         int64
}

func npRusage() npUsage {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return npUsage{}
	}
	return npUsage{
		cpu:    time.Duration(ru.Utime.Nano() + ru.Stime.Nano()),
		majflt: int64(ru.Majflt), minflt: int64(ru.Minflt), inblock: int64(ru.Inblock),
		nvcsw: int64(ru.Nvcsw), nivcsw: int64(ru.Nivcsw),
	}
}

// npCPU is the process CPU time (user+system) used so far.
func npCPU() time.Duration {
	if !logNpPhases {
		return 0
	}
	return npRusage().cpu
}

var npEnd struct {
	sync.Mutex
	start   time.Time
	names   []string
	at      map[string]time.Time
	sums    map[string]time.Duration
	use0    npUsage
	use     map[string]npUsage
	tracing *os.File
}

// NpEndStart opens the newPayload end-of-block timeline; marks before it are dropped.
// With NP_TRACE_DIR set, a payload of at least NP_TRACE_MIN_GAS gas is also written as a runtime trace.
func NpEndStart(gasUsed uint64) {
	if !logNpPhases {
		return
	}
	npEnd.Lock()
	defer npEnd.Unlock()
	npEnd.start, npEnd.names = time.Now(), npEnd.names[:0]
	npEnd.at, npEnd.sums, npEnd.use = map[string]time.Time{}, map[string]time.Duration{}, map[string]npUsage{}
	npEnd.use0 = npRusage()
	if npTraceDir != "" && gasUsed >= npTraceMinGas && npEnd.tracing == nil {
		f, err := os.Create(filepath.Join(npTraceDir, fmt.Sprintf("np-%d.trace", time.Now().UnixNano())))
		if err == nil && trace.Start(f) != nil {
			f.Close()
			f = nil
		}
		npEnd.tracing = f
	}
}

// NpEndMark records the last time the named phase ended.
func NpEndMark(name string) {
	if !logNpPhases {
		return
	}
	now := time.Now()
	npEnd.Lock()
	defer npEnd.Unlock()
	if npEnd.start.IsZero() {
		return
	}
	if _, ok := npEnd.at[name]; !ok {
		npEnd.names = append(npEnd.names, name)
	}
	npEnd.at[name] = now
	npEnd.use[name] = npRusage()
}

// NpEndAdd sums a duration under name.
func NpEndAdd(name string, d time.Duration) {
	if !logNpPhases {
		return
	}
	npEnd.Lock()
	defer npEnd.Unlock()
	if !npEnd.start.IsZero() {
		npEnd.sums[name] += d
	}
}

// NpEndLog logs every mark as an offset from NpEndStart and closes the timeline.
func NpEndLog(logger log.Logger, blk uint64) {
	if !logNpPhases {
		return
	}
	npEnd.Lock()
	defer npEnd.Unlock()
	if npEnd.start.IsZero() {
		return
	}
	kv := []any{"blk", blk, "npReply", time.Since(npEnd.start)}
	for _, n := range npEnd.names {
		kv = append(kv, n, npEnd.at[n].Sub(npEnd.start))
	}
	for _, n := range slices.Sorted(maps.Keys(npEnd.sums)) {
		kv = append(kv, n, npEnd.sums[n])
	}
	logger.Info("[np-phase] end", kv...)
	// Process CPU, faults and context switches since NpEndStart, at each mark's last time.
	u0 := npEnd.use0
	cpu := []any{"blk", blk}
	for _, n := range npEnd.names {
		u := npEnd.use[n]
		cpu = append(cpu, n, fmt.Sprintf("%s/maj%d/min%d/in%d/vcs%d/ivcs%d", u.cpu-u0.cpu,
			u.majflt-u0.majflt, u.minflt-u0.minflt, u.inblock-u0.inblock, u.nvcsw-u0.nvcsw, u.nivcsw-u0.nivcsw))
	}
	logger.Info("[np-phase] endcpu", cpu...)
	if f := npEnd.tracing; f != nil {
		trace.Stop()
		f.Close()
		logger.Info("[np-phase] trace", "blk", blk, "file", f.Name())
		npEnd.tracing = nil
	}
	npEnd.start = time.Time{}
}
