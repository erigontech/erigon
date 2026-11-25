package stagedsync

import (
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

var npEnd struct {
	sync.Mutex
	start time.Time
	names []string
	at    map[string]time.Time
	sums  map[string]time.Duration
}

// NpEndStart opens the newPayload end-of-block timeline; marks before it are dropped.
func NpEndStart() {
	if !logNpPhases {
		return
	}
	npEnd.Lock()
	defer npEnd.Unlock()
	npEnd.start, npEnd.names = time.Now(), npEnd.names[:0]
	npEnd.at, npEnd.sums = map[string]time.Time{}, map[string]time.Duration{}
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
	npEnd.start = time.Time{}
}
