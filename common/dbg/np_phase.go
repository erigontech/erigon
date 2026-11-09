package dbg

import (
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// npPhase holds one newPayload timeline. Each mark records the time since the
// previous mark, so the named durations of a line add up to its total.
// shortcut: assumes one newPayload at a time and no concurrent FCU, a concurrent call mixes the timelines; throwaway probe.
var npPhase struct {
	sync.Mutex
	active     bool
	stageEnded bool
	blk        uint64
	begin      time.Time
	last       time.Time
	stageEnd   time.Time
	pre, post  []any
	detail     []any
	params     time.Duration
}

// NpBegin starts the timeline of the newPayload for block blk.
func NpBegin(blk uint64) {
	npPhase.Lock()
	defer npPhase.Unlock()
	now := time.Now()
	npPhase.active, npPhase.stageEnded = true, false
	npPhase.blk, npPhase.begin, npPhase.last = blk, now, now
	npPhase.pre, npPhase.post, npPhase.detail = nil, nil, nil
}

// NpMark attributes the time since the previous mark to name.
func NpMark(name string) { NpMarkAt(name, time.Now()) }

// NpMarkAt is NpMark for a point recorded earlier on a goroutine that races NpStageEnd.
// A point before the previous mark gets 0 and does not move the timeline.
func NpMarkAt(name string, at time.Time) {
	npPhase.Lock()
	defer npPhase.Unlock()
	if !npPhase.active {
		return
	}
	d := max(at.Sub(npPhase.last), 0)
	if npPhase.stageEnded {
		npPhase.post = append(npPhase.post, name, d)
	} else {
		npPhase.pre = append(npPhase.pre, name, d)
	}
	if d > 0 {
		npPhase.last = at
	}
}

// NpDetail records the own time of a concurrent part. It overlaps the marks, so it is logged after the post-exec total.
func NpDetail(name string, d time.Duration) {
	npPhase.Lock()
	defer npPhase.Unlock()
	if !npPhase.active {
		return
	}
	for i := 0; i < len(npPhase.detail); i += 2 {
		if npPhase.detail[i] == name {
			npPhase.detail[i+1] = d
			return
		}
	}
	npPhase.detail = append(npPhase.detail, name, d)
}

// NpParamsDecoded records the JSON and hex decode of the newPayload params, which ends before NpBegin.
func NpParamsDecoded(d time.Duration) {
	npPhase.Lock()
	defer npPhase.Unlock()
	npPhase.params = d
}

// NpStageEnd closes the pre-stage timeline at execStart, the moment the `[np-phase] stage`
// window opens, and restarts the post-exec timeline now, the moment that window closes.
// On a multi-block fork the last block wins.
func NpStageEnd(execStart time.Time) {
	npPhase.Lock()
	defer npPhase.Unlock()
	if !npPhase.active {
		return
	}
	if !npPhase.stageEnded {
		npPhase.pre = append(npPhase.pre, "exec_setup", execStart.Sub(npPhase.last), "total", execStart.Sub(npPhase.begin))
	}
	now := time.Now()
	npPhase.stageEnded, npPhase.stageEnd, npPhase.last = true, now, now
	npPhase.post = nil
}

// NpEnd logs both lines of a newPayload that reached the end of the execution stage.
func NpEnd(logger log.Logger) {
	npPhase.Lock()
	defer npPhase.Unlock()
	if npPhase.active && npPhase.stageEnded {
		now := time.Now()
		post := append(npPhase.post, "np_tail", now.Sub(npPhase.last), "total", now.Sub(npPhase.stageEnd))
		pre := append(npPhase.pre, "rpc_params_decode", npPhase.params)
		logger.Info("[np-phase] pre-stage", append([]any{"blk", npPhase.blk}, pre...)...)
		logger.Info("[np-phase] post-exec", append(append([]any{"blk", npPhase.blk}, post...), npPhase.detail...)...)
	}
	npPhase.active = false
}
