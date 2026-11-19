package rpc

import (
	"sync/atomic"
	"time"
)

// Local fan-out probe, not for commit.
var (
	SimProbeOn    atomic.Bool
	simWakeIdx    atomic.Int64
	simWriteIdx   atomic.Int64
	SimWakeTimes  = make([]int64, 1<<16)
	SimWriteTimes = make([]int64, 1<<16)
)

func simProbe(idx *atomic.Int64, buf []int64) {
	if !SimProbeOn.Load() {
		return
	}
	i := idx.Add(1) - 1
	if int(i) < len(buf) {
		buf[i] = time.Now().UnixNano()
	}
}

// SimProbeReset returns the wake and write times recorded since the last reset.
func SimProbeReset() (wakes, writes []int64) {
	nw, nr := min(int(simWakeIdx.Swap(0)), len(SimWakeTimes)), min(int(simWriteIdx.Swap(0)), len(SimWriteTimes))
	return append([]int64(nil), SimWakeTimes[:nw]...), append([]int64(nil), SimWriteTimes[:nr]...)
}

var (
	simHeadWakeIdx, simHeadDoneIdx atomic.Int64
	simHeadWake                    = make([]int64, 1<<14)
	simHeadDone                    = make([]int64, 1<<14)
)

// SimProbeHeadWake and SimProbeHeadDone record when a newHeads subscription goroutine starts and ends its write.
func SimProbeHeadWake() { simProbe(&simHeadWakeIdx, simHeadWake) }
func SimProbeHeadDone() { simProbe(&simHeadDoneIdx, simHeadDone) }

// SimProbeHeadReset returns the newHeads wake and done times recorded since the last reset.
func SimProbeHeadReset() (wakes, dones []int64) {
	nw, nd := min(int(simHeadWakeIdx.Swap(0)), len(simHeadWake)), min(int(simHeadDoneIdx.Swap(0)), len(simHeadDone))
	return append([]int64(nil), simHeadWake[:nw]...), append([]int64(nil), simHeadDone[:nd]...)
}
