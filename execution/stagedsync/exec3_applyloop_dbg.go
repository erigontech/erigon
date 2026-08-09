package stagedsync

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Debug-only breakdown of the serial exec-loop (processResults/nextResult),
// used to attribute per-tx cost on blocks made of many cheap transactions.
// Disabled unless a caller flips applyLoopTiming, so the hot path keeps a
// single atomic load.

type applyPhase int

const (
	phaseProcessResults applyPhase = iota
	phaseNextResult
	phaseRwsIter
	phaseRecordIO
	phaseCalcFees
	phaseValidate
	phaseFlushWrites
	phaseFinalize
	phaseNormalize
	phaseFeeMerge
	phaseFinalizeMerge
	phaseBookkeeping
	phaseValidateCollect
	phasePublish
	phaseApplyWrites
	phaseApplyIndexes
	phaseSendResult
	phaseSchedule
	phaseCount
)

var applyPhaseNames = [phaseCount]string{
	"processResults(total)",
	"  nextResult(total)",
	"  rws.Iter/PopNext",
	"    RecordReads/Writes",
	"    calcFees",
	"    ValidateVersion",
	"    FlushVersionedWrites",
	"    finalize",
	"    Normalize",
	"    feeMerge(MergeInto)",
	"    finalizeMerge+flush",
	"    task bookkeeping",
	"    validate-collect",
	"    publish(total)",
	"      ApplyStateWrites",
	"      ApplyTxIndexes",
	"      sendResult",
	"    scheduleExecution",
}

var (
	applyLoopTiming atomic.Bool
	applyPhaseNs    [phaseCount]atomic.Int64
	applyPhaseCnt   [phaseCount]atomic.Int64
)

func SetApplyLoopTiming(on bool) { applyLoopTiming.Store(on) }

func ResetApplyLoopTiming() {
	for i := range applyPhaseNs {
		applyPhaseNs[i].Store(0)
		applyPhaseCnt[i].Store(0)
	}
}

func phaseStart() time.Time {
	if applyLoopTiming.Load() {
		return time.Now()
	}
	return time.Time{}
}

func phaseEnd(p applyPhase, start time.Time) {
	if start.IsZero() {
		return
	}
	applyPhaseNs[p].Add(int64(time.Since(start)))
	applyPhaseCnt[p].Add(1)
}

// ApplyLoopTimingReport renders the breakdown sorted by total time, with each
// phase's share of the serial loop.
func ApplyLoopTimingReport(txs int) string {
	total := applyPhaseNs[phaseProcessResults].Load()
	type row struct {
		name  string
		ns    int64
		calls int64
	}
	rows := make([]row, 0, phaseCount)
	for i := range applyPhaseNs {
		rows = append(rows, row{applyPhaseNames[i], applyPhaseNs[i].Load(), applyPhaseCnt[i].Load()})
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n%-24s %10s %8s %10s %9s\n", "phase", "total", "share", "calls", "ns/tx")
	var attributed int64
	for i, r := range rows {
		if applyPhase(i) != phaseProcessResults && applyPhase(i) != phaseNextResult && applyPhase(i) != phasePublish {
			attributed += r.ns
		}
	}
	rows = append(rows, row{"  UNATTRIBUTED", total - attributed, 0})
	for _, r := range rows {
		if r.ns <= 0 {
			continue
		}
		share := 0.0
		if total > 0 {
			share = 100 * float64(r.ns) / float64(total)
		}
		perTx := 0.0
		if txs > 0 {
			perTx = float64(r.ns) / float64(txs)
		}
		fmt.Fprintf(&b, "%-24s %10s %7.1f%% %10d %9.0f\n",
			r.name, time.Duration(r.ns).Round(time.Microsecond), share, r.calls, perTx)
	}
	return b.String()
}
