package stagedsync

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/state"
)

// depShapeEnabled gates the per-block dependency-DAG "parallel shape" metric.
// Read once at package init so the per-tx timing capture and the per-block
// compute+log are zero-cost when off.
var depShapeEnabled = dbg.EnvBool("DEP_SHAPE", false)

// depSample is one tx's node in the committed cross-tx dependency DAG: its wall
// time (node weight), the distinct earlier txs whose committed writes it read
// (in-edges), and the per-AccountPath edge counts.
type depSample struct {
	txIdx     int
	wallNanos int64
	preds     []int
	perPath   map[state.AccountPath]int64
}

// buildDepSample extracts tx txIdx's in-edges from its committed read-set: every
// read whose value came from an earlier in-block write (MapRead with a producer
// TxIndex in [0, txIdx)) is a true cross-tx data dependency. Reads of pre-block
// committed state (StorageRead) or of the tx's own write (WriteSetRead) are not
// edges.
func buildDepSample(txIdx int, wallNanos int64, readSet state.ReadSet) depSample {
	dep := depSample{txIdx: txIdx, wallNanos: wallNanos}
	var predSet map[int]struct{}
	readSet.RangeHeaders(func(path state.AccountPath, hdr state.ReadHeader) bool {
		if hdr.Source != state.MapRead {
			return true
		}
		p := hdr.Version.TxIndex
		if p < 0 || p >= txIdx {
			return true
		}
		if dep.perPath == nil {
			dep.perPath = make(map[state.AccountPath]int64, 4)
		}
		dep.perPath[path]++
		if predSet == nil {
			predSet = make(map[int]struct{}, 4)
		}
		predSet[p] = struct{}{}
		return true
	})
	if len(predSet) > 0 {
		dep.preds = make([]int, 0, len(predSet))
		for p := range predSet {
			dep.preds = append(dep.preds, p)
		}
		sort.Ints(dep.preds)
	}
	return dep
}

// depMetrics is the folded per-block dependency shape.
type depMetrics struct {
	criticalPathNanos int64
	criticalPathTxs   int64
	totalExecNanos    int64
	depEdges          int64
	dependentTxs      int64
	independentTxs    int64
	perPath           map[state.AccountPath]int64
}

// buildDeps folds the DAG into the block's shape: the longest weighted chain
// (critical path = inherent serial floor), edge counts, and dependent/independent
// split. A single DP pass in tx-index order suffices — every predecessor's finish
// is computed before the tx that reads it (preds are always < txIdx).
func buildDeps(deps []depSample) depMetrics {
	var m depMetrics
	if len(deps) == 0 {
		return m
	}
	order := make([]int, len(deps))
	for i := range deps {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return deps[order[a]].txIdx < deps[order[b]].txIdx })

	finish := make(map[int]int64, len(deps))
	cpLen := make(map[int]int64, len(deps))
	var critPath int64
	critTx := -1
	for _, oi := range order {
		d := &deps[oi]
		var predFinish, predChain int64
		for _, p := range d.preds {
			if f := finish[p]; f > predFinish {
				predFinish = f
				predChain = cpLen[p]
			}
		}
		finish[d.txIdx] = predFinish + d.wallNanos
		cpLen[d.txIdx] = predChain + 1
		if finish[d.txIdx] > critPath {
			critPath = finish[d.txIdx]
			critTx = d.txIdx
		}
		m.totalExecNanos += d.wallNanos
		edges := int64(len(d.preds))
		m.depEdges += edges
		if edges > 0 {
			m.dependentTxs++
		} else {
			m.independentTxs++
		}
		for path, c := range d.perPath {
			if m.perPath == nil {
				m.perPath = make(map[state.AccountPath]int64, len(d.perPath))
			}
			m.perPath[path] += c
		}
	}
	m.criticalPathNanos = critPath
	if critTx >= 0 {
		m.criticalPathTxs = cpLen[critTx]
	}
	return m
}

// serialSplit is the exec-loop serial-spine timing for one block: total wall
// spent single-threaded in nextResult, and the per-component breakdown of the
// in-order validation loop.
type serialSplit struct {
	serialNanos      int64
	valLoopNanos     int64
	commitNanos      int64
	scheduleNanos    int64
	publishNanos     int64
	stateWritesNanos int64
	acctReadNanos    int64
	delPrefixNanos   int64
	txIndexNanos     int64
	calcFeesNanos    int64
	validateNanos    int64
	flushNanos       int64
	finalizeNanos    int64
	normalizeNanos   int64
}

func (be *blockExecutor) serialTiming() serialSplit {
	var acctReadNanos, delPrefixNanos int64
	if be.blockStateCache != nil {
		acctReadNanos = be.blockStateCache.AcctReadNanos
		delPrefixNanos = be.blockStateCache.DelPrefixNanos
	}
	return serialSplit{
		serialNanos:      be.serialNanos,
		valLoopNanos:     be.valLoopNanos,
		commitNanos:      be.commitNanos,
		scheduleNanos:    be.scheduleNanos,
		publishNanos:     be.publishNanos,
		stateWritesNanos: be.stateWritesNanos,
		acctReadNanos:    acctReadNanos,
		delPrefixNanos:   delPrefixNanos,
		txIndexNanos:     be.txIndexNanos,
		calcFeesNanos:    be.calcFeesNanos,
		validateNanos:    be.validateNanos,
		flushNanos:       be.flushNanos,
		finalizeNanos:    be.finalizeNanos,
		normalizeNanos:   be.normalizeNanos,
	}
}

// logDepShape computes and logs the block's parallel-shape metric. blockIO holds
// the committed per-tx read sets; stats holds per-tx wall times; wallSpan is the
// achieved block exec wall; serial is the serial-spine timing. No-op when
// DEP_SHAPE is off.
func logDepShape(logger log.Logger, blockNum uint64, blockIO *state.VersionedIO, stats map[int]ExecutionStat, wallSpan time.Duration, serial serialSplit) {
	if !depShapeEnabled || blockIO == nil {
		return
	}
	nTx := blockIO.Len() - 1
	if nTx <= 0 {
		return
	}
	deps := make([]depSample, 0, nTx)
	for txIdx := 0; txIdx < nTx; txIdx++ {
		if !blockIO.HasReads(txIdx) {
			continue
		}
		deps = append(deps, buildDepSample(txIdx, stats[txIdx].Duration.Nanoseconds(), blockIO.ReadSet(txIdx)))
	}
	if len(deps) == 0 {
		return
	}
	m := buildDeps(deps)

	var ideal, achieved float64
	if m.criticalPathNanos > 0 {
		ideal = float64(m.totalExecNanos) / float64(m.criticalPathNanos)
	}
	if wallSpan > 0 {
		achieved = float64(m.totalExecNanos) / float64(wallSpan.Nanoseconds())
	}

	// Wall-independent concurrency: measured over the worker-active window
	// (first task start → last task end), which excludes the serial-apply
	// drain tail that inflates wallSpan. windowConc = avg concurrency across
	// that window; peakConc = max simultaneously-running committed tasks
	// (interval sweep). If windowConc >> achieved, the block wall — not the
	// worker parallelism — is what suppresses the achieved figure.
	windowConc, peakConc, windowNanos := workerWindowConcurrency(stats, m.totalExecNanos)

	// serialPct = fraction of the exec wall the single-threaded exec-loop
	// spine (nextResult) was busy; the Amdahl serial floor. High => the spine
	// is the bottleneck and parallelising it is the lever.
	var serialPct float64
	if wallSpan > 0 {
		serialPct = 100 * float64(serial.serialNanos) / float64(wallSpan.Nanoseconds())
	}

	logger.Info("[dep-shape]",
		"blk", blockNum,
		"txs", len(deps),
		"ideal", fmt.Sprintf("%.2f", ideal),
		"achieved", fmt.Sprintf("%.2f", achieved),
		"windowConc", fmt.Sprintf("%.2f", windowConc),
		"peakConc", peakConc,
		"workerWinMs", fmt.Sprintf("%.1f", float64(windowNanos)/1e6),
		"critPathTxs", m.criticalPathTxs,
		"depEdges", m.depEdges,
		"dependent", m.dependentTxs,
		"independent", m.independentTxs,
		"serialPct", fmt.Sprintf("%.1f", serialPct),
		"serialMs", fmt.Sprintf("%.1f", float64(serial.serialNanos)/1e6),
		"wallMs", fmt.Sprintf("%.1f", float64(wallSpan.Nanoseconds())/1e6),
		"valLoopMs", fmt.Sprintf("%.1f", float64(serial.valLoopNanos)/1e6),
		"commitMs", fmt.Sprintf("%.1f", float64(serial.commitNanos)/1e6),
		"scheduleMs", fmt.Sprintf("%.1f", float64(serial.scheduleNanos)/1e6),
		"publishMs", fmt.Sprintf("%.1f", float64(serial.publishNanos)/1e6),
		"stateWrMs", fmt.Sprintf("%.1f", float64(serial.stateWritesNanos)/1e6),
		"acctReadMs", fmt.Sprintf("%.1f", float64(serial.acctReadNanos)/1e6),
		"delPrefixMs", fmt.Sprintf("%.1f", float64(serial.delPrefixNanos)/1e6),
		"txIndexMs", fmt.Sprintf("%.1f", float64(serial.txIndexNanos)/1e6),
		"calcFeesMs", fmt.Sprintf("%.1f", float64(serial.calcFeesNanos)/1e6),
		"validateMs", fmt.Sprintf("%.1f", float64(serial.validateNanos)/1e6),
		"flushMs", fmt.Sprintf("%.1f", float64(serial.flushNanos)/1e6),
		"finalizeMs", fmt.Sprintf("%.1f", float64(serial.finalizeNanos)/1e6),
		"normalizeMs", fmt.Sprintf("%.1f", float64(serial.normalizeNanos)/1e6),
		"perDim", perDimString(m.perPath),
	)
}

// workerWindowConcurrency measures achieved concurrency over the worker-active
// window rather than the exec-loop block wall. It returns the average
// concurrency (totalExec / window), the peak simultaneous committed tasks (via
// an interval sweep over [StartNanos, EndNanos]), and the window span in nanos.
// Tasks whose interval was not captured (StartNanos==0) are skipped.
func workerWindowConcurrency(stats map[int]ExecutionStat, totalExecNanos int64) (avg float64, peak int, windowNanos int64) {
	type endpoint struct {
		t     int64
		delta int
	}
	eps := make([]endpoint, 0, 2*len(stats))
	var minStart, maxEnd int64
	first := true
	for _, s := range stats {
		if s.StartNanos == 0 || s.EndNanos <= s.StartNanos {
			continue
		}
		eps = append(eps, endpoint{s.StartNanos, 1}, endpoint{s.EndNanos, -1})
		if first || s.StartNanos < minStart {
			minStart = s.StartNanos
		}
		if first || s.EndNanos > maxEnd {
			maxEnd = s.EndNanos
		}
		first = false
	}
	if len(eps) == 0 {
		return 0, 0, 0
	}
	windowNanos = maxEnd - minStart
	if windowNanos > 0 {
		avg = float64(totalExecNanos) / float64(windowNanos)
	}
	sort.Slice(eps, func(i, j int) bool {
		if eps[i].t != eps[j].t {
			return eps[i].t < eps[j].t
		}
		// process ends before starts at the same instant so touching
		// intervals don't inflate the peak.
		return eps[i].delta < eps[j].delta
	})
	cur := 0
	for _, e := range eps {
		cur += e.delta
		if cur > peak {
			peak = cur
		}
	}
	return avg, peak, windowNanos
}

func perDimString(perPath map[state.AccountPath]int64) string {
	if len(perPath) == 0 {
		return ""
	}
	paths := make([]state.AccountPath, 0, len(perPath))
	for p := range perPath {
		paths = append(paths, p)
	}
	sort.Slice(paths, func(i, j int) bool { return paths[i] < paths[j] })
	var b strings.Builder
	for i, p := range paths {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s:%d", p, perPath[p])
	}
	return b.String()
}
