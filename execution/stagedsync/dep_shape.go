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
	serialNanos    int64
	valLoopNanos   int64
	scheduleNanos  int64
	publishNanos   int64
	calcFeesNanos  int64
	validateNanos  int64
	flushNanos     int64
	finalizeNanos  int64
	normalizeNanos int64
}

func (be *blockExecutor) serialTiming() serialSplit {
	return serialSplit{
		serialNanos:    be.serialNanos,
		valLoopNanos:   be.valLoopNanos,
		scheduleNanos:  be.scheduleNanos,
		publishNanos:   be.publishNanos,
		calcFeesNanos:  be.calcFeesNanos,
		validateNanos:  be.validateNanos,
		flushNanos:     be.flushNanos,
		finalizeNanos:  be.finalizeNanos,
		normalizeNanos: be.normalizeNanos,
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
		"critPathTxs", m.criticalPathTxs,
		"depEdges", m.depEdges,
		"dependent", m.dependentTxs,
		"independent", m.independentTxs,
		"serialPct", fmt.Sprintf("%.1f", serialPct),
		"serialMs", fmt.Sprintf("%.1f", float64(serial.serialNanos)/1e6),
		"wallMs", fmt.Sprintf("%.1f", float64(wallSpan.Nanoseconds())/1e6),
		"valLoopMs", fmt.Sprintf("%.1f", float64(serial.valLoopNanos)/1e6),
		"scheduleMs", fmt.Sprintf("%.1f", float64(serial.scheduleNanos)/1e6),
		"publishMs", fmt.Sprintf("%.1f", float64(serial.publishNanos)/1e6),
		"calcFeesMs", fmt.Sprintf("%.1f", float64(serial.calcFeesNanos)/1e6),
		"validateMs", fmt.Sprintf("%.1f", float64(serial.validateNanos)/1e6),
		"flushMs", fmt.Sprintf("%.1f", float64(serial.flushNanos)/1e6),
		"finalizeMs", fmt.Sprintf("%.1f", float64(serial.finalizeNanos)/1e6),
		"normalizeMs", fmt.Sprintf("%.1f", float64(serial.normalizeNanos)/1e6),
		"perDim", perDimString(m.perPath),
	)
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
