// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

// Package perfbreakdown provides a per-newPayload-block performance
// breakdown into a small set of buckets:
//
//   - ExecTotal  — wall-clock spent inside the block-execution phase.
//   - ExecIO     — sum of wall-clock spent in state reads driven by EVM
//     execution (account / code / storage reads).
//     ExecCPU = ExecTotal − ExecIO.
//   - CommitTotal — wall-clock spent inside the commitment / state-root
//     computation phase.
//   - CommitIO   — sum of wall-clock spent in trie-context Branch reads
//     during commitment. CommitCPU = CommitTotal − CommitIO.
//   - EverythingElse = Total − ExecTotal − CommitTotal.
//
// Plus the degree of parallelism actually used in each phase.
//
// The instrumentation is gated by env var ERIGON_PERF_BREAKDOWN=1; when
// off, everything is a no-op fast-path so production code pays nothing.
//
// A single profiler is active at a time (one newPayload per node). The
// engineapi entrypoint installs it via SetActive; phase entry points
// flip Phase and the leaf-level state-read / branch-read helpers route
// their elapsed time into the correct IO bucket via AddIO. Worker
// goroutines in parallel exec / parallel commitment increment the same
// counters atomically.
package perfbreakdown

import (
	"os"
	"sync/atomic"
	"time"
)

// Phase identifies the current newPayload phase.
type Phase int32

const (
	PhaseOther  Phase = iota // everything-else default
	PhaseExec                // block execution (EVM)
	PhaseCommit              // commitment / state-root computation
)

// BlockProfiler aggregates per-block timing.
type BlockProfiler struct {
	blockNum uint64
	start    time.Time

	currentPhase atomic.Int32 // Phase

	execTotalNS  atomic.Int64
	execIOTotal  atomic.Int64 // time inside state reads (account/code/storage)
	execWorkers  atomic.Int32 // observed worker count during exec
	execParallel atomic.Bool

	commitTotalNS  atomic.Int64
	commitIOTotal  atomic.Int64 // time inside TrieContext.Branch reads
	commitWorkers  atomic.Int32 // observed worker count during commit
	commitParallel atomic.Bool

	totalNS atomic.Int64
}

// enabled is set once from ERIGON_PERF_BREAKDOWN.
var enabled = func() bool {
	v := os.Getenv("ERIGON_PERF_BREAKDOWN")
	return v == "1" || v == "true" || v == "yes"
}()

// Enabled reports whether instrumentation is on for this process.
func Enabled() bool { return enabled }

// active is the currently-installed profiler (one per process at a time).
var active atomic.Pointer[BlockProfiler]

// SetActive installs p as the current profiler for newPayload bookkeeping.
// Returns the profiler so callers can chain a deferred SetActive(nil).
func SetActive(p *BlockProfiler) *BlockProfiler {
	active.Store(p)
	return p
}

// Active returns the currently-installed profiler or nil.
func Active() *BlockProfiler {
	return active.Load()
}

// New constructs a profiler and stamps the start time. Caller is expected
// to SetActive(p) and then defer p.Finish().
func New(blockNum uint64) *BlockProfiler {
	if !enabled {
		return nil
	}
	return &BlockProfiler{blockNum: blockNum, start: time.Now()}
}

// Finish marks the end of the block, clears active, and returns the
// profiler so the caller can read counters before discard. Safe on nil.
func (p *BlockProfiler) Finish() *BlockProfiler {
	if p == nil {
		return nil
	}
	p.totalNS.Store(int64(time.Since(p.start)))
	active.CompareAndSwap(p, nil)
	return p
}

// BlockNum returns the block number this profiler was installed for.
func (p *BlockProfiler) BlockNum() uint64 {
	if p == nil {
		return 0
	}
	return p.blockNum
}

// SetPhase flips the active phase. Returns the previous phase so callers
// can restore it (defer p.SetPhase(prev)).
func (p *BlockProfiler) SetPhase(ph Phase) Phase {
	if p == nil {
		return PhaseOther
	}
	prev := Phase(p.currentPhase.Swap(int32(ph)))
	return prev
}

// CurrentPhase returns the active phase, or PhaseOther if no profiler.
func (p *BlockProfiler) CurrentPhase() Phase {
	if p == nil {
		return PhaseOther
	}
	return Phase(p.currentPhase.Load())
}

// AddPhaseTotal records phase total wall-clock. Use at phase end.
func (p *BlockProfiler) AddPhaseTotal(ph Phase, d time.Duration) {
	if p == nil {
		return
	}
	switch ph {
	case PhaseExec:
		p.execTotalNS.Add(int64(d))
	case PhaseCommit:
		p.commitTotalNS.Add(int64(d))
	}
}

// AddIO routes a state-read elapsed time into the correct IO bucket
// based on the current phase. Cheap no-op when the profiler is nil.
func (p *BlockProfiler) AddIO(d time.Duration) {
	if p == nil {
		return
	}
	switch Phase(p.currentPhase.Load()) {
	case PhaseExec:
		p.execIOTotal.Add(int64(d))
	case PhaseCommit:
		p.commitIOTotal.Add(int64(d))
	}
}

// SetExecParallelism records the observed worker count during the exec
// phase. The first non-zero call wins (subsequent calls overwrite).
func (p *BlockProfiler) SetExecParallelism(workers int) {
	if p == nil {
		return
	}
	p.execWorkers.Store(int32(workers))
	p.execParallel.Store(workers > 1)
}

// SetCommitParallelism records the observed worker count during the
// commitment phase.
func (p *BlockProfiler) SetCommitParallelism(workers int) {
	if p == nil {
		return
	}
	p.commitWorkers.Store(int32(workers))
	p.commitParallel.Store(workers > 1)
}

// Result is the structured summary at end-of-block.
type Result struct {
	BlockNum uint64

	Total time.Duration

	ExecTotal time.Duration
	ExecIO    time.Duration
	ExecCPU   time.Duration // ExecTotal - ExecIO

	CommitTotal time.Duration
	CommitIO    time.Duration
	CommitCPU   time.Duration // CommitTotal - CommitIO

	EverythingElse time.Duration // Total - ExecTotal - CommitTotal

	ExecWorkers    int
	ExecParallel   bool
	CommitWorkers  int
	CommitParallel bool
}

// Snapshot extracts the structured result. Safe to call after Finish.
func (p *BlockProfiler) Snapshot() Result {
	if p == nil {
		return Result{}
	}
	r := Result{
		BlockNum:       p.blockNum,
		Total:          time.Duration(p.totalNS.Load()),
		ExecTotal:      time.Duration(p.execTotalNS.Load()),
		ExecIO:         time.Duration(p.execIOTotal.Load()),
		CommitTotal:    time.Duration(p.commitTotalNS.Load()),
		CommitIO:       time.Duration(p.commitIOTotal.Load()),
		ExecWorkers:    int(p.execWorkers.Load()),
		ExecParallel:   p.execParallel.Load(),
		CommitWorkers:  int(p.commitWorkers.Load()),
		CommitParallel: p.commitParallel.Load(),
	}
	r.ExecCPU = r.ExecTotal - r.ExecIO
	r.CommitCPU = r.CommitTotal - r.CommitIO
	r.EverythingElse = r.Total - r.ExecTotal - r.CommitTotal
	return r
}
