package stagedsync

import (
	"sync/atomic"
	"time"
)

// dbg probe (NEWPAYLOAD_PHASES only): integrates idle worker time over a block's exec
// window, split by why no task was runnable. Integrated under taskPool.mu at every push/pop.
const (
	idleWake      = iota // a task is queued, the signalled worker is not running yet
	idleWindow           // undispatched pending tasks past frontier+dispatchWindow
	idleLag              // undispatched pending tasks inside the window
	idleStaticDep        // undispatched tasks blocked on a declared (sender/recipient) dependency
	idleDepRerun         // tasks parked for a re-run until the frontier reaches their blocker
	idleTail             // nothing to run; other workers are running
	idleSpineProc        // nothing running; the exec loop is processing results
	idleSpineWait        // nothing running; the exec loop is waiting for a result
	idleReasons
)

var idleNames = [idleReasons]string{"wake", "window", "lag", "sdep", "dep", "tail", "sproc", "swait"}

type idleProbe struct {
	be      *blockExecutor
	start   time.Time
	last    time.Time
	idle    int
	acc     [idleReasons]time.Duration
	allIdle time.Duration
}

type blockIdleState struct {
	tasks       int
	dispatched  atomic.Int64
	pending     atomic.Int64
	minPending  atomic.Int64
	parkedRerun atomic.Int64
	readHit     atomic.Int64
	readMiss    atomic.Int64
}

func (be *blockExecutor) noteDispatchState() {
	if !logNpPhases {
		return
	}
	be.idleState.dispatched.Store(int64(len(be.selfLoopDispatched)))
	be.idleState.pending.Store(int64(len(be.execTasks.pending)))
	be.idleState.minPending.Store(int64(be.execTasks.minPending()))
}

func (be *blockExecutor) noteParked(pt *poolTask, delta int64) {
	if logNpPhases && pt.next == psReexec {
		be.idleState.parkedRerun.Add(delta)
	}
}

// tick must be called with p.mu held.
func (p *taskPool) tick() {
	now := time.Now()
	pr := &p.idle
	if pr.be != nil && pr.idle > 0 {
		dt := now.Sub(pr.last)
		wake := min(pr.idle, p.queue.Len())
		pr.acc[idleWake] += time.Duration(wake) * dt
		if rest := pr.idle - wake; rest > 0 {
			pr.acc[p.idleReason()] += time.Duration(rest) * dt
		}
		if pr.idle == p.workers {
			pr.allIdle += dt
		}
	}
	pr.last = now
}

func (p *taskPool) idleReason() int {
	be := p.idle.be
	st := &be.idleState
	if st.pending.Load() > 0 {
		if st.minPending.Load() <= be.committedFrontier.Load()+dispatchWindow {
			return idleLag
		}
		return idleWindow
	}
	if int(st.dispatched.Load()) < st.tasks {
		return idleStaticDep
	}
	if st.parkedRerun.Load() > 0 {
		return idleDepRerun
	}
	if p.idle.idle < p.workers {
		return idleTail
	}
	if p.pe.spineBusy.Load() {
		return idleSpineProc
	}
	return idleSpineWait
}

func (p *taskPool) beginIdleWindow(be *blockExecutor) {
	if !logNpPhases || p == nil {
		return
	}
	p.mu.Lock()
	be.idleState.tasks = len(be.tasks)
	p.tick()
	p.idle.be, p.idle.start = be, p.idle.last
	p.mu.Unlock()
}

func (p *taskPool) endIdleWindow(be *blockExecutor) (win time.Duration, acc [idleReasons]time.Duration, allIdle time.Duration, workers int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tick()
	if p.idle.be != be {
		return
	}
	win, acc, allIdle = p.idle.last.Sub(p.idle.start), p.idle.acc, p.idle.allIdle
	p.idle.be, p.idle.acc, p.idle.allIdle = nil, [idleReasons]time.Duration{}, 0
	return win, acc, allIdle, p.workers
}

// Critical-path split (NEWPAYLOAD_PHASES only): each commit-frontier step F -> F+1 is cut by
// the events of task F+1 inside it: push, pick by a worker, send of its final result, receive
// by the exec loop, commit.
const (
	workFirst = iota
	workRerun
	workRecheck
)

type taskStamps struct {
	push, pick, send, recv atomic.Int64
	kind                   atomic.Int32
}

type critPath struct {
	lastCommit                      time.Time
	spineOnly, spine, handoff, wake time.Duration
	pre                             time.Duration
	work                            [3]time.Duration
	steps, workSteps3               [3]int
}

func stampNow(a *atomic.Int64) {
	if logNpPhases {
		a.Store(time.Now().UnixNano())
	}
}

func (be *blockExecutor) noteCommitted(tx int) {
	if !logNpPhases || be.poolTasks == nil || be.poolTasks[tx] == nil {
		return
	}
	cp := &be.crit
	now := time.Now()
	cF := cp.lastCommit
	if cF.IsZero() {
		cF = be.execStarted
	}
	cp.lastCommit = now
	st := &be.poolTasks[tx].st
	at := func(a *atomic.Int64) time.Time { return time.Unix(0, a.Load()) }
	maxT := func(a, b time.Time) time.Time {
		if a.After(b) {
			return a
		}
		return b
	}
	p, q, s, r := at(&st.push), at(&st.pick), at(&st.send), at(&st.recv)
	if !r.After(cF) {
		cp.spineOnly += now.Sub(cF)
		cp.steps[0]++
		return
	}
	cp.spine += now.Sub(r)
	cp.handoff += r.Sub(maxT(s, cF))
	if !s.After(cF) {
		cp.steps[1]++
		return
	}
	cp.steps[2]++
	k := st.kind.Load()
	cp.workSteps3[k]++
	cp.work[k] += s.Sub(maxT(q, cF))
	if q.After(cF) {
		pp := maxT(p, cF)
		cp.wake += q.Sub(pp)
		cp.pre += pp.Sub(cF)
	}
}

// Spine split (NEWPAYLOAD_PHASES only): exec-loop serial time per named part, summed per block.
// Each spMark charges the time since the previous mark to its part.
const (
	spPre     = iota // block lookup, changeset accumulator
	spRec            // result bookkeeping, RecordReads
	spIdx            // indexReads
	spDirty          // markReadersDirty of the worker writes
	spWrec           // RecordWrites, re-incarnation write diff
	spQueue          // validate/exec task bookkeeping
	spVal            // dep-order validation and its bookkeeping
	spRevDep         // revalidateCommittedDependents, self-loop re-exec signal
	spReval          // finalize-boundary re-validation
	spCharge         // charge-at-validation sender
	spFee            // calcFees, tip flush
	spSeal           // MarkWritesComplete, SealUpTo
	spFinGas         // receipt offsets, gas pool, state reader
	spReceipt        // execResult.finalize
	spFlush          // merge finalize reads/writes, FlushVersionedWrites, markReadersDirty
	spView           // WithoutVanished, write view, publish queue
	spCommit         // noteCommitted, signalCommitted wake pushes
	spSched          // scheduleExecution
	spPub            // publish bookkeeping, receipt copy
	spSend           // sendResult to the apply loop
	spBlkEnd         // block-end section
	spOther          // rest of the result step
	spParts
)

var spNames = [spParts]string{"pre", "rec", "idx", "dirty", "wrec", "queue", "val", "revdep", "reval", "charge", "fee", "seal", "fingas", "receipt", "flush", "view", "commit", "sched", "pub", "send", "blkend", "other"}

type spineParts struct {
	last time.Time
	acc  [spParts]time.Duration
	n    int
}

func (be *blockExecutor) spStart(t0 time.Time) {
	if logNpPhases {
		be.sp.last = t0
		be.sp.n++
		be.spMark(spPre)
	}
}

func (be *blockExecutor) spMark(part int) {
	if !logNpPhases || be.sp.last.IsZero() {
		return
	}
	now := time.Now()
	be.sp.acc[part] += now.Sub(be.sp.last)
	be.sp.last = now
}

// spStop charges the rest of the result step to part; marks outside a step are no-ops.
func (be *blockExecutor) spStop(part int) {
	be.spMark(part)
	be.sp.last = time.Time{}
}
