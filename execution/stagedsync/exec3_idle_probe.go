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
	be.idleState.tasks = len(be.tasks)
	p.mu.Lock()
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
