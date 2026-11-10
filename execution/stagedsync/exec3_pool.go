package stagedsync

import (
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
)

// poolSlot is one permit to run: a read tx and the context bound to it. A slot given
// up by a task parked mid-EVM has no context, because the EVM stays on that context.
type poolSlot struct {
	tx kv.TemporalTx
	w  *exec.WorkerContext
}

type poolTask struct {
	be         *blockExecutor
	tv         *taskVersion
	prevWrites *state.WriteSet
	reruns     int
	slot       poolSlot
	// reexec latches a request to re-execute; done closes when the task is published.
	reexec chan struct{}
	done   chan struct{}
}

type taskPool struct {
	pe     *parallelExecutor
	slots  chan poolSlot
	idleGo chan func()

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*poolTask
	closed bool
	spare  []*exec.WorkerContext
	minted []*exec.WorkerContext
}

// writerGate wakes the readers and waiters of one task. Callers subscribe, re-check
// their condition, then block, so a fire between the check and the block is not lost.
type writerGate struct {
	mu sync.Mutex
	ch chan struct{}
}

func (g *writerGate) subscribe() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
	return g.ch
}

func (g *writerGate) fire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch != nil {
		close(g.ch)
		g.ch = nil
	}
}

// Test hooks; nil in production.
var (
	poolOnRoTxOpen func()
	poolOnRun      func(runs int)
)

func newTaskPool(pe *parallelExecutor, base []*exec.WorkerContext) *taskPool {
	p := &taskPool{pe: pe, slots: make(chan poolSlot, len(base)), idleGo: make(chan func())}
	p.cond = sync.NewCond(&p.mu)
	for _, w := range base {
		p.slots <- poolSlot{w: w}
	}
	context.AfterFunc(pe.workersCtx, func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.cond.Broadcast()
	})
	pe.runWG.Go(p.dispatchLoop)
	return p
}

func (p *taskPool) push(pt *poolTask) {
	p.mu.Lock()
	p.queue = append(p.queue, pt)
	p.mu.Unlock()
	p.cond.Signal()
}

func (p *taskPool) pop() *poolTask {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.closed && len(p.queue) == 0 {
		p.cond.Wait()
	}
	if p.closed {
		return nil
	}
	pt := p.queue[0]
	p.queue[0] = nil
	p.queue = p.queue[1:]
	return pt
}

// dispatchLoop starts queued tasks one at a time, each when a slot is free. A task
// starts only with a slot, so a woken reader queues behind one dispatcher at most,
// not behind every unstarted task of the block.
func (p *taskPool) dispatchLoop() {
	pt := p.pop()
	if pt == nil {
		return
	}
	if err := p.openReadTxs(); err != nil {
		pt.sendOperational(p, fmt.Errorf("block %d tx %d: open roTx: %w", pt.be.blockNum, pt.tv.index, err))
		return
	}
	for ; pt != nil; pt = p.pop() {
		slot, err := p.acquire()
		if err != nil {
			pt.sendOperational(p, err)
			return
		}
		task := pt
		p.goReused(func() { task.run(p, slot) })
	}
}

// openReadTxs opens every slot's read tx before any task runs, so that a task resuming
// mid-EVM always finds an open one. They stay open for the executor's life, so all slots
// read the same snapshot.
func (p *taskPool) openReadTxs() error {
	for range cap(p.slots) {
		s := <-p.slots
		tx, err := p.pe.cfg.db.BeginTemporalRo(p.pe.workersCtx) //nolint:gocritic // executor-lifetime rotx: rolled back in closeSlots
		if err != nil {
			p.slots <- s
			return err
		}
		if poolOnRoTxOpen != nil {
			poolOnRoTxOpen()
		}
		s.tx = tx
		if s.w != nil {
			if err := s.w.ResetTx(tx); err != nil {
				p.slots <- s
				return err
			}
		}
		p.slots <- s
	}
	return nil
}

// goReused runs f on an idle task goroutine when there is one, so f starts on an
// already grown stack instead of growing a new one.
func (p *taskPool) goReused(f func()) {
	select {
	case p.idleGo <- f:
		return
	default:
	}
	idle, done := p.idleGo, p.pe.workersCtx.Done()
	p.pe.runWG.Go(func() {
		for {
			f()
			f = nil
			select {
			case f = <-idle:
			case <-done:
				return
			}
		}
	})
}

func (p *taskPool) acquire() (poolSlot, error) {
	select {
	case s := <-p.slots:
		if s.w == nil {
			s.w = p.takeContext()
			if err := s.w.ResetTx(s.tx); err != nil {
				p.slots <- s
				return poolSlot{}, fmt.Errorf("bind roTx: %w", err)
			}
		}
		return s, nil
	case <-p.pe.workersCtx.Done():
		return poolSlot{}, p.pe.workersCtx.Err()
	}
}

// takeContext returns a spare context, or makes one when every context is held by a task
// parked mid-EVM.
func (p *taskPool) takeContext() *exec.WorkerContext {
	p.mu.Lock()
	if n := len(p.spare); n > 0 {
		w := p.spare[n-1]
		p.spare = p.spare[:n-1]
		p.mu.Unlock()
		return w
	}
	p.mu.Unlock()
	pe := p.pe
	w := exec.NewWorkerContext(pe.workersCtx, true, pe.taskExecMetrics, pe.cfg.db,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine, pe.cfg.dirs, pe.logger)
	_ = w.ResetState(pe.rs, nil, nil, state.NewNoopWriter(), nil)
	w.EnablePrevBlockReads(pe.prevBlocks)
	p.mu.Lock()
	p.minted = append(p.minted, w)
	p.mu.Unlock()
	return w
}

// closeSlots rolls back the read txs and closes extra contexts. Idempotent. Call after
// every task goroutine has exited: each one returns its slot on the way out.
func (p *taskPool) closeSlots() {
	for {
		select {
		case s := <-p.slots:
			if s.tx != nil {
				s.tx.Rollback()
			}
		default:
			for _, w := range p.minted {
				w.Close()
			}
			p.minted = nil
			return
		}
	}
}

func (pe *parallelExecutor) dispatchPoolTask(be *blockExecutor, tv *taskVersion) {
	pt := &poolTask{be: be, tv: tv, reexec: make(chan struct{}, 1), done: make(chan struct{})}
	be.poolTasks[tv.index] = pt
	pe.pool.push(pt)
}

func (pt *poolTask) send(p *taskPool, r *exec.TxResult) {
	select {
	case p.pe.results <- r:
	case <-p.pe.workersCtx.Done():
	}
}

func (pt *poolTask) sendOperational(p *taskPool, err error) {
	pt.send(p, &exec.TxResult{Task: pt.tv, Err: err, Operational: true})
}

func (pt *poolTask) bumpInc(p *taskPool) bool {
	be, tv := pt.be, pt.tv
	tv.version.Incarnation++
	if int(tv.version.Incarnation) > len(be.tasks)+8 {
		pt.sendOperational(p, fmt.Errorf("block %d tx %d exceeded the self-loop incarnation limit (%d) without a settled verdict",
			be.blockNum, tv.index, len(be.tasks)+8))
		return false
	}
	return true
}

const poolNamedReruns = 3

// waitCommitted blocks until the commit frontier reaches target. False on shutdown.
func (pt *poolTask) waitCommitted(p *taskPool, target int) bool {
	be, tx := pt.be, pt.tv.index
	if target >= len(be.tasks) {
		panic(fmt.Sprintf("[self-loop] block %d: wait target %d out of task range [-1,%d) for task %d — "+
			"a dependency reached waitCommitted in versionMap space (missing taskIndexOf translation); "+
			"block start TxIndex=%d", be.blockNum, target, len(be.tasks), tx, be.tasks[0].Version().TxIndex))
	}
	if target > tx {
		panic(fmt.Sprintf("[self-loop] block %d: task %d waiting for HIGHER target %d (forward dependency — invariant violation); "+
			"block start TxIndex=%d", be.blockNum, tx, target, be.tasks[0].Version().TxIndex))
	}
	for be.frontier() < target {
		wake := be.writers[target].subscribe()
		if be.frontier() >= target {
			break
		}
		select {
		case <-wake:
		case <-p.pe.workersCtx.Done():
			return false
		}
	}
	return true
}

// rerun waits for the named writer, or for the predecessor after poolNamedReruns
// re-runs, and moves the task to its next incarnation.
func (pt *poolTask) rerun(p *taskPool, named int) bool {
	target := pt.tv.index - 1
	if pt.reruns < poolNamedReruns {
		target = named
	}
	pt.reruns++
	return pt.waitCommitted(p, target) && pt.bumpInc(p)
}

func (pt *poolTask) release(p *taskPool) {
	pt.slot.w.PublishReadMetrics()
	p.slots <- pt.slot
	pt.slot = poolSlot{}
}

// run owns the task until it is published. It holds a slot only while the EVM runs.
func (pt *poolTask) run(p *taskPool, slot poolSlot) {
	be, tv := pt.be, pt.tv
	pt.slot = slot
	defer func() {
		if rec := recover(); rec != nil {
			pt.sendOperational(p, fmt.Errorf("self-loop worker panic (block %d tx %d): %v\n%s",
				be.blockNum, tv.index, rec, dbg.Stack()))
		}
		if pt.slot.tx != nil {
			p.slots <- pt.slot
			pt.slot = poolSlot{}
		}
	}()
	for {
		// Captured before the run: a frontier that reaches tx-1 during the run does not
		// make an error from reads made before it authoritative.
		finalExec := be.frontier() >= tv.index-1
		if pt.slot.tx == nil {
			s, err := p.acquire()
			if err != nil {
				pt.sendOperational(p, fmt.Errorf("block %d tx %d: acquire slot: %w", be.blockNum, tv.index, err))
				return
			}
			pt.slot = s
		}
		if poolOnRun != nil {
			poolOnRun(int(tv.version.Incarnation) + 1)
		}
		result := pt.slot.w.RunTxTask(tv)
		pt.release(p)

		named := -2
		switch txTask, ok := tv.Task.(*exec.TxTask); {
		case ok && txTask.ChargesSenderAtValidation(int(tv.version.Incarnation)) && (result.Err != nil || touchedSenderAccount(txTask, result)):
			named = tv.index - 1
		case result.Err != nil:
			if finalExec {
				pt.send(p, result)
				return
			}
			named = tv.index - 1
		case result.Dep >= 0:
			named = be.taskIndexOf(result.Dep)
		}
		if named != -2 {
			if !pt.rerun(p, named) {
				return
			}
			continue
		}

		be.selfLoopFlush(tv.version, result, pt.prevWrites)
		pt.prevWrites = result.TxOut
		valid, target, blocker := be.selfLoopEvaluate(tv, result)
		if valid {
			var ok bool
			if ok, valid, blocker = pt.settle(p, result, target); !ok {
				return
			}
		}
		if !valid {
			if !pt.rerun(p, blocker) {
				return
			}
			continue
		}

		pt.send(p, result)
		if !pt.waitReexec(p) {
			return
		}
	}
}

// settle waits for a non-BAL task's read target to commit and validates again. A BAL
// block's reads are proven by its seeded values, so it skips both. ok is false on shutdown.
func (pt *poolTask) settle(p *taskPool, result *exec.TxResult, target int) (ok, valid bool, blocker int) {
	if pt.be.versionMap.HasBAL {
		return true, true, -1
	}
	if !pt.waitCommitted(p, target) {
		return false, false, -1
	}
	valid, _, blocker = pt.be.selfLoopEvaluate(pt.tv, result)
	return true, valid, blocker
}

// waitReexec blocks a published task until it must re-execute (true, at the next
// incarnation and once the predecessor has committed) or has finished (false).
func (pt *poolTask) waitReexec(p *taskPool) bool {
	select {
	case <-pt.reexec:
		return pt.rerun(p, pt.tv.index-1)
	case <-pt.done:
		return false
	case <-p.pe.workersCtx.Done():
		return false
	}
}

// parkReader blocks reader's run on an in-flight cell of writer until the writer may have
// resolved it. The slot is given up while parked; the reader keeps its context, because the
// EVM is mid-run on it. It returns false when the reader cannot park or the executor stops.
func (be *blockExecutor) parkReader(reader, writer int, blocked func() bool) bool {
	r, w := be.taskIndexOf(reader), be.taskIndexOf(writer)
	// Engine calls in the block-init and block-end tasks hold a chain reader bound to the slot's tx.
	if w < 0 || w >= r || be.tasks[r].Version().TxIndex < 0 || be.tasks[r].Task.IsBlockEnd() {
		return false
	}
	wake := be.writers[w].subscribe()
	if !blocked() {
		return true
	}
	pt, p := be.poolTasks[r], be.pool
	own := pt.slot.w
	pt.slot.w.PublishReadMetrics()
	p.slots <- poolSlot{tx: pt.slot.tx}
	pt.slot = poolSlot{}
	woke := true
	select {
	case <-wake:
	case <-p.pe.workersCtx.Done():
		woke = false
	}
	// Unconditional even on shutdown: the run goes on until the EVM sees the cancel, so it
	// must read through its own open tx, and every slot comes back as tasks end.
	s := <-p.slots
	if s.w != nil {
		p.mu.Lock()
		p.spare = append(p.spare, s.w)
		p.mu.Unlock()
	}
	if err := own.BindTxHeld(s.tx); err != nil {
		panic(fmt.Sprintf("rebind roTx after park: %v", err))
	}
	pt.slot = poolSlot{tx: s.tx, w: own}
	return woke
}
