package stagedsync

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
)

const (
	psRun = iota
	psReexec
	psRecheck
)

type poolTask struct {
	be         *blockExecutor
	tv         *taskVersion
	next       int
	prevWrites *state.WriteSet
	result     *exec.TxResult
	reruns     int

	mu      sync.Mutex
	parked  bool
	pending bool
	fin     bool
}

type poolWorker struct {
	w  *exec.WorkerContext
	tx kv.TemporalTx
}

type taskHeap []*poolTask

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].be.blockNum != h[j].be.blockNum {
		return h[i].be.blockNum < h[j].be.blockNum
	}
	return h[i].tv.index < h[j].tv.index
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(x any)   { *h = append(*h, x.(*poolTask)) }
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

type taskPool struct {
	pe *parallelExecutor

	mu     sync.Mutex
	cond   *sync.Cond
	queue  taskHeap
	closed bool
}

// Test hooks; nil in production.
var (
	poolOnRoTxOpen func()
	poolOnRun      func(runs int)
)

func newTaskPool(pe *parallelExecutor, base []*exec.WorkerContext) *taskPool {
	p := &taskPool{pe: pe}
	p.cond = sync.NewCond(&p.mu)
	context.AfterFunc(pe.workersCtx, func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.cond.Broadcast()
	})
	for _, w := range base {
		pe.runWG.Go(func() { p.workerLoop(&poolWorker{w: w}) })
	}
	return p
}

func (p *taskPool) push(pt *poolTask) {
	if p == nil {
		return
	}
	p.mu.Lock()
	heap.Push(&p.queue, pt)
	p.mu.Unlock()
	p.cond.Signal()
}

func (p *taskPool) pop() *poolTask {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.closed && p.queue.Len() == 0 {
		p.cond.Wait()
	}
	if p.closed {
		return nil
	}
	return heap.Pop(&p.queue).(*poolTask)
}

func (p *taskPool) workerLoop(wk *poolWorker) {
	defer func() {
		if wk.tx != nil {
			wk.tx.Rollback()
		}
	}()
	for pt := p.pop(); pt != nil; pt = p.pop() {
		pt.step(p, wk)
		wk.w.PublishReadMetrics()
	}
}

func (wk *poolWorker) ensureTx(p *taskPool) error {
	if wk.tx != nil {
		return nil
	}
	tx, err := p.pe.cfg.db.BeginTemporalRo(p.pe.workersCtx) //nolint:gocritic // worker-lifetime rotx: rolled back when the worker exits
	if err != nil {
		return err
	}
	if poolOnRoTxOpen != nil {
		poolOnRoTxOpen()
	}
	if err := wk.w.ResetTx(tx); err != nil {
		tx.Rollback()
		return err
	}
	wk.tx = tx
	return nil
}

func (pe *parallelExecutor) dispatchPoolTask(be *blockExecutor, tv *taskVersion) {
	pt := &poolTask{be: be, tv: tv, next: psRun}
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

func (pt *poolTask) waitThen(t, next int) bool {
	pt.next = next
	return !pt.be.requeueAt(pt.tv.index, t, pt)
}

const poolNamedReruns = 3

func (pt *poolTask) rerunAfter(named int) bool {
	pt.reruns++
	if pt.reruns <= poolNamedReruns {
		pt.next = psReexec
		return !pt.be.requeueAtValidated(pt.tv.index, named, pt)
	}
	return pt.waitThen(pt.tv.index-1, psReexec)
}

// rerunAfterPredecessor re-runs the task once its predecessor commits: the predecessor's deferred sender charge
// (pre-Amsterdam) lands only at commit, so waking on its validation could repeat the same failure.
func (pt *poolTask) rerunAfterPredecessor() bool {
	pt.reruns++
	return pt.waitThen(pt.tv.index-1, psReexec)
}

func (pt *poolTask) step(p *taskPool, wk *poolWorker) {
	be, tv := pt.be, pt.tv
	defer func() {
		if rec := recover(); rec != nil {
			pt.sendOperational(p, fmt.Errorf("self-loop worker panic (block %d tx %d): %v\n%s",
				be.blockNum, tv.index, rec, dbg.Stack()))
		}
	}()
	for {
		switch pt.next {
		case psReexec:
			if !pt.bumpInc(p) {
				return
			}
			pt.next = psRun
		case psRecheck:
			valid, _, blocker := be.selfLoopEvaluate(tv, pt.result)
			if !valid {
				if !pt.rerunAfter(blocker) {
					return
				}
				continue
			}
			if !pt.publish(p) {
				return
			}
		case psRun:
			if !pt.run(p, wk) {
				return
			}
		}
	}
}

func (pt *poolTask) run(p *taskPool, wk *poolWorker) bool {
	be, tv := pt.be, pt.tv
	finalExec := be.frontier() >= tv.index-1
	if err := wk.ensureTx(p); err != nil {
		pt.sendOperational(p, fmt.Errorf("block %d tx %d: open roTx: %w", be.blockNum, tv.index, err))
		return false
	}
	for {
		last := be.runInc[tv.index].Load()
		if int64(tv.version.Incarnation) <= last {
			panic(fmt.Sprintf("self-loop: tx %d run at incarnation %d <= last-run %d (two runs share an incarnation)",
				tv.index, tv.version.Incarnation, last))
		}
		if be.runInc[tv.index].CompareAndSwap(last, int64(tv.version.Incarnation)) {
			break
		}
	}
	if poolOnRun != nil {
		poolOnRun(int(tv.version.Incarnation) + 1)
	}
	result := wk.w.RunTxTask(tv)
	if txTask, ok := tv.Task.(*exec.TxTask); ok && txTask.ChargesSenderAtValidation(int(tv.version.Incarnation)) &&
		(result.Err != nil || touchedSenderAccount(txTask, result)) {
		return pt.rerunAfterPredecessor()
	}
	if result.Err != nil {
		if finalExec {
			pt.send(p, result)
			return false
		}
		return pt.rerunAfterPredecessor()
	}
	if result.Dep >= 0 {
		return pt.rerunAfter(be.taskIndexOf(result.Dep))
	}
	be.selfLoopFlush(tv.version, result, pt.prevWrites)
	pt.prevWrites = result.TxOut

	valid, target, blocker := be.selfLoopEvaluate(tv, result)
	if !valid {
		return pt.rerunAfter(blocker)
	}
	return pt.settle(p, result, target)
}

func (pt *poolTask) settle(p *taskPool, result *exec.TxResult, target int) bool {
	pt.result = result
	if !pt.be.versionMap.HasBAL {
		return pt.waitThen(target, psRecheck)
	}
	return pt.publish(p)
}

func (pt *poolTask) publish(p *taskPool) bool {
	pt.send(p, pt.result)
	pt.result = nil
	pt.next = psReexec
	pt.mu.Lock()
	if pt.pending {
		pt.pending = false
		pt.mu.Unlock()
		return pt.rerunAfterPredecessor()
	}
	pt.parked = true
	pt.mu.Unlock()
	return false
}
