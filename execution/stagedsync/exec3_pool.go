package stagedsync

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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
	finalExec  bool
	aborted    bool
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

var (
	poolRoTxOpens atomic.Int64
	poolMaxRuns   atomic.Int64
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
	poolRoTxOpens.Add(1)
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

func (pt *poolTask) rawSend(p *taskPool, r *exec.TxResult) {
	select {
	case p.pe.results <- r:
	case <-p.pe.workersCtx.Done():
	}
}

func (pt *poolTask) send(p *taskPool, r *exec.TxResult) {
	if pt.aborted {
		return
	}
	pt.rawSend(p, r)
}

func (pt *poolTask) sendOperational(p *taskPool, err error) {
	if pt.aborted {
		return
	}
	pt.aborted = true
	pt.rawSend(p, &exec.TxResult{Task: pt.tv, Err: err, Operational: true})
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
	t := pt.tv.index - 1
	if pt.reruns < poolNamedReruns {
		t = named
	}
	pt.reruns++
	return pt.waitThen(t, psReexec)
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
			pt.send(p, pt.result)
			pt.result = nil
			pt.next = psReexec
			pt.mu.Lock()
			if pt.pending {
				pt.pending = false
				pt.mu.Unlock()
				if !pt.rerunAfter(tv.index - 1) {
					return
				}
				continue
			}
			pt.parked = true
			pt.mu.Unlock()
			return
		case psRun:
			if !pt.run(p, wk) {
				return
			}
		}
	}
}

func (pt *poolTask) run(p *taskPool, wk *poolWorker) bool {
	be, tv := pt.be, pt.tv
	pt.finalExec = be.frontier() >= tv.index-1
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
	for runs := int64(tv.version.Incarnation) + 1; ; {
		m := poolMaxRuns.Load()
		if runs <= m || poolMaxRuns.CompareAndSwap(m, runs) {
			break
		}
	}
	result := wk.w.RunTxTask(tv)
	if pt.aborted {
		return false
	}
	if txTask, ok := tv.Task.(*exec.TxTask); ok && txTask.ChargesSenderAtValidation(int(tv.version.Incarnation)) &&
		(result.Err != nil || touchedSenderAccount(txTask, result)) {
		return pt.rerunAfter(tv.index - 1)
	}
	if result.Err != nil {
		if pt.finalExec {
			pt.send(p, result)
			return false
		}
		return pt.rerunAfter(tv.index - 1)
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
	return pt.settle(result, target)
}

func (pt *poolTask) settle(result *exec.TxResult, target int) bool {
	pt.result = result
	if !pt.be.versionMap.HasBAL {
		return pt.waitThen(target, psRecheck)
	}
	pt.next = psRecheck
	return true
}
