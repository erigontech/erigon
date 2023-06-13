package exec22

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type TxTask struct {
	TxNum           uint64
	BlockNum        uint64
	Rules           *chain.Rules
	Header          *types.Header
	Txs             types.Transactions
	Uncles          []*types.Header
	Coinbase        libcommon.Address
	Withdrawals     types.Withdrawals
	BlockHash       libcommon.Hash
	Sender          *libcommon.Address
	SkipAnalysis    bool
	TxIndex         int // -1 for block initialisation
	Final           bool
	Tx              types.Transaction
	GetHashFn       func(n uint64) libcommon.Hash
	TxAsMessage     types.Message
	EvmBlockContext evmtypes.BlockContext

	BalanceIncreaseSet map[libcommon.Address]uint256.Int
	ReadLists          map[string]*state.KvList
	WriteLists         map[string]*state.KvList
	AccountPrevs       map[string][]byte
	AccountDels        map[string]*accounts.Account
	StoragePrevs       map[string][]byte
	CodePrevs          map[string]uint64
	Error              error
	Logs               []*types.Log
	TraceFroms         map[libcommon.Address]struct{}
	TraceTos           map[libcommon.Address]struct{}

	UsedGas uint64
}

// TxTaskQueue non-thread-safe priority-queue
type TxTaskQueue []*TxTask

func (h TxTaskQueue) Len() int {
	return len(h)
}
func (h TxTaskQueue) Less(i, j int) bool {
	return h[i].TxNum < h[j].TxNum
}

func (h TxTaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TxTaskQueue) Push(a interface{}) {
	*h = append(*h, a.(*TxTask))
}

func (h *TxTaskQueue) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// QueueWithRetry is trhead-safe priority-queue of tasks - which attempt to minimize conflict-rate (retry-rate).
// Tasks may conflict and return to queue for re-try/re-exec.
// Tasks added by method `ReTry` have higher priority than tasks added by `Add`.
// Method `Add` expecting already-ordered (by priority) tasks - doesn't do any additional sorting of new tasks.
type QueueWithRetry struct {
	closed      bool
	newTasks    chan *TxTask
	retires     TxTaskQueue
	retiresLock sync.Mutex
	capacity    int
}

func NewQueueWithRetry(capacity int) *QueueWithRetry {
	return &QueueWithRetry{newTasks: make(chan *TxTask, capacity), capacity: capacity}
}

func (q *QueueWithRetry) NewTasksLen() int { return len(q.newTasks) }
func (q *QueueWithRetry) Capacity() int    { return q.capacity }
func (q *QueueWithRetry) RetriesLen() (l int) {
	q.retiresLock.Lock()
	l = q.retires.Len()
	q.retiresLock.Unlock()
	return l
}
func (q *QueueWithRetry) RetryTxNumsList() (out []uint64) {
	q.retiresLock.Lock()
	for _, t := range q.retires {
		out = append(out, t.TxNum)
	}
	q.retiresLock.Unlock()
	return out
}
func (q *QueueWithRetry) Len() (l int) { return q.RetriesLen() + len(q.newTasks) }

// Add "new task" (which was never executed yet). May block internal channel is full.
// Expecting already-ordered tasks.
func (q *QueueWithRetry) Add(ctx context.Context, t *TxTask) {
	select {
	case q.newTasks <- t:
	case <-ctx.Done():
		return
	}
}

// ReTry returns failed (conflicted) task. It's non-blocking method.
// All failed tasks have higher priority than new one.
// No limit on amount of txs added by this method.
func (q *QueueWithRetry) ReTry(t *TxTask) {
	q.retiresLock.Lock()
	heap.Push(&q.retires, t)
	q.retiresLock.Unlock()
	if q.closed {
		return
	}
	select {
	case q.newTasks <- nil:
	default:
	}
}

// Next - blocks until new task available
func (q *QueueWithRetry) Next(ctx context.Context) (*TxTask, bool) {
	task, ok := q.popNoWait()
	if ok {
		return task, true
	}
	return q.popWait(ctx)
}

func (q *QueueWithRetry) popWait(ctx context.Context) (task *TxTask, ok bool) {
	for {
		select {
		case inTask, ok := <-q.newTasks:
			if !ok {
				q.retiresLock.Lock()
				if q.retires.Len() > 0 {
					task = heap.Pop(&q.retires).(*TxTask)
				}
				q.retiresLock.Unlock()
				return task, task != nil
			}

			q.retiresLock.Lock()
			if inTask != nil {
				heap.Push(&q.retires, inTask)
			}
			if q.retires.Len() > 0 {
				task = heap.Pop(&q.retires).(*TxTask)
			}
			q.retiresLock.Unlock()
			if task != nil {
				return task, true
			}
		case <-ctx.Done():
			return nil, false
		}
	}
}
func (q *QueueWithRetry) popNoWait() (task *TxTask, ok bool) {
	q.retiresLock.Lock()
	has := q.retires.Len() > 0
	if has { // means have conflicts to re-exec: it has higher priority than new tasks
		task = heap.Pop(&q.retires).(*TxTask)
	}
	q.retiresLock.Unlock()

	if has {
		return task, task != nil
	}

	// otherwise get some new task. non-blocking way. without adding to queue.
	for task == nil {
		select {
		case task, ok = <-q.newTasks:
			if !ok {

				return nil, false
			}
		default:
			return nil, false
		}
	}
	return task, task != nil
}

// Close safe to call multiple times
func (q *QueueWithRetry) Close() {
	if q.closed {
		return
	}
	q.closed = true
	close(q.newTasks)
}

// ResultsQueue thread-safe priority-queue of execution results
type ResultsQueue struct {
	limit  int
	closed bool

	resultCh chan *TxTask
	iter     *ResultsQueueIter
	//tick
	ticker *time.Ticker

	sync.Mutex
	results *TxTaskQueue
}

func NewResultsQueue(newTasksLimit, queueLimit int) *ResultsQueue {
	r := &ResultsQueue{
		results:  &TxTaskQueue{},
		limit:    queueLimit,
		resultCh: make(chan *TxTask, newTasksLimit),
		ticker:   time.NewTicker(2 * time.Second),
	}
	heap.Init(r.results)
	r.iter = &ResultsQueueIter{q: r, results: r.results}
	return r
}

// Add result of execution. May block when internal channel is full
func (q *ResultsQueue) Add(ctx context.Context, task *TxTask) error {
	select {
	case q.resultCh <- task: // Needs to have outside of the lock
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
func (q *ResultsQueue) drainNoBlock(task *TxTask) (resultsQueueLen int) {
	q.Lock()
	defer q.Unlock()
	if task != nil {
		heap.Push(q.results, task)
	}
	resultsQueueLen = q.results.Len()

	for {
		select {
		case txTask, ok := <-q.resultCh:
			if !ok {
				return
			}
			if txTask != nil {
				heap.Push(q.results, txTask)
				resultsQueueLen = q.results.Len()
			}
		default: // we are inside mutex section, can't block here
			return resultsQueueLen
		}
	}
}

func (q *ResultsQueue) Iter() *ResultsQueueIter {
	q.Lock()
	return q.iter
}

type ResultsQueueIter struct {
	q       *ResultsQueue
	results *TxTaskQueue //pointer to `q.results` - just to reduce amount of dereferences
}

func (q *ResultsQueueIter) Close() {
	q.q.Unlock()
}
func (q *ResultsQueueIter) HasNext(outputTxNum uint64) bool {
	return len(*q.results) > 0 && (*q.results)[0].TxNum == outputTxNum
}
func (q *ResultsQueueIter) PopNext() *TxTask {
	return heap.Pop(q.results).(*TxTask)
}

func (q *ResultsQueue) Drain(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case txTask, ok := <-q.resultCh:
		if !ok {
			return nil
		}
		q.drainNoBlock(txTask)
	case <-q.ticker.C:
		// Corner case: workers processed all new tasks (no more q.resultCh events) when we are inside Drain() func
		// it means - naive-wait for new q.resultCh events will not work here (will cause dead-lock)
		//
		// "Drain everything but don't block" - solves the prbolem, but shows poor performance
		if q.Len() > 0 {
			return nil
		}
		return q.Drain(ctx)
	}
	return nil
}

func (q *ResultsQueue) DrainNonBlocking() { q.drainNoBlock(nil) }

func (q *ResultsQueue) DropResults(f func(t *TxTask)) {
	q.Lock()
	defer q.Unlock()
Loop:
	for {
		select {
		case txTask, ok := <-q.resultCh:
			if !ok {
				break Loop
			}
			f(txTask)
		default:
			break Loop
		}
	}

	// Drain results queue as well
	for q.results.Len() > 0 {
		f(heap.Pop(q.results).(*TxTask))
	}
}

func (q *ResultsQueue) Close() {
	if q.closed {
		return
	}
	q.closed = true
	close(q.resultCh)
	q.ticker.Stop()
}
func (q *ResultsQueue) ResultChLen() int { return len(q.resultCh) }
func (q *ResultsQueue) ResultChCap() int { return cap(q.resultCh) }
func (q *ResultsQueue) Limit() int       { return q.limit }
func (q *ResultsQueue) Len() (l int) {
	q.Lock()
	l = q.results.Len()
	q.Unlock()
	return l
}
func (q *ResultsQueue) FirstTxNumLocked() uint64 { return (*q.results)[0].TxNum }
func (q *ResultsQueue) LenLocked() (l int)       { return q.results.Len() }
func (q *ResultsQueue) HasLocked() bool          { return len(*q.results) > 0 }
func (q *ResultsQueue) PushLocked(t *TxTask)     { heap.Push(q.results, t) }
func (q *ResultsQueue) Push(t *TxTask) {
	q.Lock()
	heap.Push(q.results, t)
	q.Unlock()
}
func (q *ResultsQueue) PopLocked() (t *TxTask) {
	return heap.Pop(q.results).(*TxTask)
}
func (q *ResultsQueue) Dbg() (t *TxTask) {
	if len(*q.results) > 0 {
		return (*q.results)[0]
	}
	return nil
}
