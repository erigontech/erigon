package e3types

import (
	"container/heap"
	"context"
	"sync"

	"github.com/holiman/uint256"
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
	ReadLists          map[string]*KvList
	WriteLists         map[string]*KvList
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

type TxTaskQueue []*TxTask

func (h TxTaskQueue) Len() int {
	return len(h)
}
func LenLocked(h *TxTaskQueue, lock *sync.Mutex) (l int) {
	lock.Lock()
	l = h.Len()
	lock.Unlock()
	return l
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

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys []string
	Vals [][]byte
}

func (l KvList) Len() int {
	return len(l.Keys)
}

func (l KvList) Less(i, j int) bool {
	return l.Keys[i] < l.Keys[j]
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
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
