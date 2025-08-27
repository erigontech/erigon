// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type AAValidationResult struct {
	PaymasterContext []byte
	GasUsed          uint64
}

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
	Coinbase        common.Address
	Withdrawals     types.Withdrawals
	BlockHash       common.Hash
	sender          *common.Address
	SkipAnalysis    bool
	TxIndex         int // -1 for block initialisation
	Final           bool
	Failed          bool
	Tx              types.Transaction
	GetHashFn       func(n uint64) (common.Hash, error)
	TxAsMessage     *types.Message
	EvmBlockContext evmtypes.BlockContext

	HistoryExecution bool // use history reader for that txn instead of state reader

	BalanceIncreaseSet map[common.Address]uint256.Int
	ReadLists          map[string]*state.KvList
	WriteLists         map[string]*state.KvList
	AccountPrevs       map[string][]byte
	AccountDels        map[string]*accounts.Account
	StoragePrevs       map[string][]byte
	CodePrevs          map[string]uint64
	Error              error
	Logs               []*types.Log
	TraceFroms         map[common.Address]struct{}
	TraceTos           map[common.Address]struct{}

	GasUsed uint64

	// BlockReceipts is used only by Gnosis:
	//  - it does store `proof, err := rlp.EncodeToBytes(ValidatorSetProof{Header: header, Receipts: r})`
	//  - and later read it by filter: len(l.Topics) == 2 && l.Address == s.contractAddress && l.Topics[0] == EVENT_NAME_HASH && l.Topics[1] == header.ParentHash
	// Need investigate if we can pass here - only limited amount of receipts
	// And remove this field if possible - because it will make problems for parallel-execution
	BlockReceipts types.Receipts

	Config *chain.Config

	AAValidationBatchSize uint64 // number of consecutive RIP-7560 transactions, should be 0 for single transactions and transactions that are not first in the transaction order
	InBatch               bool   // set to true for consecutive RIP-7560 transactions after the first one (first one is false)
	ValidationResults     []AAValidationResult
}
type GenericTracer interface {
	TracingHooks() *tracing.Hooks
	SetTransaction(tx types.Transaction)
	Found() bool
}

func (t *TxTask) Sender() *common.Address {
	//consumer.NewTracer().TracingHooks()
	if t.sender != nil {
		return t.sender
	}
	if sender, ok := t.Tx.GetSender(); ok {
		t.sender = &sender
		return t.sender
	}
	signer := *types.MakeSigner(t.Config, t.BlockNum, t.Header.Time)
	sender, err := signer.Sender(t.Tx)
	if err != nil {
		panic(err)
	}
	t.sender = &sender
	log.Warn("[Execution] expensive lazy sender recovery", "blockNum", t.BlockNum, "txIdx", t.TxIndex)
	return t.sender
}

func (t *TxTask) CreateReceipt(tx kv.TemporalTx) {
	if t.TxIndex < 0 {
		return
	}
	if t.Final {
		t.BlockReceipts.AssertLogIndex(t.BlockNum)
		return
	}

	var cumulativeGasUsed uint64
	var firstLogIndex uint32
	if t.TxIndex > 0 {
		prevR := t.BlockReceipts[t.TxIndex-1]
		if prevR != nil {
			cumulativeGasUsed = prevR.CumulativeGasUsed
			firstLogIndex = prevR.FirstLogIndexWithinBlock + uint32(len(prevR.Logs))
		} else {
			var err error
			var logIndexAfterTx uint32
			cumulativeGasUsed, _, logIndexAfterTx, err = rawtemporaldb.ReceiptAsOf(tx, t.TxNum)
			if err != nil {
				panic(err)
			}
			firstLogIndex = logIndexAfterTx
		}
	}

	cumulativeGasUsed += t.GasUsed
	if t.GasUsed == 0 {
		msg := fmt.Sprintf("assert: no gas used, bn=%d, tn=%d, ti=%d", t.BlockNum, t.TxNum, t.TxIndex)
		panic(msg)
	}
	r := t.createReceipt(cumulativeGasUsed, firstLogIndex)
	t.BlockReceipts[t.TxIndex] = r
}

func (t *TxTask) createReceipt(cumulativeGasUsed uint64, firstLogIndex uint32) *types.Receipt {
	logIndex := firstLogIndex
	for i := range t.Logs {
		t.Logs[i].Index = uint(logIndex)
		logIndex++
	}

	receipt := &types.Receipt{
		BlockNumber:       t.Header.Number,
		BlockHash:         t.BlockHash,
		TransactionIndex:  uint(t.TxIndex),
		Type:              t.Tx.Type(),
		GasUsed:           t.GasUsed,
		CumulativeGasUsed: cumulativeGasUsed,
		TxHash:            t.Tx.Hash(),
		Logs:              t.Logs,

		FirstLogIndexWithinBlock: firstLogIndex,
	}
	blockNum := t.Header.Number.Uint64()
	for _, l := range receipt.Logs {
		l.TxHash = receipt.TxHash
		l.BlockNumber = blockNum
		l.BlockHash = receipt.BlockHash
	}
	if t.Failed {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	// if the transaction created a contract, store the creation address in the receipt.
	if t.TxAsMessage != nil && t.TxAsMessage.To() == nil {
		receipt.ContractAddress = types.CreateAddress(*t.Sender(), t.Tx.GetNonce())
	}

	return receipt
}
func (t *TxTask) Reset() *TxTask {
	t.BalanceIncreaseSet = nil
	returnReadList(t.ReadLists)
	t.ReadLists = nil
	returnWriteList(t.WriteLists)
	t.WriteLists = nil
	t.Logs = nil
	t.TraceFroms = nil
	t.TraceTos = nil
	t.Error = nil
	t.Failed = false
	return t
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
	case <-ctx.Done():
		return
	case q.newTasks <- t:
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
	heapLimit int
	closed    bool

	resultCh chan *TxTask
	iter     *ResultsQueueIter
	//tick
	ticker *time.Ticker

	m       sync.Mutex
	results *TxTaskQueue
}

func NewResultsQueue(resultChannelLimit, heapLimit int) *ResultsQueue {
	r := &ResultsQueue{
		results:   &TxTaskQueue{},
		heapLimit: heapLimit,
		resultCh:  make(chan *TxTask, resultChannelLimit),
		ticker:    time.NewTicker(2 * time.Second),
	}
	heap.Init(r.results)
	r.iter = &ResultsQueueIter{q: r, results: r.results}
	return r
}

// Add result of execution. May block when internal channel is full
func (q *ResultsQueue) Add(ctx context.Context, task *TxTask) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.resultCh <- task: // Needs to have outside of the lock
	}
	return nil
}
func (q *ResultsQueue) drainNoBlock(ctx context.Context, task *TxTask) (err error) {
	q.m.Lock()
	defer q.m.Unlock()
	if task != nil {
		heap.Push(q.results, task)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txTask, ok := <-q.resultCh:
			if !ok {
				//log.Warn("[dbg] closed1")
				return nil
			}
			if txTask == nil {
				continue
			}
			heap.Push(q.results, txTask)
			if q.results.Len() > q.heapLimit {
				return nil
			}
		default: // we are inside mutex section, can't block here
			return nil
		}
	}
}

func (q *ResultsQueue) Iter() *ResultsQueueIter {
	q.m.Lock()
	return q.iter
}

type ResultsQueueIter struct {
	q       *ResultsQueue
	results *TxTaskQueue //pointer to `q.results` - just to reduce amount of dereferences
}

func (q *ResultsQueueIter) Close() {
	q.q.m.Unlock()
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
		if err := q.drainNoBlock(ctx, txTask); err != nil {
			return err
		}
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

// DrainNonBlocking - does drain batch of results to heap. Immediately stops at `q.limit` or if nothing to drain
func (q *ResultsQueue) DrainNonBlocking(ctx context.Context) (err error) {
	return q.drainNoBlock(ctx, nil)
}

func (q *ResultsQueue) DropResults(ctx context.Context, f func(t *TxTask)) {
	q.m.Lock()
	defer q.m.Unlock()
Loop:
	for {
		select {
		case <-ctx.Done():
			return
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
	q.m.Lock()
	defer q.m.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.resultCh)
	q.ticker.Stop()
}
func (q *ResultsQueue) ResultChLen() int { return len(q.resultCh) }
func (q *ResultsQueue) ResultChCap() int { return cap(q.resultCh) }
func (q *ResultsQueue) Limit() int       { return q.heapLimit }
func (q *ResultsQueue) Len() (l int) {
	q.m.Lock()
	l = q.results.Len()
	q.m.Unlock()
	return l
}
func (q *ResultsQueue) Capacity() int            { return q.heapLimit }
func (q *ResultsQueue) ChanLen() int             { return len(q.resultCh) }
func (q *ResultsQueue) ChanCapacity() int        { return cap(q.resultCh) }
func (q *ResultsQueue) FirstTxNumLocked() uint64 { return (*q.results)[0].TxNum }
func (q *ResultsQueue) LenLocked() (l int)       { return q.results.Len() }
func (q *ResultsQueue) HasLocked() bool          { return len(*q.results) > 0 }
func (q *ResultsQueue) PushLocked(t *TxTask)     { heap.Push(q.results, t) }
func (q *ResultsQueue) Push(t *TxTask) {
	q.m.Lock()
	heap.Push(q.results, t)
	q.m.Unlock()
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
