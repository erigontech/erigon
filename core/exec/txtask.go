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

package exec

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/vm"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

type Task interface {
	Execute(evm *vm.EVM,
		vmCfg vm.Config,
		engine consensus.Engine,
		genesis *types.Genesis,
		gasPool *core.GasPool,
		rs *state.StateV3,
		ibs *state.IntraBlockState,
		applyMessage ApplyMessage,
		stateWriter *state.StateWriterV3,
		stateReader state.ResettableStateReader,
		chainConfig *chain.Config,
		chainReader consensus.ChainReader,
		dirs datadir.Dirs,
		isMining bool) *Result
	CreateReceipt(tx kv.Tx)

	Version() state.Version
	TxHash() libcommon.Hash

	txNum() uint64

	IsBlockEnd() bool
	IsHistoric() bool
}

type Result struct {
	Task
	Err      error
	TxIn     state.VersionedReads
	TxOut    state.VersionedWrites
	TxAllOut state.VersionedWrites

	TraceFroms map[libcommon.Address]struct{}
	TraceTos   map[libcommon.Address]struct{}
}

type ErrExecAbortError struct {
	Dependency  int
	OriginError error
}

func (e ErrExecAbortError) Error() string {
	if e.Dependency >= 0 {
		return fmt.Sprintf("Execution aborted due to dependency %d", e.Dependency)
	} else {
		return "Execution aborted"
	}
}

type ApplyMessage func(evm *vm.EVM, msg core.Message, gp *core.GasPool, refunds bool, gasBailout bool) (*evmtypes.ExecutionResult, error)

type Tx struct {
}

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type TxTask struct {
	TxNum              uint64
	TxIndex            int // -1 for block initialisation
	BlockNum           uint64
	Rules              *chain.Rules
	Header             *types.Header
	Tx                 types.Transaction
	Txs                types.Transactions
	Uncles             []*types.Header
	Coinbase           libcommon.Address
	Withdrawals        types.Withdrawals
	BlockHash          libcommon.Hash
	Sender             *libcommon.Address
	SkipAnalysis       bool
	PruneNonEssentials bool
	Failed             bool
	GetHashFn          func(n uint64) libcommon.Hash
	TxAsMessage        types.Message
	EvmBlockContext    evmtypes.BlockContext
	HistoryExecution   bool // use history reader for that txn instead of state reader

	BalanceIncreaseSet map[libcommon.Address]uint256.Int
	ReadLists          state.ReadLists
	WriteLists         state.WriteLists
	AccountPrevs       map[string][]byte
	AccountDels        map[string]*accounts.Account
	StoragePrevs       map[string][]byte
	CodePrevs          map[string]uint64
	Logs               []*types.Log
	// BlockReceipts is used only by Gnosis:
	//  - it does store `proof, err := rlp.EncodeToBytes(ValidatorSetProof{Header: header, Receipts: r})`
	//  - and later read it by filter: len(l.Topics) == 2 && l.Address == s.contractAddress && l.Topics[0] == EVENT_NAME_HASH && l.Topics[1] == header.ParentHash
	// Need investigate if we can pass here - only limited amount of receipts
	// And remove this field if possible - because it will make problems for parallel-execution
	BlockReceipts types.Receipts

	UsedGas uint64

	Config *chain.Config
	Logger log.Logger
}

func (t *TxTask) txNum() uint64 {
	return t.TxNum
}

func (t *TxTask) TxHash() libcommon.Hash {
	return t.Tx.Hash()
}

func (t *TxTask) Version() state.Version {
	return state.Version{TxIndex: t.TxIndex}
}

func (t *TxTask) IsBlockEnd() bool {
	return t.TxIndex == len(t.Txs)
}

func (t *TxTask) IsHistoric() bool {
	return t.HistoryExecution
}

func (t *TxTask) CreateReceipt(tx kv.Tx) {
	if t.TxIndex < 0 || t.IsBlockEnd() {
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
			cumulativeGasUsed, _, firstLogIndex, err = rawtemporaldb.ReceiptAsOf(tx.(kv.TemporalTx), t.TxNum)
			if err != nil {
				panic(err)
			}
		}
	}

	cumulativeGasUsed += t.UsedGas

	r := t.createReceipt(cumulativeGasUsed)
	r.FirstLogIndexWithinBlock = firstLogIndex
	t.BlockReceipts[t.TxIndex] = r
}

func (t *TxTask) createReceipt(cumulativeGasUsed uint64) *types.Receipt {
	receipt := &types.Receipt{
		BlockNumber:       t.Header.Number,
		BlockHash:         t.BlockHash,
		TransactionIndex:  uint(t.TxIndex),
		Type:              t.Tx.Type(),
		GasUsed:           t.UsedGas,
		CumulativeGasUsed: cumulativeGasUsed,
		TxHash:            t.TxHash(),
		Logs:              t.Logs,
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
	//if msg.To() == nil {
	//	receipt.ContractAddress = crypto.CreateAddress(evm.Origin, tx.GetNonce())
	//}
	return receipt
}
func (t *TxTask) Reset() *TxTask {
	t.BalanceIncreaseSet = nil
	t.ReadLists.Return()
	t.ReadLists = nil
	t.WriteLists.Return()
	t.WriteLists = nil
	t.Logs = nil
	t.Failed = false
	return t
}

func (txTask *TxTask) Execute(evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3,
	ibs *state.IntraBlockState,
	applyMessage ApplyMessage,
	stateWriter *state.StateWriterV3,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	isMining bool) *Result {
	var result Result

	stateReader.SetTxNum(txTask.TxNum)
	rs.Domains().SetTxNum(txTask.TxNum)
	stateReader.ResetReadSet()
	stateWriter.ResetWriteSet()

	ibs.Reset()
	//ibs.SetTrace(true)

	rules := txTask.Rules
	var err error
	header := txTask.Header
	//fmt.Printf("txNum=%d blockNum=%d history=%t\n", txTask.TxNum, txTask.BlockNum, txTask.HistoryExecution)

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNum == 0 {

			//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
			_, ibs, err = core.GenesisToBlock(genesis, dirs, txTask.Logger)
			if err != nil {
				panic(err)
			}
			// For Genesis, rules should be empty, so that empty accounts can be included
			rules = &chain.Rules{}
			break
		}

		// Block initialisation
		//fmt.Printf("txNum=%d, blockNum=%d, initialisation of the block\n", txTask.TxNum, txTask.BlockNum)
		syscall := func(contract libcommon.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			return core.SysCallContract(contract, data, chainConfig, ibs, header, engine, constCall /* constCall */)
		}
		engine.Initialize(chainConfig, chainReader, header, ibs, syscall, txTask.Logger, nil)
		result.Err = ibs.FinalizeTx(rules, state.NewNoopWriter())
	case txTask.IsBlockEnd():
		if txTask.BlockNum == 0 {
			break
		}

		//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
		// End of block transaction in a block
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, chainConfig, ibs, header, engine, false /* constCall */)
		}

		if isMining {
			_, txTask.Txs, txTask.BlockReceipts, _, err = engine.FinalizeAndAssemble(chainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, chainReader, syscall, nil, txTask.Logger)
		} else {
			_, _, _, err = engine.Finalize(chainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, chainReader, syscall, txTask.Logger)
		}
		if err != nil {
			result.Err = err
		} else {
			//incorrect unwind to block 2
			//if err := ibs.CommitBlock(rules, rw.stateWriter); err != nil {
			//	txTask.Error = err
			//}
			result.TraceTos = map[libcommon.Address]struct{}{}
			result.TraceTos[txTask.Coinbase] = struct{}{}
			for _, uncle := range txTask.Uncles {
				result.TraceTos[uncle.Coinbase] = struct{}{}
			}
		}
	default:
		gasPool.Reset(txTask.Tx.GetGas(), chainConfig.GetMaxBlobGasPerBlock())
		vmCfg.SkipAnalysis = txTask.SkipAnalysis
		ibs.SetTxContext(txTask.TxIndex)
		msg := txTask.TxAsMessage
		if msg.FeeCap().IsZero() && engine != nil {
			// Only zero-gas transactions may be service ones
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, chainConfig, ibs, header, engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, vmCfg, rules)

		// MA applytx
		applyRes, err := applyMessage(evm, msg, gasPool, true /* refunds */, false /* gasBailout */)
		if err != nil {
			result.Err = err
		} else {
			txTask.Failed = applyRes.Failed()
			txTask.UsedGas = applyRes.UsedGas
			// Update the state with pending changes
			ibs.SoftFinalise()
			//txTask.Error = ibs.FinalizeTx(rules, noop)
			txTask.Logs = ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), txTask.BlockNum, txTask.BlockHash)
		}

	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if result.Err == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		//for addr, bal := range txTask.BalanceIncreaseSet {
		//	fmt.Printf("BalanceIncreaseSet [%x]=>[%d]\n", addr, &bal)
		//}
		if err = ibs.MakeWriteSet(rules, stateWriter); err != nil {
			panic(err)
		}
		txTask.ReadLists = stateReader.ReadSet()
		txTask.WriteLists = stateWriter.WriteSet()
		txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
	}

	return &result
}

// TxTaskQueue non-thread-safe priority-queue
type TxTaskQueue []Task

func (h TxTaskQueue) Len() int {
	return len(h)
}
func (h TxTaskQueue) Less(i, j int) bool {
	return h[i].txNum() < h[j].txNum()
}

func (h TxTaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TxTaskQueue) Push(a interface{}) {
	*h = append(*h, a.(Task))
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
	newTasks    chan Task
	retires     TxTaskQueue
	retiresLock sync.Mutex
	capacity    int
}

func NewQueueWithRetry(capacity int) *QueueWithRetry {
	return &QueueWithRetry{newTasks: make(chan Task, capacity), capacity: capacity}
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
		out = append(out, t.txNum())
	}
	q.retiresLock.Unlock()
	return out
}
func (q *QueueWithRetry) Len() (l int) { return q.RetriesLen() + len(q.newTasks) }

// Add "new task" (which was never executed yet). May block internal channel is full.
// Expecting already-ordered tasks.
func (q *QueueWithRetry) Add(ctx context.Context, t Task) {
	select {
	case <-ctx.Done():
		return
	case q.newTasks <- t:
	}
}

// ReTry returns failed (conflicted) task. It's non-blocking method.
// All failed tasks have higher priority than new one.
// No limit on amount of txs added by this method.
func (q *QueueWithRetry) ReTry(t Task) {
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
func (q *QueueWithRetry) Next(ctx context.Context) (Task, bool) {
	task, ok := q.popNoWait()
	if ok {
		return task, true
	}
	return q.popWait(ctx)
}

func (q *QueueWithRetry) popWait(ctx context.Context) (task Task, ok bool) {
	for {
		select {
		case inTask, ok := <-q.newTasks:
			if !ok {
				q.retiresLock.Lock()
				if q.retires.Len() > 0 {
					task = heap.Pop(&q.retires).(Task)
				}
				q.retiresLock.Unlock()
				return task, task != nil
			}

			q.retiresLock.Lock()
			if inTask != nil {
				heap.Push(&q.retires, inTask)
			}
			if q.retires.Len() > 0 {
				task = heap.Pop(&q.retires).(Task)
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
func (q *QueueWithRetry) popNoWait() (task Task, ok bool) {
	q.retiresLock.Lock()
	has := q.retires.Len() > 0
	if has { // means have conflicts to re-exec: it has higher priority than new tasks
		task = heap.Pop(&q.retires).(Task)
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

	resultCh chan *Result
	iter     *ResultsQueueIter
	//tick
	ticker *time.Ticker

	sync.Mutex
	results *TxTaskQueue
}

func NewResultsQueue(resultChannelLimit, heapLimit int) *ResultsQueue {
	r := &ResultsQueue{
		results:  &TxTaskQueue{},
		limit:    heapLimit,
		resultCh: make(chan *Result, resultChannelLimit),
		ticker:   time.NewTicker(2 * time.Second),
	}
	heap.Init(r.results)
	r.iter = &ResultsQueueIter{q: r}
	return r
}

// Add result of execution. May block when internal channel is full
func (q *ResultsQueue) Add(ctx context.Context, task *Result) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.resultCh <- task: // Needs to have outside of the lock
	}
	return nil
}
func (q *ResultsQueue) drainNoBlock(ctx context.Context, task *Result) error {
	q.Lock()
	defer q.Unlock()
	if task != nil {
		heap.Push(q.results, task)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txTask, ok := <-q.resultCh:
			if !ok {
				return nil
			}
			if txTask == nil {
				continue
			}
			heap.Push(q.results, txTask)
			if q.results.Len() > q.limit {
				return nil
			}
		default: // we are inside mutex section, can't block here
			return nil
		}
	}
}

func (q *ResultsQueue) Iter() *ResultsQueueIter {
	q.Lock()
	return q.iter
}

type ResultsQueueIter struct {
	q *ResultsQueue
}

func (q *ResultsQueueIter) Close() {
	q.q.Unlock()
}
func (q *ResultsQueueIter) HasNext(outputTxNum uint64) bool {
	return len(*q.q.results) > 0 && (*q.q.results)[0].txNum() == outputTxNum
}
func (q *ResultsQueueIter) PopNext() *Result {
	return heap.Pop(q.q.results).(*Result)
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

func (q *ResultsQueue) DrainNonBlocking(ctx context.Context) error { return q.drainNoBlock(ctx, nil) }

func (q *ResultsQueue) DropResults(ctx context.Context, f func(t *Result)) {
	q.Lock()
	defer q.Unlock()
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
		f(heap.Pop(q.results).(*Result))
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
func (q *ResultsQueue) FirstTxNumLocked() uint64 { return (*q.results)[0].txNum() }
func (q *ResultsQueue) LenLocked() (l int)       { return q.results.Len() }
func (q *ResultsQueue) HasLocked() bool          { return len(*q.results) > 0 }
func (q *ResultsQueue) PushLocked(t *Result)     { heap.Push(q.results, t) }
func (q *ResultsQueue) Push(t *Result) {
	q.Lock()
	heap.Push(q.results, t)
	q.Unlock()
}
func (q *ResultsQueue) PopLocked() (t *Result) {
	return heap.Pop(q.results).(*Result)
}
func (q *ResultsQueue) Dbg() (t *Result) {
	if len(*q.results) > 0 {
		return (*q.results)[0].(*Result)
	}
	return nil
}
