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
	"math/big"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/vm"
	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
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
		rs *state.StateV3Buffered,
		ibs *state.IntraBlockState,
		stateWriter state.StateWriter,
		stateReader state.ResettableStateReader,
		chainConfig *chain.Config,
		chainReader consensus.ChainReader,
		dirs datadir.Dirs,
		calcFees bool) *Result

	Version() state.Version
	VersionMap() *state.VersionMap
	VersionedReads(ibs *state.IntraBlockState) *btree.BTreeG[*state.VersionedRead]
	VersionedWrites(ibs *state.IntraBlockState) state.VersionedWrites
	Reset(ibs *state.IntraBlockState)

	Tx() types.Transaction
	TxType() uint8
	TxHash() libcommon.Hash
	TxSender() (*libcommon.Address, error)
	TxMessage() (*types.Message, error)

	BlockNumber() uint64
	BlockHash() libcommon.Hash
	BlockTime() uint64
	BlockRoot() libcommon.Hash

	IsBlockEnd() bool
	IsHistoric() bool

	// elements in dependencies -> transaction indexes on which transaction i is dependent on
	Dependencies() []int
}

// Result is the ouput of the task execute process
type Result struct {
	Task
	ExecutionResult *evmtypes.ExecutionResult
	Err             error
	Coinbase        libcommon.Address
	TxIn            *btree.BTreeG[*state.VersionedRead]
	TxOut           state.VersionedWrites

	Receipt *types.Receipt
	Logs    []*types.Log

	TraceFroms map[libcommon.Address]struct{}
	TraceTos   map[libcommon.Address]struct{}

	ShouldRerunWithoutFeeDelay bool
}

func (r *Result) CreateReceipt(prev *types.Receipt) (*types.Receipt, error) {
	txIndex := r.Task.Version().TxIndex

	if txIndex < 0 || r.IsBlockEnd() {
		return nil, nil
	}

	var cumulativeGasUsed uint64
	var firstLogIndex uint32
	if txIndex > 0 {
		if prev != nil {
			cumulativeGasUsed = prev.CumulativeGasUsed
			firstLogIndex = prev.FirstLogIndexWithinBlock + uint32(len(prev.Logs))
		}
	}

	cumulativeGasUsed += r.ExecutionResult.UsedGas

	r.Receipt = r.createReceipt(txIndex, cumulativeGasUsed)
	r.Receipt.FirstLogIndexWithinBlock = firstLogIndex
	return r.Receipt, nil
}

func (r *Result) createReceipt(txIndex int, cumulativeGasUsed uint64) *types.Receipt {
	blockNum := r.Version().BlockNum
	receipt := &types.Receipt{
		BlockNumber:       big.NewInt(int64(blockNum)),
		BlockHash:         r.BlockHash(),
		TransactionIndex:  uint(txIndex),
		Type:              r.TxType(),
		GasUsed:           r.ExecutionResult.UsedGas,
		CumulativeGasUsed: cumulativeGasUsed,
		TxHash:            r.TxHash(),
		Logs:              r.Logs,
	}

	for _, l := range receipt.Logs {
		l.TxHash = receipt.TxHash
		l.BlockNumber = blockNum
		l.BlockHash = receipt.BlockHash
	}
	if r.ExecutionResult.Failed() {
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
	Header             *types.Header
	Txs                types.Transactions
	Uncles             []*types.Header
	Withdrawals        types.Withdrawals
	SkipAnalysis       bool
	EvmBlockContext    evmtypes.BlockContext
	HistoryExecution   bool // use history reader for that txn instead of state reader
	BalanceIncreaseSet map[libcommon.Address]uint256.Int

	Incarnation int

	Config *chain.Config
	Logger log.Logger
	Trace  bool

	sender  *libcommon.Address
	message *types.Message
	signer  *types.Signer
}

func (t *TxTask) Tx() types.Transaction {
	if t.TxIndex < 0 || t.TxIndex >= len(t.Txs) {
		return nil
	}
	return t.Txs[t.TxIndex]
}

func (t *TxTask) TxType() uint8 {
	return t.Tx().Type()
}

func (t *TxTask) TxHash() libcommon.Hash {
	if t.TxIndex < 0 || t.TxIndex >= len(t.Txs) {
		return libcommon.Hash{}
	}
	return t.Tx().Hash()
}

func (t *TxTask) TxSender() (*libcommon.Address, error) {
	if t.sender != nil {
		return t.sender, nil
	}
	if t.TxIndex < 0 || t.TxIndex >= len(t.Txs) {
		return nil, nil
	}
	if sender, ok := t.Tx().GetSender(); ok {
		t.sender = &sender
		return t.sender, nil
	}
	if t.signer == nil {
		t.signer = types.MakeSigner(t.Config, t.BlockNumber(), t.Header.Time)
	}
	sender, err := t.signer.Sender(t.Tx())
	if err != nil {
		return nil, err
	}
	t.sender = &sender
	log.Warn("[Execution] expensive lazy sender recovery", "blockNum", t.BlockNumber(), "txIdx", t.TxIndex)
	return t.sender, nil
}

func (t *TxTask) TxMessage() (*types.Message, error) {
	if t.message == nil {
		var err error
		if t.signer == nil {
			t.signer = types.MakeSigner(t.Config, t.BlockNumber(), t.Header.Time)
		}
		message, err := t.Tx().AsMessage(*t.signer, t.Header.BaseFee, t.Config.Rules(t.BlockNumber(), t.BlockTime()))

		if err != nil {
			return nil, err
		}

		t.message = &message
	}

	return t.message, nil
}

func (t *TxTask) BlockNumber() uint64 {
	if t.Header == nil {
		return 0
	}
	return t.Header.Number.Uint64()
}

func (t *TxTask) BlockHash() libcommon.Hash {
	if t.Header == nil {
		return libcommon.Hash{}
	}
	return t.Header.Hash()
}

func (t *TxTask) BlockRoot() libcommon.Hash {
	if t.Header == nil {
		return libcommon.Hash{}
	}
	return t.Header.Root
}

func (t *TxTask) BlockTime() uint64 {
	if t.Header == nil {
		return 0
	}
	return t.Header.Time
}

func (t *TxTask) Version() state.Version {
	return state.Version{BlockNum: t.BlockNumber(), TxNum: t.TxNum, TxIndex: t.TxIndex}
}

func (t *TxTask) Dependencies() []int {
	return nil
}

func (t *TxTask) VersionMap() *state.VersionMap {
	return nil
}

func (t *TxTask) VersionedReads(ibs *state.IntraBlockState) *btree.BTreeG[*state.VersionedRead] {
	return ibs.VersionedReads()
}

func (t *TxTask) VersionedWrites(ibs *state.IntraBlockState) state.VersionedWrites {
	return ibs.VersionedWrites(false)
}

func (t *TxTask) IsBlockEnd() bool {
	return t.TxIndex == len(t.Txs)
}

func (t *TxTask) IsHistoric() bool {
	return t.HistoryExecution
}

func (t *TxTask) Reset(ibs *state.IntraBlockState) {
	t.BalanceIncreaseSet = nil
	ibs.Reset()
}

func (txTask *TxTask) Execute(evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3Buffered,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) *Result {
	var result Result

	ibs.SetTrace(txTask.Trace)

	rules := chainConfig.Rules(txTask.BlockNumber(), txTask.BlockTime())
	var err error
	header := txTask.Header
	//fmt.Printf("txNum=%d blockNum=%d history=%t\n", txTask.TxNum, txTask.BlockNum, txTask.HistoryExecution)

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNumber() == 0 {

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
		result.ExecutionResult = &evmtypes.ExecutionResult{}
	case txTask.IsBlockEnd():
		if txTask.BlockNumber() == 0 {
			break
		}

		//incorrect unwind to block 2
		//if err := ibs.CommitBlock(rules, rw.stateWriter); err != nil {
		//	txTask.Error = err
		//}
		result.ExecutionResult = &evmtypes.ExecutionResult{}
		result.TraceTos = map[libcommon.Address]struct{}{}
		result.TraceTos[txTask.Header.Coinbase] = struct{}{}
		for _, uncle := range txTask.Uncles {
			result.TraceTos[uncle.Coinbase] = struct{}{}
		}
	default:
		gasPool.Reset(txTask.Tx().GetGas(), chainConfig.GetMaxBlobGasPerBlock(txTask.Header.Time))
		vmCfg.SkipAnalysis = txTask.SkipAnalysis
		msg, err := txTask.TxMessage()

		if err != nil {
			result.Err = err
			return &result
		}

		if msg.FeeCap().IsZero() && engine != nil {
			// Only zero-gas transactions may be service ones
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, chainConfig, ibs, header, engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, vmCfg, rules)

		result.Coinbase = evm.Context.Coinbase

		// MA applytx
		result.ExecutionResult, result.Err = func() (*evmtypes.ExecutionResult, error) {
			message, err := txTask.TxMessage()

			if err != nil {
				return nil, ErrExecAbortError{Dependency: ibs.DepTxIndex(), OriginError: err}
			}

			// Apply the transaction to the current state (included in the env).
			if !calcFees {
				applyRes, err := core.ApplyMessageNoFeeBurnOrTip(evm, message, gasPool, true, false)

				if applyRes == nil || err != nil {
					return nil, ErrExecAbortError{Dependency: ibs.DepTxIndex(), OriginError: err}
				}

				reads := ibs.VersionedReads()
				coinbaseKey := state.SubpathKey(&evm.Context.Coinbase, state.BalancePath)

				if _, ok := reads.Get(&state.VersionedRead{Path: coinbaseKey}); ok {
					log.Debug("Coinbase is in versiopnedMap", "address", evm.Context.Coinbase)
					result.ShouldRerunWithoutFeeDelay = true
				}

				burnKey := state.SubpathKey(&applyRes.BurntContractAddress, state.BalancePath)
				if _, ok := reads.Get(&state.VersionedRead{Path: burnKey}); ok {
					log.Debug("BurntContractAddress is in versiopnedMap", "address", applyRes.BurntContractAddress)
					result.ShouldRerunWithoutFeeDelay = true
				}

				return applyRes, err
			}

			return core.ApplyMessage(evm, message, gasPool, true, false)
		}()

		if result.Err == nil {
			// TODO these can be removed - use result instead
			// Update the state with pending changes
			ibs.SoftFinalise()
			//txTask.Error = ibs.FinalizeTx(rules, noop)
			result.Logs = ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), txTask.BlockNumber(), txTask.BlockHash())
		}

	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if result.Err == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		for addr, bal := range txTask.BalanceIncreaseSet {
			fmt.Printf("BalanceIncreaseSet [%x]=>[%d]\n", addr, &bal)
		}

		if err = ibs.MakeWriteSet(rules, stateWriter); err != nil {
			panic(err)
		}

		result.TxIn = txTask.VersionedReads(ibs)
		result.TxOut = txTask.VersionedWrites(ibs)
	}

	return &result
}

// TxTaskQueue non-thread-safe priority-queue
type TxTaskQueue []Task

func (h TxTaskQueue) Len() int {
	return len(h)
}
func (h TxTaskQueue) Less(i, j int) bool {
	return h[i].Version().TxNum < h[j].Version().TxNum
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
		out = append(out, t.Version().TxNum)
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
	fmt.Println("retry", t.Version())
	defer fmt.Println("done retry")
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
	fmt.Println("popWait")
	defer func() {
		var version interface{}
		if task != nil {
			version = task.Version()
		}
		fmt.Println("done popWait", version)
	}()

	for {
		select {
		case inTask, ok := <-q.newTasks:
			fmt.Println("popWait", inTask)
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
	//tick
	ticker *time.Ticker

	sync.RWMutex
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
	return r
}

// Add result of execution. May block when internal channel is full
func (q *ResultsQueue) Add(ctx context.Context, task *Result) error {

	q.RLock()
	resultCh := q.resultCh
	q.RUnlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if resultCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultCh <- task: // Needs to have outside of the lock
		}
	}

	return nil
}

func (q *ResultsQueue) Drain(ctx context.Context, task *Result) (bool, error) {
	q.Lock()
	defer q.Unlock()

	if q.resultCh == nil {
		return true, nil
	}

	if task != nil {
		heap.Push(q.results, task)
	}

	for {
		select {
		case <-ctx.Done():
			return q.resultCh == nil, ctx.Err()
		case txTask, ok := <-q.resultCh:
			if !ok {
				return true, nil
			}
			if txTask == nil {
				continue
			}
			heap.Push(q.results, txTask)
			if q.results.Len() > q.limit {
				return q.resultCh == nil, nil
			}
		default: // we are inside mutex section, can't block here
			return q.resultCh == nil, nil
		}
	}
}

func (q *ResultsQueue) Iter() *ResultsQueueIter {
	return &ResultsQueueIter{q: q}
}

type ResultsQueueIter struct {
	q *ResultsQueue
}

func (q *ResultsQueueIter) HasNext() bool {
	q.q.RLock()
	defer q.q.RUnlock()
	return len(*q.q.results) > 0
}

func (q *ResultsQueueIter) Has(outputTxNum uint64) bool {
	q.q.RLock()
	defer q.q.RUnlock()
	return len(*q.q.results) > 0 && (*q.q.results)[0].Version().TxNum == outputTxNum
}

func (q *ResultsQueueIter) PopNext() *Result {
	q.q.Lock()
	defer q.q.Unlock()
	return heap.Pop(q.q.results).(*Result)
}

func (q *ResultsQueue) ResultCh() chan *Result {
	return q.resultCh
}

func (q *ResultsQueue) DrainNonBlocking(ctx context.Context) (bool, error) { return q.Drain(ctx, nil) }

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

	q.Lock()
	close(q.resultCh)
	q.resultCh = nil
	q.Unlock()

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
func (q *ResultsQueue) FirstTxNumLocked() uint64 { return (*q.results)[0].Version().TxNum }
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
