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
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

type Task interface {
	Execute(evm *vm.EVM,
		engine consensus.Engine,
		genesis *types.Genesis,
		ibs *state.IntraBlockState,
		stateWriter state.StateWriter,
		chainConfig *chain.Config,
		chainReader consensus.ChainReader,
		dirs datadir.Dirs,
		calcFees bool) *TxResult

	Version() state.Version
	VersionMap() *state.VersionMap
	VersionedReads(ibs *state.IntraBlockState) state.ReadSet
	VersionedWrites(ibs *state.IntraBlockState) state.VersionedWrites
	Reset(evm *vm.EVM, ibs *state.IntraBlockState, callTracer *calltracer.CallTracer) error
	ResetGasPool(*core.GasPool)

	Tx() types.Transaction
	TxType() uint8
	TxHash() common.Hash
	TxSender() (*common.Address, error)
	TxMessage() (*types.Message, error)

	BlockNumber() uint64
	BlockHash() common.Hash
	BlockTime() uint64
	BlockGasLimit() uint64
	BlockRoot() common.Hash

	GasPool() *core.GasPool

	IsBlockEnd() bool
	IsHistoric() bool

	TracingHooks() *tracing.Hooks
	// elements in dependencies -> transaction indexes on which transaction i is dependent on
	Dependencies() []int

	compare(other Task) int
	isNil() bool
}

type AAValidationResult struct {
	PaymasterContext []byte
	GasUsed          uint64
}

// TxResult is the ouput of the task execute process
type TxResult struct {
	Task
	ExecutionResult   *evmtypes.ExecutionResult
	ValidationResults []AAValidationResult
	Err               error
	Coinbase          common.Address
	TxIn              state.ReadSet
	TxOut             state.VersionedWrites

	Receipt *types.Receipt
	Logs    []*types.Log

	TraceFroms map[common.Address]struct{}
	TraceTos   map[common.Address]struct{}

	ShouldRerunWithoutFeeDelay bool
}

func (r *TxResult) compare(other *TxResult) int {
	return r.Task.compare(other.Task)
}

func (r *TxResult) isNil() bool {
	return r == nil
}

func (r *TxResult) CreateReceipt(prev *types.Receipt) (*types.Receipt, error) {
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

	cumulativeGasUsed += r.ExecutionResult.GasUsed

	var err error
	r.Receipt, err = r.createReceipt(txIndex, cumulativeGasUsed, firstLogIndex)
	return r.Receipt, err
}

func (r *TxResult) createReceipt(txIndex int, cumulativeGasUsed uint64, firstLogIndex uint32) (*types.Receipt, error) {
	logIndex := firstLogIndex
	for i := range r.Logs {
		r.Logs[i].Index = uint(logIndex)
		logIndex++
	}

	blockNum := r.Version().BlockNum
	receipt := &types.Receipt{
		BlockNumber:              big.NewInt(int64(blockNum)),
		BlockHash:                r.BlockHash(),
		TransactionIndex:         uint(txIndex),
		Type:                     r.TxType(),
		GasUsed:                  r.ExecutionResult.GasUsed,
		CumulativeGasUsed:        cumulativeGasUsed,
		TxHash:                   r.TxHash(),
		Logs:                     r.Logs,
		FirstLogIndexWithinBlock: firstLogIndex,
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
	txMessage, err := r.TxMessage()

	if err != nil {
		return nil, err
	}

	if txMessage != nil && txMessage.To() == nil {
		txSender, err := r.TxSender()
		if err != nil {
			return nil, err
		}
		receipt.ContractAddress = crypto.CreateAddress(*txSender, r.Tx().GetNonce())
	}

	return receipt, nil
}

type BlockResult struct {
	Complete bool
	Receipts types.Receipts
}

type ApplyMessage func(evm *vm.EVM, msg core.Message, gp *core.GasPool, refunds bool, gasBailout bool) (*evmtypes.ExecutionResult, error)

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
	BalanceIncreaseSet map[common.Address]uint256.Int

	Incarnation           int
	Tracer                *calltracer.CallTracer
	Hooks                 *tracing.Hooks
	Config                *chain.Config
	Engine                consensus.Engine
	Logger                log.Logger
	Trace                 bool
	AAValidationBatchSize uint64 // number of consecutive RIP-7560 transactions, should be 0 for single transactions and transactions that are not first in the transaction order
	InBatch               bool   // set to true for consecutive RIP-7560 transactions after the first one (first one is false)

	gasPool      *core.GasPool
	sender       *common.Address
	message      *types.Message
	signer       *types.Signer
	dependencies []int
}

type GenericTracer interface {
	TracingHooks() *tracing.Hooks
	SetTransaction(tx types.Transaction)
	Found() bool
}

func (t *TxTask) compare(other Task) int {
	switch {
	case t.Version().TxNum > other.Version().TxNum:
		return 1
	case t.Version().TxNum < other.Version().TxNum:
		return -1
	default:
		return 0
	}
}

func (t *TxTask) isNil() bool {
	return t == nil
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

func (t *TxTask) TxHash() common.Hash {
	if t.TxIndex < 0 || t.TxIndex >= len(t.Txs) {
		return common.Hash{}
	}
	return t.Tx().Hash()
}

func (t *TxTask) TxSender() (*common.Address, error) {
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

		t.message = message
	}

	return t.message, nil
}

func (t *TxTask) TracingHooks() *tracing.Hooks {
	return t.Hooks
}

func (t *TxTask) BlockNumber() uint64 {
	if t.Header == nil {
		return 0
	}
	return t.Header.Number.Uint64()
}

func (t *TxTask) BlockHash() common.Hash {
	if t.Header == nil {
		return common.Hash{}
	}
	return t.Header.Hash()
}

func (t *TxTask) BlockRoot() common.Hash {
	if t.Header == nil {
		return common.Hash{}
	}
	return t.Header.Root
}

func (t *TxTask) BlockTime() uint64 {
	if t.Header == nil {
		return 0
	}
	return t.Header.Time
}

func (t *TxTask) BlockGasLimit() uint64 {
	if t.Header == nil {
		return 0
	}
	return t.Header.GasLimit
}

func (t *TxTask) GasPool() *core.GasPool {
	return t.gasPool
}

func (t *TxTask) ResetGasPool(gasPool *core.GasPool) {
	t.gasPool = gasPool
}

func (t *TxTask) Version() state.Version {
	return state.Version{BlockNum: t.BlockNumber(), TxNum: t.TxNum, TxIndex: t.TxIndex}
}

func (t *TxTask) Dependencies() []int {
	if dbg.UseTxDependencies {
		if t.dependencies == nil && t.Engine != nil {
			t.dependencies = []int{}

			blockDependencies := t.Engine.TxDependencies(t.Header)

			if t.TxIndex > 0 && len(blockDependencies) > t.TxIndex {
				t.dependencies = append(t.dependencies, blockDependencies[t.TxIndex]...)
			}
		}
	}

	return t.dependencies
}

func (t *TxTask) VersionMap() *state.VersionMap {
	return nil
}

func (t *TxTask) VersionedReads(ibs *state.IntraBlockState) state.ReadSet {
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

func (t *TxTask) Reset(evm *vm.EVM, ibs *state.IntraBlockState, callTracer *calltracer.CallTracer) error {
	t.BalanceIncreaseSet = nil
	ibs.Reset()

	if t.TxIndex >= 0 {
		ibs.SetTxContext(t.BlockNumber(), t.TxIndex)
	}

	if t.TxIndex != -1 && !t.IsBlockEnd() {
		var vmCfg vm.Config
		if callTracer != nil {
			vmCfg.Tracer = callTracer.Tracer().Hooks
		}
		vmCfg.SkipAnalysis = t.SkipAnalysis
		msg, err := t.TxMessage()

		if err != nil {
			return err
		}

		evm.ResetBetweenBlocks(t.EvmBlockContext, core.NewEVMTxContext(msg), ibs, vmCfg, t.Config.Rules(t.BlockNumber(), t.BlockTime()))
	}

	return nil
}

func (txTask *TxTask) Execute(evm *vm.EVM,
	engine consensus.Engine,
	genesis *types.Genesis,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) *TxResult {
	var result TxResult

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
		syscall := func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			ret, _, err := core.SysCallContract(contract, data, chainConfig, ibs, header, engine, constCall /* constCall */, evm.Config().Tracer)
			return ret, err
		}
		engine.Initialize(chainConfig, chainReader, header, ibs, syscall, txTask.Logger, nil)
		result.Err = ibs.FinalizeTx(rules, state.NewNoopWriter())
		result.ExecutionResult = &evmtypes.ExecutionResult{}
	case txTask.IsBlockEnd():
		if txTask.BlockNumber() == 0 {
			break
		}

		result.ExecutionResult = &evmtypes.ExecutionResult{}
		result.TraceTos = map[common.Address]struct{}{}
		result.TraceTos[txTask.Header.Coinbase] = struct{}{}
		for _, uncle := range txTask.Uncles {
			result.TraceTos[uncle.Coinbase] = struct{}{}
		}
	default:
		if txTask.Tx().Type() == types.AccountAbstractionTxType {
			if !chainConfig.AllowAA {
				result.Err = errors.New("account abstraction transactions are not allowed")
				return &result
			}
			aaTxn, ok := txTask.Tx().(*types.AccountAbstractionTransaction)
			if !ok {
				result.Err = fmt.Errorf("invalid transaction type, expected AccountAbstractionTx, got %T", txTask.Tx)
				return &result
			}

			result = *txTask.executeAA(aaTxn, evm, txTask.GasPool(), ibs, chainConfig)
			break
		}

		result.Coinbase = evm.Context.Coinbase

		// MA applytx
		result.ExecutionResult, result.Err = func() (*evmtypes.ExecutionResult, error) {
			message, err := txTask.TxMessage()

			if err != nil {
				return nil, core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex(), OriginError: err}
			}

			// Apply the transaction to the current state (included in the env).
			if !calcFees {
				applyRes, applyErr := core.ApplyMessageNoFeeBurnOrTip(evm, message, txTask.GasPool(), true, false, engine)

				if applyErr != nil {
					if _, ok := applyErr.(core.ErrExecAbortError); !ok {
						return nil, core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex(), OriginError: applyErr}
					}

					return nil, applyErr
				}

				if applyRes == nil {
					return nil, core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex()}
				}

				reads := ibs.VersionedReads()

				if _, ok := reads[applyRes.BurntContractAddress][state.AccountKey{Path: state.BalancePath}]; ok {
					log.Debug("Coinbase is in versiopnedMap", "address", evm.Context.Coinbase)
					result.ShouldRerunWithoutFeeDelay = true
				}

				if _, ok := reads[evm.Context.Coinbase][state.AccountKey{Path: state.BalancePath}]; ok {
					log.Debug("BurntContractAddress is in versiopnedMap", "address", applyRes.BurntContractAddress)
					result.ShouldRerunWithoutFeeDelay = true
				}

				return applyRes, err
			}

			return core.ApplyMessage(evm, message, txTask.GasPool(), true, false, engine)
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

func (txTask *TxTask) executeAA(aaTxn *types.AccountAbstractionTransaction,
	evm *vm.EVM,
	gasPool *core.GasPool,
	ibs *state.IntraBlockState,
	chainConfig *chain.Config) *TxResult {
	var result TxResult

	if !txTask.InBatch {
		// this is the first transaction in an AA transaction batch, run all validation frames, then execute execution frames in its own txtask
		startIdx := uint64(txTask.TxIndex)
		endIdx := startIdx + txTask.AAValidationBatchSize

		validationResults := make([]AAValidationResult, txTask.AAValidationBatchSize+1)
		log.Info("🕵️‍♂️[aa] found AA bundle", "startIdx", startIdx, "endIdx", endIdx)

		var outerErr error
		for i := startIdx; i <= endIdx; i++ {
			// check if next n transactions are AA transactions and run validation
			if txTask.Txs[i].Type() == types.AccountAbstractionTxType {
				aaTxn, ok := txTask.Txs[i].(*types.AccountAbstractionTransaction)
				if !ok {
					result.Err = fmt.Errorf("invalid transaction type, expected AccountAbstractionTx, got %T", txTask.Tx)
					return &result
				}
				paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, ibs, gasPool, txTask.Header, evm, chainConfig)
				if err != nil {
					outerErr = err
					break
				}

				validationResults[i-startIdx] = AAValidationResult{
					PaymasterContext: paymasterContext,
					GasUsed:          validationGasUsed,
				}
			} else {
				outerErr = fmt.Errorf("invalid txcount, expected txn %d to be type %d", i, types.AccountAbstractionTxType)
				break
			}
		}

		if outerErr != nil {
			result.Err = outerErr
			return &result
		}

		log.Info("✅[aa] validated AA bundle", "len", startIdx-endIdx)

		result.ValidationResults = validationResults
	}

	if len(result.ValidationResults) == 0 {
		result.Err = fmt.Errorf("found RIP-7560 but no remaining validation results, txIndex %d", txTask.TxIndex)
	}

	validationRes := result.ValidationResults[0]
	result.ValidationResults = result.ValidationResults[1:]

	status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, validationRes.PaymasterContext, validationRes.GasUsed, gasPool, evm, txTask.Header, ibs)

	if err != nil {
		result.Err = err
		return &result
	}

	result.ExecutionResult.GasUsed = gasUsed
	// Update the state with pending changes
	ibs.SoftFinalise()
	result.Logs = ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), txTask.BlockNumber(), txTask.BlockHash())
	log.Info("🚀[aa] executed AA bundle transaction", "txIndex", txTask.TxIndex, "status", status)
	return &result
}

// TxTaskQueue non-thread-safe priority-queue
type queueable[T any] interface {
	compare(other T) int
	isNil() bool
}

type Queue[T queueable[T]] []T

func (h Queue[T]) Len() int {
	return len(h)
}
func (h Queue[T]) Less(i, j int) bool {
	return h[i].compare(h[j]) < 0
}

func (h Queue[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Queue[T]) Push(a any) {
	*h = append(*h, a.(T))
}

func (h *Queue[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	var none T
	old[n-1] = none
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
	retires     Queue[Task]
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
	*PriorityQueue[*TxResult]
}

func NewResultsQueue(channelLimit, heapLimit int) *ResultsQueue {
	return &ResultsQueue{NewPriorityQueue[*TxResult](channelLimit, heapLimit)}
}

func (q ResultsQueue) FirstTxNumLocked() uint64 { return (*q.results)[0].Version().TxNum }

type ResultsQueueIter struct {
	*PriorityQueueIter[*TxResult]
}

func (q ResultsQueue) Iter() *ResultsQueueIter {
	return &ResultsQueueIter{&PriorityQueueIter[*TxResult]{q: q.PriorityQueue}}
}

func (q *ResultsQueueIter) Has(outputTxNum uint64) bool {
	q.q.RLock()
	defer q.q.RUnlock()
	return len(*q.q.results) > 0 && (*q.q.results)[0].Version().TxNum == outputTxNum
}

type PriorityQueue[T queueable[T]] struct {
	limit  int
	closed bool

	resultCh   chan T
	addWaiters chan any
	//tick
	ticker *time.Ticker

	sync.RWMutex
	results *Queue[T]
}

func NewPriorityQueue[T queueable[T]](channelLimit, heapLimit int) *PriorityQueue[T] {
	r := &PriorityQueue[T]{
		results:  &Queue[T]{},
		limit:    heapLimit,
		resultCh: make(chan T, channelLimit),
		ticker:   time.NewTicker(2 * time.Second),
	}
	heap.Init(r.results)
	return r
}

// Add result of execution. May block when internal channel is full
func (q *PriorityQueue[T]) Add(ctx context.Context, item T) error {

	q.Lock()
	resultCh := q.resultCh
	addWaiters := q.addWaiters
	q.addWaiters = nil
	q.Unlock()

	if addWaiters != nil {
		close(addWaiters)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if resultCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultCh <- item: // Needs to have outside of the lock
		}
	}

	return nil
}

func (q *PriorityQueue[T]) Drain(ctx context.Context, item T) (bool, error) {
	q.Lock()
	defer q.Unlock()

	if q.resultCh == nil {
		return true, nil
	}

	if !item.isNil() {
		heap.Push(q.results, item)
	}

	for {
		select {
		case <-ctx.Done():
			return q.resultCh == nil, ctx.Err()
		case txTask, ok := <-q.resultCh:
			if !ok {
				return true, nil
			}
			if txTask.isNil() {
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

func (q *PriorityQueue[T]) Iter() *PriorityQueueIter[T] {
	return &PriorityQueueIter[T]{q: q}
}

type PriorityQueueIter[T queueable[T]] struct {
	q *PriorityQueue[T]
}

func (q *PriorityQueueIter[T]) HasNext() bool {
	q.q.RLock()
	defer q.q.RUnlock()
	return len(*q.q.results) > 0
}

func (q *PriorityQueueIter[T]) PopNext() T {
	q.q.Lock()
	defer q.q.Unlock()
	return heap.Pop(q.q.results).(T)
}

func (q *PriorityQueue[T]) ResultCh() chan T {
	return q.resultCh
}

func (q *PriorityQueue[T]) DrainNonBlocking(ctx context.Context) (bool, error) {
	var none T
	return q.Drain(ctx, none)
}

func (q *PriorityQueue[T]) Drop(ctx context.Context, f func(t T)) {
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
		f(heap.Pop(q.results).(T))
	}
}

func (q *PriorityQueue[T]) Close() {
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
func (q *PriorityQueue[T]) ResultChLen() int { return len(q.resultCh) }
func (q *PriorityQueue[T]) ResultChCap() int { return cap(q.resultCh) }
func (q *PriorityQueue[T]) Limit() int       { return q.limit }
func (q *PriorityQueue[T]) Len() (l int) {
	q.Lock()
	l = q.results.Len()
	q.Unlock()
	return l
}
func (q *ResultsQueue) LenLocked() (l int)     { return q.results.Len() }
func (q *ResultsQueue) HasLocked() bool        { return len(*q.results) > 0 }
func (q *ResultsQueue) PushLocked(t *TxResult) { heap.Push(q.results, t) }
func (q *ResultsQueue) Push(t *TxResult) {
	q.Lock()
	heap.Push(q.results, t)
	q.Unlock()
}
func (q *ResultsQueue) PopLocked() (t *TxResult) {
	return heap.Pop(q.results).(*TxResult)
}
func (q *ResultsQueue) Dbg() (t *TxResult) {
	if len(*q.results) > 0 {
		return (*q.results)[0]
	}
	return nil
}

func (q *ResultsQueue) AddWaiter(lock bool) chan any {
	if lock {
		q.Lock()
		defer q.Unlock()
	}

	if q.addWaiters == nil {
		q.addWaiters = make(chan any)
	}

	return q.addWaiters
}
