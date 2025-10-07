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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/aa"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
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
	ParentHash() common.Hash
	BlockTime() uint64
	BlockGasLimit() uint64
	BlockRoot() common.Hash

	Rules() *chain.Rules

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
	ExecutionResult   evmtypes.ExecutionResult
	ValidationResults []AAValidationResult
	Err               error
	Coinbase          common.Address
	TxIn              state.ReadSet
	TxOut             state.VersionedWrites

	Receipt *types.Receipt
	Logs    []*types.Log

	TraceFroms map[common.Address]struct{}
	TraceTos   map[common.Address]struct{}
}

func (r *TxResult) compare(other *TxResult) int {
	return r.Task.compare(other.Task)
}

func (r *TxResult) isNil() bool {
	return r == nil
}

func (r *TxResult) CreateNextReceipt(prev *types.Receipt) (*types.Receipt, error) {
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
	r.Receipt, err = r.CreateReceipt(txIndex, cumulativeGasUsed, firstLogIndex)
	return r.Receipt, err
}

func (r *TxResult) CreateReceipt(txIndex int, cumulativeGasUsed uint64, firstLogIndex uint32) (*types.Receipt, error) {
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
		receipt.ContractAddress = types.CreateAddress(*txSender, r.Tx().GetNonce())
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
	rules        *chain.Rules
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
		if tx := t.Tx(); tx != nil {
			if t.signer == nil {
				t.signer = types.MakeSigner(t.Config, t.BlockNumber(), t.Header.Time)
			}
			message, err := tx.AsMessage(*t.signer, t.Header.BaseFee, t.Rules())

			if err != nil {
				return nil, err
			}

			t.message = message
		}
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

func (t *TxTask) ParentHash() common.Hash {
	if t.Header == nil {
		return common.Hash{}
	}
	return t.Header.ParentHash
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

func (t *TxTask) Rules() *chain.Rules {
	if t.rules == nil {
		t.rules = t.EvmBlockContext.Rules(t.Config)
	}

	return t.rules
}

func (t *TxTask) ResetTx(txNum uint64, txIndex int) {
	t.TxNum = txNum
	t.TxIndex = txIndex
	t.sender = nil
	t.message = nil
	t.signer = nil
	t.dependencies = nil
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
	ibs.SetTxContext(t.BlockNumber(), t.TxIndex)

	if t.TxIndex != -1 && !t.IsBlockEnd() {
		var vmCfg vm.Config
		if callTracer != nil {
			vmCfg.Tracer = callTracer.Tracer().Hooks
		}
		msg, err := t.TxMessage()

		if err != nil {
			return err
		}

		var txContext evmtypes.TxContext
		if msg != nil {
			txContext = core.NewEVMTxContext(msg)
		}
		evm.ResetBetweenBlocks(t.EvmBlockContext, txContext, ibs, vmCfg, t.Rules())
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

	rules := txTask.Rules()

	var err error
	header := txTask.Header
	//fmt.Printf("txNum=%d blockNum=%d history=%t\n", txTask.TxNum, txTask.BlockNum, txTask.HistoryExecution)

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNumber() == 0 {

			//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
			_, ibs, err = genesiswrite.GenesisToBlock(nil, genesis, dirs, txTask.Logger)
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
			ret, err := core.SysCallContract(contract, data, chainConfig, ibs, header, engine, constCall /* constCall */, evm.Config())
			return ret, err
		}
		engine.Initialize(chainConfig, chainReader, header, ibs, syscall, txTask.Logger, nil)
		result.Err = ibs.FinalizeTx(rules, state.NewNoopWriter())
	case txTask.IsBlockEnd():
		if txTask.BlockNumber() == 0 {
			break
		}

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
		result.ExecutionResult, result.Err = func() (evmtypes.ExecutionResult, error) {
			message, err := txTask.TxMessage()

			if err != nil {
				return evmtypes.ExecutionResult{}, core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex(), OriginError: err}
			}

			// Apply the transaction to the current state (included in the env).
			var applyRes *evmtypes.ExecutionResult
			var applyErr error

			if !calcFees {
				applyRes, applyErr = core.ApplyMessageNoFeeBurnOrTip(evm, message, txTask.GasPool(), true, false, engine)
			} else {
				applyRes, applyErr = core.ApplyMessage(evm, message, txTask.GasPool(), true, false, engine)
			}

			if applyErr != nil {
				if _, ok := applyErr.(core.ErrExecAbortError); !ok {
					return evmtypes.ExecutionResult{}, core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex(), OriginError: applyErr}
				}

				return evmtypes.ExecutionResult{}, applyErr
			}

			if applyRes == nil {
				return evmtypes.ExecutionResult{}, core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex()}
			}

			return *applyRes, err
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
					outerErr = fmt.Errorf("invalid transaction type, expected AccountAbstractionTx, got %T", txTask.Tx)
					break
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

	aaTxn = txTask.Tx().(*types.AccountAbstractionTransaction) // type cast checked earlier
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
	closed   bool
	newTasks chan Task
	retires  Queue[Task]
	lock     sync.Mutex
	capacity int
}

func NewQueueWithRetry(capacity int) *QueueWithRetry {
	return &QueueWithRetry{newTasks: make(chan Task, capacity), capacity: capacity}
}

func (q *QueueWithRetry) NewTasksLen() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return len(q.newTasks)
}
func (q *QueueWithRetry) Capacity() int { return q.capacity }
func (q *QueueWithRetry) RetriesLen() (l int) {
	q.lock.Lock()
	l = q.retires.Len()
	q.lock.Unlock()
	return l
}
func (q *QueueWithRetry) RetryTxNumsList() (out []uint64) {
	q.lock.Lock()
	for _, t := range q.retires {
		out = append(out, t.Version().TxNum)
	}
	q.lock.Unlock()
	return out
}
func (q *QueueWithRetry) Len() (l int) { return q.RetriesLen() + q.NewTasksLen() }

// Add "new task" (which was never executed yet). May block internal channel is full.
// Expecting already-ordered tasks.
func (q *QueueWithRetry) Add(ctx context.Context, t Task) {
	q.lock.Lock()
	closed := q.closed
	newTasks := q.newTasks
	q.lock.Unlock()

	if !closed {
		select {
		case <-ctx.Done():
			return
		case newTasks <- t:
		}
	}
}

// ReTry returns failed (conflicted) task. It's non-blocking method.
// All failed tasks have higher priority than new one.
// No limit on amount of txs added by this method.
func (q *QueueWithRetry) ReTry(t Task) {
	q.lock.Lock()
	if q.closed {
		q.lock.Unlock()
		return
	}
	heap.Push(&q.retires, t)
	newTasks := q.newTasks
	q.lock.Unlock()
	select {
	case newTasks <- nil:
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
	q.lock.Lock()
	newTasks := q.newTasks
	q.lock.Unlock()

	if newTasks == nil {
		return q.popNoWait()
	}

	var checkEmpty = func() bool {
		q.lock.Lock()
		defer q.lock.Unlock()
		if q.closed && q.newTasks != nil && len(q.newTasks) == 0 {
			newTasks := q.newTasks
			q.newTasks = nil
			close(newTasks)
		}

		return q.newTasks == nil
	}

	if checkEmpty() {
		return nil, false
	}

	defer checkEmpty()

	for {
		q.lock.Lock()
		newTasks := q.newTasks
		q.lock.Unlock()

		select {
		case inTask, ok := <-newTasks:
			if !ok {
				q.lock.Lock()
				if q.retires.Len() > 0 {
					task = heap.Pop(&q.retires).(Task)
				}
				q.lock.Unlock()
				return task, task != nil
			}

			q.lock.Lock()
			if inTask != nil {
				heap.Push(&q.retires, inTask)
			}
			if q.retires.Len() > 0 {
				task = heap.Pop(&q.retires).(Task)
			}
			q.lock.Unlock()
			if task != nil {
				return task, true
			}
		case <-ctx.Done():
			return nil, false
		}
	}
}
func (q *QueueWithRetry) popNoWait() (task Task, ok bool) {
	q.lock.Lock()
	has := q.retires.Len() > 0
	if has { // means have conflicts to re-exec: it has higher priority than new tasks
		task = heap.Pop(&q.retires).(Task)
	}
	newTasks := q.newTasks
	q.lock.Unlock()

	if has {
		return task, task != nil
	}

	// otherwise get some new task. non-blocking way. without adding to queue.
	for task == nil && len(newTasks) > 0 {
		select {
		case task, ok = <-newTasks:
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
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.closed {
		return
	}
	q.closed = true
	if q.newTasks != nil && len(q.newTasks) == 0 {
		newTasks := q.newTasks
		q.newTasks = nil
		close(newTasks)
	}
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
	limit      int
	closed     bool
	resultCh   chan T
	addWaiters chan any
	sync.RWMutex
	results *Queue[T]
}

func NewPriorityQueue[T queueable[T]](channelLimit, heapLimit int) *PriorityQueue[T] {
	r := &PriorityQueue[T]{
		results:  &Queue[T]{},
		limit:    heapLimit,
		resultCh: make(chan T, channelLimit),
	}
	heap.Init(r.results)
	return r
}

// Add result of execution. May block when internal channel is full
func (q *PriorityQueue[T]) Add(ctx context.Context, item T) error {
	q.Lock()
	closed := q.closed
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

	if closed {
		return errors.New("can't add to closed queue")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultCh <- item: // Needs to have outside of the lock
	}

	return nil
}

func (q *PriorityQueue[T]) AwaitDrain(ctx context.Context, waitTime time.Duration) (bool, error) {
	q.Lock()
	resultCh := q.resultCh
	q.Unlock()

	if resultCh == nil {
		var none T
		return q.Drain(ctx, none)
	}

	var waitTimer *time.Timer
	var waitChan <-chan time.Time

	if waitTime > 0 {
		waitTimer = time.NewTimer(waitTime)
		waitChan = waitTimer.C
		defer waitTimer.Stop()
	}

	select {
	case <-ctx.Done():
		return q.results.Len() == 0, ctx.Err()
	case next := <-resultCh:
		return q.Drain(ctx, next)
	case <-waitChan:
		var none T
		return q.Drain(ctx, none)
	}
}

func (q *PriorityQueue[T]) Drain(ctx context.Context, item T) (bool, error) {
	q.Lock()
	defer q.Unlock()

	if !item.isNil() {
		heap.Push(q.results, item)
	}

	if q.resultCh == nil {
		return q.results.Len() == 0, nil
	} else if q.closed && len(q.resultCh) == 0 {
		close(q.resultCh)
		q.resultCh = nil
		return q.results.Len() == 0, nil
	}

	for {
		select {
		case <-ctx.Done():
			return q.resultCh == nil && q.results.Len() == 0, ctx.Err()
		case next, ok := <-q.resultCh:
			if !ok {
				return q.results.Len() == 0, nil
			}
			if next.isNil() {
				if q.closed && len(q.resultCh) == 0 {
					close(q.resultCh)
					q.resultCh = nil
					return q.results.Len() == 0, nil
				}
				continue
			}
			heap.Push(q.results, next)
			if q.closed && len(q.resultCh) == 0 {
				close(q.resultCh)
				q.resultCh = nil
				return false, nil
			}
			if q.results.Len() > q.limit {
				return false, nil
			}
		default: // we are inside mutex section, can't block here
			return q.resultCh == nil && q.results.Len() == 0, nil
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
	q.Lock()
	defer q.Unlock()
	return q.resultCh
}

func (q *PriorityQueue[T]) Close() {
	q.Lock()
	defer q.Unlock()
	if q.closed {
		return
	}
	q.closed = true

	if len(q.resultCh) == 0 {
		close(q.resultCh)
		q.resultCh = nil
	}
}

func (q *PriorityQueue[T]) Len() (l int) {
	q.Lock()
	l = len(q.resultCh) + q.results.Len()
	q.Unlock()
	return l
}

func (q *ResultsQueue) Peek() (t *TxResult) {
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
