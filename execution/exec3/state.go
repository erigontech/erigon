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

package exec3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

var noop = state.NewNoopWriter()

type WorkerMetrics struct {
	Active              activeCount
	GasUsed             activeCount
	Duration            activeDuration
	StorageReadCount    atomic.Int64
	StorageReadDuration activeDuration
	WriteDuration       activeDuration
}

func NewWorkerMetrics() *WorkerMetrics {
	return &WorkerMetrics{
		Active:              activeCount{Ema: metrics.NewEmaWithBeta[int64](0, 1, 0.2)},
		GasUsed:             activeCount{Ema: metrics.NewEma[int64](0, 0.3)},
		Duration:            activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		StorageReadDuration: activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		WriteDuration:       activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
	}
}

type activeDuration struct {
	atomic.Int64
	Ema *metrics.EMA[time.Duration]
}

func (d *activeDuration) Add(i time.Duration) {
	d.Int64.Add(int64(i))
	d.Ema.Update(i)
}

type activeCount struct {
	atomic.Int64
	Total atomic.Int64
	Ema   *metrics.EMA[int64]
}

func (c *activeCount) Add(i int64) {
	c.Int64.Add(i)
	if i > 0 {
		c.Total.Add(i)
	}
	c.Ema.Update(c.Load())
}

type Worker struct {
	lock        *sync.RWMutex
	notifier    *sync.Cond
	runnable    atomic.Bool
	logger      log.Logger
	chainDb     kv.RoDB
	chainTx     kv.Tx
	background  bool // if true - worker does manage RoTx (begin/rollback) in .ResetTx()
	blockReader services.FullBlockReader
	in          *exec.QueueWithRetry
	rs          *state.StateV3Buffered
	stateWriter state.StateWriter
	stateReader state.ResettableStateReader
	historyMode bool // if true - stateReader is HistoryReaderV3, otherwise it's state reader
	chainConfig *chain.Config

	ctx     context.Context
	engine  consensus.Engine
	genesis *types.Genesis
	results *exec.ResultsQueue
	chain   consensus.ChainReader

	evm *vm.EVM
	ibs *state.IntraBlockState

	dirs datadir.Dirs

	metrics *WorkerMetrics
}

func NewWorker(ctx context.Context, background bool, metrics *WorkerMetrics, chainDb kv.RoDB, in *exec.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, results *exec.ResultsQueue, engine consensus.Engine, dirs datadir.Dirs, logger log.Logger) *Worker {
	lock := &sync.RWMutex{}

	w := &Worker{
		lock:    lock,
		chainDb: chainDb,
		in:      in,

		logger: logger,
		ctx:    ctx,

		background:  background,
		blockReader: blockReader,

		chainConfig: chainConfig,
		genesis:     genesis,
		resultCh:    results,
		engine:      engine,

		evm: vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),

		dirs:    dirs,
		metrics: metrics,
	}
	w.runnable.Store(true)
	w.ibs = state.New(w.stateReader)
	return w
}

func (rw *Worker) LogLRUStats() { rw.evm.Config().JumpDestCache.LogStats() }

func (rw *Worker) ResetState(rs *state.StateV3Buffered, chainTx kv.Tx, stateReader state.ResettableStateReader, stateWriter state.StateWriter, accumulator *shards.Accumulator) {
	rw.lock.Lock()
	defer rw.lock.Unlock()

	rw.rs = rs
	rw.resetTx(chainTx)

	if stateReader != nil {
		rw.SetReader(stateReader)
	} else {
		rw.SetReader(state.NewBufferedReader(rs, state.NewReaderV3(rs.Domains(), rw.chainTx)))
	}

	if stateWriter != nil {
		rw.stateWriter = stateWriter
	} else {
		rw.stateWriter = state.NewWriter(rs.Domains().AsPutDel(rw.chainTx), accumulator)
	}
}

func (rw *Worker) Tx() kv.Tx        { return rw.chainTx }
func (rw *Worker) DiscardReadList() { rw.stateReader.DiscardReadList() }
func (rw *Worker) ResetTx(chainTx kv.Tx) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.resetTx(chainTx)
}

func (rw *Worker) resetTx(chainTx kv.Tx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
	}

	rw.chainTx = chainTx

	if rw.chainTx != nil {
		type resettable interface {
			SetTx(kv.Tx)
		}

		if resettable, ok := rw.stateReader.(resettable); ok {
			resettable.SetTx(rw.chainTx)
		}

		if resettable, ok := rw.stateWriter.(resettable); ok {
			resettable.SetTx(rw.chainTx)
		}

		rw.chain = consensuschain.NewReader(rw.chainConfig, rw.chainTx, rw.blockReader, rw.logger)
	} else {
		rw.chain = nil
		rw.stateReader = nil
		rw.stateWriter = nil
	}
}

func (rw *Worker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("exec3.Worker panic: %s, %s", rec, dbg.Stack())
		}
	}()

	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		result := rw.RunTxTask(txTask)
		if err := rw.results.Add(rw.ctx, result); err != nil {
			return err
		}
	}
	return nil
}

func (rw *Worker) RunTxTask(txTask exec.Task) (result *exec.TxResult) {
	//fmt.Println("RTX", txTask.Version().BlockNum, txTask.Version().TxIndex, txTask.Version().TxNum, txTask.IsBlockEnd())
	//defer fmt.Println("RTX DONE", txTask.Version().BlockNum, txTask.Version().TxIndex)
	rw.lock.Lock()
	defer rw.lock.Unlock()

	for !rw.runnable.Load() {
		rw.notifier.Wait()
	}

	if rw.metrics != nil {
		rw.metrics.Active.Add(1)
		start := time.Now()
		defer func() {
			rw.metrics.Duration.Add(time.Since(start))
			if readDuration := rw.ibs.StorageReadDuration(); readDuration > 0 {
				rw.metrics.StorageReadDuration.Add(rw.ibs.StorageReadDuration())
				rw.metrics.StorageReadCount.Add(rw.ibs.StorageReadCount())
			}
			if result != nil && result.ExecutionResult != nil {
				rw.metrics.GasUsed.Add(int64(result.ExecutionResult.GasUsed))
			}
			rw.metrics.Active.Add(-1)
		}()
	}

	result = rw.RunTxTaskNoLock(txTask)
	return result
}

// Needed to set history reader when need to offset few txs from block beginning and does not break processing,
// like compute gas used for block and then to set state reader to continue processing on latest data.
func (rw *Worker) SetReader(reader state.ResettableStateReader) {
	rw.stateReader = reader
	rw.stateReader.SetTx(rw.Tx())
	rw.ibs = state.New(rw.stateReader)

	switch reader.(type) {
	case *state.HistoryReaderV3:
		rw.historyMode = true
	case *state.ReaderV3:
		rw.historyMode = false
	default:
		rw.historyMode = false
	}
}

func (rw *Worker) RunTxTaskNoLock(txTask exec.Task) *exec.TxResult {
	if txTask.IsHistoric() && !rw.historyMode {
		// in case if we cancelled execution and commitment happened in the middle of the block, we have to process block
		// from the beginning until committed txNum and only then disable history mode.
		// Needed to correctly evaluate spent gas and other things.
		rw.SetReader(state.NewHistoryReaderV3())
	} else if !txTask.IsHistoric() && rw.historyMode {
		rw.SetReader(state.NewBufferedReader(rw.rs, state.NewReaderV3(rw.rs.Domains(), rw.chainTx)))
	}

	if rw.background && rw.chainTx == nil {
		chainTx, err := rw.chainDb.(kv.TemporalRoDB).BeginTemporalRo(rw.ctx)

	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.rs.Domains().SetTxNum(txTask.TxNum)
	rw.stateReader.ResetReadSet()
	rw.stateWriter.ResetWriteSet()

	rw.ibs.Reset()
	ibs, hooks, cc := rw.ibs, rw.hooks, rw.chainConfig
	//ibs.SetTrace(true)
	ibs.SetHooks(hooks)

	var err error
	rules, header := txTask.Rules, txTask.Header

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNum == 0 {

			//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
			_, ibs, err = core.GenesisToBlock(rw.genesis, rw.dirs, rw.logger)
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
			ret, _, err := core.SysCallContract(contract, data, cc, ibs, header, rw.engine, constCall /* constCall */, hooks)
			return ret, err
		}
		rw.engine.Initialize(cc, rw.chain, header, ibs, syscall, rw.logger, hooks)
		txTask.Error = ibs.FinalizeTx(rules, noop)
	case txTask.Final:
		if txTask.BlockNum == 0 {
			break
		}

		// End of block transaction in a block
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			ret, logs, err := core.SysCallContract(contract, data, cc, ibs, header, rw.engine, false /* constCall */, hooks)
			if err != nil {
				return nil, err
			}

			txTask.Logs = append(txTask.Logs, logs...)

			return ret, err
		}

		if isMining {
			_, txTask.Txs, txTask.BlockReceipts, _, err = rw.engine.FinalizeAndAssemble(cc, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, nil, rw.logger)
		} else {
			_, _, _, err = rw.engine.Finalize(cc, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, skipPostEvaluaion, rw.logger)
		}
		if err != nil {
			txTask.Error = err
		} else {
			txTask.TraceTos = map[common.Address]struct{}{}
			txTask.TraceTos[txTask.Coinbase] = struct{}{}
			for _, uncle := range txTask.Uncles {
				txTask.TraceTos[uncle.Coinbase] = struct{}{}
			}
		}
	default:
		rw.callTracer.Reset()
		rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
		ibs.SetTxContext(txTask.TxIndex)
		txn := txTask.Tx

		if txTask.Tx.Type() == types.AccountAbstractionTxType {
			if !cc.AllowAA {
				txTask.Error = errors.New("account abstraction transactions are not allowed")
				break
			}

			msg, err := txn.AsMessage(types.Signer{}, nil, nil)
			if err != nil {
				txTask.Error = err
				break
			}

			rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, rw.vmCfg, rules)
			rw.execAATxn(txTask)
			break
		}

		msg := txTask.TxAsMessage
		rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, rw.vmCfg, rules)

		if hooks != nil && hooks.OnTxStart != nil {
			hooks.OnTxStart(rw.evm.GetVMContext(), txn, msg.From())
		}
		// MA applytx
		applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */, rw.engine)
		if err != nil {
			txTask.Error = err
			if hooks != nil && hooks.OnTxEnd != nil {
				hooks.OnTxEnd(nil, err)
			}
		} else {
			txTask.Failed = applyRes.Failed()
			txTask.UsedGas = applyRes.UsedGas
			// Update the state with pending changes
			ibs.SoftFinalise()
			//txTask.Error = ibs.FinalizeTx(rules, noop)
			txTask.Logs = ibs.GetLogs(txTask.TxIndex, txn.Hash(), txTask.BlockNum, txTask.BlockHash)
			txTask.TraceFroms = rw.callTracer.Froms()
			txTask.TraceTos = rw.callTracer.Tos()

			txTask.CreateReceipt(rw.Tx())
			if hooks != nil && hooks.OnTxEnd != nil {
				hooks.OnTxEnd(txTask.BlockReceipts[txTask.TxIndex], nil)
			}
		}

		rw.resetTx(chainTx)
	}

	txIndex := txTask.Version().TxIndex

	var callTracer *calltracer.CallTracer

	if txIndex != -1 && !txTask.IsBlockEnd() {
		callTracer = calltracer.NewCallTracer(txTask.TracingHooks())
	}

	if err := txTask.Reset(rw.evm, rw.ibs, callTracer); err != nil {
		return &exec.TxResult{
			Task: txTask,
			Err:  err,
		}
	}

	result := txTask.Execute(rw.evm, rw.engine, rw.genesis, rw.ibs, rw.stateWriter, rw.chainConfig, rw.chain, rw.dirs, true)

	if result.Task == nil {
		result.Task = txTask
	}

	if callTracer != nil {
		result.TraceFroms = callTracer.Froms()
		result.TraceTos = callTracer.Tos()
	}

	return result
}

func (rw *Worker) execAATxn(txTask *state.TxTask) {
	if !txTask.InBatch {
		// this is the first transaction in an AA transaction batch, run all validation frames, then execute execution frames in its own txtask
		startIdx := uint64(txTask.TxIndex)
		endIdx := startIdx + txTask.AAValidationBatchSize

		validationResults := make([]state.AAValidationResult, txTask.AAValidationBatchSize+1)
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

				paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, rw.ibs, rw.taskGasPool, txTask.Header, rw.evm, rw.chainConfig)
				if err != nil {
					outerErr = err
					break
				}

				validationResults[i-startIdx] = state.AAValidationResult{
					PaymasterContext: paymasterContext,
					GasUsed:          validationGasUsed,
				}
			} else {
				outerErr = fmt.Errorf("invalid txcount, expected txn %d to be type %d", i, types.AccountAbstractionTxType)
				break
			}
		}

		if outerErr != nil {
			txTask.Error = outerErr
			return
		}
		log.Info("✅[aa] validated AA bundle", "len", startIdx-endIdx)

		txTask.ValidationResults = validationResults
	}

	if len(txTask.ValidationResults) == 0 {
		txTask.Error = fmt.Errorf("found RIP-7560 but no remaining validation results, txIndex %d", txTask.TxIndex)
	}

	aaTxn := txTask.Tx.(*types.AccountAbstractionTransaction) // type cast checked earlier
	validationRes := txTask.ValidationResults[0]
	txTask.ValidationResults = txTask.ValidationResults[1:]

	status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, validationRes.PaymasterContext, validationRes.GasUsed, rw.taskGasPool, rw.evm, txTask.Header, rw.ibs)
	if err != nil {
		txTask.Error = err
		return
	}

	txTask.Failed = status != 0
	txTask.UsedGas = gasUsed
	// Update the state with pending changes
	rw.ibs.SoftFinalise()
	txTask.Logs = rw.ibs.GetLogs(txTask.TxIndex, txTask.Tx.Hash(), txTask.BlockNum, txTask.BlockHash)
	txTask.TraceFroms = rw.callTracer.Froms()
	txTask.TraceTos = rw.callTracer.Tos()
	txTask.CreateReceipt(rw.Tx())

	log.Info("🚀[aa] executed AA bundle transaction", "txIndex", txTask.TxIndex, "status", status)
}

func NewWorkersPool(lock sync.Locker, accumulator *shards.Accumulator, logger log.Logger, hooks *tracing.Hooks, ctx context.Context, background bool, chainDb kv.RoDB, rs *state.ParallelExecutionState, in *state.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, engine consensus.Engine, workerCount int, dirs datadir.Dirs, isMining bool) (reconWorkers []*Worker, applyWorker *Worker, rws *state.ResultsQueue, clear func(), wait func()) {
	reconWorkers = make([]*Worker, workerCount)

	resultsSize := workerCount * 8
	rws = exec.NewResultsQueue(resultsSize, workerCount)

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < workerCount; i++ {
		reconWorkers[i] = NewWorker(gctx, background, metrics, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, logger)

		if rs != nil {
			reader := stateReader

			if reader == nil {
				reader = state.NewBufferedReader(rs, state.NewReaderV3(rs.Domains(), nil))
			}

			reconWorkers[i].ResetState(rs, nil, reader, stateWriter, accumulator)
		}
	}
	if background {
		for i := 0; i < workerCount; i++ {
			i := i
			g.Go(func() error {
				return reconWorkers[i].Run()
			})
		}
		wait = func() { g.Wait() }
	}

	var clearDone bool
	clear = func() {
		if clearDone {
			return
		}
		clearDone = true
		g.Wait()
		for _, w := range reconWorkers {
			w.ResetTx(nil)
		}
		//applyWorker.ResetTx(nil)
	}
	applyWorker = NewWorker(ctx, false, nil, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, logger)

	return reconWorkers, applyWorker, rws, clear, wait
}
