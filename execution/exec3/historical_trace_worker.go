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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/interfaces"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type HistoricalTraceWorker struct {
	consumer TraceConsumer
	in       *exec.QueueWithRetry
	out      *exec.ResultsQueue

	stateReader *state.HistoryReaderV3
	ibs         *state.IntraBlockState
	evm         *vm.EVM

	chainTx     kv.TemporalTx
	background  bool
	ctx         context.Context
	stateWriter state.StateWriter
	chain       consensus.ChainReader
	logger      log.Logger

	execArgs *ExecArgs

	taskGasPool *core.GasPool

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmCfg     *vm.Config
}

type TraceConsumer interface {
	//Reduce receiving results of execution. They are sorted and have no gaps.
	Reduce(br *exec.BlockResult, task *exec.TxResult, tx kv.TemporalTx) error
}

type TraceConsumerFunc func(br *exec.BlockResult, task *exec.TxResult, tx kv.TemporalTx) error

func (f TraceConsumerFunc) Reduce(br *exec.BlockResult, task *exec.TxResult, tx kv.TemporalTx) error {
	return f(br, task, tx)
}

func NewHistoricalTraceWorker(
	ctx context.Context,
	consumer TraceConsumer,
	in *exec.QueueWithRetry,
	out *exec.ResultsQueue,
	background bool,
	execArgs *ExecArgs,
	logger log.Logger,
) *HistoricalTraceWorker {
	ie := &HistoricalTraceWorker{
		consumer: consumer,
		in:       in,
		out:      out,

		logger:   logger,
		ctx:      ctx,
		execArgs: execArgs,

		background:  background,
		stateReader: state.NewHistoryReaderV3(),

		taskGasPool: new(core.GasPool),
		vmCfg:       &vm.Config{JumpDestCache: vm.NewJumpDestCache(vm.JumpDestCacheLimit)},
	}
	ie.evm = vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, execArgs.ChainConfig, *ie.vmCfg)
	ie.taskGasPool.AddBlobGas(execArgs.ChainConfig.GetMaxBlobGasPerBlock(0))
	ie.ibs = state.New(ie.stateReader)
	return ie
}

func (rw *HistoricalTraceWorker) LogStats() {
	rw.evm.Config().JumpDestCache.LogStats()
}

func (rw *HistoricalTraceWorker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("HistoricalTraceWorker panic: %s, %s", rec, dbg.Stack())
		}
	}()
	defer rw.LogStats()
	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		result := rw.RunTxTask(txTask.(*exec.TxTask))
		if err := rw.out.Add(rw.ctx, result); err != nil {
			return err
		}
	}
	return nil
}

func (rw *HistoricalTraceWorker) RunTxTask(txTask *exec.TxTask) *exec.TxResult {
	var result = exec.TxResult{
		Task: txTask,
	}

	if rw.background && rw.chainTx == nil {
		var err error
		if rw.chainTx, err = rw.execArgs.ChainDB.BeginTemporalRo(rw.ctx); err != nil {
			result.Err = err
			return &result
		}
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.execArgs.ChainConfig, rw.chainTx, rw.execArgs.BlockReader, rw.logger)
	}
	result.Err = nil

	rw.stateWriter = state.NewNoopWriter()

	rw.ibs.Reset()
	ibs, cc := rw.ibs, rw.execArgs.ChainConfig
	//ibs.SetTrace(true)

	rules := rw.execArgs.ChainConfig.Rules(txTask.BlockNumber(), txTask.BlockTime())
	header := txTask.Header
	hooks := txTask.Tracer.Tracer().Hooks
	ibs.SetHooks(hooks)

	var err error

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNumber() == 0 {
			// Genesis block
			_, ibs, err = core.GenesisToBlock(rw.execArgs.Genesis, rw.execArgs.Dirs, rw.logger)
			if err != nil {
				panic(fmt.Errorf("GenesisToBlock: %w", err))
			}
			// For Genesis, rules should be empty, so that empty accounts can be included
			rules = &chain.Rules{} //nolint
			break
		}

		// Block initialisation
		//fmt.Printf("txNum=%d, blockNum=%d, initialisation of the block\n", txTask.TxNum, txTask.BlockNum)
		syscall := func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			ret, _, err := core.SysCallContract(contract, data, cc, ibs, header, rw.execArgs.Engine, constCall /* constCall */, hooks, *rw.vmCfg)
			return ret, err
		}
		rw.execArgs.Engine.Initialize(cc, rw.chain, header, ibs, syscall, rw.logger, hooks)
		result.Err = ibs.FinalizeTx(rules, noop)
	/*case txTask.IsBlockEnd():
	if txTask.BlockNumber() == 0 {
		break
	}

	// End of block transaction in a block
	syscall := func(contract common.Address, data []byte) ([]byte, error) {
		ret, logs, err := core.SysCallContract(contract, data, cc, ibs, header, rw.execArgs.Engine, false, hooks)
		if err != nil {
			return nil, err
		}
		result.Logs = append(result.Logs, logs...)
		return ret, err
	}

	skipPostEvaluaion := false // `true` only inMining
	_, _, _, err := rw.execArgs.Engine.Finalize(cc, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, skipPostEvaluaion, rw.logger)
	if err != nil {
		result.Err = err
	} else {
		result.TraceTos = map[common.Address]struct{}{}
		result.TraceTos[result.Coinbase] = struct{}{}
		for _, uncle := range txTask.Uncles {
			result.TraceTos[uncle.Coinbase] = struct{}{}
		}
	}
	*/
	default:
		result.Err = func() error {
			rw.taskGasPool.Reset(txTask.Tx().GetGasLimit(), cc.GetMaxBlobGasPerBlock(header.Time))
			rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
			txTask.Tracer.Reset() // txTask is retryable
			rw.vmCfg.Tracer = txTask.Tracer.Tracer().Hooks
			ibs.SetTxContext(txTask.BlockNumber(), txTask.Version().TxIndex)
			txn := txTask.Tx()

			if txn.Type() == types.AccountAbstractionTxType {
				if !cc.AllowAA {
					return errors.New("account abstraction transactions are not allowed")
				}

				msg, err := txn.AsMessage(types.Signer{}, nil, nil)
				if err != nil {
					return err
				}

				rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, *rw.vmCfg, rules)
				result := rw.execAATxn(txTask)
				return result.Err
			}

			msg, err := txTask.TxMessage()
			txContext := core.NewEVMTxContext(msg)
			if rw.vmCfg.TraceJumpDest {
				txContext.TxHash = txn.Hash()
			}
			rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, txContext, ibs, *rw.vmCfg, rules)
			if hooks != nil && hooks.OnTxStart != nil {
				hooks.OnTxStart(rw.evm.GetVMContext(), txn, msg.From())
			}

			// MA applytx
			applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */, rw.execArgs.Engine)
			if err != nil {
				return err
			}

			result.ExecutionResult = applyRes
			// Update the state with pending changes
			ibs.SoftFinalise()

			result.Logs = ibs.GetLogs(txTask.TxIndex, txTask.Tx().Hash(), txTask.BlockNumber(), txTask.BlockHash())
			result.TraceFroms = txTask.Tracer.Froms()
			result.TraceTos = txTask.Tracer.Tos()

			return nil
		}()
	}

	return &result
}

func (rw *HistoricalTraceWorker) execAATxn(txTask *exec.TxTask) *exec.TxResult {
	result := &exec.TxResult{}

	if !txTask.InBatch {
		// this is the first transaction in an AA transaction batch, run all validation frames, then execute execution frames in its own txtask
		startIdx := uint64(txTask.TxIndex)
		endIdx := startIdx + txTask.AAValidationBatchSize

		validationResults := make([]exec.AAValidationResult, txTask.AAValidationBatchSize+1)
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

				paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, rw.ibs, rw.taskGasPool, txTask.Header, rw.evm, rw.execArgs.ChainConfig)
				if err != nil {
					outerErr = err
					break
				}

				validationResults[i-startIdx] = exec.AAValidationResult{
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
			return result
		}
		log.Info("✅[aa] validated AA bundle", "len", startIdx-endIdx)

		result.ValidationResults = validationResults
	}

	if len(result.ValidationResults) == 0 {
		result.Err = fmt.Errorf("found RIP-7560 but no remaining validation results, txIndex %d", txTask.TxIndex)
	}

	aaTxn := txTask.Tx().(*types.AccountAbstractionTransaction) // type cast checked earlier
	validationRes := result.ValidationResults[0]
	result.ValidationResults = result.ValidationResults[1:]

	status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, validationRes.PaymasterContext, validationRes.GasUsed, rw.taskGasPool, rw.evm, txTask.Header, rw.ibs)
	if err != nil {
		result.Err = err
		return result
	}

	result.ExecutionResult.GasUsed = gasUsed
	// Update the state with pending changes
	rw.ibs.SoftFinalise()
	result.Logs = rw.ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), txTask.BlockNumber(), txTask.BlockHash())
	result.TraceFroms = txTask.Tracer.Froms()
	result.TraceTos = txTask.Tracer.Tos()

	log.Info("🚀[aa] executed AA bundle transaction", "txIndex", txTask.TxIndex, "status", status)
	return result
}

func (rw *HistoricalTraceWorker) ResetTx(chainTx kv.TemporalTx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
		rw.chainTx = nil
	}
	if chainTx != nil {
		rw.chainTx = chainTx
		rw.stateReader.SetTx(rw.chainTx)
		//rw.stateWriter.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.execArgs.ChainConfig, rw.chainTx, rw.execArgs.BlockReader, rw.logger)
	}
}

// immutable (aka. global) params required for block execution. can instantiate once at app-start
type ExecArgs struct {
	ChainDB     kv.TemporalRoDB
	Genesis     *types.Genesis
	BlockReader services.FullBlockReader
	Engine      consensus.Engine
	Dirs        datadir.Dirs
	ChainConfig *chain.Config
	Workers     int
}

func NewHistoricalTraceWorkers(consumer TraceConsumer, cfg *ExecArgs, ctx context.Context, toTxNum uint64, in *exec.QueueWithRetry, workerCount int, outputTxNum *atomic.Uint64, logger log.Logger) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)

	// can afford big limits - because historical execution doesn't need conflicts-resolution
	resultChannelLimit := workerCount * 128
	heapLimit := workerCount * 128
	rws := exec.NewResultsQueue(resultChannelLimit, heapLimit) // workerCount * 4

	g.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("'reduce worker' paniced: %s, %s", rec, dbg.Stack())
			}
		}()
		defer rws.Close()
		return doHistoryMap(consumer, cfg, ctx, in, workerCount, rws, logger)
	})
	g.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("'reduce worker' paniced: %s, %s", rec, dbg.Stack())
			}
		}()
		return doHistoryReduce(ctx, consumer, cfg, toTxNum, outputTxNum, rws, logger)
	})
	return g
}

func doHistoryReduce(ctx context.Context, consumer TraceConsumer, cfg *ExecArgs, toTxNum uint64, outputTxNum *atomic.Uint64, rws *exec.ResultsQueue, logger log.Logger) error {
	tx, err := cfg.ChainDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var resultProcessor historicalResultProcessor

	for outputTxNum.Load() <= toTxNum {
		_, err = rws.DrainNonBlocking(ctx)
		if err != nil {
			return err
		}

		processedTxNum, _, err := resultProcessor.processResults(consumer, cfg, rws, outputTxNum.Load(), tx, true, logger)
		if err != nil {
			return fmt.Errorf("processResultQueueHistorical: %w", err)
		}
		if processedTxNum > 0 {
			outputTxNum.Store(processedTxNum)
		}
	}
	//if outputTxNum.Load() != toTxNum {
	//	return fmt.Errorf("not all txnums proceeded: toTxNum=%d, outputTxNum=%d", toTxNum, outputTxNum.Load())
	//}
	return nil
}
func doHistoryMap(consumer TraceConsumer, cfg *ExecArgs, ctx context.Context, in *exec.QueueWithRetry, workerCount int, rws *exec.ResultsQueue, logger log.Logger) error {
	workers := make([]*HistoricalTraceWorker, workerCount)
	mapGroup, ctx := errgroup.WithContext(ctx)
	// we all errors in background workers (except ctx.Cancel), because applyLoop will detect this error anyway.
	// and in applyLoop all errors are critical
	for i := 0; i < workerCount; i++ {
		i := i
		workers[i] = NewHistoricalTraceWorker(ctx, consumer, in, rws, true, cfg, logger)
		mapGroup.Go(func() error {
			return workers[i].Run()
		})
	}
	defer func() {
		mapGroup.Wait()
		for _, w := range workers {
			w.ResetTx(nil)
		}
	}()
	return mapGroup.Wait()
}

type historicalResultProcessor struct {
	blockResult *exec.BlockResult
}

func (p *historicalResultProcessor) processResults(consumer TraceConsumer, cfg *ExecArgs, rws *exec.ResultsQueue, outputTxNumIn uint64, tx kv.TemporalTx, forceStopAtBlockEnd bool, logger log.Logger) (outputTxNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := rws.Iter()

	outputTxNum = outputTxNumIn
	for rwsIt.Has(outputTxNum) {
		result := rwsIt.PopNext()
		txTask := result.Task.(*exec.TxTask)
		outputTxNum++

		hooks := result.TracingHooks()
		if result.Err != nil {
			if hooks != nil && hooks.OnTxEnd != nil {
				hooks.OnTxEnd(nil, err)
			}
			return outputTxNum, false, fmt.Errorf("bn=%d, tn=%d: %w", result.BlockNumber(), result.Version().TxNum, result.Err)
		}

		var prev *types.Receipt
		if txTask.TxIndex > 0 {
			prev = p.blockResult.Receipts[txTask.TxIndex-1]
		} else {
			// TODO get the previous reciept from the DB
		}

		receipt, err := result.CreateReceipt(prev)

		if hooks != nil && hooks.OnTxEnd != nil {
			hooks.OnTxEnd(receipt, err)
		}

		if err != nil {
			return outputTxNum, false, fmt.Errorf("bn=%d, tn=%d: %w", result.BlockNumber(), result.Version().TxNum, result.Err)
		}

		p.blockResult.Receipts = append(p.blockResult.Receipts, receipt)

		if result.IsBlockEnd() {
			if result.BlockNumber() > 0 {
				chainReader := consensuschain.NewReader(cfg.ChainConfig, tx, cfg.BlockReader, logger)
				// End of block transaction in a block
				ibs := state.New(state.NewHistoryReaderV3())
				ibs.SetTxContext(txTask.BlockNumber(), txTask.TxIndex)
				syscall := func(contract common.Address, data []byte) ([]byte, error) {
					ret, logs, err := core.SysCallContract(contract, data, cfg.ChainConfig, ibs, txTask.Header, txTask.Engine, false /* constCall */, hooks)
					result.Logs = append(result.Logs, logs...)
					return ret, err
				}

				_, _, _, err := txTask.Engine.Finalize(cfg.ChainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, p.blockResult.Receipts, txTask.Withdrawals, chainReader, syscall, true /* skipReceiptsEval */, logger)
				if err != nil {
					result.Err = err
				}
			}

			p.blockResult.Complete = true
		}

		if err := consumer.Reduce(p.blockResult, result, tx); err != nil {
			return outputTxNum, false, err
		}

		if result.IsBlockEnd() {
			p.blockResult = &exec.BlockResult{}
			stopedAtBlockEnd = true
			if forceStopAtBlockEnd {
				break
			}
		}
	}
	return
}

func CustomTraceMapReduce(fromBlock, toBlock uint64, consumer TraceConsumer, ctx context.Context, tx kv.TemporalTx, cfg *ExecArgs, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("'CustomTraceMapReduce' paniced: %s, %s", rec, dbg.Stack())
			log.Warn("[StageCustomTrace]", "err", err)
		}
	}()

	br := cfg.BlockReader
	chainConfig := cfg.ChainConfig

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.BlockReader))

	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}
	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}

	// "Map-Reduce on history" is conflict-free - means we don't need "Retry" feature.
	// But still can use this data-type as simple queue.
	in := exec.NewQueueWithRetry(10_000)
	defer in.Close()

	var WorkerCount = estimate.AlmostAllCPUs()
	if cfg.Workers > 0 {
		WorkerCount = cfg.Workers
	}

	log.Info("[Receipt] batch start", "fromBlock", fromBlock, "toBlock", toBlock, "workers", cfg.Workers, "toTxNum", toTxNum)
	getHeaderFunc := func(hash common.Hash, number uint64) (h *types.Header, err error) {
		if tx != nil && WorkerCount == 1 {
			h, err = cfg.BlockReader.Header(ctx, tx, hash, number)
			if err != nil {
				return nil, err
			}
		} else {
			err = cfg.ChainDB.View(ctx, func(tx kv.Tx) error {
				h, err = cfg.BlockReader.Header(ctx, tx, hash, number)
				return err
			})

			if err != nil {
				return nil, err
			}
		}
		return h, nil
	}

	outTxNum := &atomic.Uint64{}
	outTxNum.Store(fromTxNum)

	ctx, cancleCtx := context.WithCancel(ctx)
	workers := NewHistoricalTraceWorkers(consumer, cfg, ctx, toTxNum, in, WorkerCount, outTxNum, logger)
	defer workers.Wait()

	workersExited := &atomic.Bool{}
	go func() {
		if err := workers.Wait(); err != nil {
			cancleCtx()
		}
		workersExited.Store(true)
	}()

	inputTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(1 * time.Second)
	defer logEvery.Stop()
	for blockNum := fromBlock; blockNum <= toBlock && !workersExited.Load(); blockNum++ {
		var b *types.Block
		b, err = BlockWithSenders(ctx, nil, tx, br, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) (common.Hash, error) {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.Engine, nil /* author */, chainConfig)

		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &exec.TxTask{
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				Header:          header,
				Uncles:          b.Uncles(),
				Txs:             txs,
				SkipAnalysis:    skipAnalysis,
				EvmBlockContext: blockContext,
				Withdrawals:     b.Withdrawals(),

				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: true,
				Tracer:           calltracer.NewCallTracer(&tracing.Hooks{}),
			}

			in.Add(ctx, txTask)
			inputTxNum++

			//select {
			//case <-logEvery.C:
			//	log.Info("[dbg] in", "in", in.Len())
			//default:
			//}
		}
	}
	in.Close() //no more work. no retries in map-reduce. means can close here.

	if err := workers.Wait(); err != nil {
		return fmt.Errorf("WorkersPool: %w", err)
	}

	return nil
}

func BlockWithSenders(ctx context.Context, db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if tx == nil {
		tx, err = db.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	b, err = blockReader.BlockByNumber(context.Background(), tx, blockNum)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return b, err
}
