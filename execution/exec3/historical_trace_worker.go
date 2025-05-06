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
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
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
	vmConfig  *vm.Config
}

type TraceConsumer struct {
	//Reduce receiving results of execution. They are sorted and have no gaps.
	Reduce func(task *exec.Result, tx kv.TemporalTx) error
}

func NewHistoricalTraceWorker(
	consumer TraceConsumer,
	in *exec.QueueWithRetry,
	out *exec.ResultsQueue,
	background bool,

	ctx context.Context,
	execArgs *ExecArgs,
	logger log.Logger,
) *HistoricalTraceWorker {
	stateReader := state.NewHistoryReaderV3()
	ie := &HistoricalTraceWorker{
		consumer: consumer,
		in:       in,
		out:      out,

		execArgs: execArgs,

		stateReader: stateReader,
		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, execArgs.ChainConfig, vm.Config{}),
		vmConfig:    &vm.Config{},
		ibs:         state.New(stateReader),
		background:  background,
		ctx:         ctx,
		logger:      logger,
		taskGasPool: new(core.GasPool),
	}
	ie.ibs = state.New(ie.stateReader)

	return ie
}

func (rw *HistoricalTraceWorker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("HistoricalTraceWorker panic: %s, %s", rec, dbg.Stack())
		}
	}()
	defer rw.evm.JumpDestCache.LogStats()
	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		result := rw.RunTxTask(txTask.(*exec.TxTask))
		if err := rw.out.Add(rw.ctx, result); err != nil {
			return err
		}
	}
	return nil
}

func (rw *HistoricalTraceWorker) RunTxTask(txTask *exec.TxTask) *exec.Result {
	var result = exec.Result{
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

	rw.stateWriter = state.NewNoopWriter()

	rw.ibs.Reset()
	ibs := rw.ibs

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
		syscall := func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			ret, logs, err := core.SysCallContract(contract, data, rw.execArgs.ChainConfig, ibs, header, rw.execArgs.Engine, constCall /* constCall */, hooks)
			result.Logs = append(result.Logs, logs...)
			return ret, err
		}
		rw.execArgs.Engine.Initialize(rw.execArgs.ChainConfig, rw.chain, header, ibs, syscall, rw.logger, nil)
		result.Err = ibs.FinalizeTx(rules, noop)
	case txTask.IsBlockEnd():
		if txTask.BlockNumber() == 0 {
			break
		}

		// End of block transaction in a block
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			ret, logs, err := core.SysCallContract(contract, data, rw.execArgs.ChainConfig, ibs, header, rw.execArgs.Engine, false /* constCall */, hooks)
			result.Logs = append(result.Logs, logs...)
			return ret, err
		}

		_, _, _, err := rw.execArgs.Engine.Finalize(rw.execArgs.ChainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, result.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, true /* skipReceiptsEval */, rw.logger)
		if err != nil {
			result.Err = err
		}
	default:
		result.Err = func() error {
			rw.taskGasPool.Reset(txTask.Tx().GetGasLimit(), rw.execArgs.ChainConfig.GetMaxBlobGasPerBlock(header.Time))
			//rw.callTracer.Reset()
			rw.vmConfig.SkipAnalysis = txTask.SkipAnalysis
			rw.vmConfig.Tracer = hooks
			ibs.SetTxContext(txTask.BlockNumber(), txTask.TxIndex)
			msg, err := txTask.TxMessage()
			msg.SetCheckNonce(!rw.vmConfig.StatelessExec)

			txContext := core.NewEVMTxContext(msg)
			if rw.vmConfig.TraceJumpDest {
				txContext.TxHash = txTask.TxHash()
			}
			rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, txContext, ibs, *rw.vmConfig, rules)
			if hooks != nil && hooks.OnTxStart != nil {
				hooks.OnTxStart(rw.evm.GetVMContext(), txTask.Tx(), msg.From())
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
		return doHistoryReduce(consumer, cfg.ChainDB, ctx, toTxNum, outputTxNum, rws)
	})
	return g
}

func doHistoryReduce(consumer TraceConsumer, db kv.TemporalRoDB, ctx context.Context, toTxNum uint64, outputTxNum *atomic.Uint64, rws *exec.ResultsQueue) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for outputTxNum.Load() <= toTxNum {
		_, err = rws.DrainNonBlocking(ctx)
		if err != nil {
			return err
		}

		processedTxNum, _, err := processResultQueueHistorical(&consumer, rws, outputTxNum.Load(), tx, true)
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
		workers[i] = NewHistoricalTraceWorker(consumer, in, rws, true, ctx, cfg, logger)
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

func processResultQueueHistorical(consumer *TraceConsumer, rws *exec.ResultsQueue, outputTxNumIn uint64, tx kv.TemporalTx, forceStopAtBlockEnd bool) (outputTxNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := rws.Iter()

	outputTxNum = outputTxNumIn
	for rwsIt.Has(outputTxNum) {
		result := rwsIt.PopNext()
		outputTxNum++
		stopedAtBlockEnd = result.IsBlockEnd()

		hooks := result.Tracer.TracingHooks()
		if result.Err != nil {
			if hooks != nil && hooks.OnTxEnd != nil {
				hooks.OnTxEnd(nil, err)
			}
			return outputTxNum, false, fmt.Errorf("bn=%d, tn=%d: %w", result.BlockNumber(), result.TxNum(), result.Err)
		}

		result.CreateReceipt(tx)
		if hooks != nil && hooks.OnTxEnd != nil {
			hooks.OnTxEnd(result.Receipt, nil)
		}
		if err := consumer.Reduce(result, tx); err != nil {
			return outputTxNum, false, err
		}

		if forceStopAtBlockEnd && result.IsBlockEnd() {
			break
		}
	}
	return
}

func CustomTraceMapReduce(fromBlock, toBlock uint64, consumer TraceConsumer, ctx context.Context, tx kv.TemporalTx, cfg *ExecArgs, logger log.Logger) (err error) {
	br := cfg.BlockReader
	chainConfig := cfg.ChainConfig
	if chainConfig.ChainName == networkname.Gnosis {
		panic("gnosis consensus doesn't support parallel exec yet: https://github.com/erigontech/erigon/issues/12054")
	}

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

	// If db is empty, and have only files - then limit by files progress. Because files can have half-block progress
	ac := tx.AggTx().(*state2.AggregatorRoTx)
	stepSize := ac.StepSize()
	if ac.DbgDomain(kv.AccountsDomain).DbgMaxTxNumInDB(tx) > 1 {
		toTxNum = min(toTxNum, ac.DbgDomain(kv.AccountsDomain).FirstStepNotInFiles()*stepSize)
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

	workers := NewHistoricalTraceWorkers(consumer, cfg, ctx, toTxNum, in, WorkerCount, outTxNum, logger)
	defer workers.Wait()

	workersExited := &atomic.Bool{}
	go func() {
		workers.Wait()
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
		b, err = blockWithSenders(ctx, nil, tx, br, blockNum)
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
				BlockReceipts:    blockReceipts,
				Tracer:           calltracer.NewCallTracer(&tracing.Hooks{}),
			}

			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					return err
				}
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

func blockWithSenders(ctx context.Context, db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
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
	for _, txn := range b.Transactions() {
		_ = txn.Hash()
	}
	return b, err
}
