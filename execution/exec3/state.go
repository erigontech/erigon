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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

var noop = state.NewNoopWriter()

type Worker struct {
	lock        sync.Locker
	logger      log.Logger
	chainDb     kv.RoDB
	chainTx     kv.TemporalTx
	background  bool // if true - worker does manage RoTx (begin/rollback) in .ResetTx()
	blockReader services.FullBlockReader
	in          *state.QueueWithRetry
	rs          *state.ParallelExecutionState
	stateWriter *state.Writer
	stateReader state.ResettableStateReader
	historyMode bool // if true - stateReader is HistoryReaderV3, otherwise it's state reader
	chainConfig *chain.Config

	ctx      context.Context
	engine   consensus.Engine
	genesis  *types.Genesis
	resultCh *state.ResultsQueue
	chain    consensus.ChainReader

	callTracer  *calltracer.CallTracer
	taskGasPool *core.GasPool
	hooks       *tracing.Hooks

	evm   *vm.EVM
	ibs   *state.IntraBlockState
	vmCfg vm.Config

	dirs datadir.Dirs

	isMining bool
}

func NewWorker(lock sync.Locker, logger log.Logger, hooks *tracing.Hooks, ctx context.Context, background bool, chainDb kv.RoDB, in *state.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, results *state.ResultsQueue, engine consensus.Engine, dirs datadir.Dirs, isMining bool) *Worker {
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

		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),
		callTracer:  calltracer.NewCallTracer(hooks),
		taskGasPool: new(core.GasPool),
		hooks:       hooks,

		dirs: dirs,

		isMining: isMining,
	}
	w.taskGasPool.AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(0))
	w.vmCfg = vm.Config{Tracer: w.callTracer.Tracer().Hooks}
	w.ibs = state.New(w.stateReader)
	return w
}

func (rw *Worker) LogLRUStats() { rw.evm.Config().JumpDestCache.LogStats() }

func (rw *Worker) ResetState(rs *state.ParallelExecutionState, accumulator *shards.Accumulator) {
	rw.rs = rs
	if rw.background {
		rw.SetReader(state.NewReaderParallelV3(rs.Domains()))
	} else {
		rw.SetReader(state.NewReaderV3(rs.TemporalGetter()))
	}
	rw.stateWriter = state.NewWriter(rs.TemporalPutDel(), accumulator, 0)
}

func (rw *Worker) SetGaspool(gp *core.GasPool) {
	rw.taskGasPool = gp
}

func (rw *Worker) Tx() kv.TemporalTx { return rw.chainTx }
func (rw *Worker) DiscardReadList()  { rw.stateReader.DiscardReadList() }
func (rw *Worker) ResetTx(chainTx kv.Tx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
		rw.chainTx = nil
	}
	if chainTx != nil {
		rw.chainTx = chainTx.(kv.TemporalTx)
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.chainConfig, rw.chainTx, rw.blockReader, rw.logger)
	}
}

func (rw *Worker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("exec3.Worker panic: %s, %s", rec, dbg.Stack())
		}
	}()

	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		//fmt.Println("RTX", txTask.BlockNum, txTask.TxIndex, txTask.TxNum, txTask.Final)
		rw.RunTxTask(txTask, rw.isMining)
		if err := rw.resultCh.Add(rw.ctx, txTask); err != nil {
			return err
		}
	}
	return nil
}

func (rw *Worker) RunTxTask(txTask *state.TxTask, isMining bool) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.RunTxTaskNoLock(txTask, isMining, false)
}

// Needed to set history reader when need to offset few txs from block beginning and does not break processing,
// like compute gas used for block and then to set state reader to continue processing on latest data.
func (rw *Worker) SetReader(reader state.ResettableStateReader) {
	rw.stateReader = reader
	rw.stateReader.SetTx(rw.Tx())
	rw.ibs.Reset()
	rw.ibs = state.New(rw.stateReader)

	switch reader.(type) {
	case *state.HistoryReaderV3:
		rw.historyMode = true
	case *state.ReaderV3:
		rw.historyMode = false
	default:
		rw.historyMode = false
		//fmt.Printf("[worker] unknown reader %T: historyMode is set to disabled\n", reader)
	}
}

func (rw *Worker) RunTxTaskNoLock(txTask *state.TxTask, isMining, skipPostEvaluation bool) {
	if txTask.HistoryExecution && !rw.historyMode {
		// in case if we cancelled execution and commitment happened in the middle of the block, we have to process block
		// from the beginning until committed txNum and only then disable history mode.
		// Needed to correctly evaluate spent gas and other things.
		rw.SetReader(state.NewHistoryReaderV3())
	} else if !txTask.HistoryExecution && rw.historyMode {
		if rw.background {
			rw.SetReader(state.NewReaderParallelV3(rw.rs.Domains()))
		} else {
			rw.SetReader(state.NewReaderV3(rw.rs.TemporalGetter()))
		}
	}
	if rw.background && rw.chainTx == nil {
		var err error
		if rw.chainTx, err = rw.chainDb.(kv.TemporalRoDB).BeginTemporalRo(rw.ctx); err != nil {
			panic(err)
		}
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.chainConfig, rw.chainTx, rw.blockReader, rw.logger)
	}
	txTask.Error = nil

	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateWriter.SetTxNum(txTask.TxNum)
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
			_, ibs, err = genesiswrite.GenesisToBlock(rw.genesis, rw.dirs, rw.logger)
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
			ret, err := core.SysCallContract(contract, data, cc, ibs, header, rw.engine, constCall /* constCall */, rw.vmCfg)
			return ret, err
		}
		rw.engine.Initialize(cc, rw.chain, header, ibs, syscall, rw.logger, hooks)
		txTask.Error = ibs.FinalizeTx(rules, noop)
	case txTask.Final:
		if txTask.BlockNum == 0 {
			break
		}

		rw.callTracer.Reset()
		ibs.SetTxContext(txTask.BlockNum, txTask.TxIndex)

		// End of block transaction in a block
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			ret, err := core.SysCallContract(contract, data, cc, ibs, header, rw.engine, false /* constCall */, rw.vmCfg)
			txTask.Logs = append(txTask.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
			return ret, err
		}

		if isMining {
			_, _, err = rw.engine.FinalizeAndAssemble(cc, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, nil, rw.logger)
		} else {
			_, err = rw.engine.Finalize(cc, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, skipPostEvaluation, rw.logger)
		}
		if err != nil {
			txTask.Error = err
		} else {
			txTask.TraceFroms = rw.callTracer.Froms()
			txTask.TraceTos = rw.callTracer.Tos()
			if txTask.TraceFroms == nil {
				txTask.TraceFroms = map[common.Address]struct{}{}
			}
			if txTask.TraceTos == nil {
				txTask.TraceTos = map[common.Address]struct{}{}
			}
			txTask.TraceTos[txTask.Coinbase] = struct{}{}
			for _, uncle := range txTask.Uncles {
				txTask.TraceTos[uncle.Coinbase] = struct{}{}
			}
		}
	default:
		rw.callTracer.Reset()
		rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
		ibs.SetTxContext(txTask.BlockNum, txTask.TxIndex)
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
			txTask.GasUsed = applyRes.GasUsed
			// Update the state with pending changes
			ibs.SoftFinalise()
			//txTask.Error = ibs.FinalizeTx(rules, noop)
			txTask.Logs = ibs.GetRawLogs(txTask.TxIndex)
			txTask.TraceFroms = rw.callTracer.Froms()
			txTask.TraceTos = rw.callTracer.Tos()

			txTask.CreateReceipt(rw.Tx())
			if hooks != nil && hooks.OnTxEnd != nil {
				hooks.OnTxEnd(txTask.BlockReceipts[txTask.TxIndex], nil)
			}
		}

	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if txTask.Error == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		//for addr, bal := range txTask.BalanceIncreaseSet {
		//	fmt.Printf("BalanceIncreaseSet [%x]=>[%d]\n", addr, &bal)
		//}
		if err = ibs.MakeWriteSet(rules, rw.stateWriter); err != nil {
			panic(err)
		}
		txTask.ReadLists = rw.stateReader.ReadSet()
		txTask.WriteLists = rw.stateWriter.WriteSet()
		txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = rw.stateWriter.PrevAndDels()
	}
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
			rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(txTask.TxAsMessage), rw.ibs, rw.vmCfg, txTask.Rules)
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
		log.Info("✅[aa] validated AA bundle", "len", endIdx-startIdx+1)

		txTask.ValidationResults = validationResults
	}

	if len(txTask.ValidationResults) == 0 {
		txTask.Error = fmt.Errorf("found RIP-7560 but no remaining validation results, txIndex %d", txTask.TxIndex)
		return
	}

	aaTxn := txTask.Tx.(*types.AccountAbstractionTransaction) // type cast checked earlier
	validationRes := txTask.ValidationResults[0]
	txTask.ValidationResults = txTask.ValidationResults[1:]

	rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(txTask.TxAsMessage), rw.ibs, rw.vmCfg, txTask.Rules)
	status, gasUsed, err := aa.ExecuteAATransaction(aaTxn, validationRes.PaymasterContext, validationRes.GasUsed, rw.taskGasPool, rw.evm, txTask.Header, rw.ibs)
	if err != nil {
		txTask.Error = err
		return
	}

	txTask.Failed = status != 0
	txTask.GasUsed = gasUsed
	// Update the state with pending changes
	rw.ibs.SoftFinalise()
	txTask.Logs = rw.ibs.GetLogs(txTask.TxIndex, txTask.Tx.Hash(), txTask.BlockNum, txTask.BlockHash)
	txTask.TraceFroms = rw.callTracer.Froms()
	txTask.TraceTos = rw.callTracer.Tos()
	txTask.CreateReceipt(rw.Tx())

	log.Info("🚀[aa] executed AA bundle transaction", "txIndex", txTask.TxIndex, "status", status, "gasUsed", gasUsed)
}

func NewWorkersPool(lock sync.Locker, accumulator *shards.Accumulator, logger log.Logger, hooks *tracing.Hooks, ctx context.Context, background bool, chainDb kv.RoDB, rs *state.ParallelExecutionState, in *state.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, engine consensus.Engine, workerCount int, dirs datadir.Dirs, isMining bool) (reconWorkers []*Worker, applyWorker *Worker, rws *state.ResultsQueue, clear func(), wait func()) {
	reconWorkers = make([]*Worker, workerCount)

	resultChSize := workerCount * 8
	rws = state.NewResultsQueue(resultChSize, workerCount) // workerCount * 4
	{
		// we all errors in background workers (except ctx.Cancel), because applyLoop will detect this error anyway.
		// and in applyLoop all errors are critical
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < workerCount; i++ {
			reconWorkers[i] = NewWorker(lock, logger, hooks, ctx, background, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, isMining)
			reconWorkers[i].ResetState(rs, accumulator)
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
			cancel()
			g.Wait()
			for _, w := range reconWorkers {
				w.ResetTx(nil)
			}
			//applyWorker.ResetTx(nil)
		}
	}
	applyWorker = NewWorker(lock, logger, hooks, ctx, false, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, isMining)

	return reconWorkers, applyWorker, rws, clear, wait
}
