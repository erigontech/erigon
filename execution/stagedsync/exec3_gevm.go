package stagedsync

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm/gevm"
)

func (se *serialExecutor) executeBlockGevm(ctx context.Context, tasks []exec.Task, isInitialCycle bool) (bool, error) {
	if se.taskExecMetrics == nil {
		se.taskExecMetrics = exec.NewWorkerMetrics()
	}
	if se.blockExecMetrics == nil {
		se.blockExecMetrics = newBlockExecMetrics()
	}
	defer func(t time.Time) {
		se.blockExecMetrics.BlockCount.Add(1)
		se.blockExecMetrics.Duration.Add(time.Since(t))
	}(time.Now())

	if len(tasks) == 0 {
		return true, nil
	}
	firstTask := tasks[0].(*exec.TxTask)
	startTxIndex := len(firstTask.Txs)
	for _, task := range tasks {
		txIndex := task.(*exec.TxTask).TxIndex
		if txIndex >= 0 && txIndex < len(firstTask.Txs) {
			startTxIndex = txIndex
			break
		}
	}
	hasTxTask := startTxIndex < len(firstTask.Txs)

	blockReceipts := make([]*types.Receipt, 0, len(firstTask.Txs))
	gasPool := protocol.NewGasPool(firstTask.BlockGasLimit(), se.cfg.chainConfig.GetMaxBlobGasPerBlock(firstTask.BlockTime()))
	blockExec := gevm.NewBlockExecutor(se.cfg.chainConfig, firstTask.Header, firstTask.EvmBlockContext, se.applyTx, se.doms)
	defer blockExec.Release()
	appliedGevmState := false
	debugTx := os.Getenv("GEVM_DEBUG_TX") == "1"

	for _, task := range tasks {
		txTask := task.(*exec.TxTask)
		txTask.Config = se.cfg.chainConfig
		txTask.Engine = se.cfg.engine
		txTask.ResetGasPool(gasPool)

		var result *exec.TxResult
		switch {
		case txTask.TxIndex == -1:
			if startTxIndex == 0 && (hasTxTask || len(txTask.Txs) == 0) && se.cfg.chainConfig.IsCancun(txTask.Header.Time) {
				blockExec.ApplyBeaconRoot(txTask.Header.ParentBeaconBlockRoot)
				appliedGevmState = true
			}
			if startTxIndex == 0 && (hasTxTask || len(txTask.Txs) == 0) && se.cfg.chainConfig.IsPrague(txTask.Header.Time) {
				if err := blockExec.StoreParentHash(txTask.Header); err != nil {
					return false, err
				}
				appliedGevmState = true
			}
			blockExec.CommitTx()
			result = &exec.TxResult{Task: txTask}
			se.onBlockStart(ctx, txTask.BlockNumber(), txTask.BlockHash())
		case txTask.IsBlockEnd():
			if txTask.BlockNumber() == 0 {
				stateWriter := state.NewWriter(se.doms.AsPutDel(se.applyTx), se.accumulator, txTask.TxNum)
				if err := blockExec.ApplyGenesisState(se.cfg.genesis, stateWriter); err != nil {
					return false, err
				}
				result = &exec.TxResult{Task: txTask}
				break
			}
			finalizeReceipts := blockReceipts
			if startTxIndex > 0 && len(txTask.Txs) > 0 {
				blockStartTxNum := firstTask.TxNum - uint64(firstTask.TxIndex)
				priorReceipts, priorErr := se.derivePriorReceiptsGevm(ctx, txTask, startTxIndex, blockStartTxNum)
				if priorErr != nil {
					se.logger.Warn(fmt.Sprintf("[%s] failed to reconstruct prior receipts for partial block", se.logPrefix),
						"block", txTask.BlockNumber(), "startTxIndex", startTxIndex, "err", priorErr)
				} else {
					finalizeReceipts = append(priorReceipts, blockReceipts...)
				}
			}
			if _, err := blockExec.FinalizeBlock(se.cfg.engine, se.cfg.chainConfig, txTask.Header, txTask.Uncles, finalizeReceipts, txTask.Withdrawals); err != nil {
				return false, fmt.Errorf("%w, txnIdx=%d, %w", rules.ErrInvalidBlock, txTask.TxIndex, err)
			}
			if txTask.BlockNumber() > 0 || txTask.Withdrawals != nil || se.cfg.chainConfig.IsPrague(txTask.Header.Time) {
				appliedGevmState = true
			}
			result = &exec.TxResult{Task: txTask}
			checkBloom := !se.cfg.vmConfig.StatelessExec && !se.cfg.vmConfig.NoReceipts
			checkReceipts := checkBloom && se.cfg.chainConfig.IsByzantium(txTask.BlockNumber())
			if txTask.BlockNumber() > 0 && startTxIndex == 0 && (hasTxTask || len(txTask.Txs) == 0) {
				if err := protocol.BlockPostValidation(se.blockGasUsed, se.blobGasUsed, checkReceipts, checkBloom, blockReceipts, txTask.Header, txTask.Txs, se.cfg.chainConfig, se.logger); err != nil {
					return false, fmt.Errorf("%w, txnIdx=%d, %w", rules.ErrInvalidBlock, txTask.TxIndex, err)
				}
			}
			stateWriter := state.NewWriter(se.doms.AsPutDel(se.applyTx), se.accumulator, txTask.TxNum)
			if appliedGevmState {
				if err := blockExec.ApplyState(stateWriter); err != nil {
					return false, err
				}
			}
			if se.cfg.notifications != nil && !isInitialCycle {
				se.cfg.notifications.RecentReceipts.Add(blockReceipts, txTask.Txs, txTask.Header)
			}
		default:
			msg, err := txTask.TxMessage()
			if err != nil {
				return false, fmt.Errorf("%w, txnIdx=%d, %v", rules.ErrInvalidBlock, txTask.TxIndex, err)
			}
			var callTracer *calltracer.CallTracer
			if hooks := txTask.TracingHooks(); hooks != nil {
				callTracer = calltracer.NewCallTracer(hooks)
			}
			out, err := blockExec.ExecuteTx(txTask.Tx(), msg, callTracer)
			if err != nil {
				return false, fmt.Errorf("%w, txnIdx=%d, %v", rules.ErrInvalidBlock, txTask.TxIndex, err)
			}
			result = &exec.TxResult{
				Task:            txTask,
				ExecutionResult: out.Result,
				Logs:            out.Logs,
				TraceFroms:      out.TraceFroms,
				TraceTos:        out.TraceTos,
			}
			if callTracer != nil {
				result.TraceFroms = callTracer.Froms()
				result.TraceTos = callTracer.Tos()
			}
			se.txCount++
			se.blockGasUsed += result.ExecutionResult.BlockRegularGasUsed
			if txTask.Tx() != nil {
				se.blobGasUsed += txTask.Tx().GetBlobGas()
			}
			var receipt *types.Receipt
			if len(blockReceipts) > 0 {
				receipt, err = result.CreateNextReceipt(blockReceipts[len(blockReceipts)-1])
			} else if txTask.TxIndex > 0 {
				cumGasUsed, _, logIndexAfterTx, err := rawtemporaldb.ReceiptAsOf(se.applyTx, txTask.TxNum-1)
				if err != nil {
					return false, err
				}
				receipt, err = result.CreateReceipt(txTask.TxIndex, cumGasUsed+result.ExecutionResult.ReceiptGasUsed, logIndexAfterTx)
			} else {
				receipt, err = result.CreateNextReceipt(nil)
			}
			if err != nil {
				return false, err
			}
			blockReceipts = append(blockReceipts, receipt)
			if debugTx {
				fmt.Printf("GEVM_TX block=%d idx=%d gas=%d cumulative=%d status=%d err=%v hash=%x\n", txTask.BlockNumber(), txTask.TxIndex, result.ExecutionResult.ReceiptGasUsed, receipt.CumulativeGasUsed, receipt.Status, result.Err, txTask.Tx().Hash())
			}
			appliedGevmState = true
			if hooks := result.TracingHooks(); hooks != nil && hooks.OnTxEnd != nil {
				hooks.OnTxEnd(receipt, result.Err)
			}
		}

		var applyReceipt *types.Receipt
		if txTask.TxIndex >= 0 && txTask.TxIndex-startTxIndex >= 0 && txTask.TxIndex-startTxIndex < len(blockReceipts) {
			applyReceipt = blockReceipts[txTask.TxIndex-startTxIndex]
		}
		if !txTask.HistoryExecution {
			if err := se.rs.ApplyStateWrites(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum, nil, nil, txTask.Rules(), nil); err != nil {
				return false, err
			}
			if err := se.rs.ApplyTxIndexes(se.applyTx, txTask.TxNum, applyReceipt, se.blobGasUsed, result.Logs, result.TraceFroms, result.TraceTos); err != nil {
				return false, err
			}
			if err := se.rs.CommitStepBoundary(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum); err != nil {
				return false, err
			}
		}

		se.doms.SetTxNum(txTask.TxNum)
		se.lastBlockResult = &blockResult{BlockNum: txTask.BlockNumber(), lastTxNum: txTask.TxNum}
		se.lastExecutedTxNum.Store(int64(txTask.TxNum))
		se.lastExecutedBlockNum.Store(int64(txTask.BlockNumber()))

		if txTask.IsBlockEnd() {
			se.executedGas.Add(int64(se.blockGasUsed))
			se.blockGasUsed = 0
			se.blockStateGasUsed = 0
			se.blobGasUsed = 0
			gasPool = nil
		}
	}
	return true, nil
}

func (se *serialExecutor) derivePriorReceiptsGevm(ctx context.Context, txTask *exec.TxTask, startTxIndex int, blockStartTxNum uint64) (types.Receipts, error) {
	if startTxIndex <= 0 {
		return nil, nil
	}
	blockHash := txTask.Header.Hash()
	blockNum := txTask.Header.Number.Uint64()
	cached := make(types.Receipts, 0, startTxIndex)
	allCached := true
	for i := 0; i < startTxIndex && i < len(txTask.Txs); i++ {
		receipt, ok, err := rawdb.ReadReceiptCacheV2(se.applyTx, rawdb.RCacheV2Query{
			BlockNum:      blockNum,
			BlockHash:     blockHash,
			TxnHash:       txTask.Txs[i].Hash(),
			TxNum:         blockStartTxNum + uint64(i),
			DontCalcBloom: true,
		})
		if err != nil || !ok {
			allCached = false
			break
		}
		cached = append(cached, receipt)
	}
	if allCached && len(cached) == startTxIndex {
		return cached, nil
	}

	reader := state.NewHistoryReaderV3(se.applyTx, blockStartTxNum)
	historyBlockCtx := txTask.EvmBlockContext
	historyBlockCtx.GetHash = protocol.GetHashFn(txTask.Header, func(hash common.Hash, number uint64) (*types.Header, error) {
		return se.cfg.blockReader.Header(ctx, se.applyTx, hash, number)
	})
	priorExec := gevm.NewBlockExecutorWithReader(se.cfg.chainConfig, txTask.Header, historyBlockCtx, reader)
	defer priorExec.Release()
	priorGp := protocol.NewGasPool(txTask.Header.GasLimit, se.cfg.chainConfig.GetMaxBlobGasPerBlock(txTask.Header.Time))
	receipts := make(types.Receipts, 0, startTxIndex)
	for i := 0; i < startTxIndex && i < len(txTask.Txs); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		task := *txTask
		task.ResetTx(blockStartTxNum+uint64(i), i)
		task.ResetGasPool(priorGp)
		msg, err := task.TxMessage()
		if err != nil {
			return nil, fmt.Errorf("derive prior GEVM receipt tx %d: %w", i, err)
		}
		out, err := priorExec.ExecuteTx(task.Tx(), msg, nil)
		if err != nil {
			return nil, fmt.Errorf("derive prior GEVM receipt tx %d: %w", i, err)
		}
		result := &exec.TxResult{
			Task:            &task,
			ExecutionResult: out.Result,
			Logs:            out.Logs,
			TraceFroms:      out.TraceFroms,
			TraceTos:        out.TraceTos,
		}
		var receipt *types.Receipt
		if len(receipts) > 0 {
			receipt, err = result.CreateNextReceipt(receipts[len(receipts)-1])
		} else {
			receipt, err = result.CreateNextReceipt(nil)
		}
		if err != nil {
			return nil, fmt.Errorf("derive prior GEVM receipt tx %d: %w", i, err)
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}
