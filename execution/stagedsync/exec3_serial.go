package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
)

type serialExecutor struct {
	txExecutor
	// outputs
	txCount         uint64
	gasUsed         uint64
	blobGasUsed     uint64
	lastBlockResult *blockResult
	worker          *exec3.Worker
}

func (se *serialExecutor) LogExecuted() {
	se.progress.LogExecuted(se.rs.StateV3, se)
}

func (se *serialExecutor) LogCommitted(commitStart time.Time, committedBlocks uint64, committedTransactions uint64, committedGas uint64, stepsInDb float64, lastProgress commitment.CommitProgress) {
	se.committedGas += int64(committedGas)
	se.txExecutor.lastCommittedBlockNum += committedBlocks
	se.txExecutor.lastCommittedTxNum += committedTransactions
	se.progress.LogCommitted(se.rs.StateV3, se, commitStart, stepsInDb, lastProgress)
}

func (se *serialExecutor) LogComplete(stepsInDb float64) {
	se.progress.LogComplete(se.rs.StateV3, se, stepsInDb)
}

func (se *serialExecutor) wait(ctx context.Context) error {
	return nil
}

func (se *serialExecutor) lastCommittedBlockNum() uint64 {
	return se.txExecutor.lastCommittedBlockNum
}

func (se *serialExecutor) lastCommittedTxNum() uint64 {
	return se.txExecutor.lastCommittedTxNum
}

func (se *serialExecutor) commit(ctx context.Context, execStage *StageState, tx kv.TemporalRwTx, asyncTxChan mdbx.TxApplyChan, useExternalTx bool) (kv.TemporalRwTx, time.Duration, error) {
	return se.txExecutor.commit(ctx, execStage, tx, useExternalTx, se.resetWorkers)
}

func (se *serialExecutor) resetWorkers(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.TemporalTx) (err error) {

	if se.worker == nil {
		se.taskExecMetrics = exec3.NewWorkerMetrics()
		se.worker = exec3.NewWorker(context.Background(), false, se.taskExecMetrics,
			se.cfg.db, nil, se.cfg.blockReader, se.cfg.chainConfig, se.cfg.genesis, nil, se.cfg.engine, se.cfg.dirs, se.logger)
	}

	if se.applyTx != applyTx {
		if se.applyTx != nil {
			se.applyTx.Rollback()
		}

		if applyTx != nil {
			se.applyTx = applyTx
		} else {
			applyTx, err := se.cfg.db.BeginRo(ctx) //nolint
			if err != nil {
				applyTx.Rollback()
				return err
			}
			se.applyTx = applyTx.(kv.TemporalTx)
		}
	}

	se.worker.ResetState(rs, se.applyTx, nil, nil, nil)

	return nil
}

func (se *serialExecutor) execute(ctx context.Context, tasks []exec.Task, isInitialCycle bool, profile bool) (cont bool, err error) {
	blockReceipts := make([]*types.Receipt, 0, len(tasks))
	var startTxIndex int

	if len(tasks) > 0 {
		// During the first block execution, we may have half-block data in the snapshots.
		// Thus, we need to skip the first txs in the block, however, this causes the GasUsed to be incorrect.
		// So we need to skip that check for the first block, if we find half-executed data (startTxIndex>0).
		startTxIndex = tasks[0].(*exec.TxTask).TxIndex
		if startTxIndex < 0 {
			startTxIndex = 0
		}
	}

	var gasPool *core.GasPool
	for _, task := range tasks {
		txTask := task.(*exec.TxTask)

		if gasPool == nil {
			gasPool = core.NewGasPool(task.BlockGasLimit(), se.cfg.chainConfig.GetMaxBlobGasPerBlock(tasks[0].BlockTime()))
		}

		txTask.ResetGasPool(gasPool)
		txTask.Config = se.cfg.chainConfig
		txTask.Engine = se.cfg.engine

		result := se.worker.RunTxTask(txTask)

		if err := func() error {
			if errors.Is(result.Err, context.Canceled) {
				return result.Err
			}
			if result.Err != nil {
				return fmt.Errorf("%w, txnIdx=%d, %v", consensus.ErrInvalidBlock, txTask.TxIndex, result.Err) //same as in stage_exec.go
			}

			se.txCount++
			se.gasUsed += result.ExecutionResult.GasUsed
			mxExecTransactions.Add(1)

			if txTask.Tx() != nil {
				se.blobGasUsed += txTask.Tx().GetBlobGas()
			}

			if txTask.IsBlockEnd() && txTask.BlockNumber() > 0 {
				//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
				// End of block transaction in a block
				ibs := state.New(state.NewReaderV3(se.rs.Domains().AsGetter(se.applyTx)))
				ibs.SetTxContext(txTask.BlockNumber(), txTask.TxIndex)
				syscall := func(contract common.Address, data []byte) ([]byte, error) {
					ret, err := core.SysCallContract(contract, data, se.cfg.chainConfig, ibs, txTask.Header, se.cfg.engine, false /* constCall */, *se.cfg.vmConfig)
					if err != nil {
						return nil, err
					}
					result.Logs = append(result.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
					return ret, err
				}

				chainReader := consensuschain.NewReader(se.cfg.chainConfig, se.applyTx, se.cfg.blockReader, se.logger)

				if se.isMining {
					_, _, err = se.cfg.engine.FinalizeAndAssemble(
						se.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles,
						blockReceipts, txTask.Withdrawals, chainReader, syscall, nil, se.logger)
				} else {
					_, err = se.cfg.engine.Finalize(
						se.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles,
						blockReceipts, txTask.Withdrawals, chainReader, syscall, false, se.logger)
				}

				if err != nil {
					return fmt.Errorf("%w, txnIdx=%d, %w", consensus.ErrInvalidBlock, txTask.TxIndex, err)
				}

				if !se.isMining && startTxIndex == 0 && !isInitialCycle {
					se.cfg.notifications.RecentLogs.Add(blockReceipts)
				}
				checkReceipts := !se.cfg.vmConfig.StatelessExec && se.cfg.chainConfig.IsByzantium(txTask.BlockNumber()) && !se.cfg.vmConfig.NoReceipts && !se.isMining
				if txTask.BlockNumber() > 0 && startTxIndex == 0 {
					//Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
					if err := core.BlockPostValidation(se.gasUsed, se.blobGasUsed, checkReceipts, blockReceipts, txTask.Header, se.isMining, txTask.Txs, se.cfg.chainConfig, se.logger); err != nil {
						return fmt.Errorf("%w, txnIdx=%d, %w", consensus.ErrInvalidBlock, txTask.TxIndex, err) //same as in stage_exec.go
					}
				}

				stateWriter := state.NewWriter(se.doms.AsPutDel(se.applyTx), nil, txTask.TxNum)

				if err = ibs.MakeWriteSet(txTask.Rules(), stateWriter); err != nil {
					panic(err)
				}
			} else if txTask.TxIndex >= 0 {
				var prev *types.Receipt
				if txTask.TxIndex > 0 && txTask.TxIndex-startTxIndex > 0 {
					prev = blockReceipts[txTask.TxIndex-startTxIndex-1]
				} else {
					// TODO get the previous reciept from the DB
				}

				receipt, err := result.CreateNextReceipt(prev)
				if err != nil {
					return err
				}
				blockReceipts = append(blockReceipts, receipt)

				if hooks := result.TracingHooks(); hooks != nil && hooks.OnTxEnd != nil {
					hooks.OnTxEnd(receipt, result.Err)
				}
			} else {
				se.onBlockStart(ctx, txTask.BlockNumber(), txTask.BlockHash())
			}

			if se.cfg.syncCfg.ChaosMonkey && se.enableChaosMonkey {
				chaosErr := chaos_monkey.ThrowRandomConsensusError(false, txTask.TxIndex, se.cfg.badBlockHalt, result.Err)
				if chaosErr != nil {
					log.Warn("Monkey in a consensus")
					return chaosErr
				}
			}
			return nil
		}(); err != nil {
			if errors.Is(err, context.Canceled) {
				return false, err
			}
			se.logger.Warn(fmt.Sprintf("[%s] Execution failed", se.logPrefix),
				"block", txTask.BlockNumber(), "txNum", txTask.TxNum, "header-hash", txTask.Header.Hash().String(), "err", err, "inMem", se.inMemExec)
			if se.cfg.hd != nil && se.cfg.hd.POSSync() && errors.Is(err, consensus.ErrInvalidBlock) {
				se.cfg.hd.ReportBadHeaderPoS(txTask.Header.Hash(), txTask.Header.ParentHash)
			}
			if se.cfg.badBlockHalt {
				return false, err
			}
			if errors.Is(err, consensus.ErrInvalidBlock) {
				if se.u != nil {
					if err := se.u.UnwindTo(txTask.BlockNumber()-1, BadBlock(txTask.Header.Hash(), err), se.applyTx); err != nil {
						return false, err
					}
				}
			} else {
				if se.u != nil {
					if err := se.u.UnwindTo(txTask.BlockNumber()-1, ExecUnwind, se.applyTx); err != nil {
						return false, err
					}
				}
				return false, err
			}
			return false, nil
		}

		var logIndexAfterTx uint32
		var cumGasUsed uint64
		if !txTask.IsBlockEnd() {
			if txTask.TxIndex >= 0 {
				receipt := blockReceipts[txTask.TxIndex-startTxIndex]
				if receipt != nil {
					logIndexAfterTx = receipt.FirstLogIndexWithinBlock + uint32(len(result.Logs))
					cumGasUsed = receipt.CumulativeGasUsed
				}
			}
		} else {
			if se.cfg.chainConfig.Bor != nil && txTask.TxIndex >= 1 {
				var lastReceipt *types.Receipt
				// get last receipt and store the last log index + 1
				if len(blockReceipts) >= txTask.TxIndex-startTxIndex {
					lastReceipt = blockReceipts[txTask.TxIndex-startTxIndex-1]
				}

				if lastReceipt == nil {
					if startTxIndex > 0 {
						// if we're in the startup block and the last tx has been skipped we'll
						// need to run it as a historic tx to recover its logs
						prevTask := *txTask
						prevTask.HistoryExecution = true
						prevTask.ResetTx(txTask.TxNum-1, txTask.TxIndex-1)
						result := se.worker.RunTxTaskNoLock(&prevTask)
						if result.Err != nil {
							return false, fmt.Errorf("error while finding last receipt: %w", result.Err)
						}
						var cumulativeGasUsed uint64
						var logIndexAfterTx uint32
						if txTask.TxIndex > 1 {
							cumulativeGasUsed, _, logIndexAfterTx, err = rawtemporaldb.ReceiptAsOf(se.applyTx, txTask.TxNum-2)
							if err != nil {
								return false, err
							}
						}
						lastReceipt, err = result.CreateReceipt(txTask.TxIndex-1,
							cumulativeGasUsed+result.ExecutionResult.GasUsed, logIndexAfterTx)
						if err != nil {
							return false, err
						}
					} else {
						return false, fmt.Errorf("receipt is nil but should be populated, txIndex=%d, block=%d", txTask.TxIndex-1, txTask.BlockNumber())
					}
				}
				if len(lastReceipt.Logs) > 0 {
					firstIndex := lastReceipt.Logs[len(lastReceipt.Logs)-1].Index + 1
					logIndexAfterTx = uint32(firstIndex) + uint32(len(result.Logs))
					cumGasUsed = lastReceipt.CumulativeGasUsed
				}
			}
		}

		if !txTask.HistoryExecution {
			if rawtemporaldb.ReceiptStoresFirstLogIdx(se.applyTx) {
				logIndexAfterTx -= uint32(len(result.Logs))
			}
			if err := rawtemporaldb.AppendReceipt(se.doms.AsPutDel(se.applyTx), logIndexAfterTx, cumGasUsed, se.blobGasUsed, txTask.TxNum); err != nil {
				return false, err
			}
		}

		var applyReceipt *types.Receipt
		if txTask.TxIndex >= 0 && txTask.TxIndex < len(blockReceipts) {
			applyReceipt = blockReceipts[txTask.TxIndex]
		}

		if err := se.rs.ApplyTxState(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum, state.StateUpdates{},
			txTask.BalanceIncreaseSet, applyReceipt, result.Logs, result.TraceFroms, result.TraceTos,
			se.cfg.chainConfig, txTask.Rules(), txTask.HistoryExecution); err != nil {
			return false, err
		}

		se.doms.SetTxNum(txTask.TxNum)
		se.doms.SetBlockNum(txTask.BlockNumber())
		se.lastBlockResult = &blockResult{
			BlockNum:  txTask.BlockNumber(),
			lastTxNum: txTask.TxNum,
		}
		se.lastExecutedTxNum.Store(int64(txTask.TxNum))
		se.lastExecutedBlockNum.Store(int64(txTask.BlockNumber()))

		if task.IsBlockEnd() {
			se.executedGas.Add(int64(se.gasUsed))
			se.gasUsed = 0
			se.blobGasUsed = 0
			gasPool = nil
		}
	}

	return true, nil
}
