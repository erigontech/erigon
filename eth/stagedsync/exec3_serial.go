package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	chaos_monkey "github.com/erigontech/erigon/tests/chaos-monkey"
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

func (se *serialExecutor) LogExecuted(tx kv.Tx) {
	se.progress.LogExecuted(tx, se.rs.StateV3, se)
}

func (se *serialExecutor) LogCommitted(tx kv.Tx, commitStart time.Time) {
	se.progress.LogCommitted(tx, commitStart, se.rs.StateV3, se)
}

func (se *serialExecutor) LogComplete(tx kv.Tx) {
	se.progress.LogComplete(tx, se.rs.StateV3, se)
}

func (se *serialExecutor) wait(ctx context.Context) error {
	return nil
}

func (se *serialExecutor) commit(ctx context.Context, execStage *StageState, tx kv.RwTx, asyncTxChan mdbx.TxApplyChan, useExternalTx bool) (kv.RwTx, time.Duration, error) {
	return se.txExecutor.commit(ctx, execStage, tx, useExternalTx, se.resetWorkers)
}

func (se *serialExecutor) resetWorkers(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.Tx) (err error) {

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
			se.applyTx, err = se.cfg.db.BeginRo(context.Background()) //nolint
			if err != nil {
				se.applyTx.Rollback()
				return err
			}
		}
	}

	se.worker.ResetTx(se.applyTx)
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
			mxExecGas.Add(float64(result.ExecutionResult.GasUsed))
			mxExecTransactions.Add(1)

			if txTask.Tx() != nil {
				se.blobGasUsed += txTask.Tx().GetBlobGas()
			}

			if txTask.IsBlockEnd() {
				//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
				// End of block transaction in a block
				ibs := state.New(state.NewReaderV3(se.rs.Domains().AsGetter(se.applyTx)))
				ibs.SetTxContext(txTask.BlockNumber(), txTask.TxIndex)
				syscall := func(contract common.Address, data []byte) ([]byte, error) {
					ret, err := core.SysCallContract(contract, data, se.cfg.chainConfig, ibs, txTask.Header, se.cfg.engine, false /* constCall */, se.hooks, vm.Config{})
					if err != nil {
						return nil, err
					}
					result.Logs = append(result.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
					return ret, err
				}

				chainReader := consensuschain.NewReader(se.cfg.chainConfig, se.applyTx, se.cfg.blockReader, se.logger)

				if se.isMining {
					_, txTask.Txs, blockReceipts, _, err = se.cfg.engine.FinalizeAndAssemble(
						se.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles,
						blockReceipts, txTask.Withdrawals, chainReader, syscall, nil, se.logger)
				} else {
					_, _, _, err = se.cfg.engine.Finalize(
						se.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles,
						blockReceipts, txTask.Withdrawals, chainReader, syscall, false, se.logger)
				}

				if !se.isMining && !se.inMemExec && startTxIndex == 0 && !isInitialCycle {
					// note this assumes the bloach reciepts is a fixed array shared by
					// all tasks - if that changes this will need to change - robably need to
					// add this to the executor
					se.cfg.notifications.RecentLogs.Add(blockReceipts)
				}
				checkReceipts := !se.cfg.vmConfig.StatelessExec && se.cfg.chainConfig.IsByzantium(txTask.BlockNumber()) && !se.cfg.vmConfig.NoReceipts && !se.isMining
				if txTask.BlockNumber() > 0 && startTxIndex == 0 { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
					if err := core.BlockPostValidation(se.gasUsed, se.blobGasUsed, checkReceipts, blockReceipts, txTask.Header, se.isMining, txTask.Txs, se.cfg.chainConfig, se.logger); err != nil {
						return fmt.Errorf("%w, txnIdx=%d, %v", consensus.ErrInvalidBlock, txTask.TxIndex, err) //same as in stage_exec.go
					}
				}

				stateWriter := state.NewWriter(se.doms.AsPutDel(se.applyTx), nil, txTask.TxNum)

				if err = ibs.MakeWriteSet(se.cfg.chainConfig.Rules(txTask.BlockNumber(), txTask.BlockTime()), stateWriter); err != nil {
					panic(err)
				}
			} else if txTask.TxIndex >= 0 {
				var prev *types.Receipt
				if txTask.TxIndex > 0 && txTask.TxIndex-startTxIndex > 0 {
					prev = blockReceipts[txTask.TxIndex-startTxIndex-1]
				} else {
					// TODO get the previous reciept from the DB
				}

				receipt, err := result.CreateReceipt(prev)
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
			}
			return false, nil
		}

		if !task.IsBlockEnd() {
			var receipt *types.Receipt
			if txTask.TxIndex >= 0 {
				receipt = blockReceipts[txTask.TxIndex-startTxIndex]
			}
			if err := rawtemporaldb.AppendReceipt(se.doms.AsPutDel(se.applyTx), receipt, se.blobGasUsed, txTask.TxNum); err != nil {
				return false, err
			}
		} else {
			if se.cfg.chainConfig.Bor != nil && txTask.TxIndex >= 1 {
				var lastReceipt *types.Receipt
				// get last receipt and store the last log index + 1
				if len(blockReceipts) >= txTask.TxIndex {
					lastReceipt = blockReceipts[txTask.TxIndex-1]
				}

				if lastReceipt == nil {
					if startTxIndex > 0 {
						// if we're in the startup block and the last tx has been skilled we'll
						// need to run it as a historic tx to recover its logs
						prevTask := *txTask
						prevTask.HistoryExecution = true
						prevTask.ResetTx(txTask.TxNum-1, txTask.TxIndex-1)
						result := se.worker.RunTxTaskNoLock(&prevTask)
						if result.Err != nil {
							return false, result.Err
						}
						lastReceipt, err = result.CreateReceipt(nil)
						if err != nil {
							return false, err
						}
					} else {
						return false, fmt.Errorf("receipt is nil but should be populated, txIndex=%d, block=%d", txTask.TxIndex-1, txTask.BlockNumber())
					}
				}
				if len(lastReceipt.Logs) > 0 {
					firstIndex := lastReceipt.Logs[len(lastReceipt.Logs)-1].Index + 1
					receipt := types.Receipt{
						CumulativeGasUsed:        lastReceipt.CumulativeGasUsed,
						FirstLogIndexWithinBlock: uint32(firstIndex),
					}
					if err := rawtemporaldb.AppendReceipt(se.doms.AsPutDel(se.applyTx), &receipt, se.blobGasUsed, txTask.TxNum); err != nil {
						return false, err
					}
				}
			}
		}

		if err := se.rs.ApplyState4(ctx, se.applyTx, txTask.BlockNumber(), txTask.TxNum, nil,
			txTask.BalanceIncreaseSet, blockReceipts, result.Logs, result.TraceFroms, result.TraceTos,
			se.cfg.chainConfig, se.cfg.chainConfig.Rules(txTask.BlockNumber(), txTask.BlockTime()), txTask.HistoryExecution); err != nil {
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
