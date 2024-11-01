package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/shards"
)

type serialExecutor struct {
	cfg                ExecuteBlockCfg
	execStage          *StageState
	agg                *state2.Aggregator
	rs                 *state.StateV3
	doms               *state2.SharedDomains
	accumulator        *shards.Accumulator
	u                  Unwinder
	isMining           bool
	inMemExec          bool
	skipPostEvaluation bool
	applyTx            kv.RwTx
	worker             *exec3.Worker
	outputTxNum        *atomic.Uint64
	logger             log.Logger

	// outputs
	txCount     uint64
	usedGas     uint64
	blobGasUsed uint64
}

func (se *serialExecutor) wait() error {
	return nil
}

func (se *serialExecutor) status(ctx context.Context, commitThreshold uint64) error {
	return nil
}

func (se *serialExecutor) execute(ctx context.Context, tasks []*state.TxTask) (cont bool, err error) {
	for _, txTask := range tasks {
		if txTask.Error != nil {
			return false, nil
		}

		se.worker.RunTxTaskNoLock(txTask, se.isMining)
		if err := func() error {
			if errors.Is(txTask.Error, context.Canceled) {
				return txTask.Error
			}
			if txTask.Error != nil {
				return fmt.Errorf("%w, txnIdx=%d, %v", consensus.ErrInvalidBlock, txTask.TxIndex, txTask.Error) //same as in stage_exec.go
			}

			se.txCount++
			se.usedGas += txTask.UsedGas
			mxExecGas.Add(float64(txTask.UsedGas))
			mxExecTransactions.Add(1)

			if txTask.Tx != nil {
				se.blobGasUsed += txTask.Tx.GetBlobGas()
			}

			txTask.CreateReceipt(se.applyTx)

			if txTask.Final {
				if !se.isMining && !se.inMemExec && !se.skipPostEvaluation && !se.execStage.CurrentSyncCycle.IsInitialCycle {
					// note this assumes the bloach reciepts is a fixed array shared by
					// all tasks - if that changes this will need to change - robably need to
					// add this to the executor
					se.cfg.notifications.RecentLogs.Add(txTask.BlockReceipts)
				}
				checkReceipts := !se.cfg.vmConfig.StatelessExec && se.cfg.chainConfig.IsByzantium(txTask.BlockNum) && !se.cfg.vmConfig.NoReceipts && !se.isMining
				if txTask.BlockNum > 0 && !se.skipPostEvaluation { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
					if err := core.BlockPostValidation(se.usedGas, se.blobGasUsed, checkReceipts, txTask.BlockReceipts, txTask.Header, se.isMining); err != nil {
						return fmt.Errorf("%w, txnIdx=%d, %v", consensus.ErrInvalidBlock, txTask.TxIndex, err) //same as in stage_exec.go
					}
				}
			}
			//if !se.execStage.CurrentSyncCycle.IsInitialCycle && rand2.Int()%1500 == 0 && txTask.TxIndex == 0 && !se.cfg.badBlockHalt {
			//	return fmt.Errorf("monkey in the datacenter: %w", consensus.ErrInvalidBlock)
			//}
			return nil
		}(); err != nil {
			if errors.Is(err, context.Canceled) {
				return false, err
			}
			se.logger.Warn(fmt.Sprintf("[%s] Execution failed", se.execStage.LogPrefix()),
				"block", txTask.BlockNum, "txNum", txTask.TxNum, "hash", txTask.Header.Hash().String(), "err", err)
			if se.cfg.hd != nil && se.cfg.hd.POSSync() && errors.Is(err, consensus.ErrInvalidBlock) {
				se.cfg.hd.ReportBadHeaderPoS(txTask.Header.Hash(), txTask.Header.ParentHash)
			}
			if se.cfg.badBlockHalt {
				return false, err
			}
			if errors.Is(err, consensus.ErrInvalidBlock) {
				if se.u != nil {
					if err := se.u.UnwindTo(txTask.BlockNum-1, BadBlock(txTask.Header.Hash(), err), se.applyTx); err != nil {
						return false, err
					}
				}
			} else {
				if se.u != nil {
					if err := se.u.UnwindTo(txTask.BlockNum-1, ExecUnwind, se.applyTx); err != nil {
						return false, err
					}
				}
			}
			return false, nil
		}

		if !txTask.Final {
			var receipt *types.Receipt
			if txTask.TxIndex >= 0 && !txTask.Final {
				receipt = txTask.BlockReceipts[txTask.TxIndex]
			}
			if err := rawtemporaldb.AppendReceipt(se.doms, receipt, se.blobGasUsed); err != nil {
				return false, err
			}
		}

		// MA applystate
		if err := se.rs.ApplyState4(ctx, txTask); err != nil {
			return false, err
		}

		se.outputTxNum.Add(1)
	}

	return true, nil
}

func (se *serialExecutor) tx() kv.RwTx {
	return se.applyTx
}

func (se *serialExecutor) readState() *state.StateV3 {
	return se.rs
}

func (se *serialExecutor) domains() *state2.SharedDomains {
	return se.doms
}

func (se *serialExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header) {
	var err error
	h, err = se.cfg.blockReader.Header(ctx, se.applyTx, hash, number)
	if err != nil {
		panic(err)
	}
	return h
}

func (se *serialExecutor) commit(ctx context.Context, txNum uint64, blockNum uint64, useExternalTx bool) (t2 time.Duration, err error) {
	se.doms.Close()
	if err = se.execStage.Update(se.applyTx, blockNum); err != nil {
		return 0, err
	}

	se.applyTx.CollectMetrics()

	if !useExternalTx {
		tt := time.Now()
		if err = se.applyTx.Commit(); err != nil {
			return 0, err
		}

		t2 = time.Since(tt)
		se.agg.BuildFilesInBackground(se.outputTxNum.Load())

		se.applyTx, err = se.cfg.db.BeginRw(context.Background()) //nolint
		if err != nil {
			return t2, err
		}
	}
	se.doms, err = state2.NewSharedDomains(se.applyTx, se.logger)
	if err != nil {
		return t2, err
	}
	se.doms.SetTxNum(txNum)
	se.rs = state.NewStateV3(se.doms, se.logger)

	se.worker.ResetTx(se.applyTx)
	se.worker.ResetState(se.rs, se.accumulator)

	return t2, nil
}
