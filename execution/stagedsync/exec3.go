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

package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cmp"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/turbo/shards"
)

// Cases:
//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
func restoreTxNum(ctx context.Context, cfg *ExecuteBlockCfg, applyTx kv.Tx, doms *dbstate.SharedDomains, maxBlockNum uint64) (
	inputTxNum uint64, maxTxNum uint64, offsetFromBlockBeginning uint64, err error) {

	txNumsReader := cfg.blockReader.TxnumReader(ctx)

	inputTxNum = doms.TxNum()

	if nothing, err := nothingToExec(applyTx, txNumsReader, inputTxNum); err != nil {
		return 0, 0, 0, err
	} else if nothing {
		return 0, 0, 0, err
	}

	maxTxNum, err = txNumsReader.Max(applyTx, maxBlockNum)
	if err != nil {
		return 0, 0, 0, err
	}

	blockNum, ok, err := txNumsReader.FindBlockNum(applyTx, doms.TxNum())
	if err != nil {
		return 0, 0, 0, err
	}
	if !ok {
		lb, lt, _ := txNumsReader.Last(applyTx)
		fb, ft, _ := txNumsReader.First(applyTx)
		return 0, 0, 0, fmt.Errorf("seems broken TxNums index not filled. can't find blockNum of txNum=%d; in db: (%d-%d, %d-%d)", inputTxNum, fb, lb, ft, lt)
	}
	{
		max, _ := txNumsReader.Max(applyTx, blockNum)
		if doms.TxNum() == max {
			blockNum++
		}
	}

	min, err := txNumsReader.Min(applyTx, blockNum)
	if err != nil {
		return 0, 0, 0, err
	}

	if doms.TxNum() > min {
		// if stopped in the middle of the block: start from beginning of block.
		// first part will be executed in HistoryExecution mode
		offsetFromBlockBeginning = doms.TxNum() - min
	}

	inputTxNum = min

	//_max, _ := txNumsReader.Max(applyTx, blockNum)
	//fmt.Printf("[commitment] found domain.txn %d, inputTxn %d, offset %d. DB found block %d {%d, %d}\n", doms.TxNum(), inputTxNum, offsetFromBlockBeginning, blockNum, _min, _max)
	doms.SetBlockNum(blockNum)
	doms.SetTxNum(inputTxNum)
	return inputTxNum, maxTxNum, offsetFromBlockBeginning, nil
}

func nothingToExec(applyTx kv.Tx, txNumsReader rawdbv3.TxNumsReader, inputTxNum uint64) (bool, error) {
	_, lastTxNum, err := txNumsReader.Last(applyTx)
	if err != nil {
		return false, err
	}
	return lastTxNum == inputTxNum, nil
}

func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, workerCount int, cfg ExecuteBlockCfg, txc wrap.TxContainer,
	parallel bool, //nolint
	maxBlockNum uint64,
	logger log.Logger,
	hooks *tracing.Hooks,
	initialCycle bool,
	isMining bool,
) (execErr error) {
	inMemExec := txc.Doms != nil

	blockReader := cfg.blockReader
	chainConfig := cfg.chainConfig
	totalGasUsed := uint64(0)

	useExternalTx := txc.Tx != nil
	var applyTx kv.TemporalRwTx

	if useExternalTx {
		var ok bool
		applyTx, ok = txc.Tx.(kv.TemporalRwTx)
		if !ok {
			applyTx, ok = txc.Ttx.(kv.TemporalRwTx)

			if !ok {
				return errors.New("txc.Tx is not a temporal tx")
			}
		}
	} else {
		var err error
		temporalDb, ok := cfg.db.(kv.TemporalRwDB)
		if !ok {
			return errors.New("cfg.db is not a temporal db")
		}
		applyTx, err = temporalDb.BeginTemporalRw(ctx) //nolint
		if err != nil {
			return err
		}
		defer func() { // need callback - because tx may be committed
			applyTx.Rollback()
		}()
	}

	agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if !inMemExec && !isMining {
		agg.SetCollateAndBuildWorkers(min(2, estimate.StateV3Collate.Workers()))
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	} else {
		agg.SetCompressWorkers(1)
		agg.SetCollateAndBuildWorkers(1)
	}

	var err error
	var doms *dbstate.SharedDomains
	if inMemExec {
		doms = txc.Doms
	} else {
		var err error
		doms, err = dbstate.NewSharedDomains(applyTx, log.New())
		// if we are behind the commitment, we can't execute anything
		// this can heppen if progress in domain is higher than progress in blocks
		if errors.Is(err, commitmentdb.ErrBehindCommitment) {
			return nil
		}
		if err != nil {
			return err
		}
		defer doms.Close()
	}

	var (
		stageProgress = execStage.BlockNumber
		outputTxNum   = atomic.Uint64{}
		blockNum      = doms.BlockNum()
	)

	if maxBlockNum < blockNum {
		return nil
	}

	outputTxNum.Store(doms.TxNum())
	agg.BuildFilesInBackground(outputTxNum.Load())

	var (
		inputTxNum               uint64
		offsetFromBlockBeginning uint64
		maxTxNum                 uint64
	)

	if applyTx != nil {
		if inputTxNum, maxTxNum, offsetFromBlockBeginning, err = restoreTxNum(ctx, &cfg, applyTx, doms, maxBlockNum); err != nil {
			return err
		}
	} else {
		if err := cfg.db.View(ctx, func(tx kv.Tx) (err error) {
			inputTxNum, maxTxNum, offsetFromBlockBeginning, err = restoreTxNum(ctx, &cfg, tx, doms, maxBlockNum)
			return err
		}); err != nil {
			return err
		}
	}

	if maxTxNum == 0 {
		return nil
	}

	shouldReportToTxPool := cfg.notifications != nil && !isMining && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(doms, cfg.syncCfg, logger))

	commitThreshold := cfg.batchSize.Bytes()

	logInterval := 20 * time.Second
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	flushEvery := time.NewTicker(2 * time.Second)
	defer flushEvery.Stop()
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	var executor executor
	var executorContext context.Context
	var executorCancel context.CancelFunc

	if parallel {
		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:                   cfg,
				rs:                    rs,
				doms:                  doms,
				agg:                   agg,
				isMining:              isMining,
				inMemExec:             inMemExec,
				logger:                logger,
				logPrefix:             execStage.LogPrefix(),
				progress:              NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:     execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:                 hooks,
				lastCommittedTxNum:    doms.TxNum(),
				lastCommittedBlockNum: blockNum,
			},
			workerCount: workerCount,
		}

		executorContext, executorCancel = pe.run(ctx)

		defer executorCancel()

		executor = pe
	} else {
		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:                   cfg,
				rs:                    rs,
				doms:                  doms,
				agg:                   agg,
				u:                     u,
				isMining:              isMining,
				inMemExec:             inMemExec,
				applyTx:               applyTx,
				logger:                logger,
				logPrefix:             execStage.LogPrefix(),
				progress:              NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:     execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:                 hooks,
				lastCommittedTxNum:    doms.TxNum(),
				lastCommittedBlockNum: blockNum,
			},
		}

		executor = se
	}

	executor.resetWorkers(ctx, rs, applyTx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx)
	defer func() {
		executor.LogComplete(stepsInDb)
	}()

	computeCommitmentDuration := time.Duration(0)
	blockNum = executor.domains().BlockNum()

	if maxBlockNum < blockNum {
		return nil
	}

	var uncommitedGas uint64
	var b *types.Block

	var readAhead chan uint64
	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	if !execStage.CurrentSyncCycle.IsInitialCycle {
		var clean func()

		readAhead, clean = exec3.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}

	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)

	if blockLimit > 0 && min(blockNum+blockLimit, maxBlockNum) > blockNum+16 || maxBlockNum > blockNum+16 {
		execType := "serial"
		if parallel {
			execType = "parallel"
		}

		log.Info(fmt.Sprintf("[%s] %s starting", execStage.LogPrefix(), execType),
			"from", blockNum, "to", min(blockNum+blockLimit, maxBlockNum), "fromTxNum", doms.TxNum(), "initialBlockTxOffset", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx, "inMem", inMemExec)
	}

	if !parallel {
		se := executor.(*serialExecutor)

		execErr = func() error {
			havePartialBlock := false

			for ; blockNum <= maxBlockNum; blockNum++ {
				shouldGenerateChangesets := shouldGenerateChangeSets(cfg, blockNum, maxBlockNum, initialCycle)
				changeSet := &changeset.StateChangeSet{}
				if shouldGenerateChangesets && blockNum > 0 {
					executor.domains().SetChangesetAccumulator(changeSet)
				}

				select {
				case readAhead <- blockNum:
				default:
				}

				b, err = exec3.BlockWithSenders(ctx, cfg.db, applyTx, blockReader, blockNum)
				if err != nil {
					return err
				}
				if b == nil {
					// TODO: panic here and see that overall process deadlock
					return fmt.Errorf("nil block %d", blockNum)
				}

				txs := b.Transactions()
				header := b.HeaderNoCopy()
				totalGasUsed += b.GasUsed()
				getHashFnMutex := sync.Mutex{}

				blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
					getHashFnMutex.Lock()
					defer getHashFnMutex.Unlock()
					return executor.getHeader(ctx, hash, number)
				}), cfg.engine, cfg.author, chainConfig)

				if accumulator != nil {
					txs, err := blockReader.RawTransactions(context.Background(), applyTx, b.NumberU64(), b.NumberU64())
					if err != nil {
						return err
					}
					accumulator.StartChange(header, txs, false)
				}

				var txTasks []exec.Task

				for txIndex := -1; txIndex <= len(txs); txIndex++ {
					// Do not oversend, wait for the result heap to go under certain size
					txTask := &exec.TxTask{
						TxNum:           inputTxNum,
						TxIndex:         txIndex,
						Header:          header,
						Uncles:          b.Uncles(),
						Txs:             txs,
						EvmBlockContext: blockContext,
						Withdrawals:     b.Withdrawals(),

						// use history reader instead of state reader to catch up to the tx where we left off
						HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),
						Trace:            dbg.TraceTx(blockNum, txIndex),
						Hooks:            hooks,
						Logger:           logger,
					}

					if txTask.TxNum > 0 && txTask.TxNum <= outputTxNum.Load() {
						havePartialBlock = true
						inputTxNum++
						continue
					}

					txTasks = append(txTasks, txTask)
					stageProgress = blockNum
					inputTxNum++
				}

				continueLoop, err := se.execute(ctx, txTasks, execStage.CurrentSyncCycle.IsInitialCycle, false)

				if err != nil {
					return err
				}

				uncommitedGas = uint64(se.executedGas.Load() - int64(se.committedGas))

				if !continueLoop {
					return nil
				}

				if !dbg.BatchCommitments || shouldGenerateChangesets {
					start := time.Now()
					if dbg.TraceBlock(blockNum) {
						se.doms.SetTrace(true, false)
					}
					rh, err := executor.domains().ComputeCommitment(ctx, applyTx, true, blockNum, inputTxNum, execStage.LogPrefix(), nil)
					se.doms.SetTrace(false, false)

					if err != nil {
						return err
					}

					computeCommitmentDuration += time.Since(start)
					executor.domains().SavePastChangesetAccumulator(b.Hash(), blockNum, changeSet)
					if !inMemExec {
						if err := changeset.WriteDiffSet(applyTx, blockNum, b.Hash(), changeSet); err != nil {
							return err
						}
					}
					executor.domains().SetChangesetAccumulator(nil)

					if !isMining && !bytes.Equal(rh, header.Root.Bytes()) {
						logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", execStage.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
						return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNum)
					}
				}

				if dbg.StopAfterBlock > 0 && blockNum == dbg.StopAfterBlock {
					panic(fmt.Sprintf("stopping: block %d complete", blockNum))
					//return fmt.Errorf("stopping: block %d complete", blockNum)
				}

				if offsetFromBlockBeginning > 0 {
					// after history execution no offset will be required
					offsetFromBlockBeginning = 0
				}

				select {
				case <-logEvery.C:
					if inMemExec || isMining {
						break
					}

					executor.LogExecuted()

					//TODO: https://github.com/erigontech/erigon/issues/10724
					//if executor.tx().(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).CanPrune(executor.tx(), outputTxNum.Load()) {
					//	//small prune cause MDBX_TXN_FULL
					//	if _, err := executor.tx().(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
					//		return err
					//	}
					//}

					isBatchFull := executor.readState().SizeEstimate() >= commitThreshold
					canPrune := dbstate.AggTx(applyTx).CanPrune(applyTx, outputTxNum.Load())
					needCalcRoot := isBatchFull || havePartialBlock || canPrune
					// If we have a partial first block it may not be validated, then we should compute root hash ASAP for fail-fast

					// this will only happen for the first executed block
					havePartialBlock = false

					if !needCalcRoot {
						break
					}

					resetExecGauges(ctx)

					var (
						commitStart   = time.Now()
						pruneDuration time.Duration
					)

					se := executor.(*serialExecutor)

					ok, times, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
					if err != nil {
						return err
					} else if !ok {
						return nil
					}

					resetCommitmentGauges(ctx)

					computeCommitmentDuration += times.ComputeCommitment
					flushDuration := times.Flush

					se.txExecutor.lastCommittedBlockNum = b.NumberU64()
					se.txExecutor.lastCommittedTxNum = inputTxNum

					timeStart := time.Now()

					pruneTimeout := 250 * time.Millisecond
					if initialCycle {
						pruneTimeout = 10 * time.Hour

						if err = applyTx.GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
							return err
						}
					}

					if _, err := applyTx.PruneSmallBatches(ctx, pruneTimeout); err != nil {
						return err
					}

					pruneDuration = time.Since(timeStart)

					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx)

					var commitDuration time.Duration
					applyTx, commitDuration, err = executor.(*serialExecutor).commit(ctx, execStage, applyTx, nil, useExternalTx)
					if err != nil {
						return err
					}
					// on chain-tip: if batch is full then stop execution - to allow stages commit
					if !initialCycle {
						if isBatchFull {
							return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch is full"}
						}

						if canPrune {
							return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch can be pruned"}
						}
					}

					if !useExternalTx {
						executor.LogCommitted(commitStart, 0, 0, uncommitedGas, stepsInDb, commitment.CommitProgress{})
					}

					uncommitedGas = 0

					logger.Info("Committed", "time", time.Since(commitStart),
						"block", executor.domains().BlockNum(), "txNum", executor.domains().TxNum(),
						"step", fmt.Sprintf("%.1f", float64(executor.domains().TxNum())/float64(agg.StepSize())),
						"flush", flushDuration, "compute commitment", computeCommitmentDuration, "tx.commit", commitDuration, "prune", pruneDuration)
				default:
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if blockLimit > 0 && blockNum-startBlockNum+1 >= blockLimit {
					return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
				}
			}

			return nil
		}()

		if u != nil && !u.HasUnwindPoint() {
			if b != nil {
				if execErr != nil {
					if errors.Is(execErr, ErrWrongTrieRoot) {
						execErr = handleIncorrectRootHashError(
							b.NumberU64(), b.Hash(), b.ParentHash(), applyTx, cfg, execStage, maxBlockNum, logger, u)
					}
				} else {
					_, _, err = flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
					if err != nil {
						return err
					}

					se.txExecutor.lastCommittedBlockNum = b.NumberU64()
					committedTransactions := inputTxNum - se.txExecutor.lastCommittedTxNum
					se.txExecutor.lastCommittedTxNum = inputTxNum

					commitStart := time.Now()
					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx)
					applyTx, _, err = se.commit(ctx, execStage, applyTx, nil, useExternalTx)
					if err != nil {
						return err
					}

					if !useExternalTx {
						executor.LogCommitted(commitStart, 0, committedTransactions, uncommitedGas, stepsInDb, commitment.CommitProgress{})
					}

					uncommitedGas = 0
				}
			} else {
				fmt.Printf("[dbg] mmmm... do we need action here????\n")
			}
		}
	} else {
		pe := executor.(*parallelExecutor)

		var asyncTxChan mdbx.TxApplyChan
		var asyncTx kv.Tx

		switch applyTx := applyTx.(type) {
		case *temporal.RwTx:
			temporalTx := applyTx.AsyncClone(mdbx.NewAsyncRwTx(applyTx.RwTx, 1000))
			asyncTxChan = temporalTx.ApplyChan()
			asyncTx = temporalTx
		default:
			return fmt.Errorf("expected *temporal.RwTx: got %T", applyTx)
		}

		applyResults := make(chan applyResult, 100_000)

		maxExecBlockNum := maxBlockNum
		if blockLimit > 0 && blockNum+uint64(blockLimit) < maxBlockNum {
			maxExecBlockNum = blockNum + blockLimit - 1
		}

		if err := executor.executeBlocks(executorContext, asyncTx, blockNum, maxExecBlockNum, readAhead, applyResults); err != nil {
			return err
		}

		var lastExecutedLog time.Time
		var lastCommitedLog time.Time
		var lastBlockResult blockResult
		var uncommittedBlocks int64
		var uncommittedTransactions uint64
		var uncommittedGas int64
		var flushPending bool

		execErr = func() error {
			defer func() {
				if rec := recover(); rec != nil {
					pe.logger.Warn("["+execStage.LogPrefix()+"] rw panic", "rec", rec, "stack", dbg.Stack())
				} else if err != nil && !errors.Is(err, context.Canceled) {
					pe.logger.Warn("["+execStage.LogPrefix()+"] rw exit", "err", err)
				} else {
					pe.logger.Debug("[" + execStage.LogPrefix() + "] rw exit")
				}
			}()

			shouldGenerateChangesets := shouldGenerateChangeSets(cfg, blockNum, maxBlockNum, initialCycle)
			changeSet := &changeset.StateChangeSet{}
			if shouldGenerateChangesets && blockNum > 0 {
				executor.domains().SetChangesetAccumulator(changeSet)
			}

			blockUpdateCount := 0
			blockApplyCount := 0

			for {
				select {
				case request := <-asyncTxChan:
					request.Apply()
				case applyResult := <-applyResults:
					switch applyResult := applyResult.(type) {
					case *txResult:
						uncommittedGas += applyResult.gasUsed
						uncommittedTransactions++
						pe.rs.SetTxNum(applyResult.blockNum, applyResult.txNum)
						if dbg.TraceApply && dbg.TraceBlock(applyResult.blockNum) {
							pe.rs.SetTrace(true)
							fmt.Println(applyResult.blockNum, "apply", applyResult.txNum, applyResult.stateUpdates.UpdateCount())
						}
						blockUpdateCount += applyResult.stateUpdates.UpdateCount()
						err := pe.rs.ApplyTxState(ctx, applyTx, applyResult.blockNum, applyResult.txNum, applyResult.stateUpdates,
							nil, applyResult.receipt, applyResult.logs, applyResult.traceFroms, applyResult.traceTos,
							pe.cfg.chainConfig, applyResult.rules, false)
						blockApplyCount += applyResult.stateUpdates.UpdateCount()
						pe.rs.SetTrace(false)
						if err != nil {
							return err
						}
					case *blockResult:
						if applyResult.BlockNum > 0 && !applyResult.isPartial { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
							checkReceipts := !cfg.vmConfig.StatelessExec &&
								cfg.chainConfig.IsByzantium(applyResult.BlockNum) &&
								!cfg.vmConfig.NoReceipts && !isMining

							b, err = blockReader.BlockByHash(ctx, applyTx, applyResult.BlockHash)

							if err != nil {
								return fmt.Errorf("can't retrieve block %d: for post validation: %w", applyResult.BlockNum, err)
							}

							if b.NumberU64() != applyResult.BlockNum {
								return fmt.Errorf("block numbers don't match expected: %d: got: %d for hash %x", applyResult.BlockNum, b.NumberU64(), applyResult.BlockHash)
							}

							if blockUpdateCount != applyResult.ApplyCount {
								return fmt.Errorf("block %d: applyCount mismatch: got: %d expected %d", applyResult.BlockNum, blockUpdateCount, applyResult.ApplyCount)
							}

							if err := core.BlockPostValidation(applyResult.GasUsed, applyResult.BlobGasUsed, checkReceipts, applyResult.Receipts,
								b.HeaderNoCopy(), pe.isMining, b.Transactions(), pe.cfg.chainConfig, pe.logger); err != nil {
								dumpTxIODebug(applyResult.BlockNum, applyResult.TxIO)
								return fmt.Errorf("%w, block=%d, %v", consensus.ErrInvalidBlock, applyResult.BlockNum, err) //same as in stage_exec.go
							}

							if !isMining && !applyResult.isPartial && !execStage.CurrentSyncCycle.IsInitialCycle {
								cfg.notifications.RecentLogs.Add(applyResult.Receipts)
							}
						}

						if applyResult.BlockNum > lastBlockResult.BlockNum {
							uncommittedBlocks++
							pe.doms.SetTxNum(applyResult.lastTxNum)
							pe.doms.SetBlockNum(applyResult.BlockNum)
							lastBlockResult = *applyResult
						}

						flushPending = pe.rs.SizeEstimate() > pe.cfg.batchSize.Bytes()

						if !dbg.DiscardCommitment() {
							if !dbg.BatchCommitments || shouldGenerateChangesets || lastBlockResult.BlockNum == maxExecBlockNum ||
								(flushPending && lastBlockResult.BlockNum > pe.lastCommittedBlockNum()) {

								resetExecGauges(ctx)

								if dbg.TraceApply && dbg.TraceBlock(applyResult.BlockNum) {
									fmt.Println(applyResult.BlockNum, "applied count", blockApplyCount, "last tx", applyResult.lastTxNum)
								}

								var trace bool
								if dbg.TraceBlock(applyResult.BlockNum) {
									fmt.Println(applyResult.BlockNum, "Commitment")
									trace = true
								}
								pe.doms.SetTrace(trace, !dbg.BatchCommitments)

								commitProgress := make(chan *commitment.CommitProgress, 100)

								go func() {
									logEvery := time.NewTicker(20 * time.Second)
									commitStart := time.Now()

									defer logEvery.Stop()
									var lastProgress commitment.CommitProgress

									var prevCommitedBlocks uint64
									var prevCommittedTransactions uint64
									var prevCommitedGas uint64

									logCommitted := func(commitProgress commitment.CommitProgress) {
										// this is an approximation of blcok prgress - it assumnes an
										// even distribution of keys to blocks
										if commitProgress.KeyIndex > 0 {
											progress := float64(commitProgress.KeyIndex) / float64(commitProgress.UpdateCount)
											committedGas := uint64(float64(uncommittedGas) * progress)
											committedTransactions := uint64(float64(uncommittedTransactions) * progress)
											commitedBlocks := uint64(float64(uncommittedBlocks) * progress)

											if committedTransactions-prevCommittedTransactions > 0 {
												pe.LogCommitted(commitStart,
													commitedBlocks-prevCommitedBlocks,
													committedTransactions-prevCommittedTransactions,
													committedGas-prevCommitedGas, stepsInDb, commitProgress)
											}

											lastCommitedLog = time.Now()
											prevCommitedBlocks = commitedBlocks
											prevCommittedTransactions = committedTransactions
											prevCommitedGas = committedGas
										}

										if pe.agg.HasBackgroundFilesBuild() {
											logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
										}
									}

									for {
										select {
										case <-ctx.Done():
											return
										case progress, ok := <-commitProgress:
											if !ok {
												if time.Since(lastCommitedLog) > logInterval/20 {
													logCommitted(lastProgress)
												}
												return
											}
											lastProgress = *progress
										case <-logEvery.C:
											if time.Since(lastCommitedLog) > logInterval-(logInterval/90) {
												logCommitted(lastProgress)
											}
										}
									}
								}()

								if time.Since(lastExecutedLog) > logInterval/50 {
									pe.LogExecuted()
									lastExecutedLog = time.Now()
								}

								rh, err := pe.doms.ComputeCommitment(ctx, applyTx, true, applyResult.BlockNum, applyResult.lastTxNum, pe.logPrefix, commitProgress)
								close(commitProgress)
								captured := pe.doms.SetTrace(false, false)
								if err != nil {
									return err
								}
								resetCommitmentGauges(ctx)

								executor.domains().SavePastChangesetAccumulator(applyResult.BlockHash, blockNum, changeSet)
								if !inMemExec {
									if err := changeset.WriteDiffSet(applyTx, blockNum, applyResult.BlockHash, changeSet); err != nil {
										return err
									}
								}
								executor.domains().SetChangesetAccumulator(nil)

								if !bytes.Equal(rh, applyResult.StateRoot.Bytes()) {
									logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", pe.logPrefix, applyResult.BlockNum, rh, applyResult.StateRoot.Bytes(), applyResult.BlockHash))
									if !dbg.BatchCommitments {
										for _, line := range captured {
											fmt.Println(line)
										}

										dumpTxIODebug(applyResult.BlockNum, applyResult.TxIO)
									}

									return handleIncorrectRootHashError(
										applyResult.BlockNum, applyResult.BlockHash, applyResult.ParentHash,
										applyTx, cfg, execStage, maxBlockNum, logger, u)
								}
								// fix these here - they will contain estimates after commit logging
								pe.txExecutor.lastCommittedBlockNum = lastBlockResult.BlockNum
								pe.txExecutor.lastCommittedTxNum = lastBlockResult.lastTxNum
								uncommittedBlocks = 0
								uncommittedGas = 0
								uncommittedGas = 0
							}
						}

						blockUpdateCount = 0
						blockApplyCount = 0

						if dbg.StopAfterBlock > 0 && applyResult.BlockNum == dbg.StopAfterBlock {
							return fmt.Errorf("stopping: block %d complete", applyResult.BlockNum)
						}

						if applyResult.BlockNum == maxExecBlockNum {
							switch {
							case applyResult.BlockNum == maxBlockNum:
								return nil
							case blockLimit > 0:
								return &ErrLoopExhausted{From: startBlockNum, To: applyResult.BlockNum, Reason: "block limit reached"}
							default:
								return nil
							}
						}

						if shouldGenerateChangesets && blockNum > 0 {
							changeSet = &changeset.StateChangeSet{}
							executor.domains().SetChangesetAccumulator(changeSet)
						}
					}
				case <-executorContext.Done():
					err = executor.wait(ctx)
					return fmt.Errorf("executor context failed: %w", err)
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					if time.Since(lastExecutedLog) > logInterval-(logInterval/90) {
						lastExecutedLog = time.Now()
						pe.LogExecuted()
						if pe.agg.HasBackgroundFilesBuild() {
							logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
						}
					}
				case <-flushEvery.C:
					if flushPending {
						if !initialCycle {
							return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch is full"}
						}

						if applyTx, err = pe.flushAndCommit(ctx, execStage, applyTx, asyncTxChan, useExternalTx); err != nil {
							return fmt.Errorf("flush failed: %w", err)
						}

						flushPending = false
					}
				}
			}
		}()

		executorCancel()

		if execErr != nil {
			if !(errors.Is(execErr, context.Canceled) || errors.Is(execErr, &ErrLoopExhausted{})) {
				return execErr
			}
		}

		if applyTx, err = pe.flushAndCommit(ctx, execStage, applyTx, asyncTxChan, useExternalTx); err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
	}

	if err := executor.wait(ctx); err != nil {
		return fmt.Errorf("executer wait failed: %w", err)
	}

	if false && !inMemExec {
		dumpPlainStateDebug(applyTx, executor.domains())
	}

	lastCommitedStep := kv.Step((executor.lastCommittedTxNum()) / doms.StepSize())
	lastFrozenStep := applyTx.StepsInFiles(kv.CommitmentDomain)

	if lastCommitedStep > 0 && lastCommitedStep <= lastFrozenStep && !dbg.DiscardCommitment() {
		logger.Warn("["+execStage.LogPrefix()+"] can't persist comittement: txn step frozen",
			"block", executor.lastCommittedBlockNum(), "txNum", executor.lastCommittedTxNum(), "step", lastCommitedStep,
			"lastFrozenStep", lastFrozenStep, "lastFrozenTxNum", ((lastFrozenStep+1)*kv.Step(doms.StepSize()))-1)
		return fmt.Errorf("can't persist comittement for blockNum %d, txNum %d: step %d is frozen",
			executor.lastCommittedBlockNum(), executor.lastCommittedTxNum(), lastCommitedStep)
	}

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	if !shouldReportToTxPool && cfg.notifications != nil && cfg.notifications.Accumulator != nil && !isMining && b != nil {
		// No reporting to the txn pool has been done since we are not within the "state-stream" window.
		// However, we should still at the very least report the last block number to it, so it can update its block progress.
		// Otherwise, we can get in a deadlock situation when there is a block building request in environments where
		// the Erigon process is the only block builder (e.g. some Hive tests, kurtosis testnets with one erigon block builder, etc.)
		cfg.notifications.Accumulator.StartChange(b.HeaderNoCopy(), nil, false /* unwind */)
	}

	return execErr
}

func dumpTxIODebug(blockNum uint64, txIO *state.VersionedIO) {
	maxTxIndex := len(txIO.Inputs()) - 1

	for txIndex := -1; txIndex < maxTxIndex; txIndex++ {
		txIncarnation := txIO.ReadSetIncarnation(txIndex)

		fmt.Println(
			fmt.Sprintf("%d (%d.%d) RD", blockNum, txIndex, txIncarnation), txIO.ReadSet(txIndex).Len(),
			"WRT", len(txIO.WriteSet(txIndex)))

		var reads []*state.VersionedRead
		txIO.ReadSet(txIndex).Scan(func(vr *state.VersionedRead) bool {
			reads = append(reads, vr)
			return true
		})

		slices.SortFunc(reads, func(a, b *state.VersionedRead) int { return a.Address.Cmp(b.Address) })

		for _, vr := range reads {
			fmt.Println(fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation), "RD", vr.String())
		}

		var writes []*state.VersionedWrite

		for _, vw := range txIO.WriteSet(txIndex) {
			writes = append(writes, vw)
		}

		slices.SortFunc(writes, func(a, b *state.VersionedWrite) int { return a.Address.Cmp(b.Address) })

		for _, vw := range writes {
			fmt.Println(fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation), "WRT", vw.String())
		}
	}
}

// nolint
func dumpPlainStateDebug(tx kv.TemporalRwTx, doms *dbstate.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx)
	}

	{
		it, err := tx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
		if err != nil {
			panic(err)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			a := accounts.NewAccount()
			accounts.DeserialiseV3(&a, v)
			fmt.Printf("%x, %d, %d, %d, %x\n", k, &a.Balance, a.Nonce, a.Incarnation, a.CodeHash)
		}
	}
	{
		it, err := tx.Debug().RangeLatest(kv.StorageDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
		}
	}
	{
		it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
			if bytes.Equal(k, []byte("state")) {
				fmt.Printf("state: t=%d b=%d\n", binary.BigEndian.Uint64(v[:8]), binary.BigEndian.Uint64(v[8:]))
			}
		}
	}
}

func handleIncorrectRootHashError(blockNumber uint64, blockHash common.Hash, parentHash common.Hash, applyTx kv.TemporalRwTx, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, logger log.Logger, u Unwinder) error {
	if cfg.badBlockHalt {
		return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNumber)
	}
	if cfg.hd != nil && cfg.hd.POSSync() {
		cfg.hd.ReportBadHeaderPoS(blockHash, parentHash)
	}
	minBlockNum := e.BlockNumber
	if maxBlockNum <= minBlockNum {
		return nil
	}

	unwindToLimit, err := rawtemporaldb.CanUnwindToBlockNum(applyTx)
	if err != nil {
		return err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, maxUnwindJumpAllowance, (maxBlockNum-minBlockNum)/2)
	unwindTo := maxBlockNum - jump

	// protect from too far unwind
	allowedUnwindTo, ok, err := rawtemporaldb.CanUnwindBeforeBlockNum(unwindTo, applyTx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: requested=%d, minAllowed=%d", ErrTooDeepUnwind, unwindTo, allowedUnwindTo)
	}
	logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
	if u != nil {
		if err := u.UnwindTo(allowedUnwindTo, BadBlock(blockHash, ErrInvalidStateRootHash), applyTx); err != nil {
			return err
		}
	}
	return nil
}

type FlushAndComputeCommitmentTimes struct {
	Flush             time.Duration
	ComputeCommitment time.Duration
}

// flushAndCheckCommitmentV3 - does write state to db and then check commitment
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.TemporalRwTx, doms *dbstate.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, parallel bool, logger log.Logger, u Unwinder, inMemExec bool) (ok bool, times FlushAndComputeCommitmentTimes, err error) {
	start := time.Now()
	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	if !parallel {
		if err := e.Update(applyTx, maxBlockNum); err != nil {
			return false, times, err
		}
		if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
			return false, times, fmt.Errorf("writing plain state version: %w", err)
		}
	}

	domsFlushFn := func() (bool, FlushAndComputeCommitmentTimes, error) {
		if !inMemExec {
			start = time.Now()
			err := doms.Flush(ctx, applyTx)
			times.Flush = time.Since(start)
			if err != nil {
				return false, times, err
			}
		}
		return true, times, nil
	}

	if header == nil {
		return false, times, errors.New("header is nil")
	}

	if dbg.DiscardCommitment() {
		return domsFlushFn()
	}
	if doms.BlockNum() != header.Number.Uint64() {
		panic(fmt.Errorf("%d != %d", doms.BlockNum(), header.Number.Uint64()))
	}

	computedRootHash, err := doms.ComputeCommitment(ctx, applyTx, true, header.Number.Uint64(), doms.TxNum(), e.LogPrefix(), nil)

	times.ComputeCommitment = time.Since(start)
	if err != nil {
		return false, times, fmt.Errorf("compute commitment: %w", err)
	}

	if cfg.blockProduction {
		header.Root = common.BytesToHash(computedRootHash)
		return true, times, nil
	}
	if !bytes.Equal(computedRootHash, header.Root.Bytes()) {
		logger.Warn(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), computedRootHash, header.Root.Bytes(), header.Hash()))
		err = handleIncorrectRootHashError(header.Number.Uint64(), header.Hash(), header.ParentHash,
			applyTx, cfg, e, maxBlockNum, logger, u)
		return false, times, err
	}
	return domsFlushFn()

}

func shouldGenerateChangeSets(cfg ExecuteBlockCfg, blockNum, maxBlockNum uint64, initialCycle bool) bool {
	if cfg.syncCfg.AlwaysGenerateChangesets {
		return true
	}
	if blockNum < cfg.blockReader.FrozenBlocks() {
		return false
	}
	if initialCycle {
		return false
	}
	// once past the initial cycle, make sure to generate changesets for the last blocks that fall in the reorg window
	return blockNum+cfg.syncCfg.MaxReorgDepth >= maxBlockNum
}
