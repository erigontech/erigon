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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cmp"
	"github.com/erigontech/erigon-lib/common/dbg"
	metrics2 "github.com/erigontech/erigon-lib/common/metrics"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var (
	mxExecStepsInDB    = metrics.NewGauge(`exec_steps_in_db`) //nolint
	mxExecRepeats      = metrics.NewCounter(`exec_repeats`)   //nolint
	mxExecTriggers     = metrics.NewCounter(`exec_triggers`)  //nolint
	mxExecTransactions = metrics.NewCounter(`exec_txns`)
	mxExecGas          = metrics.NewCounter(`exec_gas`)
	mxExecBlocks       = metrics.NewGauge("exec_blocks")

	mxMgas = metrics.NewGauge(`exec_mgas`)
)

const (
	changesetSafeRange = 32 // Safety net for long-sync, keep last 32 changesets
)

func NewProgress(prevOutputBlockNum, commitThreshold uint64, workersCount int, updateMetrics bool, logPrefix string, logger log.Logger) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum, commitThreshold: commitThreshold, workersCount: workersCount, logPrefix: logPrefix, logger: logger}
}

type Progress struct {
	prevTime           time.Time
	prevTxCount        uint64
	prevGasUsed        uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
	commitThreshold    uint64

	workersCount int
	logPrefix    string
	logger       log.Logger
}

func (p *Progress) Log(suffix string, rs *state.StateV3, in *state.QueueWithRetry, rws *state.ResultsQueue, txCount uint64, gas uint64, inputBlockNum uint64, outputBlockNum uint64, outTxNum uint64, repeatCount uint64, idxStepsAmountInDB float64, shouldGenerateChangesets bool, inMemExec bool) {
	mxExecStepsInDB.Set(idxStepsAmountInDB * 100)
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	//var repeatRatio float64
	//if doneCount > p.prevCount {
	//	repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(doneCount-p.prevCount)
	//}

	if len(suffix) > 0 {
		suffix = " " + suffix
	}

	if shouldGenerateChangesets {
		suffix += " Commit every block"
	}

	gasSec := uint64(float64(gas-p.prevGasUsed) / interval.Seconds())
	txSec := uint64(float64(txCount-p.prevTxCount) / interval.Seconds())
	diffBlocks := max(int(outputBlockNum)-int(p.prevOutputBlockNum)+1, 0)

	p.logger.Info(fmt.Sprintf("[%s]"+suffix, p.logPrefix),
		"blk", outputBlockNum,
		"blks", diffBlocks,
		"blk/s", fmt.Sprintf("%.1f", float64(diffBlocks)/interval.Seconds()),
		"txs", txCount-p.prevTxCount,
		"tx/s", common.PrettyCounter(txSec),
		"gas/s", common.PrettyCounter(gasSec),
		//"pipe", fmt.Sprintf("(%d+%d)->%d/%d->%d/%d", in.NewTasksLen(), in.RetriesLen(), rws.ResultChLen(), rws.ResultChCap(), rws.Len(), rws.Limit()),
		//"repeatRatio", fmt.Sprintf("%.2f%%", repeatRatio),
		//"workers", p.workersCount,
		"buf", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"stepsInDB", fmt.Sprintf("%.2f", idxStepsAmountInDB),
		"step", fmt.Sprintf("%.1f", float64(outTxNum)/float64(config3.DefaultStepSize)),
		"inMem", inMemExec,
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)

	p.prevTime = currentTime
	p.prevTxCount = txCount
	p.prevGasUsed = gas
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

// Cases:
//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
func restoreTxNum(ctx context.Context, cfg *ExecuteBlockCfg, applyTx kv.Tx, doms *state2.SharedDomains, maxBlockNum uint64) (
	inputTxNum uint64, maxTxNum uint64, offsetFromBlockBeginning uint64, err error) {

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))

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

	ok, _blockNum, err := txNumsReader.FindBlockNum(applyTx, doms.TxNum())
	if err != nil {
		return 0, 0, 0, err
	}
	if !ok {
		_lb, _lt, _ := txNumsReader.Last(applyTx)
		_fb, _ft, _ := txNumsReader.First(applyTx)
		return 0, 0, 0, fmt.Errorf("seems broken TxNums index not filled. can't find blockNum of txNum=%d; in db: (%d-%d, %d-%d)", inputTxNum, _fb, _lb, _ft, _lt)
	}
	{
		_max, _ := txNumsReader.Max(applyTx, _blockNum)
		if doms.TxNum() == _max {
			_blockNum++
		}
	}

	_min, err := txNumsReader.Min(applyTx, _blockNum)
	if err != nil {
		return 0, 0, 0, err
	}

	if doms.TxNum() > _min {
		// if stopped in the middle of the block: start from beginning of block.
		// first part will be executed in HistoryExecution mode
		offsetFromBlockBeginning = doms.TxNum() - _min
	}

	inputTxNum = _min

	//_max, _ := txNumsReader.Max(applyTx, blockNum)
	//fmt.Printf("[commitment] found domain.txn %d, inputTxn %d, offset %d. DB found block %d {%d, %d}\n", doms.TxNum(), inputTxNum, offsetFromBlockBeginning, blockNum, _min, _max)
	doms.SetBlockNum(_blockNum)
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
	initialCycle bool,
	isMining bool,
) error {
	// TODO: e35 doesn't support parallel-exec yet
	parallel = false //nolint
	if parallel && cfg.chainConfig.ChainName == networkname.Gnosis {
		panic("gnosis consensus doesn't support parallel exec yet: https://github.com/erigontech/erigon/issues/12054")
	}

	blockReader := cfg.blockReader
	chainConfig := cfg.chainConfig
	totalGasUsed := uint64(0)
	start := time.Now()
	defer func() {
		if totalGasUsed > 0 {
			mxMgas.Set((float64(totalGasUsed) / 1e6) / time.Since(start).Seconds())
		}
	}()

	applyTx := txc.Tx
	useExternalTx := applyTx != nil
	if !useExternalTx {
		if !parallel {
			var err error
			applyTx, err = cfg.db.BeginRw(ctx) //nolint
			if err != nil {
				return err
			}
			defer func() { // need callback - because tx may be committed
				applyTx.Rollback()
			}()
		}
	}
	agg := cfg.db.(state2.HasAgg).Agg().(*state2.Aggregator)
	if initialCycle {
		agg.SetCollateAndBuildWorkers(min(2, estimate.StateV3Collate.Workers()))
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	} else {
		agg.SetCompressWorkers(1)
		agg.SetCollateAndBuildWorkers(1)
	}

	pruneNonEssentials := cfg.prune.History.Enabled() && cfg.prune.History.PruneTo(execStage.BlockNumber) == execStage.BlockNumber

	var err error
	inMemExec := txc.Doms != nil
	var doms *state2.SharedDomains
	if inMemExec {
		doms = txc.Doms
	} else {
		var err error
		doms, err = state2.NewSharedDomains(applyTx, log.New())
		// if we are behind the commitment, we can't execute anything
		// this can heppen if progress in domain is higher than progress in blocks
		if errors.Is(err, state2.ErrBehindCommitment) {
			return nil
		}
		if err != nil {
			return err
		}
		defer doms.Close()
	}
	txNumInDB := doms.TxNum()

	var (
		inputTxNum               = doms.TxNum()
		stageProgress            = execStage.BlockNumber
		outputTxNum              = atomic.Uint64{}
		blockComplete            = atomic.Bool{}
		outputBlockNum           = stages.SyncMetrics[stages.Execution]
		inputBlockNum            = &atomic.Uint64{}
		offsetFromBlockBeginning uint64
		blockNum, maxTxNum       uint64
	)

	blockNum = doms.BlockNum()
	outputTxNum.Store(doms.TxNum())

	if maxBlockNum < blockNum {
		return nil
	}

	shouldGenerateChangesets := maxBlockNum-blockNum <= changesetSafeRange || cfg.syncCfg.AlwaysGenerateChangesets
	if blockNum < cfg.blockReader.FrozenBlocks() {
		shouldGenerateChangesets = false
	}

	if maxBlockNum > blockNum+16 {
		log.Info(fmt.Sprintf("[%s] starting", execStage.LogPrefix()),
			"from", blockNum, "to", maxBlockNum, "fromTxNum", doms.TxNum(), "offsetFromBlockBeginning", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx, "inMem", inMemExec)
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	var count uint64

	shouldReportToTxPool := cfg.notifications != nil && !isMining && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3(doms, logger)

	////TODO: owner of `resultCh` is main goroutine, but owner of `retryQueue` is applyLoop.
	// Now rwLoop closing both (because applyLoop we completely restart)
	// Maybe need split channels? Maybe don't exit from ApplyLoop? Maybe current way is also ok?

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

	applyWorker := cfg.applyWorker
	if isMining {
		applyWorker = cfg.applyWorkerMining
	}
	defer applyWorker.LogLRUStats()

	applyWorker.ResetState(rs, accumulator)

	commitThreshold := cfg.batchSize.Bytes()

	// TODO are these dups ?
	processed := NewProgress(blockNum, commitThreshold, workerCount, true, execStage.LogPrefix(), logger)
	progress := NewProgress(blockNum, commitThreshold, workerCount, false, execStage.LogPrefix(), logger)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	pruneEvery := time.NewTicker(2 * time.Second)
	defer pruneEvery.Stop()

	var logGas uint64
	var stepsInDB float64
	var executor executor

	if parallel {
		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:            cfg,
				execStage:      execStage,
				rs:             rs,
				doms:           doms,
				agg:            agg,
				accumulator:    accumulator,
				isMining:       isMining,
				inMemExec:      inMemExec,
				applyTx:        applyTx,
				applyWorker:    applyWorker,
				outputTxNum:    &outputTxNum,
				outputBlockNum: stages.SyncMetrics[stages.Execution],
				logger:         logger,
			},
			shouldGenerateChangesets: shouldGenerateChangesets,
			workerCount:              workerCount,
			pruneEvery:               pruneEvery,
			logEvery:                 logEvery,
			progress:                 progress,
		}

		executorCancel := pe.run(ctx, maxTxNum, logger)
		defer executorCancel()

		defer func() {
			processed.Log("Done", executor.readState(), nil, pe.rws, 0 /*txCount - TODO*/, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets, inMemExec)
		}()

		executor = pe
	} else {
		applyWorker.ResetTx(applyTx)
		doms.SetTx(applyTx)

		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:            cfg,
				execStage:      execStage,
				rs:             rs,
				doms:           doms,
				agg:            agg,
				u:              u,
				isMining:       isMining,
				inMemExec:      inMemExec,
				applyTx:        applyTx,
				applyWorker:    applyWorker,
				outputTxNum:    &outputTxNum,
				outputBlockNum: stages.SyncMetrics[stages.Execution],
				logger:         logger,
			},
		}

		defer func() {
			processed.Log("Done", executor.readState(), nil, nil, se.txCount, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets, inMemExec)
		}()

		executor = se
	}

	blockComplete.Store(true)

	ts := time.Duration(0)
	blockNum = executor.domains().BlockNum()
	outputTxNum.Store(executor.domains().TxNum())

	if maxBlockNum < blockNum {
		return nil
	}

	if maxBlockNum > blockNum+16 {
		log.Info(fmt.Sprintf("[%s] starting", execStage.LogPrefix()),
			"from", blockNum, "to", maxBlockNum, "fromTxNum", executor.domains().TxNum(), "offsetFromBlockBeginning", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx)
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	var readAhead chan uint64
	if !parallel {
		// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
		// can't use OS-level ReadAhead - because Data >> RAM
		// it also warmsup state a bit - by touching senders/coninbase accounts and code
		if !execStage.CurrentSyncCycle.IsInitialCycle {
			var clean func()

			readAhead, clean = blocksReadAhead(ctx, &cfg, 4, true)
			defer clean()
		}
	}

	var b *types.Block

	// Only needed by bor chains
	shouldGenerateChangesetsForLastBlocks := cfg.chainConfig.Bor != nil

Loop:
	for ; blockNum <= maxBlockNum; blockNum++ {
		// set shouldGenerateChangesets=true if we are at last n blocks from maxBlockNum. this is as a safety net in chains
		// where during initial sync we can expect bogus blocks to be imported.
		if !shouldGenerateChangesets && shouldGenerateChangesetsForLastBlocks && blockNum > cfg.blockReader.FrozenBlocks() && blockNum+changesetSafeRange >= maxBlockNum {
			aggTx := executor.tx().(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
			aggTx.RestrictSubsetFileDeletions(true)
			start := time.Now()
			executor.domains().SetChangesetAccumulator(nil) // Make sure we don't have an active changeset accumulator
			// First compute and commit the progress done so far
			if _, err := executor.domains().ComputeCommitment(ctx, true, blockNum, execStage.LogPrefix()); err != nil {
				return err
			}
			ts += time.Since(start)
			aggTx.RestrictSubsetFileDeletions(false)
			shouldGenerateChangesets = true // now we can generate changesets for the safety net
		}
		changeset := &state2.StateChangeSet{}
		if shouldGenerateChangesets && blockNum > 0 {
			executor.domains().SetChangesetAccumulator(changeset)
		}
		if !parallel {
			select {
			case readAhead <- blockNum:
			default:
			}
		}
		inputBlockNum.Store(blockNum)
		executor.domains().SetBlockNum(blockNum)

		b, err = blockWithSenders(ctx, cfg.db, executor.tx(), blockReader, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		metrics2.UpdateBlockConsumerPreExecutionDelay(b.Time(), blockNum, logger)
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum, header.Time)

		getHashFnMute := &sync.Mutex{}
		getHashFn := core.GetHashFn(header, func(hash common.Hash, number uint64) (h *types.Header) {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return executor.getHeader(ctx, hash, number)
		})
		totalGasUsed += b.GasUsed()
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, cfg.author /* author */, chainConfig)
		// print type of engine
		if parallel {
			if err := executor.status(ctx, commitThreshold); err != nil {
				return err
			}
		} else if accumulator != nil {
			txs, err := blockReader.RawTransactions(context.Background(), executor.tx(), b.NumberU64(), b.NumberU64())
			if err != nil {
				return err
			}
			accumulator.StartChange(b.NumberU64(), b.Hash(), txs, false)
		}

		rules := chainConfig.Rules(blockNum, b.Time())
		blockReceipts := make(types.Receipts, len(txs))
		// During the first block execution, we may have half-block data in the snapshots.
		// Thus, we need to skip the first txs in the block, however, this causes the GasUsed to be incorrect.
		// So we skip that check for the first block, if we find half-executed data.
		skipPostEvaluation := false
		var usedGas uint64
		var txTasks []*state.TxTask
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &state.TxTask{
				BlockNum:           blockNum,
				Header:             header,
				Coinbase:           b.Coinbase(),
				Uncles:             b.Uncles(),
				Rules:              rules,
				Txs:                txs,
				TxNum:              inputTxNum,
				TxIndex:            txIndex,
				BlockHash:          b.Hash(),
				SkipAnalysis:       skipAnalysis,
				Final:              txIndex == len(txs),
				GetHashFn:          getHashFn,
				EvmBlockContext:    blockContext,
				Withdrawals:        b.Withdrawals(),
				PruneNonEssentials: pruneNonEssentials,

				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),

				BlockReceipts: blockReceipts,

				Config: chainConfig,
			}
			if txTask.HistoryExecution && usedGas == 0 {
				usedGas, _, _, err = rawtemporaldb.ReceiptAsOf(executor.tx().(kv.TemporalTx), txTask.TxNum)
				if err != nil {
					return err
				}
			}

			if cfg.genesis != nil {
				txTask.Config = cfg.genesis.Config
			}

			if txTask.TxNum <= txNumInDB && txTask.TxNum > 0 {
				inputTxNum++
				skipPostEvaluation = true
				continue
			}
			executor.domains().SetTxNum(txTask.TxNum)
			executor.domains().SetBlockNum(txTask.BlockNum)

			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					return err
				}
			}

			txTasks = append(txTasks, txTask)
			stageProgress = blockNum
			inputTxNum++
		}

		if parallel {
			if _, err := executor.execute(ctx, txTasks); err != nil {
				return err
			}
			agg.BuildFilesInBackground(outputTxNum.Load())
		} else {
			se := executor.(*serialExecutor)

			se.skipPostEvaluation = skipPostEvaluation

			continueLoop, err := se.execute(ctx, txTasks)

			if err != nil {
				return err
			}

			count += uint64(len(txTasks))
			logGas += se.usedGas

			se.usedGas = 0
			se.blobGasUsed = 0

			if !continueLoop {
				break Loop
			}
		}

		mxExecBlocks.Add(1)

		if shouldGenerateChangesets {
			aggTx := executor.tx().(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
			aggTx.RestrictSubsetFileDeletions(true)
			start := time.Now()
			_ /*rh*/, err := executor.domains().ComputeCommitment(ctx, true, blockNum, execStage.LogPrefix())
			if err != nil {
				return err
			}

			//if !bytes.Equal(rh, header.Root.Bytes()) {
			//	logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", execStage.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
			//	return errors.New("wrong trie root")
			//}

			ts += time.Since(start)
			aggTx.RestrictSubsetFileDeletions(false)
			executor.domains().SavePastChangesetAccumulator(b.Hash(), blockNum, changeset)
			if !inMemExec {
				if err := state2.WriteDiffSet(executor.tx(), blockNum, b.Hash(), changeset); err != nil {
					return err
				}
			}
			executor.domains().SetChangesetAccumulator(nil)
		}

		mxExecBlocks.Add(1)

		if offsetFromBlockBeginning > 0 {
			// after history execution no offset will be required
			offsetFromBlockBeginning = 0
		}

		// MA commitTx
		if !parallel {
			if !inMemExec && !isMining {
				metrics2.UpdateBlockConsumerPostExecutionDelay(b.Time(), blockNum, logger)
			}

			select {
			case <-logEvery.C:
				if inMemExec || isMining {
					break
				}

				stepsInDB := rawdbhelpers.IdxStepsCountV3(executor.tx())
				progress.Log("", executor.readState(), nil, nil, count, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets, inMemExec)

				//TODO: https://github.com/erigontech/erigon/issues/10724
				//if executor.tx().(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).CanPrune(executor.tx(), outputTxNum.Load()) {
				//	//small prune cause MDBX_TXN_FULL
				//	if _, err := executor.tx().(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
				//		return err
				//	}
				//}

				aggregatorRo := executor.tx().(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)

				needCalcRoot := executor.readState().SizeEstimate() >= commitThreshold ||
					skipPostEvaluation || // If we skip post evaluation, then we should compute root hash ASAP for fail-fast
					aggregatorRo.CanPrune(executor.tx(), outputTxNum.Load()) // if have something to prune - better prune ASAP to keep chaindata smaller
				if !needCalcRoot {
					break
				}

				var (
					commitStart = time.Now()
					tt          = time.Now()

					t1, t3 time.Duration
				)

				if ok, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), executor.tx(), executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec); err != nil {
					return err
				} else if !ok {
					break Loop
				}

				t1 = time.Since(tt) + ts

				tt = time.Now()
				if _, err := aggregatorRo.PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
					return err
				}
				t3 = time.Since(tt)

				t2, err := executor.(*serialExecutor).commit(ctx, inputTxNum, outputBlockNum.GetValueUint64(), useExternalTx)
				if err != nil {
					return err
				}

				// on chain-tip: if batch is full then stop execution - to allow stages commit
				if !execStage.CurrentSyncCycle.IsInitialCycle {
					break Loop
				}
				logger.Info("Committed", "time", time.Since(commitStart),
					"block", executor.domains().BlockNum(), "txNum", executor.domains().TxNum(),
					"step", fmt.Sprintf("%.1f", float64(executor.domains().TxNum())/float64(agg.StepSize())),
					"flush+commitment", t1, "tx.commit", t2, "prune", t3)
			default:
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	//log.Info("Executed", "blocks", inputBlockNum.Load(), "txs", outputTxNum.Load(), "repeats", mxExecRepeats.GetValueUint64())

	//fmt.Println("WAIT")
	executor.wait()

	if u != nil && !u.HasUnwindPoint() {
		if b != nil {
			_, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), executor.tx(), executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
			if err != nil {
				return err
			}
		} else {
			fmt.Printf("[dbg] mmmm... do we need action here????\n")
		}
	}

	//dumpPlainStateDebug(executor.tx(), executor.domains())

	if !useExternalTx && executor.tx() != nil {
		if err = executor.tx().Commit(); err != nil {
			return err
		}
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	return nil
}

// nolint
func dumpPlainStateDebug(tx kv.RwTx, doms *state2.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx)
	}
	{
		it, err := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).RangeLatest(tx, kv.AccountsDomain, nil, nil, -1)
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
		it, err := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).RangeLatest(tx, kv.StorageDomain, nil, nil, -1)
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
		it, err := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).RangeLatest(tx, kv.CommitmentDomain, nil, nil, -1)
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

// flushAndCheckCommitmentV3 - does write state to db and then check commitment
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.RwTx, doms *state2.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, parallel bool, logger log.Logger, u Unwinder, inMemExec bool) (bool, error) {

	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	if !parallel {
		if err := e.Update(applyTx, maxBlockNum); err != nil {
			return false, err
		}
		if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
			return false, fmt.Errorf("writing plain state version: %w", err)
		}
	}

	if header == nil {
		return false, errors.New("header is nil")
	}

	if dbg.DiscardCommitment() {
		return true, nil
	}
	if doms.BlockNum() != header.Number.Uint64() {
		panic(fmt.Errorf("%d != %d", doms.BlockNum(), header.Number.Uint64()))
	}

	rh, err := doms.ComputeCommitment(ctx, true, header.Number.Uint64(), e.LogPrefix())
	if err != nil {
		return false, fmt.Errorf("StateV3.Apply: %w", err)
	}
	if cfg.blockProduction {
		header.Root = common.BytesToHash(rh)
		return true, nil
	}
	if bytes.Equal(rh, header.Root.Bytes()) {
		if !inMemExec {
			if err := doms.Flush(ctx, applyTx); err != nil {
				return false, err
			}
			if err = applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).PruneCommitHistory(ctx, applyTx, nil); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
	if cfg.badBlockHalt {
		return false, errors.New("wrong trie root")
	}
	if cfg.hd != nil && cfg.hd.POSSync() {
		cfg.hd.ReportBadHeaderPoS(header.Hash(), header.ParentHash)
	}
	minBlockNum := e.BlockNumber
	if maxBlockNum <= minBlockNum {
		return false, nil
	}

	aggTx := applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
	unwindToLimit, err := aggTx.CanUnwindToBlockNum(applyTx)
	if err != nil {
		return false, err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, 1000, (maxBlockNum-minBlockNum)/2)
	unwindTo := maxBlockNum - jump

	// protect from too far unwind
	allowedUnwindTo, ok, err := aggTx.CanUnwindBeforeBlockNum(unwindTo, applyTx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("%w: requested=%d, minAllowed=%d", ErrTooDeepUnwind, unwindTo, allowedUnwindTo)
	}
	logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
	if u != nil {
		if err := u.UnwindTo(allowedUnwindTo, BadBlock(header.Hash(), ErrInvalidStateRootHash), applyTx); err != nil {
			return false, err
		}
	}
	return false, nil
}

func blockWithSenders(ctx context.Context, db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	if tx == nil {
		tx, err = db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	b, err = blockReader.BlockByNumber(ctx, tx, blockNum)
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
