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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cmp"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
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
	changesetSafeRange     = 32   // Safety net for long-sync, keep last 32 changesets
	maxUnwindJumpAllowance = 1000 // Maximum number of blocks we are allowed to unwind
)

func NewProgress(prevOutputBlockNum, commitThreshold uint64, workersCount int, logPrefix string, logger log.Logger) *Progress {
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

func (p *Progress) Log(suffix string, rs *state.ParallelExecutionState, in *state.QueueWithRetry, rws *state.ResultsQueue, txCount uint64, gas uint64, inputBlockNum uint64, outputBlockNum uint64, outTxNum uint64, repeatCount uint64, idxStepsAmountInDB float64, commitEveryBlock bool, inMemExec bool) {
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

	if commitEveryBlock {
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

	_blockNum, ok, err := txNumsReader.FindBlockNum(applyTx, doms.TxNum())
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
	hooks *tracing.Hooks,
	initialCycle bool,
	isMining bool,
) (execErr error) {
	inMemExec := txc.Doms != nil
	// TODO: e35 doesn't support parallel-exec yet
	parallel = false //nolint

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

	chainReader := NewChainReaderImpl(cfg.chainConfig, applyTx, blockReader, logger)
	agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if !inMemExec && !isMining {
		if initialCycle {
			agg.SetCollateAndBuildWorkers(min(2, estimate.StateV3Collate.Workers()))
			agg.SetMergeWorkers(min(1, estimate.StateV3Collate.Workers()))
			agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
		} else {
			agg.SetCollateAndBuildWorkers(1)
			agg.SetMergeWorkers(1)
			agg.SetCompressWorkers(1)
		}
	}

	var err error
	var doms *dbstate.SharedDomains
	if inMemExec {
		doms = txc.Doms
	} else {
		var err error
		temporalTx, ok := applyTx.(kv.TemporalTx)
		if !ok {
			return errors.New("applyTx is not a temporal transaction")
		}
		doms, err = dbstate.NewSharedDomains(temporalTx, log.New())
		// if we are behind the commitment, we can't execute anything
		// this can heppen if progress in domain is higher than progress in blocks
		if errors.Is(err, dbstate.ErrBehindCommitment) {
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
	rs := state.NewParallelExecutionState(doms, applyTx, cfg.syncCfg, cfg.chainConfig.Bor != nil, logger)

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
	progress := NewProgress(blockNum, commitThreshold, workerCount, execStage.LogPrefix(), logger)

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
			progress.Log("Done", executor.readState(), nil, pe.rws, 0 /*txCount - TODO*/, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets, inMemExec)
		}()

		executor = pe
	} else {
		applyWorker.ResetTx(applyTx)

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
			progress.Log("Done", executor.readState(), nil, nil, se.txCount, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets || cfg.syncCfg.KeepExecutionProofs, inMemExec)
		}()

		executor = se
	}

	blockComplete.Store(true)

	computeCommitmentDuration := time.Duration(0)
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
	if !isMining && !inMemExec && execStage.CurrentSyncCycle.IsInitialCycle {
		// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
		// can't use OS-level ReadAhead - because Data >> RAM
		// it also warmsup state a bit - by touching senders/coninbase accounts and code
		var clean func()

		readAhead, clean = exec3.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}

	var b *types.Block

	// Only needed by bor chains
	shouldGenerateChangesetsForLastBlocks := cfg.chainConfig.Bor != nil
	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)
	var errExhausted *ErrLoopExhausted

Loop:
	for ; blockNum <= maxBlockNum; blockNum++ {
		// set shouldGenerateChangesets=true if we are at last n blocks from maxBlockNum. this is as a safety net in chains
		// where during initial sync we can expect bogus blocks to be imported.
		if !shouldGenerateChangesets && shouldGenerateChangesetsForLastBlocks && blockNum > cfg.blockReader.FrozenBlocks() && blockNum+changesetSafeRange >= maxBlockNum {
			start := time.Now()
			executor.domains().SetChangesetAccumulator(nil) // Make sure we don't have an active changeset accumulator
			// First compute and commit the progress done so far
			if _, err := executor.domains().ComputeCommitment(ctx, true, blockNum, inputTxNum, execStage.LogPrefix()); err != nil {
				return err
			}
			computeCommitmentDuration += time.Since(start)
			shouldGenerateChangesets = true // now we can generate changesets for the safety net
		}
		changeset := &dbstate.StateChangeSet{}
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

		if b.NumberU64() == 0 {
			if hooks != nil && hooks.OnGenesisBlock != nil {
				hooks.OnGenesisBlock(b, cfg.genesis.Alloc)
			}
		} else {
			if hooks != nil && hooks.OnBlockStart != nil {
				hooks.OnBlockStart(tracing.BlockEvent{
					Block:     b,
					TD:        chainReader.GetTd(b.ParentHash(), b.NumberU64()-1),
					Finalized: chainReader.CurrentFinalizedHeader(),
					Safe:      chainReader.CurrentSafeHeader(),
				})
			}
		}

		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum, header.Time)

		getHashFnMute := &sync.Mutex{}
		getHashFn := core.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return executor.getHeader(ctx, hash, number)
		})
		totalGasUsed += b.GasUsed()
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, cfg.author /* author */, chainConfig)
		gp := new(core.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(b.Time()))

		// print type of engine
		if parallel {
			if err := executor.status(ctx, commitThreshold); err != nil {
				if b.NumberU64() > 0 && hooks != nil && hooks.OnBlockEnd != nil {
					hooks.OnBlockEnd(err)
				}
				return err
			}
		} else if accumulator != nil {
			txs, err := blockReader.RawTransactions(context.Background(), executor.tx(), b.NumberU64(), b.NumberU64())
			if err != nil {
				if b.NumberU64() > 0 && hooks != nil && hooks.OnBlockEnd != nil {
					hooks.OnBlockEnd(err)
				}
				return err
			}
			accumulator.StartChange(header, txs, false)
		}

		rules := blockContext.Rules(chainConfig)
		blockReceipts := make(types.Receipts, len(txs))
		// During the first block execution, we may have half-block data in the snapshots.
		// Thus, we need to skip the first txs in the block, however, this causes the GasUsed to be incorrect.
		// So we skip that check for the first block, if we find half-executed data.
		skipPostEvaluation := false
		var gasUsed uint64
		var txTasks []*state.TxTask
		var validationResults []state.AAValidationResult
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &state.TxTask{
				BlockNum:        blockNum,
				Header:          header,
				Coinbase:        b.Coinbase(),
				Uncles:          b.Uncles(),
				Rules:           rules,
				Txs:             txs,
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				BlockHash:       b.Hash(),
				SkipAnalysis:    skipAnalysis,
				Final:           txIndex == len(txs),
				GetHashFn:       getHashFn,
				EvmBlockContext: blockContext,
				Withdrawals:     b.Withdrawals(),

				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),

				BlockReceipts: blockReceipts,

				Config: chainConfig,

				ValidationResults: validationResults,
			}
			if txTask.HistoryExecution && gasUsed == 0 {
				gasUsed, _, _, err = rawtemporaldb.ReceiptAsOf(executor.tx().(kv.TemporalTx), txTask.TxNum)
				if err != nil {
					if b.NumberU64() > 0 && hooks != nil && hooks.OnBlockEnd != nil {
						hooks.OnBlockEnd(err)
					}
					return err
				}
			}

			if cfg.genesis != nil {
				txTask.Config = cfg.genesis.Config
			}

			if txTask.TxNum <= txNumInDB && txTask.TxNum > 0 && !cfg.blockProduction {
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
					if b.NumberU64() > 0 && hooks != nil && hooks.OnBlockEnd != nil {
						hooks.OnBlockEnd(err)
					}
					return err
				}
			}

			txTasks = append(txTasks, txTask)
			stageProgress = blockNum
			inputTxNum++
		}

		// check for consecutive RIP-7560 sequence
		var isAASequence bool
		for _, txTask := range txTasks {
			txIndex := txTask.TxIndex
			if txIndex < 0 || txIndex > len(txs)-1 {
				continue
			}

			if txTask.Tx.Type() != types.AccountAbstractionTxType {
				isAASequence = false
				continue
			}
			if isAASequence {
				continue
			}

			aaBatchSize := uint64(0)
			for _, tt := range txTasks {
				if tt.TxIndex > txIndex && tt.Tx != nil && tt.Tx.Type() == types.AccountAbstractionTxType {
					aaBatchSize++
					tt.InBatch = true
				} else {
					break
				}
			}

			txTask.AAValidationBatchSize = aaBatchSize
			isAASequence = true
		}

		if parallel {
			_, err := executor.execute(ctx, txTasks, nil /*gasPool*/) // For now don't use block's gas pool for parallel
			if b.NumberU64() > 0 && hooks != nil && hooks.OnBlockEnd != nil {
				hooks.OnBlockEnd(err)
			}
			if err != nil {
				return err
			}

			agg.BuildFilesInBackground(outputTxNum.Load())
		} else {
			se := executor.(*serialExecutor)

			se.skipPostEvaluation = skipPostEvaluation

			continueLoop, err := se.execute(ctx, txTasks, gp)
			if b.NumberU64() > 0 && hooks != nil && hooks.OnBlockEnd != nil {
				hooks.OnBlockEnd(err)
			}
			if err != nil {
				return err
			}

			count += uint64(len(txTasks))
			logGas += se.gasUsed

			se.gasUsed = 0
			se.blobGasUsed = 0

			if !continueLoop {
				break Loop
			}
		}

		mxExecBlocks.Add(1)

		if shouldGenerateChangesets || cfg.syncCfg.KeepExecutionProofs {
			start := time.Now()
			_ /*rh*/, err := executor.domains().ComputeCommitment(ctx, true, blockNum, inputTxNum, execStage.LogPrefix())
			if err != nil {
				return err
			}

			//if !bytes.Equal(rh, header.Root.Bytes()) {
			//	logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", execStage.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
			//	return errors.New("wrong trie root")
			//}

			computeCommitmentDuration += time.Since(start)
			if shouldGenerateChangesets {
				executor.domains().SavePastChangesetAccumulator(b.Hash(), blockNum, changeset)
				if !inMemExec {
					if err := dbstate.WriteDiffSet(executor.tx(), blockNum, b.Hash(), changeset); err != nil {
						return err
					}
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
			select {
			case <-logEvery.C:
				if inMemExec || isMining {
					break
				}

				stepsInDB := rawdbhelpers.IdxStepsCountV3(executor.tx())
				progress.Log("", executor.readState(), nil, nil, count, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets, inMemExec)

				//TODO: https://github.com/erigontech/erigon/issues/10724
				//if executor.tx().(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).CanPrune(executor.tx(), outputTxNum.Load()) {
				//	//small prune cause MDBX_TXN_FULL
				//	if _, err := executor.tx().(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
				//		return err
				//	}
				//}

				aggregatorRo := dbstate.AggTx(executor.tx())

				isBatchFull := executor.readState().SizeEstimate() >= commitThreshold
				canPrune := aggregatorRo.CanPrune(executor.tx(), outputTxNum.Load())
				needCalcRoot := isBatchFull ||
					skipPostEvaluation || // If we skip post evaluation, then we should compute root hash ASAP for fail-fast
					canPrune // if have something to prune - better prune ASAP to keep chaindata smaller
				if !needCalcRoot {
					break
				}

				var (
					commitStart = time.Now()

					pruneDuration time.Duration
				)
				ok, times, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), executor.tx(), executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
				if err != nil {
					return err
				} else if !ok {
					break Loop
				}

				computeCommitmentDuration += times.ComputeCommitment
				flushDuration := times.Flush

				timeStart := time.Now()

				// allow greedy prune on non-chain-tip
				pruneTimeout := 250 * time.Millisecond
				if initialCycle {
					pruneTimeout = 10 * time.Hour

					if err = executor.tx().(kv.TemporalRwTx).GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
						return err
					}
				}

				if _, err := aggregatorRo.PruneSmallBatches(ctx, pruneTimeout, executor.tx()); err != nil {
					return err
				}
				pruneDuration = time.Since(timeStart)

				commitDuration, err := executor.(*serialExecutor).commit(ctx, inputTxNum, outputBlockNum.GetValueUint64(), useExternalTx)
				if err != nil {
					return err
				}

				// on chain-tip: if batch is full then stop execution - to allow stages commit
				if !initialCycle && isBatchFull {
					errExhausted = &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch is full"}
					break Loop
				}
				if !initialCycle && canPrune {
					errExhausted = &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch can be pruned"}
					break Loop
				}
				if initialCycle {
					logger.Info("Committed", "time", time.Since(commitStart),
						"block", outputBlockNum.GetValueUint64(), "txNum", inputTxNum,
						"step", fmt.Sprintf("%.1f", float64(inputTxNum)/float64(agg.StepSize())),
						"flush", flushDuration, "compute commitment", computeCommitmentDuration, "tx.commit", commitDuration, "prune", pruneDuration)
				}
			default:
			}
		}

		if blockLimit > 0 && blockNum-startBlockNum+1 >= blockLimit {
			errExhausted = &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
			break
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
			_, _, err = flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), executor.tx(), executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
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

	if errExhausted != nil && blockNum < maxBlockNum {
		// special err allows the loop to continue, caller will call us again to continue from where we left off
		// only return it if we haven't reached the maxBlockNum
		return errExhausted
	}

	return nil
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

func handleIncorrectRootHashError(header *types.Header, applyTx kv.TemporalRwTx, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, logger log.Logger, u Unwinder) (bool, error) {
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

	unwindToLimit, err := applyTx.Debug().CanUnwindToBlockNum()
	if err != nil {
		return false, err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, maxUnwindJumpAllowance, (maxBlockNum-minBlockNum)/2)
	unwindTo := maxBlockNum - jump

	// protect from too far unwind
	allowedUnwindTo, ok, err := applyTx.Debug().CanUnwindBeforeBlockNum(unwindTo)
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

type FlushAndComputeCommitmentTimes struct {
	Flush             time.Duration
	ComputeCommitment time.Duration
}

// flushAndCheckCommitmentV3 - does write state to db and then check commitment
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.RwTx, doms *dbstate.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, parallel bool, logger log.Logger, u Unwinder, inMemExec bool) (ok bool, times FlushAndComputeCommitmentTimes, err error) {
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

	if header == nil {
		return false, times, errors.New("header is nil")
	}

	if dbg.DiscardCommitment() {
		return true, times, nil
	}
	if doms.BlockNum() != header.Number.Uint64() {
		panic(fmt.Errorf("%d != %d", doms.BlockNum(), header.Number.Uint64()))
	}

	computedRootHash, err := doms.ComputeCommitment(ctx, true, header.Number.Uint64(), doms.TxNum(), e.LogPrefix())
	times.ComputeCommitment = time.Since(start)
	if err != nil {
		return false, times, fmt.Errorf("ParallelExecutionState.Apply: %w", err)
	}

	if cfg.blockProduction {
		header.Root = common.BytesToHash(computedRootHash)
		return true, times, nil
	}
	if !bytes.Equal(computedRootHash, header.Root.Bytes()) {
		logger.Warn(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), computedRootHash, header.Root.Bytes(), header.Hash()))
		ok, err = handleIncorrectRootHashError(header, applyTx.(kv.TemporalRwTx), cfg, e, maxBlockNum, logger, u)
		return ok, times, err
	}
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
	return b, err
}
