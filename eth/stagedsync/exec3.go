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
	metrics2 "github.com/erigontech/erigon-lib/common/metrics"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
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

func NewProgress(initialBlockNum, initialTxNum, commitThreshold uint64, updateMetrics bool, logPrefix string, logger log.Logger) *Progress {
	now := time.Now()
	return &Progress{
		initialTime:           now,
		initialTxNum:          initialTxNum,
		initialBlockNum:       initialBlockNum,
		prevExecTime:          now,
		prevExecutedBlockNum:  initialBlockNum,
		prevExecutedTxNum:     initialTxNum,
		prevCommitTime:        now,
		prevCommittedBlockNum: initialBlockNum,
		prevCommittedTxNum:    initialTxNum,
		commitThreshold:       commitThreshold,
		logPrefix:             logPrefix,
		logger:                logger}
}

type Progress struct {
	initialTime           time.Time
	initialTxNum          uint64
	initialBlockNum       uint64
	prevExecTime          time.Time
	prevExecutedBlockNum  uint64
	prevExecutedTxNum     uint64
	prevExecutedGas       uint64
	prevExecCount         uint64
	prevAbortCount        uint64
	prevInvalidCount      uint64
	prevReadCount         uint64
	prevWriteCount        uint64
	prevCommitTime        time.Time
	prevCommittedBlockNum uint64
	prevCommittedTxNum    uint64
	prevCommittedGas      uint64
	commitThreshold       uint64

	logPrefix string
	logger    log.Logger
}

func (p *Progress) LogExecuted(rs *state.StateV3, ex executor) {
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevExecTime)

	var suffix string
	var parallelExecVals []interface{}
	var tx *txExecutor

	switch ex := ex.(type) {
	case *parallelExecutor:
		tx = &ex.txExecutor
		suffix = " parallel"

		execCount := uint64(tx.execCount.Load())
		abortCount := uint64(tx.abortCount.Load())
		invalidCount := uint64(tx.invalidCount.Load())
		readCount := uint64(tx.readCount.Load())
		writeCount := uint64(tx.writeCount.Load())

		execDiff := execCount - p.prevExecCount

		var repeats = max(int(execDiff)-int(max(int(tx.lastExecutedTxNum)-int(p.prevExecutedTxNum), 0)), 0)
		var repeatRatio float64

		if repeats > 0 {
			repeatRatio = 100.0 * float64(repeats) / float64(execDiff)
		}

		parallelExecVals = []interface{}{
			"exec", common.PrettyCounter(execDiff),
			"repeat%", fmt.Sprintf("%.2f", repeatRatio),
			"abort", common.PrettyCounter(abortCount - p.prevAbortCount),
			"invalid", common.PrettyCounter(invalidCount - p.prevInvalidCount),
			"rd", common.PrettyCounter(readCount - p.prevReadCount),
			"wrt", common.PrettyCounter(writeCount - p.prevWriteCount),
			"rd/s", common.PrettyCounter(uint64(float64(readCount-p.prevReadCount) / interval.Seconds())),
			"wrt/s", common.PrettyCounter(uint64(float64(writeCount-p.prevWriteCount) / interval.Seconds())),
		}

		mxExecRepeats.AddInt(int(repeats))
		mxExecTriggers.AddInt(int(execCount))

		p.prevExecCount = execCount
		p.prevAbortCount = abortCount
		p.prevInvalidCount = invalidCount
		p.prevReadCount = readCount
		p.prevWriteCount = writeCount
	case *serialExecutor:
		tx = &ex.txExecutor
		suffix = " serial"
	}

	executedGasSec := uint64(float64(tx.executedGas-p.prevExecutedGas) / interval.Seconds())
	executedTxSec := uint64(float64(tx.lastExecutedTxNum-p.prevExecutedTxNum) / interval.Seconds())
	executedDiffBlocks := max(int(tx.lastExecutedBlockNum)-int(p.prevExecutedBlockNum), 0)
	executedDiffTxs := uint64(max(int(tx.lastExecutedTxNum)-int(p.prevExecutedTxNum), 0))

	p.log("executed", suffix, tx, rs, interval, tx.lastExecutedBlockNum, executedDiffBlocks,
		executedDiffTxs, executedTxSec, executedGasSec, false, parallelExecVals)

	p.prevExecTime = currentTime

	if tx.lastExecutedBlockNum > 0 {
		p.prevExecutedTxNum = tx.lastExecutedTxNum
		p.prevExecutedGas = tx.executedGas
		p.prevExecutedBlockNum = tx.lastExecutedBlockNum
	}
}

func (p *Progress) LogCommitted(commitStart time.Time, rs *state.StateV3, ex executor) {
	var tx *txExecutor
	var suffix string

	switch ex := ex.(type) {
	case *parallelExecutor:
		tx = &ex.txExecutor
		suffix = " parallel"

	case *serialExecutor:
		tx = &ex.txExecutor
		suffix = " serial"
	}

	if p.prevCommitTime.Before(commitStart) {
		p.prevCommitTime = commitStart
	}

	currentTime := time.Now()
	interval := currentTime.Sub(p.prevExecTime)

	if tx.shouldGenerateChangesets {
		suffix += "(commit every block)"
	}

	committedGasSec := uint64(float64(tx.committedGas-p.prevCommittedGas) / interval.Seconds())
	committedTxSec := uint64(float64(tx.lastCommittedTxNum-p.prevCommittedTxNum) / interval.Seconds())
	committedDiffBlocks := max(int(tx.lastCommittedBlockNum)-int(p.prevCommittedBlockNum), 0)

	p.log("committed", suffix, tx, rs, interval, tx.lastCommittedBlockNum, committedDiffBlocks,
		tx.lastCommittedTxNum-p.prevCommittedTxNum, committedTxSec, committedGasSec, true, nil)

	p.prevCommitTime = currentTime

	if tx.lastCommittedTxNum > 0 {
		p.prevCommittedTxNum = tx.lastCommittedTxNum
		p.prevCommittedGas = tx.committedGas
		p.prevCommittedBlockNum = tx.lastCommittedBlockNum
	}
}

func (p *Progress) LogComplete(rs *state.StateV3, ex executor) {
	interval := time.Since(p.initialTime)
	var tx *txExecutor
	var suffix string

	switch ex := ex.(type) {
	case *parallelExecutor:
		tx = &ex.txExecutor
		suffix = " parallel"

	case *serialExecutor:
		tx = &ex.txExecutor
		suffix = " serial"
	}

	gas := tx.committedGas

	if gas == 0 {
		gas = tx.executedGas
	}

	lastTxNum := tx.lastCommittedTxNum

	if lastTxNum == 0 {
		lastTxNum = tx.lastExecutedTxNum
	}

	lastBlockNum := tx.lastCommittedBlockNum

	if lastBlockNum == 0 {
		lastBlockNum = tx.lastExecutedBlockNum
	}

	gasSec := uint64(float64(gas) / interval.Seconds())
	txSec := uint64(float64(lastTxNum-p.initialTxNum) / interval.Seconds())
	diffBlocks := max(int(lastBlockNum)-int(p.initialBlockNum), 0)

	p.log("done", suffix, tx, rs, interval, lastBlockNum, diffBlocks, lastTxNum-p.initialTxNum, txSec, gasSec, true, nil)
}

func (p *Progress) log(mode string, suffix string, tx *txExecutor, rs *state.StateV3, interval time.Duration,
	blk uint64, blks int, txs uint64, txsSec uint64, gasSec uint64, logSteps bool, extraVals []interface{}) {

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()

	stepsInDb := rawdbhelpers.IdxStepsCountV3(tx.tx())
	mxExecStepsInDB.Set(stepsInDb * 100)

	if len(suffix) > 0 {
		suffix += " "
	}

	vals := []interface{}{
		"blk", blk,
		"blks", blks,
		"blk/s", common.PrettyCounter(uint64(float64(blks) / interval.Seconds())),
		"txs", common.PrettyCounter(txs),
		"tx/s", common.PrettyCounter(txsSec),
		"gas/s", common.PrettyCounter(gasSec),
	}

	if len(extraVals) > 0 {
		vals = append(vals, extraVals...)
	}

	if logSteps {
		vals = append(vals, []interface{}{
			"stepsInDB", fmt.Sprintf("%.2f", stepsInDb),
			"step", fmt.Sprintf("%.1f", float64(tx.lastCommittedTxNum)/float64(config3.DefaultStepSize)),
		}...)
	}

	vals = append(vals, []interface{}{
		"buf", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		"inMem", tx.inMemExec,
	}...)

	p.logger.Info(fmt.Sprintf("[%s]%s%s", p.logPrefix, suffix, mode), vals...)
}

// Cases:
//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
func restoreTxNum(ctx context.Context, cfg *ExecuteBlockCfg, applyTx kv.Tx, doms *libstate.SharedDomains, maxBlockNum uint64) (
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

	ok, blockNum, err := txNumsReader.FindBlockNum(applyTx, doms.TxNum())
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
	initialCycle bool,
	isMining bool,
) error {
	inMemExec := txc.Doms != nil

	blockReader := cfg.blockReader
	chainConfig := cfg.chainConfig
	totalGasUsed := uint64(0)
	start := time.Now()
	defer func() {
		if totalGasUsed > 0 {
			mxMgas.Set((float64(totalGasUsed) / 1e6) / time.Since(start).Seconds())
		}
	}()

	useExternalTx := txc.Tx != nil
	var applyTx kv.RwTx

	if useExternalTx {
		applyTx = txc.Tx
	} else {
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
	agg := cfg.db.(libstate.HasAgg).Agg().(*libstate.Aggregator)
	if !inMemExec && !isMining {
		agg.SetCollateAndBuildWorkers(min(2, estimate.StateV3Collate.Workers()))
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	} else {
		agg.SetCompressWorkers(1)
		agg.SetCollateAndBuildWorkers(1)
	}

	var err error
	var doms *libstate.SharedDomains
	if inMemExec {
		doms = txc.Doms
	} else {
		doms, err = libstate.NewSharedDomains(applyTx, log.New())
		// if we are behind the commitment, we can't execute anything
		// this can heppen if progress in domain is higher than progress in blocks
		if errors.Is(err, libstate.ErrBehindCommitment) {
			return nil
		}
		if err != nil {
			return err
		}
		defer doms.Close()
	}

	var (
		inputTxNum               = doms.TxNum()
		stageProgress            = execStage.BlockNumber
		outputTxNum              = atomic.Uint64{}
		blockComplete            = atomic.Bool{}
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

	agg.BuildFilesInBackground(outputTxNum.Load())

	shouldReportToTxPool := cfg.notifications != nil && !isMining && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(doms, logger))

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
	defer applyWorker.LogLRUStats()

	applyWorker.ResetState(rs, nil, nil, accumulator)

	commitThreshold := cfg.batchSize.Bytes()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	pruneEvery := time.NewTicker(2 * time.Second)
	defer pruneEvery.Stop()

	var executor executor

	if parallel {
		if useExternalTx {
			switch tx := applyTx.(type) {
			case *mdbx.MdbxTx:
				applyTx = mdbx.NewAsyncRwTx(tx, 1000)
			case *temporal.RwTx:
				tx.RwTx = mdbx.NewAsyncRwTx(tx.RwTx, 1000)
				applyTx = tx
			}
		}

		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:                      cfg,
				execStage:                execStage,
				rs:                       rs,
				doms:                     doms,
				agg:                      agg,
				accumulator:              accumulator,
				shouldGenerateChangesets: shouldGenerateChangesets,
				isMining:                 isMining,
				inMemExec:                inMemExec,
				applyTx:                  applyTx,
				logger:                   logger,
				progress:                 NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
			},
			workerCount: workerCount,
			pruneEvery:  pruneEvery,
			logEvery:    logEvery,
		}

		executorCancel := pe.run(ctx)
		defer executorCancel()

		executor = pe
	} else {
		applyWorker.ResetTx(applyTx)

		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:                      cfg,
				execStage:                execStage,
				rs:                       rs,
				doms:                     doms,
				agg:                      agg,
				u:                        u,
				isMining:                 isMining,
				inMemExec:                inMemExec,
				shouldGenerateChangesets: shouldGenerateChangesets,
				applyTx:                  applyTx,
				logger:                   logger,
				progress:                 NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
			},
			applyWorker: applyWorker,
		}

		executor = se
	}

	defer func() {
		executor.LogComplete()
	}()

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

	var uncommittedGas uint64
	var b *types.Block

	err = func() error {
		var readAhead chan uint64
		// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
		// can't use OS-level ReadAhead - because Data >> RAM
		// it also warmsup state a bit - by touching senders/coninbase accounts and code
		if !execStage.CurrentSyncCycle.IsInitialCycle {
			var clean func()

			readAhead, clean = blocksReadAhead(ctx, &cfg, 4, true)
			defer clean()
		}

		// Only needed by bor chains
		shouldGenerateChangesetsForLastBlocks := false //cfg.chainConfig.Bor != nil

		for ; blockNum <= maxBlockNum; blockNum++ {
			// set shouldGenerateChangesets=true if we are at last n blocks from maxBlockNum. this is as a safety net in chains
			// where during initial sync we can expect bogus blocks to be imported.
			if !shouldGenerateChangesets && shouldGenerateChangesetsForLastBlocks && blockNum > cfg.blockReader.FrozenBlocks() && blockNum+changesetSafeRange >= maxBlockNum {
				aggTx := libstate.AggTx(executor.tx())
				aggTx.RestrictSubsetFileDeletions(true)
				start := time.Now()
				executor.domains().SetChangesetAccumulator(nil) // Make sure we don't have an active changeset accumulator
				// First compute and commit the progress done so far
				if _, err := executor.domains().ComputeCommitment(ctx, executor.tx(), true, blockNum, execStage.LogPrefix()); err != nil {
					return err
				}
				ts += time.Since(start)
				aggTx.RestrictSubsetFileDeletions(false)
				shouldGenerateChangesets = true // now we can generate changesets for the safety net
			}
			changeset := &libstate.StateChangeSet{}
			if shouldGenerateChangesets && blockNum > 0 {
				executor.domains().SetChangesetAccumulator(changeset)
			}

			select {
			case readAhead <- blockNum:
			default:
			}

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
			totalGasUsed += b.GasUsed()
			getHashFnMutex := sync.Mutex{}

			blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
				getHashFnMutex.Lock()
				defer getHashFnMutex.Unlock()
				return executor.getHeader(ctx, hash, number)
			}), cfg.engine, cfg.author, chainConfig)

			if !parallel && accumulator != nil {
				txs, err := blockReader.RawTransactions(context.Background(), executor.tx(), b.NumberU64(), b.NumberU64())
				if err != nil {
					return err
				}
				accumulator.StartChange(b.NumberU64(), b.Hash(), txs, false)
			}

			// During the first block execution, we may have half-block data in the snapshots.
			// Thus, we need to skip the first txs in the block, however, this causes the GasUsed to be incorrect.
			// So we skip that check for the first block, if we find half-executed data.
			skipPostEvaluation := false
			var txTasks []exec.Task

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
					HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),
					Config:           chainConfig,
					Trace:            false, //blockNum == 14936592 || blockNum == 14753281 || blockNum == 14935178 || blockNum == 14935090,
				}

				if cfg.genesis != nil {
					txTask.Config = cfg.genesis.Config
				}

				if txTask.TxNum <= doms.TxNum() && txTask.TxNum > 0 {
					inputTxNum++
					skipPostEvaluation = true
					continue
				}

				txTasks = append(txTasks, txTask)
				stageProgress = blockNum
				inputTxNum++
			}

			if parallel {
				if _, err := executor.execute(ctx, txTasks, false); err != nil {
					return err
				}
				agg.BuildFilesInBackground(outputTxNum.Load())
			} else {
				se := executor.(*serialExecutor)
				se.skipPostEvaluation = skipPostEvaluation

				//fmt.Println("Block",  blockNum)

				continueLoop, err := se.execute(ctx, txTasks, false)

				if err != nil {
					return err
				}

				if blockNum == 66921971 {
					fmt.Println("Complete", blockNum)
					panic("done")
				}

				uncommittedGas = se.executedGas - se.committedGas
				mxExecBlocks.Add(1)

				if !continueLoop {
					return nil
				}

				if false /*shouldGenerateChangesets*/ {
					aggTx := libstate.AggTx(executor.tx())
					aggTx.RestrictSubsetFileDeletions(true)
					start := time.Now()
					rh, err := executor.domains().ComputeCommitment(ctx, executor.tx(), true, blockNum, execStage.LogPrefix())
					if err != nil {
						return err
					}
					if !bytes.Equal(rh, header.Root.Bytes()) {
						logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", execStage.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
						return errors.New("wrong trie root")
					}

					ts += time.Since(start)
					aggTx.RestrictSubsetFileDeletions(false)
					executor.domains().SavePastChangesetAccumulator(b.Hash(), blockNum, changeset)
					if !inMemExec {
						if err := libstate.WriteDiffSet(executor.tx(), blockNum, b.Hash(), changeset); err != nil {
							return err
						}
					}
					executor.domains().SetChangesetAccumulator(nil)
				}
			}

			if offsetFromBlockBeginning > 0 {
				// after history execution no offset will be required
				offsetFromBlockBeginning = 0
			}

			// MA commitTx
			if parallel {
				blockResult := executor.processEvents(ctx, false)

				if blockResult != nil {
					if blockResult.Err != nil {
						return blockResult.Err
					}

					if blockResult.complete {
						outputTxNum.Store(blockResult.lastTxNum)
						stages.SyncMetrics[stages.Execution].SetUint64(blockResult.BlockNum)
					}
				}
			} else {
				if !inMemExec && !isMining {
					metrics2.UpdateBlockConsumerPostExecutionDelay(b.Time(), blockNum, logger)
				}

				select {
				case <-logEvery.C:
					if inMemExec || isMining {
						break
					}

					executor.LogExecuted()

					//TODO: https://github.com/erigontech/erigon/issues/10724
					//if executor.tx().(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).CanPrune(executor.tx(), outputTxNum.Load()) {
					//	//small prune cause MDBX_TXN_FULL
					//	if _, err := executor.tx().(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
					//		return err
					//	}
					//}

					aggregatorRo := libstate.AggTx(executor.tx())

					needCalcRoot := executor.readState().SizeEstimate() >= commitThreshold ||
						skipPostEvaluation //|| // If we skip post evaluation, then we should compute root hash ASAP for fail-fast
						//TEMP aggregatorRo.CanPrune(executor.tx(), outputTxNum.Load()) // if have something to prune - better prune ASAP to keep chaindata smaller
					if !needCalcRoot {
						break
					}

					var (
						commitStart = time.Now()
						tt          = time.Now()

						t1, t3 time.Duration
					)

					se := executor.(*serialExecutor)

					if ok, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), executor.tx(), executor.domains(), cfg, execStage, stageProgress, logger, u, inMemExec); err != nil {
						return err
					} else {
						if !ok {
							return nil
						}

						se.lastCommittedBlockNum = b.NumberU64()
						se.lastCommittedTxNum = inputTxNum
						se.committedGas += uncommittedGas
						uncommittedGas = 0
					}

					executor.LogCommitted(commitStart)

					t1 = time.Since(tt) + ts

					tt = time.Now()
					if _, err := aggregatorRo.PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
						return err
					}
					t3 = time.Since(tt)

					t2, err := se.commit(ctx, inputTxNum, useExternalTx)
					if err != nil {
						return err
					}

					// on chain-tip: if batch is full then stop execution - to allow stages commit
					if !execStage.CurrentSyncCycle.IsInitialCycle {
						return nil
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

		return nil
	}()

	fmt.Println("Process Events")

	for {
		blockResult := executor.processEvents(ctx, true)

		if blockResult == nil {
			break
		}

		if blockResult.Err != nil {
			return blockResult.Err
		}

		if blockResult.complete {
			outputTxNum.Store(blockResult.lastTxNum)
			stages.SyncMetrics[stages.Execution].SetUint64(blockResult.BlockNum)
		}

	}

	//log.Info("Executed", "blocks", inputBlockNum.Load(), "txs", outputTxNum.Load(), "repeats", mxExecRepeats.GetValueUint64())

	fmt.Println("Wait")
	executor.wait(ctx)

	if u != nil && !u.HasUnwindPoint() {
		if b != nil {
			commitStart := time.Now()

			_, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), executor.tx(), executor.domains(), cfg, execStage, stageProgress, logger, u, inMemExec)
			if err != nil {
				return err
			}

			if !parallel {
				se := executor.(*serialExecutor)
				se.lastCommittedBlockNum = b.NumberU64()
				se.lastCommittedTxNum = inputTxNum
				se.committedGas += uncommittedGas
				uncommittedGas = 0

				executor.LogCommitted(commitStart)
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
func dumpPlainStateDebug(tx kv.RwTx, doms *libstate.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx)
	}
	{
		it, err := libstate.AggTx(tx).RangeLatest(tx, kv.AccountsDomain, nil, nil, -1)
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
		it, err := libstate.AggTx(tx).RangeLatest(tx, kv.StorageDomain, nil, nil, -1)
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
		it, err := libstate.AggTx(tx).RangeLatest(tx, kv.CommitmentDomain, nil, nil, -1)
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
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.RwTx, doms *libstate.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, logger log.Logger, u Unwinder, inMemExec bool) (bool, error) {

	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	if err := e.Update(applyTx, maxBlockNum); err != nil {
		return false, err
	}
	if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
		return false, fmt.Errorf("writing plain state version: %w", err)
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

	rh, err := doms.ComputeCommitment(ctx, applyTx, true, header.Number.Uint64(), e.LogPrefix())
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
			if err = libstate.AggTx(applyTx).PruneCommitHistory(ctx, applyTx, nil); err != nil {
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

	aggTx := libstate.AggTx(applyTx)
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
