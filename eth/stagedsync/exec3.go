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

	"github.com/erigontech/erigon-lib/commitment"
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
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
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
	prevActivations       int64
	prevTaskDuration      time.Duration
	prevTaskReadDuration  time.Duration
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

func (p *Progress) LogExecuted(tx kv.Tx, rs *state.StateV3, ex executor) {
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevExecTime)

	var suffix string
	var parallelExecVals []interface{}
	var te *txExecutor

	switch ex := ex.(type) {
	case *parallelExecutor:
		te = &ex.txExecutor
		suffix = " parallel"

		execCount := uint64(te.execCount.Load())
		abortCount := uint64(te.abortCount.Load())
		invalidCount := uint64(te.invalidCount.Load())
		readCount := uint64(te.readCount.Load())
		writeCount := uint64(te.writeCount.Load())

		execDiff := execCount - p.prevExecCount

		var repeats = max(int(execDiff)-int(max(int(te.lastExecutedTxNum)-int(p.prevExecutedTxNum), 0)), 0)
		var repeatRatio float64

		if repeats > 0 {
			repeatRatio = 100.0 * float64(repeats) / float64(execDiff)
		}

		var readRatio float64
		var avgTaskDur time.Duration
		var avgReadDur time.Duration
		var actualWorkersPerSec int64
		var intervalWorkersPerSec int64

		taskDur := time.Duration(ex.execMetrics.Duration.Load())
		readDur := time.Duration(ex.execMetrics.ReadDuration.Load())
		activations := ex.execMetrics.Active.Total.Load()

		curTaskDur := taskDur - p.prevTaskDuration
		curReadDur := readDur - p.prevTaskReadDuration
		curActivations := activations - int64(p.prevActivations)

		p.prevTaskDuration = taskDur
		p.prevTaskReadDuration = readDur
		p.prevActivations = activations

		if curActivations > 0 {
			avgTaskDur = curTaskDur / time.Duration(curActivations)
			avgReadDur = curReadDur / time.Duration(curActivations)
			actualWorkersPerSec = int64(float64(curActivations) / curTaskDur.Seconds())
			intervalWorkersPerSec = int64(float64(curActivations) / interval.Seconds())

			if avgTaskDur > 0 {
				readRatio = 100.0 * float64(avgReadDur) / float64(avgTaskDur)
			}
		}

		parallelExecVals = []interface{}{
			"exec", common.PrettyCounter(execDiff),
			"repeat%", fmt.Sprintf("%.2f", repeatRatio),
			"abort", common.PrettyCounter(abortCount - p.prevAbortCount),
			"invalid", common.PrettyCounter(invalidCount - p.prevInvalidCount),
			"workers", fmt.Sprintf("%d(%d)", ex.execMetrics.Active.Ema.Get(), actualWorkersPerSec/intervalWorkersPerSec),
			"tskd", fmt.Sprintf("%dµs", avgTaskDur.Microseconds()),
			"tskrd", fmt.Sprintf("%dµs(%.2f%%)", avgReadDur.Microseconds(), readRatio),
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
		te = &ex.txExecutor
		suffix = " serial"
	}

	executedGasSec := uint64(float64(te.executedGas-p.prevExecutedGas) / interval.Seconds())
	var executedTxSec uint64

	if te.lastExecutedTxNum > p.prevExecutedTxNum {
		executedTxSec = uint64(float64(te.lastExecutedTxNum-p.prevExecutedTxNum) / interval.Seconds())
	}
	executedDiffBlocks := max(int(te.lastExecutedBlockNum)-int(p.prevExecutedBlockNum), 0)
	executedDiffTxs := uint64(max(int(te.lastExecutedTxNum)-int(p.prevExecutedTxNum), 0))

	p.log("executed", suffix, tx, te, rs, interval, te.lastExecutedBlockNum, executedDiffBlocks,
		executedDiffTxs, executedTxSec, executedGasSec, false, parallelExecVals)

	p.prevExecTime = currentTime

	if te.lastExecutedBlockNum > 0 {
		p.prevExecutedTxNum = te.lastExecutedTxNum
		p.prevExecutedGas = te.executedGas
		p.prevExecutedBlockNum = te.lastExecutedBlockNum
	}
}

func (p *Progress) LogCommitted(tx kv.Tx, commitStart time.Time, rs *state.StateV3, ex executor) {
	var te *txExecutor
	var suffix string

	switch ex := ex.(type) {
	case *parallelExecutor:
		te = &ex.txExecutor
		suffix = " parallel"

	case *serialExecutor:
		te = &ex.txExecutor
		suffix = " serial"
	}

	if p.prevCommitTime.Before(commitStart) {
		p.prevCommitTime = commitStart
	}

	currentTime := time.Now()
	interval := currentTime.Sub(p.prevExecTime)

	if te.shouldGenerateChangesets {
		suffix += "(commit every block)"
	}

	committedGasSec := uint64(float64(te.committedGas-p.prevCommittedGas) / interval.Seconds())
	var committedTxSec uint64
	if te.lastCommittedTxNum > p.prevCommittedTxNum {
		committedTxSec = uint64(float64(te.lastCommittedTxNum-p.prevCommittedTxNum) / interval.Seconds())
	}
	committedDiffBlocks := max(int(te.lastCommittedBlockNum)-int(p.prevCommittedBlockNum), 0)

	p.log("committed", suffix, tx, te, rs, interval, te.lastCommittedBlockNum, committedDiffBlocks,
		te.lastCommittedTxNum-p.prevCommittedTxNum, committedTxSec, committedGasSec, true, nil)

	p.prevCommitTime = currentTime

	if te.lastCommittedTxNum > 0 {
		p.prevCommittedTxNum = te.lastCommittedTxNum
		p.prevCommittedGas = te.committedGas
		p.prevCommittedBlockNum = te.lastCommittedBlockNum
	}
}

func (p *Progress) LogComplete(tx kv.Tx, rs *state.StateV3, ex executor) {
	interval := time.Since(p.initialTime)
	var te *txExecutor
	var suffix string

	switch ex := ex.(type) {
	case *parallelExecutor:
		te = &ex.txExecutor
		suffix = " parallel"

	case *serialExecutor:
		te = &ex.txExecutor
		suffix = " serial"
	}

	gas := te.committedGas

	if gas == 0 {
		gas = te.executedGas
	}

	lastTxNum := te.lastCommittedTxNum

	if lastTxNum == 0 {
		lastTxNum = te.lastExecutedTxNum
	}

	lastBlockNum := te.lastCommittedBlockNum

	if lastBlockNum == 0 {
		lastBlockNum = te.lastExecutedBlockNum
	}

	gasSec := uint64(float64(gas) / interval.Seconds())
	var txSec uint64
	if lastTxNum > p.initialTxNum {
		txSec = uint64((float64(lastTxNum) - float64(p.initialTxNum)) / interval.Seconds())
	}
	diffBlocks := max(int(lastBlockNum)-int(p.initialBlockNum), 0)

	p.log("done", suffix, tx, te, rs, interval, lastBlockNum, diffBlocks, lastTxNum-p.initialTxNum, txSec, gasSec, true, nil)
}

func (p *Progress) log(mode string, suffix string, tx kv.Tx, te *txExecutor, rs *state.StateV3, interval time.Duration,
	blk uint64, blks int, txs uint64, txsSec uint64, gasSec uint64, logSteps bool, extraVals []interface{}) {

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()

	stepsInDb := rawdbhelpers.IdxStepsCountV3(tx)
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
			"step", fmt.Sprintf("%.1f", float64(te.lastCommittedTxNum)/float64(config3.DefaultStepSize)),
		}...)
	}

	vals = append(vals, []interface{}{
		"buf", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		"inMem", te.inMemExec,
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

var tracedBlocks map[uint64]struct{}
var tracedTxIndexes map[int64]struct{}

func traceBlock(blockNum uint64) bool {
	if tracedBlocks == nil {
		tracedBlocks = map[uint64]struct{}{}
		for _, blockNum := range dbg.TraceBlocks {
			tracedBlocks[blockNum] = struct{}{}
		}
	}

	_, ok := tracedBlocks[blockNum]
	return ok
}

func traceTx(blockNum uint64, txIndex int) bool {
	if !traceBlock(blockNum) {
		return false
	}

	if tracedTxIndexes == nil {
		tracedTxIndexes = map[int64]struct{}{}
		for _, index := range dbg.TraceTxIndexes {
			tracedTxIndexes[index] = struct{}{}
		}
	}

	if len(tracedTxIndexes) != 0 {
		if _, ok := tracedTxIndexes[int64(txIndex)]; !ok {
			return false
		}
	}

	return true
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
		var err error
		applyTx, err = cfg.db.BeginRw(ctx) //nolint
		if err != nil {
			return err
		}
		defer func() { // need callback - because tx may be committed
			applyTx.Rollback()
		}()
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

	shouldGenerateChangesets := maxBlockNum-blockNum <= changesetSafeRange || cfg.syncCfg.AlwaysGenerateChangesets
	if blockNum < cfg.blockReader.FrozenBlocks() {
		shouldGenerateChangesets = false
	}

	shouldReportToTxPool := cfg.notifications != nil && !isMining && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(doms, logger))

	defer cfg.applyWorker.LogLRUStats()

	commitThreshold := cfg.batchSize.Bytes()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	flushEvery := time.NewTicker(2 * time.Second)
	defer flushEvery.Stop()

	var executor executor
	var executorContext context.Context
	var executorCancel context.CancelFunc

	if parallel {
		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:                      cfg,
				rs:                       rs,
				doms:                     doms,
				agg:                      agg,
				accumulator:              accumulator,
				shouldGenerateChangesets: shouldGenerateChangesets,
				isMining:                 isMining,
				inMemExec:                inMemExec,
				logger:                   logger,
				logPrefix:                execStage.LogPrefix(),
				progress:                 NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:        execStage.CurrentSyncCycle.IsInitialCycle,
			},
			workerCount: workerCount,
		}

		executorContext, executorCancel = pe.run(ctx)

		defer executorCancel()

		executor = pe
	} else {
		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:                      cfg,
				rs:                       rs,
				doms:                     doms,
				agg:                      agg,
				u:                        u,
				isMining:                 isMining,
				inMemExec:                inMemExec,
				shouldGenerateChangesets: shouldGenerateChangesets,
				applyTx:                  applyTx,
				logger:                   logger,
				logPrefix:                execStage.LogPrefix(),
				progress:                 NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:        execStage.CurrentSyncCycle.IsInitialCycle,
			},
		}

		executor = se
	}

	executor.resetWorkers(ctx, rs, applyTx)

	defer func() {
		executor.LogComplete(applyTx)
	}()

	ts := time.Duration(0)
	blockNum = executor.domains().BlockNum()

	if maxBlockNum < blockNum {
		return nil
	}

	if maxBlockNum > blockNum+16 {
		log.Info(fmt.Sprintf("[%s] starting", execStage.LogPrefix()),
			"from", blockNum, "to", maxBlockNum, "fromTxNum", outputTxNum.Load(), "offsetFromBlockBeginning", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx)
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	var uncommittedGas uint64
	var b *types.Block

	var readAhead chan uint64
	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	if !execStage.CurrentSyncCycle.IsInitialCycle {
		var clean func()

		readAhead, clean = blocksReadAhead(ctx, &cfg, 4, true)
		defer clean()
	}

	if !parallel {
		err = func() error {
			// Only needed by bor chains
			shouldGenerateChangesetsForLastBlocks := false //cfg.chainConfig.Bor != nil
			havePartialBlock := false

			for ; blockNum <= maxBlockNum; blockNum++ {
				// set shouldGenerateChangesets=true if we are at last n blocks from maxBlockNum. this is as a safety net in chains
				// where during initial sync we can expect bogus blocks to be imported.
				if !shouldGenerateChangesets && shouldGenerateChangesetsForLastBlocks && blockNum > cfg.blockReader.FrozenBlocks() && blockNum+changesetSafeRange >= maxBlockNum {
					aggTx := libstate.AggTx(applyTx)
					aggTx.RestrictSubsetFileDeletions(true)
					start := time.Now()
					executor.domains().SetChangesetAccumulator(nil) // Make sure we don't have an active changeset accumulator
					// First compute and commit the progress done so far
					if _, err := executor.domains().ComputeCommitment(ctx, applyTx, true, blockNum, execStage.LogPrefix()); err != nil {
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

				b, err = blockWithSenders(ctx, cfg.db, applyTx, blockReader, blockNum)
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
					txs, err := blockReader.RawTransactions(context.Background(), applyTx, b.NumberU64(), b.NumberU64())
					if err != nil {
						return err
					}
					accumulator.StartChange(b.NumberU64(), b.Hash(), txs, false)
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
						SkipAnalysis:    skipAnalysis,
						EvmBlockContext: blockContext,
						Withdrawals:     b.Withdrawals(),

						// use history reader instead of state reader to catch up to the tx where we left off
						HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),
						Config:           chainConfig,
						Trace:            traceTx(blockNum, txIndex),
					}

					if cfg.genesis != nil {
						txTask.Config = cfg.genesis.Config
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

				se := executor.(*serialExecutor)

				continueLoop, err := se.execute(ctx, txTasks, execStage.CurrentSyncCycle.IsInitialCycle, false)

				if err != nil {
					return err
				}

				uncommittedGas = se.executedGas - se.committedGas
				mxExecBlocks.Add(1)

				if !continueLoop {
					return nil
				}

				if shouldGenerateChangesets {
					aggTx := libstate.AggTx(applyTx)
					aggTx.RestrictSubsetFileDeletions(true)
					start := time.Now()
					if traceBlock(blockNum) {
						se.doms.SetTrace(true)
					}
					rh, err := executor.domains().ComputeCommitment(ctx, applyTx, true, blockNum, execStage.LogPrefix())
					se.doms.SetTrace(false)
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
						if err := libstate.WriteDiffSet(applyTx, blockNum, b.Hash(), changeset); err != nil {
							return err
						}
					}
					executor.domains().SetChangesetAccumulator(nil)
				}

				if dbg.StopAfterBlock > 0 && blockNum == dbg.StopAfterBlock {
					return fmt.Errorf("stopping: block %d complete", blockNum)
				}

				if offsetFromBlockBeginning > 0 {
					// after history execution no offset will be required
					offsetFromBlockBeginning = 0
				}

				// MA commitTx
				if !inMemExec && !isMining {
					metrics2.UpdateBlockConsumerPostExecutionDelay(b.Time(), blockNum, logger)
				}

				select {
				case <-logEvery.C:
					if inMemExec || isMining {
						break
					}

					executor.LogExecuted(applyTx)

					//TODO: https://github.com/erigontech/erigon/issues/10724
					//if executor.tx().(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).CanPrune(executor.tx(), outputTxNum.Load()) {
					//	//small prune cause MDBX_TXN_FULL
					//	if _, err := executor.tx().(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
					//		return err
					//	}
					//}

					needCalcRoot := executor.readState().SizeEstimate() >= commitThreshold || havePartialBlock
					// If we have a partial first block it may not be validated, then we should compute root hash ASAP for fail-fast
					//TEMP aggregatorRo.CanPrune(executor.tx(), outputTxNum.Load()) // if have something to prune - better prune ASAP to keep chaindata smaller

					// this will only happen for the first executed block
					havePartialBlock = false

					if !needCalcRoot {
						break
					}

					var (
						commitStart = time.Now()
						tt          = time.Now()

						t1 time.Duration
					)

					se := executor.(*serialExecutor)

					if ok, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, executor.domains(), cfg, execStage, stageProgress, 10*time.Hour, logger, u, inMemExec); err != nil {
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

					executor.LogCommitted(applyTx, commitStart)

					t1 = time.Since(tt) + ts

					var t2 time.Duration
					applyTx, t2, err = se.commit(ctx, execStage, applyTx, nil, useExternalTx)
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
						"flush+commitment+prune", t1, "tx.commit", t2)
				default:
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			return nil
		}()
	} else {
		pe := executor.(*parallelExecutor)

		var asyncTxChan mdbx.TxApplyChan
		var asyncTx kv.Tx

		switch applyTx := applyTx.(type) {
		case *mdbx.MdbxTx:
			asyncTx = mdbx.NewAsyncTx(applyTx, 1000)
			asyncTxChan = asyncTx.(mdbx.TxApplySource).ApplyChan()
		case *temporal.RwTx:
			temporalTx := applyTx.AsyncClone(mdbx.NewAsyncRwTx(applyTx.RwTx, 1000))
			asyncTxChan = temporalTx.ApplyChan()
			asyncTx = temporalTx
		}

		applyResults := make(chan applyResult, 100_000)

		if err := executor.executeBlocks(ctx, asyncTx, blockNum, maxBlockNum, readAhead, applyResults); err != nil {
			return err
		}

		var lastBlockResult blockResult
		var uncommittedGas uint64
		var flushPending bool

		err = func() error {
			defer func() {
				if rec := recover(); rec != nil {
					pe.logger.Warn("["+execStage.LogPrefix()+"] rw panic", "rec", rec, "stack", dbg.Stack())
				} else if err != nil && !errors.Is(err, context.Canceled) {
					pe.logger.Warn("["+execStage.LogPrefix()+"] rw exit", "err", err)
				} else {
					pe.logger.Debug("[" + execStage.LogPrefix() + "] rw exit")
				}
			}()

			for {
				select {
				case request := <-asyncTxChan:
					request.Apply()
				case applyResult := <-applyResults:
					switch applyResult := applyResult.(type) {
					case *txResult:
						pe.executedGas += applyResult.gasUsed
						pe.lastExecutedTxNum = applyResult.txNum

						pe.rs.SetTxNum(applyResult.txNum, applyResult.blockNum)

						if err := pe.rs.ApplyState4(ctx, applyTx,
							applyResult.blockNum, applyResult.txNum, applyResult.writeSet,
							nil, applyResult.logs, applyResult.traceFroms, applyResult.traceTos,
							pe.cfg.chainConfig, pe.cfg.chainConfig.Rules(applyResult.blockNum, applyResult.blockTime), false); err != nil {
							return err
						}

					case *blockResult:
						if applyResult.BlockNum > lastBlockResult.BlockNum {
							pe.doms.SetTxNum(applyResult.lastTxNum)
							pe.doms.SetBlockNum(applyResult.BlockNum)
							lastBlockResult = *applyResult
							pe.lastExecutedBlockNum = applyResult.BlockNum
							uncommittedGas += applyResult.GasUsed
						}

						flushPending = pe.rs.SizeEstimate() > pe.cfg.batchSize.Bytes()

						if !dbg.DiscardCommitment() {
							if shouldGenerateChangesets || lastBlockResult.BlockNum == maxBlockNum ||
								(flushPending && lastBlockResult.BlockNum > pe.lastCommittedBlockNum) {
								var trace bool
								if traceBlock(applyResult.BlockNum) {
									trace = true
								}
								pe.doms.SetTrace(trace)
								if !dbg.BatchCommitments {
									commitment.Captured = []string{}
								}
								rh, err := pe.doms.ComputeCommitment(ctx, applyTx, true, applyResult.BlockNum, pe.logPrefix)
								pe.doms.SetTrace(false)
								captured := commitment.Captured
								commitment.Captured = nil
								if err != nil {
									return err
								}
								if !bytes.Equal(rh, applyResult.StateRoot.Bytes()) {
									logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", pe.logPrefix, applyResult.BlockNum, rh, applyResult.StateRoot.Bytes(), applyResult.BlockHash))
									if !dbg.BatchCommitments {
										for _, line := range captured {
											fmt.Println(line)
										}

										maxTxIndex := len(applyResult.TxIO.Inputs()) - 1

										for txIndex := -1; txIndex < maxTxIndex; txIndex++ {
											fmt.Println(
												fmt.Sprintf("%d (%d) RD", applyResult.BlockNum, txIndex), applyResult.TxIO.ReadSet(txIndex).Len(),
												"WRT", len(applyResult.TxIO.WriteSet(txIndex)))

											applyResult.TxIO.ReadSet(txIndex).Scan(func(vr *state.VersionedRead) bool {
												fmt.Println(fmt.Sprintf("%d (%d)", applyResult.BlockNum, txIndex), "RD", vr.String())
												return true
											})

											for _, vw := range applyResult.TxIO.WriteSet(txIndex) {
												fmt.Println(fmt.Sprintf("%d (%d)", applyResult.BlockNum, txIndex), "WRT", vw.String())
											}
										}
									}
									return errors.New("wrong trie root")
								}

								pe.lastCommittedBlockNum = lastBlockResult.BlockNum
								pe.lastCommittedTxNum = lastBlockResult.lastTxNum
								pe.committedGas += uncommittedGas
								uncommittedGas = 0
							}
						}

						//fmt.Println("Block Complete", blockResult.BlockNum)
						if dbg.StopAfterBlock > 0 && applyResult.BlockNum == dbg.StopAfterBlock {
							return fmt.Errorf("stopping: block %d complete", applyResult.BlockNum)
						}

						if maxBlockNum == applyResult.BlockNum {
							return nil
						}

					}
				case <-executorContext.Done():
					err = executor.wait(ctx)
					return fmt.Errorf("executor context failed: %w", err)
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					pe.LogExecuted(applyTx)
					if pe.agg.HasBackgroundFilesBuild() {
						logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
					}
				case <-flushEvery.C:
					if flushPending {
						flushPending = false

						if !pe.inMemExec {
							if err := pe.doms.Flush(ctx, applyTx, 150*time.Millisecond); err != nil {
								return err
							}
						}

						var t2 time.Duration
						commitStart := time.Now()
						applyTx, t2, err = pe.commit(ctx, execStage, applyTx, asyncTxChan, useExternalTx)
						if err != nil {
							return err
						}
						logger.Info("Committed", "time", time.Since(commitStart), "commit", t2)
					}
				}
			}
		}()

		executorCancel()

		if err != nil {
			if !errors.Is(err, context.Canceled) {
				return err
			}
		}

		if err := pe.doms.Flush(ctx, applyTx, 150*time.Millisecond); err != nil {
			return err
		}

		if execStage != nil {
			if err := execStage.Update(applyTx, lastBlockResult.BlockNum); err != nil {
				return err
			}
		}
	}

	executor.wait(ctx)

	if !parallel && u != nil && !u.HasUnwindPoint() {
		if b != nil {
			commitStart := time.Now()

			_, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, executor.domains(), cfg, execStage, stageProgress, 150*time.Millisecond, logger, u, inMemExec)
			if err != nil {
				return err
			}

			se := executor.(*serialExecutor)
			se.lastCommittedBlockNum = b.NumberU64()
			se.lastCommittedTxNum = inputTxNum
			se.committedGas += uncommittedGas
			uncommittedGas = 0

			_, _, err = se.commit(ctx, execStage, applyTx, nil, useExternalTx)
			if err != nil {
				return err
			}

			executor.LogCommitted(applyTx, commitStart)
		} else {
			fmt.Printf("[dbg] mmmm... do we need action here????\n")
		}
	}

	//dumpPlainStateDebug(executor.tx(), executor.domains())

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	return nil
}

// nolint
func dumpPlainStateDebug(tx kv.RwTx, doms *libstate.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx, 0)
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
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.RwTx, doms *libstate.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, pruneTimeout time.Duration, logger log.Logger, u Unwinder, inMemExec bool) (bool, error) {

	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
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
			if err := doms.Flush(ctx, applyTx, pruneTimeout); err != nil {
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
