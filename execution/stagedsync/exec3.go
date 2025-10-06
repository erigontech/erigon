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
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cmp"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/turbo/shards"
	"golang.org/x/sync/errgroup"
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
		defer func() {
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
		blockNum      = doms.BlockNum()
		initialTxNum  = doms.TxNum()
	)

	if maxBlockNum < blockNum {
		return nil
	}

	agg.BuildFilesInBackground(doms.TxNum())

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
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx)
	blockNum = doms.BlockNum()

	if maxBlockNum < blockNum {
		return nil
	}

	var lastHeader *types.Header

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

	var lastCommittedTxNum uint64
	var lastCommittedBlockNum uint64

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
				progress:              NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:     execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:                 hooks,
				lastCommittedTxNum:    doms.TxNum(),
				lastCommittedBlockNum: blockNum,
			},
			workerCount: workerCount,
		}

		defer func() {
			pe.LogComplete(stepsInDb)
		}()

		flushEvery := time.NewTicker(2 * time.Second)
		defer flushEvery.Stop()

		lastHeader, applyTx, execErr = pe.exec(ctx, execStage, u, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
			initialTxNum, inputTxNum, useExternalTx, initialCycle, applyTx, accumulator, readAhead, logEvery, flushEvery)

		lastCommittedBlockNum = pe.lastCommittedBlockNum
		lastCommittedTxNum = pe.lastCommittedTxNum
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
				progress:              NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:     execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:                 hooks,
				lastCommittedTxNum:    doms.TxNum(),
				lastCommittedBlockNum: blockNum,
			}}

		defer func() {
			se.LogComplete(stepsInDb)
		}()

		lastHeader, applyTx, execErr = se.exec(ctx, execStage, u, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
			initialTxNum, inputTxNum, useExternalTx, initialCycle, applyTx, accumulator, readAhead, logEvery)

		if u != nil && !u.HasUnwindPoint() {
			if lastHeader != nil {
				switch {
				case errors.Is(execErr, ErrWrongTrieRoot):
					execErr = handleIncorrectRootHashError(
						lastHeader.Number.Uint64(), lastHeader.Hash(), lastHeader.ParentHash, applyTx, cfg, execStage, maxBlockNum, logger, u)
				case errors.Is(execErr, context.Canceled):
					return err
				default:
					_, _, err = flushAndCheckCommitmentV3(ctx, lastHeader, applyTx, se.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
					if err != nil {
						return err
					}

					se.lastCommittedBlockNum = lastHeader.Number.Uint64()
					committedTransactions := inputTxNum - se.lastCommittedTxNum
					se.lastCommittedTxNum = inputTxNum

					commitStart := time.Now()
					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx)
					applyTx, _, err = se.commit(ctx, execStage, applyTx, nil, useExternalTx)
					if err != nil {
						return err
					}

					if !useExternalTx {
						se.LogCommitted(commitStart, 0, committedTransactions, 0, stepsInDb, commitment.CommitProgress{})
					}
				}
			} else {
				if execErr != nil {
					switch {
					case errors.Is(execErr, ErrWrongTrieRoot):
						return fmt.Errorf("can't handle incorrect root err: %w", execErr)
					case errors.Is(execErr, context.Canceled):
						return err
					}
				} else {
					return fmt.Errorf("last processed block unexpectedly nil")
				}
			}
		}

		lastCommittedBlockNum = se.lastCommittedBlockNum
		lastCommittedTxNum = se.lastCommittedTxNum
	}

	if false && !inMemExec {
		dumpPlainStateDebug(applyTx, doms)
	}

	lastCommitedStep := kv.Step((lastCommittedTxNum) / doms.StepSize())
	lastFrozenStep := applyTx.StepsInFiles(kv.CommitmentDomain)

	if lastCommitedStep > 0 && lastCommitedStep <= lastFrozenStep && !dbg.DiscardCommitment() {
		logger.Warn("["+execStage.LogPrefix()+"] can't persist comittement: txn step frozen",
			"block", lastCommittedBlockNum, "txNum", lastCommittedTxNum, "step", lastCommitedStep,
			"lastFrozenStep", lastFrozenStep, "lastFrozenTxNum", ((lastFrozenStep+1)*kv.Step(doms.StepSize()))-1)
		return fmt.Errorf("can't persist comittement for blockNum %d, txNum %d: step %d is frozen",
			lastCommittedBlockNum, lastCommittedTxNum, lastCommitedStep)
	}

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}

	agg.BuildFilesInBackground(doms.TxNum())

	if !shouldReportToTxPool && cfg.notifications != nil && cfg.notifications.Accumulator != nil && !isMining && lastHeader != nil {
		// No reporting to the txn pool has been done since we are not within the "state-stream" window.
		// However, we should still at the very least report the last block number to it, so it can update its block progress.
		// Otherwise, we can get in a deadlock situation when there is a block building request in environments where
		// the Erigon process is the only block builder (e.g. some Hive tests, kurtosis testnets with one erigon block builder, etc.)
		cfg.notifications.Accumulator.StartChange(lastHeader, nil, false /* unwind */)
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

type txExecutor struct {
	sync.RWMutex
	cfg              ExecuteBlockCfg
	agg              *dbstate.Aggregator
	rs               *state.StateV3Buffered
	doms             *dbstate.SharedDomains
	u                Unwinder
	isMining         bool
	inMemExec        bool
	applyTx          kv.TemporalTx
	logger           log.Logger
	logPrefix        string
	progress         *Progress
	taskExecMetrics  *exec3.WorkerMetrics
	blockExecMetrics *blockExecMetrics
	hooks            *tracing.Hooks

	lastExecutedBlockNum  atomic.Int64
	lastExecutedTxNum     atomic.Int64
	executedGas           atomic.Int64
	lastCommittedBlockNum uint64
	lastCommittedTxNum    uint64
	committedGas          int64

	execLoopGroup *errgroup.Group

	execRequests chan *execRequest
	execCount    atomic.Int64
	abortCount   atomic.Int64
	invalidCount atomic.Int64
	readCount    atomic.Int64
	writeCount   atomic.Int64

	enableChaosMonkey bool
}

func (te *txExecutor) readState() *state.StateV3Buffered {
	return te.rs
}

func (te *txExecutor) domains() *dbstate.SharedDomains {
	return te.doms
}

func (te *txExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header, err error) {
	if te.applyTx != nil {
		err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
			h, err = te.cfg.blockReader.Header(ctx, te.applyTx, hash, number)
			return err
		})

		if err != nil {
			return nil, err
		}
	} else {
		if err := te.cfg.db.View(ctx, func(tx kv.Tx) (err error) {
			h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
			return err
		}); err != nil {
			return nil, err
		}
	}

	return h, nil
}

func (te *txExecutor) onBlockStart(ctx context.Context, blockNum uint64, blockHash common.Hash) {
	defer func() {
		if rec := recover(); rec != nil {
			te.logger.Warn("hook paniced: %s", rec, "stack", dbg.Stack())
		}
	}()

	if te.hooks == nil {
		return
	}

	if blockHash == (common.Hash{}) {
		te.logger.Warn("hooks ignored: zero block hash")
		return
	}

	if blockNum == 0 {
		if te.hooks.OnGenesisBlock != nil {
			var b *types.Block
			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				return err
			}); err != nil {
				te.logger.Warn("hook: OnGenesisBlock: abandoned", "err", err)
			}
			te.hooks.OnGenesisBlock(b, te.cfg.genesis.Alloc)
		}
	} else {
		if te.hooks.OnBlockStart != nil {
			var b *types.Block
			var td *big.Int
			var finalized *types.Header
			var safe *types.Header

			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				if err != nil {
					return err
				}
				chainReader := NewChainReaderImpl(te.cfg.chainConfig, te.applyTx, te.cfg.blockReader, te.logger)
				td = chainReader.GetTd(b.ParentHash(), b.NumberU64()-1)
				finalized = chainReader.CurrentFinalizedHeader()
				safe = chainReader.CurrentSafeHeader()
				return nil
			}); err != nil {
				te.logger.Warn("hook: OnBlockStart: abandoned", "err", err)
			}

			te.hooks.OnBlockStart(tracing.BlockEvent{
				Block:     b,
				TD:        td,
				Finalized: finalized,
				Safe:      safe,
			})
		}
	}
}

func (te *txExecutor) executeBlocks(ctx context.Context, tx kv.Tx, blockNum uint64, maxBlockNum uint64, initialTxNum uint64, readAhead chan uint64, applyResults chan applyResult) error {
	inputTxNum, _, offsetFromBlockBeginning, err := restoreTxNum(ctx, &te.cfg, tx, te.doms, maxBlockNum)

	if err != nil {
		return err
	}

	if te.execLoopGroup == nil {
		return errors.New("no exec group")
	}

	te.execLoopGroup.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("exec blocks panic: %s", rec)
			} else if err != nil && !errors.Is(err, context.Canceled) {
				err = fmt.Errorf("exec blocks error: %w", err)
			} else {
				te.logger.Debug("[" + te.logPrefix + "] exec blocks exit")
			}
		}()

		for ; blockNum <= maxBlockNum; blockNum++ {
			select {
			case readAhead <- blockNum:
			default:
			}

			var b *types.Block
			err := tx.Apply(ctx, func(tx kv.Tx) error {
				b, err = exec3.BlockWithSenders(ctx, te.cfg.db, tx, te.cfg.blockReader, blockNum)
				return err
			})
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("nil block %d", blockNum)
			}

			txs := b.Transactions()
			header := b.HeaderNoCopy()
			getHashFnMutex := sync.Mutex{}

			blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, func(hash common.Hash, number uint64) (h *types.Header, err error) {
				getHashFnMutex.Lock()
				defer getHashFnMutex.Unlock()
				err = tx.Apply(ctx, func(tx kv.Tx) (err error) {
					h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
					return err
				})

				if err != nil {
					return nil, err
				}

				return h, err
			}), te.cfg.engine, te.cfg.author, te.cfg.chainConfig)

			var txTasks []exec.Task

			for txIndex := -1; txIndex <= len(txs); txIndex++ {
				if inputTxNum > 0 && inputTxNum <= initialTxNum {
					inputTxNum++
					continue
				}

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
					Config:           te.cfg.chainConfig,
					Engine:           te.cfg.engine,
					Trace:            dbg.TraceTx(blockNum, txIndex),
					Hooks:            te.hooks,
					Logger:           te.logger,
				}

				txTasks = append(txTasks, txTask)
				inputTxNum++
			}

			te.execRequests <- &execRequest{
				b.Number().Uint64(), b.Hash(),
				core.NewGasPool(b.GasLimit(), te.cfg.chainConfig.GetMaxBlobGasPerBlock(b.Time())),
				txTasks, applyResults, false,
			}

			mxExecBlocks.Add(1)

			if offsetFromBlockBeginning > 0 {
				// after history execution no offset will be required
				offsetFromBlockBeginning = 0
			}
		}

		return nil
	})

	return nil
}

func (te *txExecutor) commit(ctx context.Context, execStage *StageState, tx kv.TemporalRwTx, useExternalTx bool, resetWorkers func(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.TemporalTx) error) (kv.TemporalRwTx, time.Duration, error) {
	err := execStage.Update(tx, te.lastCommittedBlockNum)

	if err != nil {
		return nil, 0, err
	}

	_, err = rawdb.IncrementStateVersion(tx)

	if err != nil {
		return nil, 0, fmt.Errorf("writing plain state version: %w", err)
	}

	tx.CollectMetrics()

	var t2 time.Duration

	if !useExternalTx {
		tt := time.Now()
		err = tx.Commit()

		if err != nil {
			return nil, 0, err
		}

		t2 = time.Since(tt)
		dbtx, err := te.cfg.db.BeginRw(ctx) //nolint
		if err != nil {
			return nil, t2, err
		}

		tx = dbtx.(kv.TemporalRwTx)
	}

	err = resetWorkers(ctx, te.rs, tx)

	if err != nil {
		if !useExternalTx {
			tx.Rollback()
		}

		return nil, t2, err
	}

	if !useExternalTx {
		te.agg.BuildFilesInBackground(te.lastCommittedTxNum)
	}

	if !te.inMemExec {
		te.doms.ClearRam(false)
	}

	return tx, t2, nil
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
