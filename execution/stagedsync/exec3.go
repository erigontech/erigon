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
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cmp"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
)

// Cases:
//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
func restoreTxNum(ctx context.Context, cfg *ExecuteBlockCfg, applyTx kv.Tx, currentTxNum uint64, maxBlockNum uint64) (
	inputTxNum uint64, maxTxNum uint64, offsetFromBlockBeginning uint64, blockNum uint64, err error) {

	txNumsReader := cfg.blockReader.TxnumReader()

	inputTxNum = currentTxNum

	lastBlockNum, lastTxNum, err := txNumsReader.Last(applyTx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if lastTxNum == inputTxNum {
		// nothing to exec - return last committed block so caller can sync stage progress
		return 0, 0, 0, lastBlockNum, nil
	}

	maxTxNum, err = txNumsReader.Max(ctx, applyTx, maxBlockNum)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	blockNum, ok, err := txNumsReader.FindBlockNum(ctx, applyTx, currentTxNum)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if !ok {
		lb, lt, _ := txNumsReader.Last(applyTx)
		fb, ft, _ := txNumsReader.First(applyTx)
		return 0, 0, 0, 0, fmt.Errorf("seems broken TxNums index not filled. can't find blockNum of txNum=%d; in db: (%d-%d, %d-%d)", inputTxNum, fb, lb, ft, lt)
	}
	{
		max, _ := txNumsReader.Max(ctx, applyTx, blockNum)
		if currentTxNum == max {
			blockNum++
		}
	}

	min, err := txNumsReader.Min(ctx, applyTx, blockNum)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	if currentTxNum > min {
		// if stopped in the middle of the block: start from beginning of block.
		// first part will be executed in HistoryExecution mode
		offsetFromBlockBeginning = currentTxNum - min
	}

	inputTxNum = min

	return inputTxNum, maxTxNum, offsetFromBlockBeginning, blockNum, nil
}

func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, cfg ExecuteBlockCfg,
	doms *execctx.SharedDomains, rwTx kv.TemporalRwTx,
	parallel bool, //nolint
	maxBlockNum uint64,
	logger log.Logger) (execErr error) {
	isForkValidation := execStage.SyncMode() == stages.ModeForkValidation

	isApplyingBlocks := execStage.SyncMode() == stages.ModeApplyingBlocks
	initialCycle := execStage.CurrentSyncCycle.IsInitialCycle
	hooks := cfg.vmConfig.Tracer
	applyTx := rwTx
	initialTxNum, blockNum, err := doms.SeekCommitment(ctx, applyTx)
	if err != nil {
		return err
	}

	agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if isApplyingBlocks {
		if initialCycle {
			agg.PresetNonChainTipConcurrency()
		} else {
			agg.PresetChainTipConcurrency()
		}
	}

	if maxBlockNum < blockNum {
		return nil
	}

	// Background collation removed from exec code — collation is now managed
	// by CollateAndPrune in the FCU/stage loop (forkchoice.go).
	// Block-snapshot boundary gating happens inside readyForCollation.

	var (
		inputTxNum               uint64
		offsetFromBlockBeginning uint64
		maxTxNum                 uint64
	)

	if inputTxNum, maxTxNum, offsetFromBlockBeginning, blockNum, err = restoreTxNum(ctx, &cfg, applyTx, initialTxNum, maxBlockNum); err != nil {
		return err
	}

	if maxTxNum == 0 {
		// nothing to exec, make sure the stage is in sync with the sd
		// TODO: route through block overlay once serial path initialises one
		if execStage.BlockNumber < blockNum {
			return execStage.Update(rwTx, blockNum)
		}
		return nil
	}

	shouldReportToTxPool := cfg.notifications != nil && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(doms, cfg.syncCfg.PersistReceiptsCacheV2, logger))

	commitThreshold := cfg.batchSize.Bytes()

	logInterval := 20 * time.Second
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx, applyTx.Debug().StepSize())

	if maxBlockNum < blockNum {
		return nil
	}

	var lastHeader *types.Header

	var readAhead chan uint64

	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)

	var lastCommittedTxNum uint64
	var lastCommittedBlockNum uint64

	doms.EnableParaTrieDB(cfg.db)
	doms.EnableTrieWarmup(true)
	doms.SetDeferCommitmentUpdates(false)
	// Enable deferred commitment updates for fork validation and parallel initial sync.
	// Deferred updates batch commitment calculations to block boundaries rather than
	// per-transaction, significantly reducing re-org validation overhead.
	// For the parallel path during initial sync, Flush() now includes pending updates,
	// so they are no longer silently discarded between StageLoopIteration cycles.
	if isForkValidation || (parallel && isApplyingBlocks) {
		doms.SetDeferCommitmentUpdates(true)
	}
	defer doms.SetDeferCommitmentUpdates(false)
	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	if !execStage.CurrentSyncCycle.IsInitialCycle && isApplyingBlocks {
		var clean func()

		readAhead, clean = exec.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}
	if parallel {
		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:               cfg,
				rs:                rs,
				doms:              doms,
				agg:               agg,
				isForkValidation:  isForkValidation,
				isApplyingBlocks:  isApplyingBlocks,
				logger:            logger,
				logPrefix:         execStage.LogPrefix(),
				progress:          NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey: execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:             hooks,
			},
			workerCount: cfg.syncCfg.ExecWorkerCount,
		}
		pe.lastCommittedTxNum.Store(inputTxNum)
		// blockNum is the next block to execute (from doms.BlockNum()), so the last
		// committed block is blockNum-1. LogCommitments uses Add to accumulate deltas
		// on top of this value, so initializing to blockNum would double-count.
		if blockNum > 0 {
			pe.lastCommittedBlockNum.Store(blockNum - 1)
		}

		defer func() {
			pe.LogComplete(stepsInDb)
		}()

		lastHeader, applyTx, execErr = pe.exec(ctx, execStage, u, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
			initialTxNum, inputTxNum, initialCycle, applyTx, stepsInDb, accumulator, readAhead, logEvery)

		lastCommittedBlockNum = pe.lastCommittedBlockNum.Load()
		lastCommittedTxNum = pe.lastCommittedTxNum.Load()
	} else {
		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:               cfg,
				rs:                rs,
				doms:              doms,
				agg:               agg,
				u:                 u,
				isForkValidation:  isForkValidation,
				isApplyingBlocks:  isApplyingBlocks,
				applyTx:           applyTx,
				logger:            logger,
				logPrefix:         execStage.LogPrefix(),
				progress:          NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey: execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:             hooks,
			}}
		se.lastCommittedTxNum.Store(inputTxNum)
		se.lastCommittedBlockNum.Store(blockNum)

		defer func() {
			if isApplyingBlocks {
				se.LogComplete(stepsInDb)
			}
		}()

		lastHeader, applyTx, execErr = se.exec(ctx, execStage, u, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
			initialTxNum, inputTxNum, initialCycle, applyTx, accumulator, readAhead, logEvery)

		if u != nil && !u.HasUnwindPoint() {
			if lastHeader != nil {
				switch {
				case execErr == nil || errors.Is(execErr, &ErrLoopExhausted{}):
					_, _, err = computeAndCheckCommitmentV3(ctx, lastHeader, applyTx, se.domains(), cfg, execStage, parallel, logger, u)
					if err != nil {
						return err
					}

					// Per-block validation runs inline inside each block's apply loop iteration
					// (see blockValidator). No aggregate post-loop Wait needed.

					se.lastCommittedBlockNum.Store(lastHeader.Number.Uint64())
					// Get current txNum from the last executed block
					currentTxNum, err := cfg.blockReader.TxnumReader().Max(ctx, applyTx, lastHeader.Number.Uint64())
					if err != nil {
						return err
					}
					committedTransactions := currentTxNum - se.lastCommittedTxNum.Load()
					se.lastCommittedTxNum.Store(currentTxNum)

					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx, applyTx.Debug().StepSize())

					if initialCycle {
						se.LogCommitments(committedTransactions, stepsInDb, commitment.CommitProgress{})
					}
				case errors.Is(execErr, ErrWrongTrieRoot):
					execErr = handleIncorrectRootHashError(
						lastHeader.Number.Uint64(), lastHeader.Hash(), applyTx, cfg, execStage, logger, u)
				default:
					return execErr
				}
			} else {
				if execErr != nil {
					switch {
					case errors.Is(execErr, ErrWrongTrieRoot):
						return fmt.Errorf("can't handle incorrect root err: %w", execErr)
					case errors.Is(execErr, &ErrLoopExhausted{}):
						break
					default:
						return execErr
					}
				} else {
					return fmt.Errorf("last processed block unexpectedly nil")
				}
			}
		}

		lastCommittedBlockNum = se.lastCommittedBlockNum.Load()
		lastCommittedTxNum = se.lastCommittedTxNum.Load()
	}

	// If execution already failed with ErrInvalidBlock, skip the step-frozen check
	// and propagate the original error directly. The step-frozen check only makes
	// sense when execution succeeded partially and we need to persist the commitment.
	if execErr != nil && errors.Is(execErr, rules.ErrInvalidBlock) {
		// BAD_BLOCK_HALT (env var) gates the os.Exit path; cfg.badBlockHalt alone
		// (set by NewInMemoryExecution for fork validation) must NOT exit — it
		// expects the error to propagate so the caller can finish in-memory
		// validation. Both flags must be true.
		//
		// Intentional os.Exit: BAD_BLOCK_HALT is a debug switch whose whole purpose
		// is to freeze process state at the bad block. Returning would run deferred
		// rollback/commit/flush paths and overwrite the very state we want to
		// inspect. Mirrors the design documented in PR #19803. Applies to both
		// serial and parallel paths uniformly.
		if cfg.badBlockHalt && dbg.BadBlockHalt {
			logger.Error(fmt.Sprintf("[%s] BAD_BLOCK_HALT: halting on invalid block (debug mode, no commit)", execStage.LogPrefix()), "err", execErr)
			os.Exit(1)
		}
		return execErr
	}

	lastCommittedStep := kv.Step((lastCommittedTxNum) / doms.StepSize())
	// applyTx may be stale after parallel execution (the underlying mdbx tx
	// was invalidated by Flush/CommitAndBegin). Use a fresh roTx for the check.
	var lastFrozenStep kv.Step
	if stepCheckTx, stepErr := cfg.db.BeginTemporalRo(ctx); stepErr == nil {
		lastFrozenStep = kv.Step(stepCheckTx.StepsInFiles(kv.CommitmentDomain))
		stepCheckTx.Rollback()
	}

	if lastCommittedStep > 0 && lastCommittedStep < lastFrozenStep && !dbg.DiscardCommitment() {
		logger.Warn("["+execStage.LogPrefix()+"] can't persist commitment: txn step frozen",
			"block", lastCommittedBlockNum, "txNum", lastCommittedTxNum, "step", lastCommittedStep,
			"lastFrozenStep", lastFrozenStep, "lastFrozenTxNum", ((lastFrozenStep+1)*kv.Step(doms.StepSize()))-1)
		return fmt.Errorf("can't persist commitment for blockNum %d, txNum %d: step %d is frozen",
			lastCommittedBlockNum, lastCommittedTxNum, lastCommittedStep)
	}

	if !shouldReportToTxPool && cfg.notifications != nil && cfg.notifications.Accumulator != nil && lastHeader != nil {
		// No reporting to the txn pool has been done since we are not within the "state-stream" window.
		// However, we should still at the very least report the last block number to it, so it can update its block progress.
		// Otherwise, we can get in a deadlock situation when there is a block building request in environments where
		// the Erigon process is the only block builder (e.g. some Hive tests, kurtosis testnets with one erigon block builder, etc.)
		cfg.notifications.Accumulator.StartChange(lastHeader, nil, false /* unwind */)
	}

	return execErr
}

type txExecutor struct {
	sync.RWMutex
	cfg              ExecuteBlockCfg
	agg              *dbstate.Aggregator
	rs               *state.StateV3Buffered
	doms             *execctx.SharedDomains
	u                Unwinder
	isForkValidation bool
	isApplyingBlocks bool
	applyTx          kv.TemporalTx
	logger           log.Logger
	logPrefix        string
	progress         *Progress
	taskExecMetrics  *exec.WorkerMetrics
	blockExecMetrics *blockExecMetrics
	hooks            *tracing.Hooks

	lastExecutedBlockNum  atomic.Int64
	lastExecutedTxNum     atomic.Int64
	executedGas           atomic.Int64
	lastCommittedBlockNum atomic.Uint64
	lastCommittedTxNum    atomic.Uint64
	committedGas          atomic.Int64

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

func (te *txExecutor) domains() *execctx.SharedDomains {
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
			var td *uint256.Int
			var finalized *types.Header
			var safe *types.Header

			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				if err != nil {
					return err
				}
				chainReader := exec.NewChainReader(te.cfg.chainConfig, te.applyTx, te.cfg.blockReader, te.logger)
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

func (te *txExecutor) executeBlocks(ctx context.Context, startBlockNum uint64, maxBlockNum uint64, blockLimit uint64, initialTxNum uint64, inputTxNum uint64, readAhead chan uint64, initialCycle bool, applyResults chan applyResult, blockRequests chan *blockRequest, commitResults ...chan applyResult) error {
	if te.execLoopGroup == nil {
		return errors.New("no exec group")
	}

	te.execLoopGroup.Go(func() (err error) {
		// Do NOT close channels here. The exec loop closes them
		// after processing all blocks (via pe.commitResultsCh/applyResultsCh
		// deferred close, or via the ctx.Done drain path).
		// Closing here would race with the exec loop sending results.
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("exec blocks panic: %s", rec)
			} else if err != nil && !errors.Is(err, context.Canceled) {
				err = fmt.Errorf("exec blocks error: %w", err)
			} else {
				te.logger.Debug("[" + te.logPrefix + "] exec blocks exit")
			}
		}()

		// Channel close is handled by pe.execLoop's deferred close.
		// Do NOT close channels here — execLoop owns the lifecycle.

		// Open a thread-local roTx for block metadata and StepsInFiles.
		// Must NOT use the stageloop's rwTx — it's thread-bound.
		execRoTx, err := te.cfg.db.BeginTemporalRo(ctx)
		if err != nil {
			return fmt.Errorf("executeBlocks: open roTx: %w", err)
		}
		defer execRoTx.Rollback()

		var blockTx kv.Tx
		if overlay := te.doms.BlockOverlay(); overlay != nil {
			blockTx = overlay.NewReadView(execRoTx)
		} else {
			blockTx = execRoTx
		}

		// Use the max of all state domain steps (not just commitment) to
		// determine which txNums need history reads.
		cmtStep := execRoTx.StepsInFiles(kv.CommitmentDomain)
		acctStep := execRoTx.StepsInFiles(kv.AccountsDomain)
		storStep := execRoTx.StepsInFiles(kv.StorageDomain)
		codeStep := execRoTx.StepsInFiles(kv.CodeDomain)
		maxStateStep := max(acctStep, storStep, codeStep)
		if maxStateStep > cmtStep {
			return fmt.Errorf("snapshot step misalignment: state domains (accounts=%d, storage=%d, code=%d) ahead of commitment=%d — snapshot files need rebuilding",
				acctStep, storStep, codeStep, cmtStep)
		}
		lastFrozenStep := cmtStep

		var lastFrozenTxNum uint64
		if lastFrozenStep > 0 {
			lastFrozenTxNum = uint64((lastFrozenStep+1)*kv.Step(te.doms.StepSize())) - 1
		}

		for blockNum := startBlockNum; blockNum <= maxBlockNum; blockNum++ {
			select {
			case readAhead <- blockNum:
			default:
			}

			var canonicalHash common.Hash
			canonicalHash, err = rawdb.ReadCanonicalHash(blockTx, blockNum)
			if err != nil {
				return err
			}
			b, ok := te.cfg.readAheader.ReadBlockWithSenders(canonicalHash)
			if b == nil || !ok {
				b, err = exec.BlockWithSenders(ctx, te.cfg.db, blockTx, te.cfg.blockReader, blockNum)
			}
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("nil block %d", blockNum)
			}
			go warmTxsHashes(b)

			var dbBAL types.BlockAccessList
			// Read BAL through blockTx (overlay or execRoTx) — do NOT open
			// a separate db.View() as it can deadlock with the stageloop's
			// RW transaction when BlockOverlay is active.
			data, err := rawdb.ReadBlockAccessListBytes(blockTx, b.Hash(), blockNum)
			if err != nil {
				return err
			}
			if len(data) > 0 && !dbg.IgnoreBAL {
				dbBAL, err = types.DecodeBlockAccessListBytes(data)
				if err != nil {
					return fmt.Errorf("decode block access list: %w", err)
				}
				if err := dbBAL.Validate(); err != nil {
					return fmt.Errorf("invalid block access list: %w", err)
				}
			}

			txs := b.Transactions()
			header := b.HeaderNoCopy()

			// BlockContext: workers override GetHash with their own per-worker
			// function (installWorkerGetHash) using their own roTx. The
			// placeholder here uses execRoTx for the serial path fallback.
			blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, func(hash common.Hash, number uint64) (h *types.Header, err error) {
				h, err = te.cfg.blockReader.Header(ctx, blockTx, hash, number)
				if h == nil && err == nil {
					h = &types.Header{}
				}
				return h, err
			}), te.cfg.engine, te.cfg.author, te.cfg.chainConfig)

			var txTasks []exec.Task
			// Per-block committed state cache for parallel workers' GetCommittedState.
			blockStateCache := state.NewBlockStateCache()

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
					HistoryExecution: lastFrozenTxNum > 0 && inputTxNum <= lastFrozenTxNum,
					Config:           te.cfg.chainConfig,
					Engine:           te.cfg.engine,
					Trace:            dbg.TraceTx(blockNum, txIndex),
					Hooks:            te.hooks,
					Logger:           te.logger,
					BlockStateCache:  blockStateCache,
				}

				txTasks = append(txTasks, txTask)
				inputTxNum++
			}

			lastExecutedStep := kv.Step(inputTxNum / te.doms.StepSize())

			// if we're in the initialCycle before we consider the blockLimit we need to make sure we keep executing
			// until we reach a transaction whose commitment which is writable to the db, otherwise the update will get lost
			var exhausted *ErrLoopExhausted
			if shouldMarkExhaustedAtBlock(initialCycle, lastExecutedStep, lastFrozenStep, dbg.DiscardCommitment(), blockLimit, blockNum, startBlockNum, maxBlockNum) {
				exhausted = &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
			}
			var commitCh chan applyResult
			if len(commitResults) > 0 {
				commitCh = commitResults[0]
			}
			// Heads-up to the commitment calculator, ahead of the block's
			// txResult/blockResult stream and on its own channel. inputTxNum
			// has been advanced past this block's tasks by the loop above,
			// so inputTxNum-1 is the block's final txNum.
			if blockRequests != nil {
				select {
				case blockRequests <- &blockRequest{
					blockNum:  b.NumberU64(),
					blockHash: b.Hash(),
					stateRoot: header.Root,
					lastTxNum: inputTxNum - 1,
					bal:       dbBAL,
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case te.execRequests <- &execRequest{b.NumberU64(), b.Hash(),
				protocol.NewGasPool(b.GasLimit(), te.cfg.chainConfig.GetMaxBlobGasPerBlock(b.Time())),
				dbBAL, txTasks, applyResults, commitCh, false, exhausted}:
			case <-ctx.Done():
				return ctx.Err()
			}
			mxExecBlocks.Add(1)

			if exhausted != nil {
				break
			}
		}

		// Channels closed by deferred close above.
		return nil
	})

	return nil
}

func handleIncorrectRootHashError(blockNumber uint64, blockHash common.Hash, applyTx kv.TemporalRwTx, cfg ExecuteBlockCfg, s *StageState, logger log.Logger, u Unwinder) error {
	if cfg.badBlockHalt {
		return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNumber)
	}
	minBlockNum := s.BlockNumber
	if blockNumber <= minBlockNum {
		return nil
	}

	unwindToLimit, err := rawtemporaldb.CanUnwindToBlockNum(applyTx)
	if err != nil {
		return err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, maxUnwindJumpAllowance, (blockNumber-minBlockNum)/2)
	unwindTo := blockNumber - jump

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

// computeAndCheckCommitmentV3 - does write state to db and then check commitment
func computeAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.TemporalRwTx, doms *execctx.SharedDomains, cfg ExecuteBlockCfg, e *StageState, parallel bool, logger log.Logger, u Unwinder) (ok bool, times FlushAndComputeCommitmentTimes, err error) {
	if header == nil {
		return false, times, errors.New("header is nil")
	}

	start := time.Now()
	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	// TODO: route stage updates through block overlay once serial path initialises one
	if !parallel {
		if err := e.Update(applyTx, header.Number.Uint64()); err != nil {
			return false, times, err
		}
		if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
			return false, times, fmt.Errorf("writing plain state version: %w", err)
		}
	}

	if dbg.DiscardCommitment() {
		return true, times, nil
	}

	// Use applyTx, not a fresh BeginTemporalRo: Headers wrote MaxTxNum for
	// header.Number to applyTx in this batch and a fresh RO snapshot would
	// miss it, silently falling back to the previous block's max txNum via
	// c.Last(). Pairing that stale txNum with header.Number in
	// KeyCommitmentState makes the next iter's SeekCommitment loop back —
	// see issue #21171.
	txNumsReader := cfg.blockReader.TxnumReader()
	blockTxNum, err := txNumsReader.Max(ctx, applyTx, header.Number.Uint64())
	if err != nil {
		return false, times, err
	}
	computedRootHash, err := doms.ComputeCommitment(ctx, applyTx, true, header.Number.Uint64(), blockTxNum, e.LogPrefix(), nil)

	times.ComputeCommitment = time.Since(start)
	if err != nil {
		return false, times, fmt.Errorf("compute commitment: %w", err)
	}

	if !bytes.Equal(computedRootHash, header.Root[:]) {
		logger.Warn(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), computedRootHash, header.Root[:], header.Hash()))
		err = handleIncorrectRootHashError(header.Number.Uint64(), header.Hash(), applyTx, cfg, e, logger, u)
		return false, times, err
	}
	return true, times, nil

}

// shouldMarkExhaustedAtBlock decides whether the per-cycle block-limit
// has been crossed at the current block — which causes executeBlocks to
// stamp the dispatched blockResult with `Exhausted` and break out of
// its loop. The exec loop sees the Exhausted flag, fires its
// partial-batch flush, and the apply loop returns ErrLoopExhausted so
// the stage loop resumes from the next block.
//
// Two gates protect the initial cycle:
//  1. !initialCycle — later cycles enforce blockLimit unconditionally.
//  2. On initialCycle, only enforce when we have at least one frozen
//     step worth of work AND we're not in DiscardCommitment debug mode
//     (otherwise the partial-batch flush would lose the commitment
//     that's still pending in sd.mem). See exec3.go's call site for
//     the historical reasoning.
//
// blockNum != maxBlockNum guards against marking the goal block as
// exhausted — the goal block already triggers a clean reachedMaxBlock
// exit and shouldn't be relabeled as "more work pending".
//
// Pure function so the precedence is unit-testable. See
// TestShouldMarkExhaustedAtBlock.
func shouldMarkExhaustedAtBlock(initialCycle bool, lastExecutedStep, lastFrozenStep kv.Step, discardCommitment bool, blockLimit, blockNum, startBlockNum, maxBlockNum uint64) bool {
	if initialCycle {
		if !(lastExecutedStep > 0 && lastExecutedStep > lastFrozenStep && !discardCommitment) {
			return false
		}
	}
	if blockLimit == 0 {
		return false
	}
	if blockNum-startBlockNum+1 < blockLimit {
		return false
	}
	if blockNum == maxBlockNum {
		return false
	}
	return true
}

func shouldGenerateChangeSets(cfg ExecuteBlockCfg, blockNum, maxBlockNum uint64) bool {
	if cfg.syncCfg.AlwaysGenerateChangesets {
		return true
	}
	if blockNum < cfg.blockReader.FrozenBlocks() {
		return false
	}
	// Generate changesets for blocks within the reorg window of the batch end,
	// so the node can handle reorgs at the tip.
	return blockNum+cfg.syncCfg.MaxReorgDepth >= maxBlockNum
}

// changesetWindowStart returns the first block in [startBlockNum, maxBlockNum]
// for which shouldGenerateChangeSets is true, or math.MaxUint64 when there is
// none. Parallel exec gates per-block changeset capture and the commitment
// calculator's per-block mode on this boundary.
func changesetWindowStart(alwaysGenerateChangesets bool, maxReorgDepth uint64, frozenBlocks uint64, startBlockNum uint64, maxBlockNum uint64) uint64 {
	if alwaysGenerateChangesets {
		return startBlockNum
	}
	windowStart := startBlockNum
	if maxBlockNum > maxReorgDepth {
		windowStart = max(windowStart, maxBlockNum-maxReorgDepth)
	}
	windowStart = max(windowStart, frozenBlocks)
	if windowStart > maxBlockNum {
		return math.MaxUint64
	}
	return windowStart
}
