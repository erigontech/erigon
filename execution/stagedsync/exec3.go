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
	commonerrors "github.com/erigontech/erigon/common/errors"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/receipts"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
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

// execRange is the resolved block/txNum window the executor runs over. The
// stage wrapper resolves it (SeekCommitment + restoreTxNum) and passes it in;
// the exec core does not touch the stage's DB metadata to derive it.
type execRange struct {
	blockNum                 uint64
	initialTxNum             uint64
	inputTxNum               uint64
	offsetFromBlockBeginning uint64
	maxBlockNum              uint64
}

// execV3Outcome carries what the stage wrapper needs after the parallel
// executor returns. applyTx is the live post-exec tx (parallel exec may have
// rolled the stageloop tx via Flush/CommitAndBegin, leaving the caller's rwTx
// stale); the failed* fields name the block implicated in a bad-block unwind.
type execV3Outcome struct {
	lastHeader            *types.Header
	applyTx               kv.TemporalRwTx
	lastCommittedBlockNum uint64
	failedBlock           uint64
	failedHash            common.Hash
}

// execV3 runs the parallel executor over the resolved window rng. It is
// stage-agnostic: the caller owns SeekCommitment/restoreTxNum (upstream) and
// stage-progress update / bad-block unwind (downstream, via the returned
// outcome). This lets both SpawnExecuteBlocksStage and an ephemeral single-block
// replay drive the same execution path. Unexported: it takes package-internal
// types (execRange, blockSource) and has no external callers.
func execV3(ctx context.Context,
	cfg ExecuteBlockCfg,
	doms *execctx.SharedDomains, rwTx kv.TemporalRwTx,
	syncMode stages.Mode, initialCycle bool, logPrefix string,
	rng execRange, blockSrc blockSource,
	logger log.Logger) (out execV3Outcome, execErr error) {

	isForkValidation := syncMode == stages.ModeForkValidation
	isApplyingBlocks := syncMode == stages.ModeApplyingBlocks
	hooks := cfg.vmConfig.Tracer
	applyTx := rwTx
	initialTxNum := rng.initialTxNum
	blockNum := rng.blockNum
	inputTxNum := rng.inputTxNum
	offsetFromBlockBeginning := rng.offsetFromBlockBeginning
	maxBlockNum := rng.maxBlockNum

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

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx, doms.StepSize())

	if maxBlockNum < blockNum {
		return out, nil
	}

	var readAhead chan uint64
	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)

	// Thread the cfg's exec-only flag into the domains so IsUnfrozenStepEdge uses
	// it rather than reading dbg.DiscardCommitment() independently (single source).
	doms.SetDiscardCommitment(cfg.discardCommitment)

	// Exec-only mode (discardCommitment) runs no trie work, so skip the trie
	// setup entirely: EnableParaTrieDB after the witness seed's DomainPut touches
	// would panic on the dropped sequential-buffer keys (ERIGON_COMMITMENT_PARALLEL).
	if !cfg.discardCommitment {
		doms.EnableParaTrieDB(cfg.db)
		doms.EnableTrieWarmup(true)
		doms.SetDeferCommitmentUpdates(false)
		// Enable deferred commitment updates for fork validation and parallel initial sync.
		// Deferred updates batch commitment calculations to block boundaries rather than
		// per-transaction, significantly reducing re-org validation overhead.
		// For the parallel path during initial sync, Flush() now includes pending updates,
		// so they are no longer silently discarded between StageLoopIteration cycles.
		if isForkValidation || isApplyingBlocks {
			doms.SetDeferCommitmentUpdates(true)
		}
		defer doms.SetDeferCommitmentUpdates(false)
	}
	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	if !initialCycle && isApplyingBlocks {
		var clean func()

		readAhead, clean = exec.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}

	pe := &parallelExecutor{
		txExecutor: txExecutor{
			cfg:               cfg,
			rs:                rs,
			doms:              doms,
			isForkValidation:  isForkValidation,
			isApplyingBlocks:  isApplyingBlocks,
			logger:            logger,
			logPrefix:         logPrefix,
			progress:          NewProgress(blockNum, inputTxNum, commitThreshold, false, logPrefix, logger),
			enableChaosMonkey: initialCycle,
			hooks:             hooks,
			blockSrc:          blockSrc,
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

	var lastHeader *types.Header
	lastHeader, applyTx, execErr = pe.exec(ctx, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
		initialTxNum, inputTxNum, initialCycle, applyTx, stepsInDb, accumulator, readAhead, logEvery)

	out = execV3Outcome{
		lastHeader:            lastHeader,
		applyTx:               applyTx,
		lastCommittedBlockNum: pe.lastCommittedBlockNum.Load(),
		failedBlock:           pe.failedBlock,
		failedHash:            pe.failedHash,
	}

	execErr = execV3Finalize(ctx, execErr, cfg, doms, pe.lastCommittedTxNum.Load(), out.lastCommittedBlockNum,
		lastHeader, shouldReportToTxPool, logPrefix, logger)
	return out, execErr
}

// execV3Serial runs the legacy serial executor. It stays welded to the stage
// (execStage/u) — serial is scheduled for removal and does not need the
// stage-agnostic split ExecV3 has. The stage wrapper calls it directly.
func execV3Serial(ctx context.Context,
	execStage *StageState, u Unwinder, cfg ExecuteBlockCfg,
	doms *execctx.SharedDomains, rwTx kv.TemporalRwTx,
	rng execRange, logger log.Logger) (execErr error) {

	isForkValidation := execStage.SyncMode() == stages.ModeForkValidation
	isApplyingBlocks := execStage.SyncMode() == stages.ModeApplyingBlocks
	initialCycle := execStage.CurrentSyncCycle.IsInitialCycle
	hooks := cfg.vmConfig.Tracer
	applyTx := rwTx
	initialTxNum := rng.initialTxNum
	blockNum := rng.blockNum
	inputTxNum := rng.inputTxNum
	offsetFromBlockBeginning := rng.offsetFromBlockBeginning
	maxBlockNum := rng.maxBlockNum
	var err error

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

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx, doms.StepSize())

	if maxBlockNum < blockNum {
		return nil
	}

	var lastHeader *types.Header
	var readAhead chan uint64
	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)

	doms.EnableParaTrieDB(cfg.db)
	doms.EnableTrieWarmup(true)
	doms.SetDeferCommitmentUpdates(false)
	if isForkValidation {
		doms.SetDeferCommitmentUpdates(true)
	}
	defer doms.SetDeferCommitmentUpdates(false)
	if !initialCycle && isApplyingBlocks {
		var clean func()

		readAhead, clean = exec.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}

	se := &serialExecutor{
		txExecutor: txExecutor{
			cfg:               cfg,
			rs:                rs,
			doms:              doms,
			u:                 u,
			isForkValidation:  isForkValidation,
			isApplyingBlocks:  isApplyingBlocks,
			applyTx:           applyTx,
			logger:            logger,
			logPrefix:         execStage.LogPrefix(),
			progress:          NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
			enableChaosMonkey: initialCycle,
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
				_, _, err = computeAndCheckCommitmentV3(ctx, lastHeader, applyTx, se.domains(), cfg, execStage, false, logger, u)
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

				stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx, doms.StepSize())

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

	return execV3Finalize(ctx, execErr, cfg, doms, se.lastCommittedTxNum.Load(), se.lastCommittedBlockNum.Load(),
		lastHeader, shouldReportToTxPool, execStage.LogPrefix(), logger)
}

// execV3Finalize runs the post-exec tail shared by ExecV3 and execV3Serial: the
// BAD_BLOCK_HALT debug exit, the frozen-step commitment guard, and the tx-pool
// block-progress notification. It never unwinds — that is the caller's job.
func execV3Finalize(ctx context.Context, execErr error, cfg ExecuteBlockCfg, doms *execctx.SharedDomains,
	lastCommittedTxNum, lastCommittedBlockNum uint64, lastHeader *types.Header,
	shouldReportToTxPool bool, logPrefix string, logger log.Logger) error {

	// If execution failed with ErrInvalidBlock, skip the step-frozen check and
	// propagate the error so the caller can unwind. The step-frozen check only
	// makes sense when execution succeeded and we need to persist the commitment.
	if execErr != nil && errors.Is(execErr, rules.ErrInvalidBlock) {
		// Intentional os.Exit under BAD_BLOCK_HALT (both the env flag dbg.BadBlockHalt
		// and cfg.badBlockHalt): a debug switch whose whole purpose is to freeze
		// process state at the bad block. Returning would run deferred
		// rollback/commit/flush and overwrite the state we want to inspect.
		// cfg.badBlockHalt alone (fork validation) must NOT exit — it needs the
		// error to propagate for in-memory validation.
		if cfg.badBlockHalt && dbg.BadBlockHalt {
			logger.Error(fmt.Sprintf("[%s] BAD_BLOCK_HALT: halting on invalid block (debug mode, no commit)", logPrefix), "err", execErr)
			os.Exit(1) //nolint:gocritic // exitAfterDefer: intentional process halt without running deferred rollback to preserve state
		}
		return execErr
	}

	lastCommittedStep := kv.Step(lastCommittedTxNum / doms.StepSize())
	// applyTx may be stale after parallel execution (the underlying mdbx tx
	// was invalidated by Flush/CommitAndBegin). Use a fresh roTx for the check.
	var lastFrozenStep kv.Step
	if stepCheckTx, stepErr := cfg.db.BeginTemporalRo(ctx); stepErr == nil {
		lastFrozenStep = kv.Step(stepCheckTx.StepsInFiles(kv.CommitmentDomain))
		stepCheckTx.Rollback()
	}

	if lastCommittedStep > 0 && lastCommittedStep < lastFrozenStep && !cfg.discardCommitment {
		logger.Warn("["+logPrefix+"] can't persist commitment: txn step frozen",
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

	// blockSrc, when non-nil, overrides the default DB-backed block source —
	// e.g. an ephemeral single-block source with no DB behind it.
	blockSrc blockSource

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

// A wrong root under fork validation means a payload the CL offered was rejected,
// which is the answer the CL asked for, not a fault of this node.
func (te *txExecutor) logWrongTrieRoot(msg string) {
	if te.isForkValidation {
		te.logger.Warn(msg)
		return
	}
	te.logger.Error(msg)
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

// reconstructPriorReceipts re-derives receipts of a resumed block's prefix txs
// (executed in an earlier batch) so Finalize and the notification cache can see
// the block's full receipt set.
//
// Best-effort. At a mid-block step boundary the committed domain latest is the
// step-edge value, not the block-start pre-state, so the prefix is not always
// reconstructable (and minimal nodes retain no receipts at all). Callers MUST
// treat a failure as non-fatal: the node still resumes from a mid-step boundary
// and the block's own receipts and cumulative gas stay correct — only the prior
// receipts are absent (block then left not receipts-complete).
func (te *txExecutor) reconstructPriorReceipts(ctx context.Context, applyTx kv.TemporalTx, header *types.Header, txs types.Transactions, startTxIndex int, blockStartTxNum uint64) (types.Receipts, error) {
	priorIbs := state.New(state.NewHistoryReaderV3(applyTx, blockStartTxNum))
	defer priorIbs.Close()
	priorGp := protocol.NewGasPool(header.GasLimit, te.cfg.chainConfig.GetMaxBlobGasPerBlock(header.Time))
	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		return te.cfg.blockReader.Header(ctx, applyTx, hash, number)
	}
	priorReceipts, err := receipts.DerivePriorReceipts(ctx, te.cfg.chainConfig, te.cfg.engine, header, txs, startTxIndex, blockStartTxNum, applyTx, priorIbs, priorGp, getHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct prior receipts for partial block %d (startTxIndex %d): %w", header.Number.Uint64(), startTxIndex, err)
	}
	return priorReceipts, nil
}

func (te *txExecutor) onBlockStart(ctx context.Context, block *types.Block) {
	defer func() {
		if rec := recover(); rec != nil {
			te.logger.Warn("hook panicked", "panic", rec, "stack", dbg.Stack())
		}
	}()

	if te.hooks == nil {
		return
	}

	blockNum := block.NumberU64()
	blockHash := block.Hash()
	if blockHash == (common.Hash{}) {
		te.logger.Warn("hooks ignored: zero block hash")
		return
	}

	if blockNum == 0 {
		if te.hooks.OnGenesisBlock != nil {
			te.hooks.OnGenesisBlock(block, te.cfg.genesis.Alloc)
		}
	} else {
		if te.hooks.OnBlockStart != nil {
			var td *uint256.Int
			var finalized *types.Header
			var safe *types.Header

			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				chainReader := exec.NewChainReader(te.cfg.chainConfig, tx, te.cfg.blockReader, te.logger)
				td = chainReader.GetTd(block.ParentHash(), blockNum-1)
				finalized = chainReader.CurrentFinalizedHeader()
				safe = chainReader.CurrentSafeHeader()
				return nil
			}); err != nil {
				te.logger.Warn("hook: OnBlockStart: abandoned", "err", err)
			}

			te.hooks.OnBlockStart(tracing.BlockEvent{
				Block:     block,
				TD:        td,
				Finalized: finalized,
				Safe:      safe,
			})
		}
	}
}

func blockAccessListBytes(blockTx kv.Getter, block *types.Block, blockNum uint64) ([]byte, error) {
	data := block.BlockAccessList()
	if len(data) == 0 && block.HeaderNoCopy().HasNonEmptyBAL() {
		return rawdb.ReadBlockAccessListBytes(blockTx, block.Hash(), blockNum)
	}
	return data, nil
}

func recoveredPanicError(operation string, recovered any) error {
	if err, ok := recovered.(error); ok {
		return fmt.Errorf("%s panic: %w", operation, err)
	}
	return fmt.Errorf("%s panic: %v", operation, recovered)
}

func (te *txExecutor) executeBlocks(ctx context.Context, startBlockNum uint64, maxBlockNum uint64, blockLimit uint64, initialTxNum uint64, inputTxNum uint64, readAhead chan uint64, initialCycle bool, applyResults chan applyResult, blockRequests chan *blockRequest, commitResults chan applyResult) error {
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
				err = recoveredPanicError("exec blocks", rec)
				return
			}
			if err = commonerrors.NilIfCanceled(err); err != nil {
				err = fmt.Errorf("exec blocks error: %w", err)
			} else {
				te.logger.Debug("[" + te.logPrefix + "] exec blocks exit")
			}
		}()

		// execLoop owns the apply/commit channels, but blockRequests is closed
		// by its sole sender (this goroutine) — closing it from execLoop would
		// race this send select and panic on "send on closed channel".
		if blockRequests != nil {
			defer close(blockRequests)
		}

		if te.cfg.syncCfg.ChaosMonkey && te.enableChaosMonkey {
			if chaosErr := chaos_monkey.ThrowPreExecutionError(); chaosErr != nil {
				return chaosErr
			}
		}

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

		src := te.blockSrc
		if src == nil {
			src = &dbBlockSource{cfg: &te.cfg, blockTx: blockTx, cur: startBlockNum, max: maxBlockNum}
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

		for {
			var b *types.Block
			var dbBAL types.BlockAccessList
			var blockNum uint64
			var more bool
			b, dbBAL, blockNum, more, err = src.next(ctx)
			if err != nil {
				return err
			}
			if !more {
				break
			}

			select {
			case readAhead <- blockNum:
			default:
			}
			go warmTxsHashes(b)

			// dbBAL is fed by the block source (src.next -> blockAndBAL), which
			// prefers the payload-carried BAL and falls back to the DB sidecar.
			header := b.HeaderNoCopy()
			if dbBAL == nil && !dbg.IgnoreBAL && te.cfg.chainConfig.IsAmsterdam(header.Time) && header.HasBAL() {
				te.logger.Debug("executing block without a BAL", "blockNum", blockNum)
			}
			if dbg.TraceBALFeed {
				if dbBAL != nil {
					fmt.Printf("BAL-FEED blk=%d accounts=%d\n", blockNum, len(dbBAL))
				} else if te.cfg.chainConfig.IsAmsterdam(header.Time) {
					fmt.Printf("BAL-MISSING blk=%d\n", blockNum)
				}
			}

			txs := b.Transactions()

			// BlockContext: workers override GetHash with their own per-worker
			// function (installWorkerGetHash) using their own roTx; this
			// placeholder resolves ancestor headers via the block source.
			blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
				return src.header(ctx, hash, number)
			}), te.cfg.engine, te.cfg.author, te.cfg.chainConfig)

			var txTasks []exec.Task
			// Per-block committed state cache for parallel workers' GetCommittedState.
			blockStateCache := state.NewBlockStateCache()

			blockStartTxNum := inputTxNum
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
			if shouldMarkExhaustedAtBlock(initialCycle, lastExecutedStep, lastFrozenStep, te.cfg.discardCommitment, blockLimit, blockNum, startBlockNum, maxBlockNum) {
				exhausted = &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
			}
			// Heads-up to the commitment calculator, ahead of the block's
			// txResult/blockResult stream and on its own channel. inputTxNum
			// has been advanced past this block's tasks by the loop above,
			// so inputTxNum-1 is the block's final txNum.
			if blockRequests != nil {
				select {
				case blockRequests <- &blockRequest{
					blockNum:   b.NumberU64(),
					blockHash:  b.Hash(),
					stateRoot:  header.Root,
					firstTxNum: blockStartTxNum,
					lastTxNum:  inputTxNum - 1,
					blockTime:  header.Time,
					bal:        dbBAL,
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case te.execRequests <- &execRequest{
				block:         b,
				gasPool:       protocol.NewGasPool(b.GasLimit(), te.cfg.chainConfig.GetMaxBlobGasPerBlock(b.Time())),
				accessList:    dbBAL,
				tasks:         txTasks,
				applyResults:  applyResults,
				commitResults: commitResults,
				exhausted:     exhausted,
			}:
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

	if cfg.discardCommitment {
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
// exhausted — the goal block already triggers a clean stopReachedMax
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
