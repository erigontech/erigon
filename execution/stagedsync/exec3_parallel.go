package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/shards"
)

/*
ExecV3 - parallel execution. Has many layers of abstractions - each layer does accumulate
state changes (updates) and can "atomically commit all changes to underlying layer of abstraction"

Layers from top to bottom:
- IntraBlockState - used to exec txs. It does store inside all updates of given txn.
Can understand if txn failed or OutOfGas - then revert all changes.
Each parallel-worker have own IntraBlockState.
IntraBlockState does commit changes to lower-abstraction-level by method `ibs.MakeWriteSet()`

- versionedWriteCollector - txs which executed by parallel workers can conflict with each-other.
This writer collects updates as a flat VersionedWrites slice and sends them to conflict-resolution.
Until conflict-resolution succeeds - none of execution updates must pass to lower-abstraction-level.
Object TxTask it's just set of small buffers (readset + writeset) for each transaction.
Write to TxTask happens by code like `txTask.ReadLists = rw.stateReader.ReadSet()`.

- TxTask - objects coming from parallel-workers to conflict-resolution goroutine (ApplyLoop and method ReadsValid).
Flush of data to lower-level-of-abstraction is done by method `agg.ApplyState` (method agg.ApplyHistory exists
only for performance - to reduce time of RwLock on state, but by meaning `ApplyState+ApplyHistory` it's 1 method to
flush changes from TxTask to lower-level-of-abstraction).

- ParallelExecutionState - it's all updates which are stored in RAM - all parallel workers can see this updates.
Execution of txs always done on Valid version of state (no partial-updates of state).
Flush of updates to lower-level-of-abstractions done by method `ParallelExecutionState.Flush`.
On this level-of-abstraction also exists ReaderV3.
IntraBlockState does call ReaderV3, and ReaderV3 call ParallelExecutionState(in-mem-cache) or DB (RoTx).
WAL - also on this level-of-abstraction - agg.ApplyHistory does write updates from TxTask to WAL.
WAL it's like ParallelExecutionState just without reading api (can only write there). WAL flush to disk periodically (doesn't need much RAM).

- RoTx - see everything what committed to DB. Commit is done by rwLoop goroutine.
rwloop does:
  - stop all Workers
  - call ParallelExecutionState.Flush()
  - commit
  - open new RoTx
  - set new RoTx to all Workers
  - start Worker start workers

When rwLoop has nothing to do - it does Prune, or flush of WAL to RwTx (agg.rotate+agg.Flush)
*/

type parallelExecutor struct {
	txExecutor
	execWorkers    []*exec.Worker
	stopWorkers    func()
	waitWorkers    func()
	in             *exec.QueueWithRetry
	rws            *exec.ResultsQueue
	workerCount    int
	blockExecutors map[uint64]*blockExecutor
	// applyResultsCh and commitResultsCh are set before execLoop starts.
	// The exec loop closes them on exit to signal the apply loop and
	// calculator to drain.
	applyResultsCh  chan applyResult
	commitResultsCh chan applyResult
	// blockRequestsCh feeds the calculator per-block heads-up messages on
	// its own channel, so a blockRequest is never trapped behind a block's
	// txResults on the result fan-out. Closed by closeApplyChannels.
	blockRequestsCh chan *blockRequest
	maxBlockNum     uint64 // set before execLoop; exec loop exits when reached
	// reachedMaxBlock is set by the exec loop when it exits cleanly because
	// blockResult.BlockNum >= maxBlockNum (i.e. all requested work is done),
	// as opposed to sizeEst > batchLimit (more work pending). The apply loop
	// uses this to decide whether to return ErrLoopExhausted (more work) or
	// nil (clean exit). Read after applyResults is closed; safe under happens-
	// before because the exec loop sets it before triggering the channel close.
	reachedMaxBlock atomic.Bool
	// accumulator for txpool state-diff notifications; set before execLoop
	// starts so that AuRa system-call nonce changes are emitted per block.
	accumulator *shards.Accumulator
	// changesetAccumulator state owned by the exec loop. Accessing or mutating
	// this is the exec loop's responsibility — putting it here (rather than on
	// the apply-loop side) ensures all sd.mem mutations originate from a single
	// goroutine and avoids the data race between SetChangesetAccumulator
	// (apply loop) and ApplyStateWrites (exec loop, via SysCallContract for
	// block-end system calls) on SharedDomains.mem.
	shouldGenerateChangesets bool
	currentChangeSet         *changeset.StateChangeSet
	// currentChangeSetBlock is the block number currentChangeSet belongs to
	// (0 == none). Tracked so ensureChangesetAccumulator can be a no-op when the
	// accumulator is already installed for the block whose writes are about to
	// be applied — making changeset capture robust against blocks scheduled out
	// of band (e.g. processRequest scheduling the first block of a new request
	// after the blockExecutors map went empty mid-batch, with no preceding
	// blockResult to trigger the install at the rotation site below).
	currentChangeSetBlock uint64
}

// ensureChangesetAccumulator makes pe.currentChangeSet point at a fresh,
// block-specific StateChangeSet before any of blockNum's sd.mem writes are
// applied. Idempotent. Exec-loop only — it mutates SharedDomains.mem via
// SetChangesetAccumulator, which must be single-writer (see the comment on
// currentChangeSet).
func (pe *parallelExecutor) ensureChangesetAccumulator(blockNum uint64) {
	if !pe.shouldGenerateChangesets || blockNum == 0 || blockNum > pe.maxBlockNum {
		return
	}
	if pe.currentChangeSet != nil && pe.currentChangeSetBlock == blockNum {
		return
	}
	// A previous block's accumulator is normally saved+cleared at its
	// blockResult; if one is still installed here for a different block the
	// rotation was missed — overwrite (the previous block's changeset was
	// already saved at its blockResult, so nothing is lost).
	pe.currentChangeSet = &changeset.StateChangeSet{}
	pe.currentChangeSetBlock = blockNum
	pe.domains().SetChangesetAccumulator(pe.currentChangeSet)
}

// clearChangesetAccumulator detaches the current changeset accumulator after
// its block's changeset has been saved. Exec-loop only.
func (pe *parallelExecutor) clearChangesetAccumulator() {
	pe.domains().SetChangesetAccumulator(nil)
	pe.currentChangeSet = nil
	pe.currentChangeSetBlock = 0
}

func (pe *parallelExecutor) exec(ctx context.Context, execStage *StageState, u Unwinder,
	startBlockNum uint64, offsetFromBlockBeginning uint64, maxBlockNum uint64, blockLimit uint64,
	initialTxNum uint64, inputTxNum uint64, initialCycle bool, rwTx kv.TemporalRwTx,
	stepsInDb float64, accumulator *shards.Accumulator, readAhead chan uint64, logEvery *time.Ticker) (*types.Header, kv.TemporalRwTx, error) {

	// Do NOT set pe.applyTx to the stageloop's rwTx — the rwTx is thread-bound
	// and cannot be shared with the execLoop goroutine. The execLoop creates
	// its own roTx at line 571. executeBlocks uses its own roTx too.

	// applyResults receives completed block/tx results from execLoop for the apply goroutine.
	// commitResults receives the same stream for the commitment calculator.
	// Both are fed by the fan-out in the execLoop's blockExecutor.
	applyResults := make(chan applyResult, 2_048)
	commitResults := make(chan applyResult, 2_048)
	blockRequests := make(chan *blockRequest, 2_048)

	// rootResults receives per-block commitment roots from the calculator.
	rootResults := make(chan commitmentResult, 64)

	if blockLimit > 0 && min(startBlockNum+blockLimit, maxBlockNum) > startBlockNum+16 || maxBlockNum > startBlockNum+16 {
		log.Info(fmt.Sprintf("[%s] parallel starting", execStage.LogPrefix()),
			"from", startBlockNum, "to", maxBlockNum, "limit", startBlockNum+blockLimit-1, "initialTxNum", initialTxNum,
			"initialBlockTxOffset", offsetFromBlockBeginning, "initialCycle", initialCycle,
			"isForkValidation", pe.isForkValidation, "isApplyingBlocks", pe.isApplyingBlocks)
	}

	// restoreTxNum must run before pe.run() so that doms.SetTxNum() completes
	// before any goroutine reads txNum (via AsGetter/GetLatest).
	restoredTxNum, _, _, _, err := restoreTxNum(ctx, &pe.cfg, rwTx, inputTxNum, maxBlockNum)
	if err != nil {
		return nil, rwTx, err
	}

	// Set accumulator before pe.run() so execLoop sees it without a race.
	pe.accumulator = accumulator

	executorContext, executorCancel, err := pe.run(ctx)
	defer executorCancel()

	if err != nil {
		return nil, rwTx, err
	}

	if err := pe.resetWorkers(ctx, pe.rs, rwTx); err != nil {
		return nil, rwTx, err
	}

	// Disable inline TouchKey — the commitment calculator accumulates touches
	// via its own Updates buffer (TouchUpdates from VersionedWrites).
	pe.rs.Domains().SetDisableInlineTouchKey(true)
	defer pe.rs.Domains().SetDisableInlineTouchKey(false)
	// Parallel exec needs in-mem history reads enabled for the calculator
	// goroutine. Capture the caller's setting first and restore it on exit
	// — the previous defer-to-false (b72aa7b4f7 #20805) hardcoded the
	// post-exec value to false regardless of what the caller had set,
	// which broke post-exec callers (engine API forkchoice_updated's
	// GetAsOf, post-batch trie-root computation, RPC reads) with
	// "GetAsOf called on TemporalMemBatch with inMemHistoryReads disabled"
	// or with partial-state reads. Repro: EEST
	// test_gas_limit_below_minimum[gas_limit_5000] in parallel mode; same
	// root cause likely behind mainnet from-0 parallel wrong-trie-root at
	// block 131578.
	prevInMemHistoryReads := pe.rs.Domains().InMemHistoryReads()
	pe.rs.Domains().SetInMemHistoryReads(true)
	defer pe.rs.Domains().SetInMemHistoryReads(prevInMemHistoryReads)

	// Trie warmup left enabled for the parallel path. Original disable was
	// based on a calculator/warmer interaction concern that turned out to be
	// overly conservative — the Warmuper's reads are independent of the
	// calculator's SetUpdates call. Removing the disable produced an 8×
	// throughput improvement on the perf-devnet-3 SSTORE-bloated benchmark
	// (block 24358306) by letting the Warmuper pre-fetch branch data while
	// EVM execution runs. See #20920 for the canonical perf measurement.

	// Skip step-boundary commitment — the calculator handles this.
	pe.rs.StateV3.SetSkipStepBoundaryCommitment(true)
	defer pe.rs.StateV3.SetSkipStepBoundaryCommitment(false)

	// Store channels and limits on pe so execLoop can access them.
	pe.applyResultsCh = applyResults
	pe.commitResultsCh = commitResults
	pe.blockRequestsCh = blockRequests
	pe.maxBlockNum = maxBlockNum

	// Configure changeset capture and seed the initial accumulator BEFORE
	// the exec loop / executeBlocks goroutines start touching sd.mem. The
	// exec loop owns all subsequent SetChangesetAccumulator transitions
	// (per-block save/clear/install) so apply-loop and exec-loop sd.mem
	// writes never race on SharedDomains.mem.
	pe.shouldGenerateChangesets = shouldGenerateChangeSets(pe.cfg, startBlockNum, maxBlockNum)
	pe.ensureChangesetAccumulator(startBlockNum)

	// Start the commitment calculator. forcePerBlockCompute mirrors serial's
	// per-block gate (exec3_serial.go: `if !dbg.BatchCommitments ||
	// shouldGenerateChangesets || KeepExecutionProofs`). When changesets
	// must be generated (for unwind/reorg) the calculator must compute
	// per-block — otherwise batch-mode dedupes branch updates across the
	// batch and flushes them all into the last block's changeset, which
	// fails on subsequent reorgs.
	forcePerBlockCompute := pe.shouldGenerateChangesets || pe.cfg.syncCfg.KeepExecutionProofs
	calculator, err := newCommitmentCalculator(executorContext, pe.rs.Domains(), pe.cfg.db, pe.logPrefix, pe.logger, forcePerBlockCompute, commitResults, blockRequests, rootResults)
	if err != nil {
		return nil, nil, err
	}
	calculator.Start(executorContext)
	defer calculator.Stop()

	if err := pe.executeBlocks(executorContext, startBlockNum, maxBlockNum, blockLimit, initialTxNum, restoredTxNum, readAhead, initialCycle, applyResults, blockRequests, commitResults); err != nil {
		return nil, rwTx, err
	}

	var lastExecutedLog time.Time
	var lastBlockResult blockResult
	var lastHeader *types.Header
	var uncommittedBlocks int64
	var uncommittedTransactions uint64
	var uncommittedGas int64
	var hasLoggedExecution bool
	var hasLoggedCommittments atomic.Bool
	var commitStart time.Time

	var lastProgress commitment.CommitProgress

	execErr := func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				pe.logger.Warn("["+execStage.LogPrefix()+"] rw panic", "rec", rec, "stack", dbg.Stack())
			} else if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, &ErrLoopExhausted{})) {
				pe.logger.Warn("["+execStage.LogPrefix()+"] rw exit", "err", err, "stack", dbg.Stack())
			} else {
				pe.logger.Debug("[" + execStage.LogPrefix() + "] rw exit")
			}
		}()

		// Open a thread-local read-only tx for domain operations. The apply loop
		// must not use the rwTx for domain reads — rwTx is thread-bound to the
		// caller goroutine and will be used only for flush/unwind/stage-update.
		applyRoTx, err := pe.cfg.db.BeginTemporalRo(ctx)
		if err != nil {
			return fmt.Errorf("apply loop: open roTx: %w", err)
		}
		defer applyRoTx.Rollback()

		// pe.shouldGenerateChangesets and pe.currentChangeSet were set up
		// before pe.run/executeBlocks launched their goroutines (above the
		// calculator.Start call). Per-block accumulator save/clear/install
		// transitions are driven from the exec loop's blockResult handler.

		// appliedBlocks tracks blockNums that completed full apply-loop
		// processing (including post-block validation). Used at exit to
		// detect "the channel closed cleanly but a block was silently
		// missed" — i.e. block N's blockResult never arrived and we
		// returned nil anyway. Without this check those bugs silently let
		// invalid blocks become canonical.
		appliedBlocks := make(map[uint64]struct{})

		// txResultBlocks tracks every blockNum that had AT LEAST ONE
		// tx-result reach the apply loop. The completeness check at
		// channel-close compares this against appliedBlocks: any block
		// whose tx-results arrived but whose blockResult never did is a
		// silent failure (validator never fired for it).
		txResultBlocks := make(map[uint64]struct{})

		// rootResultsClosed records whether the calculator's rootResults
		// channel has closed. We disable that select-arm by setting the
		// local rootResults variable to nil (nil channels are never
		// ready), but later code that drains rootResults after
		// applyResults closes must skip the drain entirely if the
		// channel is already known closed — `for cr := range nilChan`
		// would hang forever.
		rootResultsClosed := false

		// deferredRootErr stashes ErrWrongTrieRoot from the calculator so a
		// later blockResult's post-execution validator (bad gas used, bad
		// receipts, bad bloom, etc.) can supersede it. Block-validation
		// errors take precedence over trie-root mismatches: a tx returning
		// the wrong error category here breaks eest's validation taxonomy
		// (the test expects the specific block-level error, not the
		// downstream trie consequence). Surfaced only after applyResults
		// closes and no block-validation error fired.
		var deferredRootErr error

		// blockUpdateCount/blockApplyCount count individual VersionedWrite entries
		// (balance, nonce, incarnation, codeHash, code, storage, selfDestruct are
		// separate entries).  This differs from the old StateUpdates count which
		// grouped all fields of one account as a single entry.  The values are only
		// used for an internal consistency check (blockUpdateCount==ApplyCount) and
		// trace output, so the change in semantics does not affect correctness.
		blockUpdateCount := 0
		blockApplyCount := 0
		// Collect per-tx writes so we can notify the accumulator AFTER
		// StartChange (which arrives with the blockResult, after all txResults).
		var pendingAccumulatorWrites []state.VersionedWrites

		// handleCommitResult processes a single commitment result from the
		// calculator. Defined here so both the blockResult handler and the
		// rootResults case in the main select can use it.
		handleCommitResult := func(cr commitmentResult) error {
			if cr.err != nil {
				// Lazy-load / ComputeCommitment errors from the calculator
				// don't wrap ErrWrongTrieRoot. Treating them as a wrong-root
				// would mark a valid block as bad (ReportBadHeaderPoS) and
				// trigger an unwind that throws away valid state. Fail fast
				// instead and preserve the original error in the message.
				if !errors.Is(cr.err, ErrWrongTrieRoot) {
					return fmt.Errorf("[%s] commitment: %w", pe.logPrefix, cr.err)
				}
				pe.logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x (%v)",
					pe.logPrefix, cr.blockNum, cr.rootHash, cr.err))
				if initialCycle {
					return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, cr.blockNum)
				}
				return handleIncorrectRootHashError(cr.blockNum, lastBlockResult.BlockHash, lastBlockResult.ParentHash, rwTx, pe.cfg, execStage, pe.logger, u)
			}
			pe.txExecutor.lastCommittedBlockNum.Store(cr.blockNum)
			pe.txExecutor.lastCommittedTxNum.Store(cr.txNum)
			return nil
		}

		// processCommit runs handleCommitResult and defers ErrWrongTrieRoot
		// instead of returning it. Other commitment errors stay fast-fail.
		// The deferred root error is surfaced only after applyResults
		// closes, so any block-validation error that fires for the same or
		// a later block first returns from the applyResults branch with
		// ErrInvalidBlock — matching serial's "validation precedes
		// commitment" ordering and keeping eest's error categorisation
		// honest.
		processCommit := func(cr commitmentResult) error {
			err := handleCommitResult(cr)
			if err == nil {
				return nil
			}
			if errors.Is(err, ErrWrongTrieRoot) {
				if deferredRootErr == nil {
					deferredRootErr = err
				}
				return nil
			}
			return err
		}

		// Apply loop: exits ONLY when applyResults is closed by the exec loop.
		// Do NOT add ctx.Done or executorContext.Done cases here — the exec
		// loop owns shutdown sequencing. Adding context checks here causes
		// the apply loop to exit before the calculator finishes, leaving
		// sd.mem inconsistent with the commitment boundary.
		for {
			select {
			case applyResult, ok := <-applyResults:
				if !ok {
					// Exec loop closed the channel — batch is complete.
					// Drain calculator results, then exit. Skip the drain
					// if rootResults already closed (its select-arm was
					// disabled by setting rootResults=nil; ranging a nil
					// channel hangs forever).
					if !rootResultsClosed {
						for cr := range rootResults {
							if err := processCommit(cr); err != nil {
								return err
							}
						}
					}
					if lastBlockResult.BlockNum > 0 {
						pe.txExecutor.lastCommittedBlockNum.Store(lastBlockResult.BlockNum)
						pe.txExecutor.lastCommittedTxNum.Store(lastBlockResult.lastTxNum)
					}
					// Two reasons the exec loop closes the channel:
					//   (1) sizeEst > batchLimit — flush and tell the stage loop
					//       there is more work pending (ErrLoopExhausted)
					//   (2) blockResult.BlockNum >= pe.maxBlockNum — we processed
					//       every block we were asked to; clean exit, not "more work"
					// Fork validation (StateStep, single-block batches) only ever hits
					// case (2); returning ErrLoopExhausted there causes the stage loop
					// to error with "unexpected state step has more work".
					// Completeness check: when the exec loop closes the apply channel,
					// every block whose tx-results arrived must also have produced a
					// blockResult. Otherwise the per-block validator never fires for
					// it and an invalid block becomes canonical.
					//
					// We track this two ways:
					//   - appliedBlocks: blockResults we fully processed (validated)
					//   - txResultBlocks: any block we saw at least one tx-result for
					// A block in txResultBlocks but not in appliedBlocks means
					// its tx-results arrived but the trailing blockResult never did
					// — exactly the silent-failure mode this catches.
					//
					// Not reaching maxBlockNum is a normal partial-batch state: when
					// the exec loop hits its size budget mid-batch it returns nil with
					// reachedMaxBlock=false, the apply loop drops out via the
					// ErrLoopExhausted return below, and the stage loop resumes from
					// lastBlockResult+1 in a follow-up call. Each block still executes
					// exactly once across the two batches, so we deliberately do NOT
					// flag maxBlockNum-not-applied here.
					if missing := applyLoopMissingBlocks(txResultBlocks, appliedBlocks); len(missing) > 0 {
						return fmt.Errorf("%w: apply loop exited (reachedMaxBlock=%v lastBlockResult=%d maxBlockNum=%d) but %d block(s) had tx-results without a blockResult: %v",
							rules.ErrInvalidBlock, pe.reachedMaxBlock.Load(), lastBlockResult.BlockNum, pe.maxBlockNum, len(missing), missing)
					}
					// Surface the deferred trie-root error here: no
					// block-validation error fired during the drain, so
					// the wrong-root stands.
					if deferredRootErr != nil {
						return deferredRootErr
					}
					if pe.reachedMaxBlock.Load() {
						return nil
					}
					// Clean exit even without the reachedMaxBlock flag: the exec loop
					// can exit through `rws.ResultCh closed` / `rws.Drain returned closed`
					// (execLoopExitCheck) for a single-block fork-validation batch where
					// the result heap empties before the main loop reaches the
					// execLoopShouldExit precedence check. In that path nobody flips
					// reachedMaxBlock, even though every block in [startBlockNum, maxBlockNum]
					// has been applied. Returning ErrLoopExhausted here makes the stage
					// loop report "has more work" and the engine API surfaces "unexpected
					// state step has more work" (TestEngineApiEmptyBlockProduction and the
					// engine-API cluster). When the applied range is complete, treat it as
					// a clean batch end.
					if lastBlockResult.BlockNum >= pe.maxBlockNum && len(applyLoopMissingBlocks(txResultBlocks, appliedBlocks)) == 0 {
						return nil
					}
					return &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult.BlockNum, Reason: "block batch is full"}
				}
				switch applyResult := applyResult.(type) {
				case *txResult:
					txResultBlocks[applyResult.blockNum] = struct{}{}
					uncommittedGas += applyResult.blockGasUsed
					uncommittedTransactions++
					if dbg.TraceApply && dbg.TraceBlock(applyResult.blockNum) {
						pe.rs.SetTrace(true)
						fmt.Println(applyResult.blockNum, "apply", applyResult.txNum, len(applyResult.writes))
					}
					blockUpdateCount += len(applyResult.writes)
					// All ApplyStateWrites + ApplyTxIndexes run in the execLoop
					// (sole sd.mem writer). The apply loop here only collects
					// accumulator notifications and per-tx counters.
					if pe.accumulator != nil {
						pendingAccumulatorWrites = append(pendingAccumulatorWrites, applyResult.writes)
					}
					blockApplyCount += len(applyResult.writes)
					pe.rs.SetTrace(false)
				case *blockResult:
					// Apply loop is the canonical error-emission point for
					// block-validity rejections (insufficient funds, gas
					// overflow, finalize rejection, scheduler-exhausted
					// incarnations). The worker plumbs the diagnosis through
					// blockResult.Err via nextResult → processResults → the
					// exec loop's sendResult — and the exec loop exits
					// immediately after sending (see the matching guard in
					// the exec loop). The calculator skips its compute for
					// this block (see committer.go case *blockResult). So
					// here, on the apply side: mark the block applied (so
					// the channel-close completeness check doesn't
					// double-report as silent miss), drop pending
					// accumulator notifications (we never announce invalid
					// blocks), and surface the worker's err. Single emission
					// point — no errors.Join of competing diagnostics.
					if applyResult.Err != nil {
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						pendingAccumulatorWrites = pendingAccumulatorWrites[:0]
						return applyResult.Err
					}
					// StartChange + NotifyAccumulator must both run in the apply
					// goroutine — keeps all accumulator access single-threaded
					// (avoids data race with the executor goroutine).
					// StartChange must come BEFORE NotifyAccumulator because it
					// initialises the latestChange entry that ChangeAccount etc. write into.
					if pe.accumulator != nil && applyResult.Header != nil {
						rawTxs, marshalErr := types.MarshalTransactionsBinary(applyResult.Txs)
						if marshalErr != nil {
							return fmt.Errorf("marshal transactions for accumulator, block %d: %w", applyResult.BlockNum, marshalErr)
						}
						pe.accumulator.StartChange(applyResult.Header, rawTxs, false)
						for _, writes := range pendingAccumulatorWrites {
							state.NotifyAccumulator(pe.accumulator, writes)
						}
						pendingAccumulatorWrites = pendingAccumulatorWrites[:0]
					}

					// Cache flush happens in the execLoop (before blockResult is sent).
					// sd.mem already has all TX writes when we reach here.

					var blockValidatorWaiter *blockValidator
					if applyResult.BlockNum > 0 && !applyResult.isPartial { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
						checkBloom := !pe.cfg.vmConfig.StatelessExec && !pe.cfg.vmConfig.NoReceipts
						checkReceipts := checkBloom && pe.cfg.chainConfig.IsByzantium(applyResult.BlockNum)

						b, err := pe.cfg.blockReader.BlockByHash(ctx, rwTx, applyResult.BlockHash)

						if err != nil {
							return fmt.Errorf("can't retrieve block %d: for post validation: %w", applyResult.BlockNum, err)
						}
						if b == nil {
							return fmt.Errorf("nil block %d (hash %x)", applyResult.BlockNum, applyResult.BlockHash)
						}

						lastHeader = b.HeaderNoCopy()

						if lastHeader.Number.Uint64() != applyResult.BlockNum {
							return fmt.Errorf("block numbers don't match expected: %d: got: %d for hash %x", applyResult.BlockNum, lastHeader.Number.Uint64(), applyResult.BlockHash)
						}

						if blockUpdateCount != applyResult.ApplyCount {
							return fmt.Errorf("block %d: applyCount mismatch: got: %d expected %d", applyResult.BlockNum, blockUpdateCount, applyResult.ApplyCount)
						}

						// Spawn per-block validation in a goroutine — the result is
						// joined via Wait() below, after the other per-result work
						// has had a chance to run in parallel with validation.
						blockValidatorWaiter = newBlockValidator(applyResult.BlockGasUsed, applyResult.BlobGasUsed, checkReceipts, checkBloom, applyResult.Receipts,
							lastHeader, b.Transactions(), pe.cfg.chainConfig, pe.logger)

						if !applyResult.isPartial && !execStage.CurrentSyncCycle.IsInitialCycle {
							pe.cfg.notifications.RecentReceipts.Add(applyResult.Receipts, b.Transactions(), lastHeader)
						}
					}

					if applyResult.BlockNum > lastBlockResult.BlockNum {
						uncommittedBlocks++
						pe.doms.SetTxNum(applyResult.lastTxNum)
						lastBlockResult = *applyResult
					}

					blockUpdateCount = 0
					blockApplyCount = 0

					// ClearAccountsCache moved to execLoop (producer side).
					// blockApplied removed — state writes and Flush happen in
					// the execLoop before the blockResult crosses the channel.
					// The apply loop only does indexes.

					// Commitment is computed by the commitmentCalculator goroutine.
					// Post-execution validation (receipts, BAL) runs here. The
					// per-block blockValidator was spawned earlier (~30 LOC up)
					// and runs concurrently with the work above; Wait() joins it.
					if err := blockValidatorWaiter.Wait(); err != nil {
						return fmt.Errorf("%w, block=%d, %v", rules.ErrInvalidBlock, applyResult.BlockNum, err)
					}

					if pe.cfg.chainConfig.IsAmsterdam(applyResult.BlockTime) || pe.cfg.experimentalBAL {
						err = ProcessBAL(rwTx, lastHeader, applyResult.TxIO, pe.cfg.chainConfig.IsAmsterdam(applyResult.BlockTime), pe.cfg.experimentalBAL, pe.cfg.dirs.DataDir)
						if err != nil {
							return err
						}
					}

					// Mark this block as fully applied. The exit-completeness
					// check at channel-close compares this set against the
					// expected [startBlockNum, maxBlockNum] range to detect
					// "block silently missed".
					appliedBlocks[applyResult.BlockNum] = struct{}{}

					// SavePastChangesetAccumulator + SetChangesetAccumulator(nil) +
					// rotation-to-next-block accumulator are all driven by the exec
					// loop now (see execLoop's blockResult handling), so the apply
					// loop must NOT touch SharedDomains.mem here. Doing so used to
					// race with the exec loop's ApplyStateWrites for the next block.

					if dbg.StopAfterBlock > 0 && applyResult.BlockNum == dbg.StopAfterBlock {
						pe.logger.Warn(fmt.Sprintf("[%s] STOP_AFTER_BLOCK reached, exiting without commit (debug mode)", pe.logPrefix), "block", applyResult.BlockNum)
						// Intentional os.Exit: STOP_AFTER_BLOCK is a debug switch used to
						// capture state at exactly N blocks executed. The DB is left as it
						// was *before* this block was applied so the next run reproduces
						// the stop point with the same input. Returning would run deferred
						// commit/flush paths and overwrite the very state we want to
						// preserve. Mirrors the design documented in PR #19803 — debug
						// only, never set in production.
						os.Exit(0)
					}
				}

			case cr, ok := <-rootResults:
				if !ok {
					// rootResults closed by the calculator on Stop.
					//
					// Do NOT return here. The apply loop must keep draining
					// applyResults until the EXEC LOOP closes that channel —
					// otherwise we race with sendResult and drop the trailing
					// blockResult, which makes invalid blocks become canonical
					// without ever reaching the per-block validator.
					//
					// Switch the rootResults case to the never-ready nil channel
					// so this select arm doesn't busy-spin on the closed channel.
					// rootResultsClosed makes the applyResults-close branch
					// skip the `for cr := range rootResults` drain (which would
					// hang forever on the nil channel).
					rootResults = nil
					rootResultsClosed = true
					continue
				}
				if err := processCommit(cr); err != nil {
					return err
				}
			case <-logEvery.C:
				if time.Since(lastExecutedLog) > logInterval-(logInterval/90) {
					hasLoggedExecution = true
					lastExecutedLog = time.Now()
					pe.LogExecution()
					if pe.agg.HasBackgroundFilesBuild() {
						pe.logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
					}
				}
			}
		}
	}()

	executorCancel()

	if !hasLoggedExecution {
		pe.LogExecution()
	}

	// Wait for all goroutines to complete before reading shared state.
	if waitErr := pe.wait(ctx); waitErr != nil {
		if execErr == nil {
			execErr = waitErr
		} else {
			execErr = errors.Join(execErr, waitErr)
		}
	}

	// Commitment is computed per-block by the calculator. Stage progress
	// is updated in handleCommitResult when results are consumed.

	if !hasLoggedCommittments.Load() && !commitStart.IsZero() {
		pe.LogCommitments(0, stepsInDb, lastProgress)
	}

	if execErr != nil {
		if !(errors.Is(execErr, context.Canceled) || errors.Is(execErr, &ErrLoopExhausted{})) {
			if lastHeader != nil {
				pe.logger.Warn(fmt.Sprintf("[%s] Execution failed", pe.logPrefix), "err", execErr, "block", lastHeader.Number.Uint64(), "hash", lastHeader.Hash())
			} else {
				pe.logger.Warn(fmt.Sprintf("[%s] Execution failed", pe.logPrefix), "err", execErr)
			}
			if errors.Is(execErr, rules.ErrInvalidBlock) {
				if pe.cfg.hd != nil && pe.cfg.hd.POSSync() && lastHeader != nil {
					pe.cfg.hd.ReportBadHeaderPoS(lastHeader.Hash(), lastHeader.ParentHash)
				}
				if pe.cfg.badBlockHalt {
					return nil, rwTx, execErr
				}
				if u != nil && lastHeader != nil {
					unwindTo := uint64(0)
					if lastHeader.Number.Uint64() > 0 {
						unwindTo = lastHeader.Number.Uint64() - 1
					}
					if err := u.UnwindTo(unwindTo, BadBlock(lastHeader.Hash(), execErr), rwTx); err != nil {
						return nil, rwTx, err
					}
				}
			}
			return nil, rwTx, execErr
		}
	}

	// Do NOT flush here — the stageloop owns flush/commit lifecycle.
	// pe.exec() returns with sd.mem containing the batch's writes.
	// The stageloop will flush, commit the rwTx, and create a new sd.
	//
	// But DO update stage progress so the stageloop knows we advanced.
	if (execErr == nil || errors.Is(execErr, &ErrLoopExhausted{})) && rwTx != nil {
		overlay := pe.doms.BlockOverlay()
		if overlay != nil {
			if err = execStage.Update(overlay, pe.lastCommittedBlockNum.Load()); err != nil {
				return nil, rwTx, err
			}
		} else {
			if err = execStage.Update(rwTx, pe.lastCommittedBlockNum.Load()); err != nil {
				return nil, rwTx, err
			}
		}
	}

	return lastHeader, rwTx, execErr
}

func (pe *parallelExecutor) LogExecution() {
	pe.progress.LogExecution(pe.rs.StateV3, pe)
	if domainMetrics := pe.domains().LogMetrics(); len(domainMetrics) > 0 {
		pe.logger.Info(fmt.Sprintf("[%s] domain reads", pe.logPrefix), domainMetrics...)
	}
	for domain, domainMetrics := range pe.domains().DomainLogMetrics() {
		pe.logger.Debug(fmt.Sprintf("[%s] %s", pe.logPrefix, domain), domainMetrics...)
	}
}

func (pe *parallelExecutor) LogCommitments(committedTransactions uint64, stepsInDb float64, lastProgress commitment.CommitProgress) {
	pe.txExecutor.lastCommittedTxNum.Add(committedTransactions)
	pe.progress.LogCommitments(pe.rs.StateV3, pe, stepsInDb, lastProgress)
	if domainMetrics := pe.domains().LogMetrics(); len(domainMetrics) > 0 {
		pe.logger.Info(fmt.Sprintf("[%s] domain reads", pe.logPrefix), domainMetrics...)
	}
	for domain, domainMetrics := range pe.domains().DomainLogMetrics() {
		pe.logger.Debug(fmt.Sprintf("[%s] %s", pe.logPrefix, domain), domainMetrics...)
	}
}

// triggerBatchCommitment sends a commitComputeRequest to the calculator so it
// computes the batch commitment before the exec loop exits and closes channels.
func (pe *parallelExecutor) triggerBatchCommitment(ctx context.Context) {
	if pe.commitResultsCh == nil {
		return
	}
	defer func() {
		// The exec loop closes commitResultsCh on shutdown; a concurrent send
		// here would panic with "send on closed channel". That race is benign:
		// the calculator is already shutting down and the request would be
		// dropped anyway. Recover only that specific panic and re-raise anything
		// else so real bugs still surface.
		if rec := recover(); rec != nil {
			if e, ok := rec.(runtime.Error); ok && strings.Contains(e.Error(), "send on closed channel") {
				return
			}
			panic(rec)
		}
	}()
	select {
	case pe.commitResultsCh <- &commitComputeRequest{}:
	case <-ctx.Done():
	}
}

func (pe *parallelExecutor) LogComplete(stepsInDb float64) {
	pe.progress.LogComplete(pe.rs.StateV3, pe, stepsInDb)
	if domainMetrics := pe.domains().LogMetrics(); len(domainMetrics) > 0 {
		pe.logger.Info(fmt.Sprintf("[%s] domains", pe.logPrefix), domainMetrics...)
	}
	for domain, domainMetrics := range pe.domains().DomainLogMetrics() {
		pe.logger.Debug(fmt.Sprintf("[%s] %s", pe.logPrefix, domain), domainMetrics...)
	}
}

func (pe *parallelExecutor) resetWorkers(ctx context.Context, rs *state.StateV3Buffered, _ kv.TemporalTx) error {
	pe.Lock()
	defer pe.Unlock()

	for _, worker := range pe.execWorkers {
		// parallel workers hold thier own tx don't pass in an externals tx
		worker.ResetState(rs, nil, nil, state.NewLightCollector(), nil)
	}

	return nil
}

func (pe *parallelExecutor) execLoop(ctx context.Context) (err error) {
	// The exec loop is the owner of shutdown sequencing. On exit it
	// closes commitResults then applyResults, causing the calculator
	// and apply loop to drain and exit.
	//
	// Note: pe.applyTx is the stageloop's rwTx (externally supplied).
	// Do NOT rollback it here — the stageloop owns its lifecycle.

	defer pe.closeApplyChannels()
	defer func() {
		// Close the exec loop's own RO tx — prevents leak across batches.
		if pe.applyTx != nil {
			pe.applyTx.Rollback()
			pe.applyTx = nil
		}
	}()
	defer func() {
		if rec := recover(); rec != nil {
			pe.logger.Warn("["+pe.logPrefix+"] exec loop panic", "rec", rec, "stack", dbg.Stack())
		} else if err != nil && !errors.Is(err, context.Canceled) {
			pe.logger.Warn("["+pe.logPrefix+"] exec loop error", "err", err)
		} else {
			pe.logger.Debug("[" + pe.logPrefix + "] exec loop exit")
		}
	}()

	pe.RLock()
	applyTx := pe.applyTx
	pe.RUnlock()

	for {
		err := func() error {
			pe.Lock()
			defer pe.Unlock()
			if applyTx != pe.applyTx {
				if applyTx != nil {
					applyTx.Rollback()
				}
			}

			if pe.applyTx == nil {
				pe.applyTx, err = pe.cfg.db.BeginTemporalRo(ctx) //nolint

				if err != nil {
					return err
				}

				applyTx = pe.applyTx
			}
			return nil
		}()

		if err != nil {
			return err
		}

		// Limit how many blocks can be pending in pe.blockExecutors simultaneously.
		// processRequest is non-blocking (it just stores blocks in the map), so
		// without this check execRequests drains instantly and pe.blockExecutors
		// grows unbounded — holding all decoded TxTask objects in RAM.
		// Setting pendingCh to nil causes the select to skip that case entirely,
		// applying backpressure that propagates to executeBlocks.func1.
		const maxPendingBlocks = 32
		pe.RLock()
		pendingBlocks := len(pe.blockExecutors)
		pe.RUnlock()
		var pendingCh chan *execRequest
		if pendingBlocks < maxPendingBlocks {
			pendingCh = pe.execRequests
		}

		select {
		case exec := <-pendingCh:
			if err := pe.processRequest(ctx, exec); err != nil {
				return err
			}
			continue
		case <-ctx.Done():
			// Context cancelled (executeBlocks returned from errgroup, or
			// executor cleanup ran). Drain any remaining worker results,
			// forward any completed blockResults to the apply loop +
			// commitment calculator, then exit. Without forwarding the
			// trailing blockResult, the apply loop sees only the channel
			// close and never observes that maxBlockNum was reached —
			// which makes single-block fork validation see "more work
			// pending" when there is none.
			for {
				select {
				case nextResult, ok := <-pe.rws.ResultCh():
					if !ok {
						return pe.execLoopExitCheck("ctx-done-drain: rws.ResultCh closed")
					}
					if closed, err := pe.rws.Drain(ctx, nextResult); err != nil || closed {
						if err != nil {
							return err
						}
						return pe.execLoopExitCheck("ctx-done-drain: rws.Drain returned closed")
					}
					blockResult, err := pe.processResults(ctx, applyTx)
					if err != nil {
						return err
					}
					if blockResult != nil {
						pe.RLock()
						blockExecutor, exists := pe.blockExecutors[blockResult.BlockNum]
						pe.RUnlock()
						if exists {
							pe.lastExecutedBlockNum.Store(int64(blockResult.BlockNum))
							if err := blockExecutor.sendResult(ctx, blockResult); err != nil {
								return err
							}
							// See main exec-loop path: invalid blockResult is
							// the apply loop's signal — don't schedule next.
							if blockResult.Err != nil {
								return nil
							}
							pe.Lock()
							delete(pe.blockExecutors, blockResult.BlockNum)
							pe.Unlock()
							pe.scheduleNextPending(ctx)
							if blockResult.BlockNum >= pe.maxBlockNum {
								pe.reachedMaxBlock.Store(true)
							}
						}
					}
				default:
					return pe.execLoopExitCheck("ctx-done-drain: no more pending results")
				}
			}
		case nextResult, ok := <-pe.rws.ResultCh():
			if !ok {
				return pe.execLoopExitCheck("main-select: rws.ResultCh closed")
			}
			closed, err := pe.rws.Drain(ctx, nextResult)
			if err != nil {
				return err
			}
			if closed {
				return pe.execLoopExitCheck("main-select: rws.Drain returned closed")
			}
		}

		blockResult, err := pe.processResults(ctx, applyTx)

		if err != nil {
			return err
		}

		if blockResult != nil {
			pe.RLock()
			blockExecutor, ok := pe.blockExecutors[blockResult.BlockNum]
			pe.RUnlock()

			if ok {
				pe.lastExecutedBlockNum.Store(int64(blockResult.BlockNum))
				pe.execCount.Add(int64(blockExecutor.cntExec))
				pe.abortCount.Add(int64(blockExecutor.cntAbort))
				pe.invalidCount.Add(int64(blockExecutor.cntValidationFail))
				pe.readCount.Add(blockExecutor.blockIO.ReadCount())
				pe.writeCount.Add(blockExecutor.blockIO.WriteCount())

				if !blockExecutor.execStarted.IsZero() {
					pe.blockExecMetrics.Duration.Add(time.Since(blockExecutor.execStarted))
					pe.blockExecMetrics.BlockCount.Add(1)
				}
				// Snapshot the just-completed block's changeset BEFORE sending the
				// blockResult, so that the commitment calculator (which consumes
				// blockResults on a separate goroutine) can find this block's
				// saved changeset via GetChangesetByBlockNum at compute time.
				// In per-block compute mode (shouldGenerateChangesets), the
				// calculator switches the accumulator to this saved CS for the
				// duration of ComputeCommitment (committer.go:computeWithBlockAccumulator)
				// so branch writes land in block N's CS rather than whatever the
				// exec loop has installed as current. If we saved AFTER sendResult,
				// the calculator could race ahead and look up an unsaved CS,
				// causing branch deltas to leak into the next block's CS and
				// produce wrong-trie-root chains on subsequent reorg-driven
				// re-execution (see TestRecreateAndRewind reproducer). Clearing
				// the live accumulator and the local pointer must still happen
				// here (in the exec loop) so the rotation-to-next-block install
				// at line 893-895 is serialized with the exec loop's other
				// sd.mem writes (system calls, finalize, ApplyStateWrites for
				// the next block).
				// Belt-and-braces: an empty block (no tx-results reaching
				// processResults) may not have triggered the install — create
				// its (empty) accumulator so it gets saved like every other block.
				pe.ensureChangesetAccumulator(blockResult.BlockNum)
				if pe.currentChangeSet != nil {
					pe.domains().SavePastChangesetAccumulator(blockResult.BlockHash, blockResult.BlockNum, pe.currentChangeSet)
				}
				if err := blockExecutor.sendResult(ctx, blockResult); err != nil {
					return err
				}
				pe.clearChangesetAccumulator()

				// Block-validity rejection: the apply loop will consume
				// blockResult and return its Err; the calculator skips the
				// commitment compute (see committer.go case *blockResult).
				// Exit the exec loop here so we don't schedule the next
				// pending block on top of partial / now-discarded state —
				// the apply loop's Err is the canonical signal, surfaced
				// through errgroup to the caller. Leaving scheduling running
				// would race with the apply loop's Err return: the
				// commitment calculator could compute on partial state, the
				// next block's executor could start against stale sd.mem,
				// and errors.Join would weld competing diagnostics.
				if blockResult.Err != nil {
					return nil
				}

				pe.Lock()
				delete(pe.blockExecutors, blockResult.BlockNum)
				pe.Unlock()
				pe.scheduleNextPending(ctx)

				// Use AfterCommitment estimate (2x) in per-block mode since
				// commitment is already computed. BeforeCommitment (4x) is
				// for batch mode where commitment hasn't run yet.
				var sizeEst uint64
				if dbg.BatchCommitments {
					sizeEst = pe.rs.SizeEstimateBeforeCommitment()
				} else {
					sizeEst = pe.rs.SizeEstimateAfterCommitment()
				}
				batchLimit := pe.cfg.batchSize.Bytes()
				// We are inside the `blockResult != nil` branch, so at least one
				// complete (non-partial) block has been applied in this batch.
				// That is enough to safely trigger a batch commit on size.
				switch execLoopShouldExit(blockResult, sizeEst, batchLimit, pe.maxBlockNum, dbg.StopAfterBlock) {
				case execLoopExitMaxReached:
					pe.reachedMaxBlock.Store(true)
					pe.triggerBatchCommitment(ctx)
					return nil
				case execLoopExitSizeLimit, execLoopExitExhausted, execLoopExitStopAfter:
					pe.triggerBatchCommitment(ctx)
					return nil
				}
			}

			// State writes and Flush happen in the execLoop (before the
			// blockResult is sent). sd.mem is already up to date.
			// No need to wait for the apply loop — it only does indexes.
			pe.RLock()
			blockExecutor, ok = pe.blockExecutors[blockResult.BlockNum+1]
			pe.RUnlock()

			if ok {
				// Fast-path install of the next block's changeset accumulator,
				// still in the exec loop (single-writer). If the next block's
				// executor isn't in the map yet this is a no-op; processResults
				// then installs it lazily on the block's first apply.
				pe.ensureChangesetAccumulator(blockExecutor.blockNum)
				pe.onBlockStart(ctx, blockExecutor.blockNum, blockExecutor.blockHash)
				blockExecutor.execStarted = time.Now()
				blockExecutor.scheduleExecution(ctx, pe)
			}
		}
	}
}

func (pe *parallelExecutor) processRequest(ctx context.Context, execRequest *execRequest) (err error) {
	// Validate state cache before processing block - ensures cache is cleared after reorgs
	// This matches the behavior in serial execution (exec3_serial.go)
	if len(execRequest.tasks) > 0 {
		if txTask, ok := execRequest.tasks[0].(*exec.TxTask); ok && txTask.Header != nil {
			parentHash := txTask.Header.ParentHash
			blockHash := execRequest.blockHash
			if stateCache := pe.doms.GetStateCache(); stateCache != nil {
				stateCache.ValidateAndPrepare(parentHash, blockHash)
			}
		}
	}

	prevSenderTx := map[accounts.Address]int{}
	var scheduleable *blockExecutor
	var executor *blockExecutor

	for i, txTask := range execRequest.tasks {
		t := &execTask{
			Task:               txTask,
			index:              i,
			shouldDelayFeeCalc: true,
		}

		blockNum := t.Version().BlockNum

		if executor == nil {
			var ok bool
			executor, ok = pe.blockExecutors[blockNum]

			if !ok {
				executor = newBlockExec(blockNum, execRequest.blockHash, execRequest.gasPool, execRequest.accessList, execRequest.applyResults, execRequest.commitResults, execRequest.profile, execRequest.exhausted)
			}
		}

		executor.tasks = append(executor.tasks, t)
		executor.results = append(executor.results, nil)
		executor.txIncarnations = append(executor.txIncarnations, 0)
		executor.execFailed = append(executor.execFailed, 0)
		executor.execAborted = append(executor.execAborted, 0)

		executor.estimateDeps[len(executor.tasks)-1] = []int{}

		executor.execTasks.pushPending(i)
		executor.validateTasks.pushPending(i)

		switch {
		case len(t.Dependencies()) > 0:
			for _, depTxIndex := range t.Dependencies() {
				executor.execTasks.addDependency(depTxIndex+1, i)
			}
			executor.execTasks.clearPending(i)
		case len(execRequest.accessList) != 0:
			// if we have an access list we can assume that all
			// writes are already in the shared memory map so
			// we can go ahead and schedule all tx jobs
			// optimistically without needing to worry about
			// clashes, this should signifigatly improve tx
			// concurrency
			break
		default:
			sender, err := t.TxSender()
			if err != nil {
				return err
			}
			if !sender.IsNil() {
				if tx, ok := prevSenderTx[sender]; ok {
					executor.execTasks.addDependency(tx, i)
					executor.execTasks.clearPending(i)
				}

				prevSenderTx[sender] = i
			}
		}

		if t.IsBlockEnd() {
			pe.Lock()
			if len(pe.blockExecutors) == 0 {
				pe.blockExecutors = map[uint64]*blockExecutor{
					blockNum: executor,
				}
				scheduleable = executor
			} else {
				pe.blockExecutors[t.Version().BlockNum] = executor
			}
			pe.Unlock()

			executor = nil
		}
	}

	if scheduleable != nil {
		pe.blockExecMetrics.BlockCount.Add(1)
		scheduleable.execStarted = time.Now()
		scheduleable.scheduleExecution(ctx, pe)
	}

	return nil
}

// applyLoopMissingBlocks returns the blockNums in txResultBlocks that
// did not produce a corresponding blockResult — meaning the per-block
// validator never fired for them and an invalid block could become
// canonical. Returns nil if every block whose tx-results arrived also
// produced a blockResult.
//
// Does NOT flag maxBlockNum when !reachedMaxBlock: a partial batch
// (size-limit hit) legitimately stops short of maxBlockNum, and the
// stage loop's ErrLoopExhausted handling resumes from the next block
// in a follow-up call. Flagging maxBlockNum here turns that legitimate
// path into a spurious InvalidBlock error — the BenchmarkFeeHistory
// 200-block fixture exhausts the 5MB batch budget at block 114 and
// previously errored despite blocks 1..114 being applied cleanly.
//
// Pure function — extracted from the apply loop's channel-close branch
// so the invariant is unit-testable. See TestApplyLoopMissingBlocks.
func applyLoopMissingBlocks(txResultBlocks, appliedBlocks map[uint64]struct{}) []uint64 {
	var missing []uint64
	for n := range txResultBlocks {
		if _, ok := appliedBlocks[n]; !ok {
			missing = append(missing, n)
		}
	}
	return missing
}

// execLoopExitDecision is the result of evaluating the exec-loop's
// per-blockResult exit conditions. Values are ordered by precedence:
// later conditions only matter if no earlier one fired.
type execLoopExitDecision int

const (
	// execLoopContinue: keep processing — no exit condition met.
	execLoopContinue execLoopExitDecision = iota
	// execLoopExitSizeLimit: rs.SizeEstimate*Commitment crossed the
	// configured batch budget; the partial-batch flush path runs.
	execLoopExitSizeLimit
	// execLoopExitMaxReached: blockResult.BlockNum >= maxBlockNum;
	// the caller flips reachedMaxBlock so the apply loop returns
	// nil (clean batch end) rather than ErrLoopExhausted.
	execLoopExitMaxReached
	// execLoopExitExhausted: executeBlocks dispatched its final
	// blockResult with .Exhausted set (per-cycle block limit hit).
	// Without honoring this the exec loop parks forever waiting
	// for work the dispatcher will never produce.
	execLoopExitExhausted
	// execLoopExitStopAfter: dbg.StopAfterBlock crossed (debug only).
	execLoopExitStopAfter
)

// execLoopShouldExit evaluates the exec-loop's per-blockResult exit
// decision in priority order. Pure function so the precedence is
// unit-testable; the production code at exec3_parallel.go around line
// 864 calls this and dispatches based on the returned decision.
//
// Priority order (matches production):
//  1. sizeEst > batchLimit         (size-limit batch flush — most urgent)
//  2. blockResult.BlockNum >= max  (clean end — flip reachedMaxBlock)
//  3. blockResult.Exhausted != nil (per-cycle dispatch limit hit)
//  4. dbg.StopAfterBlock crossed   (debug-only halt)
//  5. otherwise execLoopContinue   (schedule next block)
//
// Reordering any of these silently changes which exit branch wins when
// two conditions overlap (e.g. final block of a cycle that also crosses
// the size limit), which is why the test pins the exact precedence.
// See TestExecLoopShouldExitPriority.
func execLoopShouldExit(blockResult *blockResult, sizeEst, batchLimit, maxBlockNum, stopAfterBlock uint64) execLoopExitDecision {
	if sizeEst > batchLimit {
		return execLoopExitSizeLimit
	}
	if blockResult.BlockNum >= maxBlockNum {
		return execLoopExitMaxReached
	}
	if blockResult.Exhausted != nil {
		return execLoopExitExhausted
	}
	if stopAfterBlock > 0 && blockResult.BlockNum >= stopAfterBlock {
		return execLoopExitStopAfter
	}
	return execLoopContinue
}

// closeApplyChannels closes the apply-loop-bound channels in the order
// the calculator and apply loop require: commitResults FIRST so the
// calculator drains and closes rootResults, then applyResults so the
// apply loop sees its channel close after the calculator is done. The
// inverse order would let the apply loop exit while the calculator is
// still publishing — the trailing commitment write would land on a
// closed channel and panic.
//
// "close of closed channel" panics inside safeClose are benign — it
// just means the channel was already closed by another shutdown path.
// Recover only that specific panic and re-raise anything else so real
// bugs still surface.
//
// Returns the names of the channels closed in the order they were
// closed. The production call site discards this (deferred-call
// return values are ignored); tests use it to deterministically
// verify the close order without racing on observer-goroutine
// wakeups. See TestApplyLoopChannelCloseOrder.
func (pe *parallelExecutor) closeApplyChannels() (closedOrder []string) {
	safeClose := func(ch chan applyResult, name string) {
		defer func() {
			if rec := recover(); rec != nil {
				if e, ok := rec.(runtime.Error); ok && strings.Contains(e.Error(), "close of closed channel") {
					return
				}
				panic(rec)
			}
		}()
		close(ch)
		closedOrder = append(closedOrder, name)
	}
	if pe.commitResultsCh != nil {
		safeClose(pe.commitResultsCh, "commitResults")
		pe.commitResultsCh = nil
	}
	// blockRequests has a single sender (executeBlocks), so a plain close is
	// race-free; the nil-guard keeps closeApplyChannels idempotent.
	if pe.blockRequestsCh != nil {
		close(pe.blockRequestsCh)
		pe.blockRequestsCh = nil
		closedOrder = append(closedOrder, "blockRequests")
	}
	if pe.applyResultsCh != nil {
		safeClose(pe.applyResultsCh, "applyResults")
		pe.applyResultsCh = nil
	}
	return
}

// execLoopExitCheck enforces the completeness invariant for the exec
// loop's clean exit paths: all blocks the loop was asked to process must
// be drained from pe.blockExecutors. A non-empty map at exit means a
// block was scheduled (or queued) but never produced a blockResult,
// which previously caused "block accepted when it should have been
// rejected" failures (the apply loop never received the block, post-
// validation never fired). Converts that silent-success path into a
// loud InvalidBlock error so the failure surfaces through InsertChain.
//
// The reason argument tags the call site (which silent-return path
// triggered the check) so a failure log identifies the exit path
// involved without needing a stack trace.
func (pe *parallelExecutor) execLoopExitCheck(reason string) error {
	pe.RLock()
	pendingBlocks := len(pe.blockExecutors)
	var pendingNums []uint64
	if pendingBlocks > 0 {
		pendingNums = make([]uint64, 0, pendingBlocks)
		for n := range pe.blockExecutors {
			pendingNums = append(pendingNums, n)
		}
	}
	pe.RUnlock()
	if pendingBlocks > 0 {
		return fmt.Errorf("%w: parallel exec loop exited with %d block(s) still pending in pe.blockExecutors %v (reason=%s)",
			rules.ErrInvalidBlock, pendingBlocks, pendingNums, reason)
	}
	return nil
}

// scheduleNextPending picks the lowest-numbered block still queued in
// pe.blockExecutors and starts its execution. Called after a completed
// block is removed from the map so that any block previously enqueued by
// processRequest while the slot was busy actually gets scheduled. Without
// this, processRequest only schedules when the map was empty at insert
// time — a block enqueued while the previous block is still in flight
// becomes orphaned in the map, the apply loop never receives its result,
// and post-block validation silently never fires.
func (pe *parallelExecutor) scheduleNextPending(ctx context.Context) {
	pe.Lock()
	if len(pe.blockExecutors) == 0 {
		pe.Unlock()
		return
	}
	var nextNum uint64
	first := true
	for n := range pe.blockExecutors {
		if first || n < nextNum {
			nextNum = n
			first = false
		}
	}
	next := pe.blockExecutors[nextNum]
	pe.Unlock()
	if next == nil || !next.execStarted.IsZero() {
		// Already running (or scheduled).
		return
	}
	pe.blockExecMetrics.BlockCount.Add(1)
	next.execStarted = time.Now()
	next.scheduleExecution(ctx, pe)
}

func (pe *parallelExecutor) processResults(ctx context.Context, applyTx kv.TemporalTx) (blockResult *blockResult, err error) {
	rwsIt := pe.rws.Iter()
	for rwsIt.HasNext() && blockResult == nil {
		txResult := rwsIt.PopNext()

		if pe.cfg.syncCfg.ChaosMonkey && pe.enableChaosMonkey {
			chaosErr := chaos_monkey.ThrowRandomConsensusError(false, txResult.Version().TxIndex, pe.cfg.badBlockHalt, txResult.Err)
			if chaosErr != nil {
				log.Warn("Monkey in consensus")
				return blockResult, chaosErr
			}
		}

		pe.RLock()
		blockExecutor, ok := pe.blockExecutors[txResult.Version().BlockNum]
		pe.RUnlock()

		if !ok {
			return nil, fmt.Errorf("unknown block: %d", txResult.Version().BlockNum)
		}

		// Ensure this block's changeset accumulator is installed before its
		// writes are applied — covers blocks scheduled out of band (with no
		// preceding blockResult to trigger the fast-path install above).
		pe.ensureChangesetAccumulator(txResult.Version().BlockNum)

		blockResult, err = blockExecutor.nextResult(ctx, pe, txResult, applyTx)

		if err != nil {
			return blockResult, err
		}
	}

	return blockResult, nil
}

func (pe *parallelExecutor) run(ctx context.Context) (context.Context, context.CancelFunc, error) {
	// execRequests holds one entry per decoded block (each containing all its TxTasks).
	// A large buffer causes the block-loader goroutine to race far ahead of the apply
	// loop, accumulating all decoded transaction objects in memory simultaneously.
	// 128 blocks (~25 k txns on mainnet) is sufficient to keep workers busy.
	pe.execRequests = make(chan *execRequest, 128)
	// Clear stale blockExecutors from previous batch — unprocessed blocks left
	// in the map after a "batch full" exit would prevent the first block of the
	// new batch from being scheduled (processRequest only schedules when map is empty).
	pe.blockExecutors = nil
	// in is the per-transaction work queue consumed by OCC workers.
	// 2048 entries keeps all workers saturated without unbounded accumulation.
	pe.in = exec.NewQueueWithRetry(2_048)

	pe.taskExecMetrics = exec.NewWorkerMetrics()
	pe.blockExecMetrics = newBlockExecMetrics()

	execLoopCtx, execLoopCtxCancel := context.WithCancel(ctx)
	pe.execLoopGroup, execLoopCtx = errgroup.WithContext(execLoopCtx)

	var err error
	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers, err = exec.NewWorkersPool(
		execLoopCtx, nil, true, pe.cfg.db, nil, nil, nil, pe.in,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine,
		pe.workerCount+1, pe.taskExecMetrics, pe.cfg.dirs, pe.logger)

	if err != nil {
		return execLoopCtx, execLoopCtxCancel, err
	}

	pe.execLoopGroup.Go(func() error {
		defer pe.rws.Close()
		defer pe.in.Release()
		pe.resetWorkers(execLoopCtx, pe.rs, nil)
		return pe.execLoop(execLoopCtx)
	})

	return execLoopCtx, func() {
		execLoopCtxCancel()

		pe.in.Release()
		pe.stopWorkers()

		_ = pe.wait(ctx)
	}, nil
}

func (pe *parallelExecutor) wait(ctx context.Context) error {
	doneCh := make(chan error, 1)

	go func() {
		if pe.execLoopGroup != nil {
			err := pe.execLoopGroup.Wait()
			if err != nil && !errors.Is(err, context.Canceled) {
				doneCh <- err
				return
			}
			pe.waitWorkers()
		}
		doneCh <- nil
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-doneCh:
			return err
		}
	}
}

type applyResult any

type blockResult struct {
	BlockNum        uint64
	BlockTime       uint64
	BlockHash       common.Hash
	ParentHash      common.Hash
	StateRoot       common.Hash
	Err             error
	BlockGasUsed    uint64
	BlobGasUsed     uint64
	lastTxNum       uint64
	complete        bool
	isPartial       bool
	ApplyCount      int
	TxIO            *state.VersionedIO
	Receipts        types.Receipts
	Stats           map[int]ExecutionStat
	Deps            *state.DAG
	AllDeps         map[int]map[int]bool
	Exhausted       *ErrLoopExhausted
	Header          *types.Header      // for accumulator.StartChange in apply loop
	Txs             types.Transactions // for accumulator.StartChange in apply loop
	blockStateCache *state.BlockStateCache
}

type txResult struct {
	blockNum              uint64
	txNum                 uint64
	blockGasUsed          int64
	blobGasUsed           uint64
	cumulativeBlobGasUsed uint64
	receipt               *types.Receipt
	logs                  []*types.Log
	traceFroms            map[accounts.Address]struct{}
	traceTos              map[accounts.Address]struct{}
	writes                state.VersionedWrites
	rules                 *chain.Rules
	isFinalize            bool // block-end finalize writes — apply to sd.mem directly
}

// blockRequest is the commitment calculator's per-block heads-up, sent by the
// dispatch layer on its own channel — ahead of, and separate from, the
// block's txResult/blockResult stream so it is never trapped behind a prior
// block's results. It carries the block identity and the block's BAL (nil
// when none), from which the calculator selects its per-block mode.
type blockRequest struct {
	blockNum  uint64
	blockHash common.Hash
	stateRoot common.Hash
	bal       types.BlockAccessList
}

// calcMode is the commitment calculator's per-block strategy.
type calcMode uint8

const (
	// calcModeIncremental accumulates per-tx writes from the result stream
	// then computes — today's behaviour, and the fallback when a block has
	// no BAL.
	calcModeIncremental calcMode = iota
	// calcModeBALDriven loads the changed-key set from the block's BAL up
	// front so the trie fold need not wait for the per-tx stream. Selected
	// when the block carries a BAL and BAL I/O is enabled.
	calcModeBALDriven
)

type execTask struct {
	exec.Task
	index              int
	shouldDelayFeeCalc bool
}

type execResult struct {
	*exec.TxResult
	writes state.VersionedWrites
}

func (result *execResult) finalize(prevReceipt *types.Receipt, engine rules.Engine, vm *state.VersionMap, stateReader state.StateReader, stateWriter state.StateWriter, balActive bool) (*types.Receipt, state.ReadSet, state.VersionedWrites, error) {
	task, ok := result.Task.(*taskVersion)

	if !ok {
		return nil, nil, nil, fmt.Errorf("unexpected task type: %T", result.Task)
	}

	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex
	txIncarnation := task.Version().Incarnation

	txTrace := dbg.TraceTransactionIO &&
		(dbg.TraceTx(blockNum, txIndex) || dbg.TraceAccount(result.Coinbase.Handle()) || dbg.TraceAccount(result.ExecutionResult.BurntContractAddress.Handle()))

	var tracePrefix string
	if txTrace {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation)
		fmt.Println(tracePrefix, "finalize")
		defer fmt.Println(tracePrefix, "done finalize")
	}

	// With split fee logic: the worker debits the sender (gas payment +
	// refund) but does NOT credit coinbase or burnt contract
	// (shouldDelayFeeCalc=true → noFeeBurnAndTip=true). The finalize
	// reads the correct coinbase/burnt base from the versionMap and
	// adds FeeTipped/FeeBurnt. No StripBalanceWrite needed.

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil, nil, nil, nil
	}

	rules := txTask.EvmBlockContext.Rules(txTask.Config)

	if txIndex < 0 || task.IsBlockEnd() {
		// System TXs use full IBS reconstruction — they don't go through
		// the worker execution path so fee splitting doesn't apply.
		// Strip coinbase/burnt for these since they may have stale writes.
		txOut, coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta := result.TxOut.StripBalanceWrite(result.Coinbase, result.TxIn)
		result.TxOut = txOut
		txOut, _, _, _ = result.TxOut.StripBalanceWrite(result.ExecutionResult.BurntContractAddress, result.TxIn)
		result.TxOut = txOut
		delete(result.TxIn, result.Coinbase)
		delete(result.TxIn, result.ExecutionResult.BurntContractAddress)
		_, _, _ = coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta
		return result.finalizeSystemTx(task, txTask, rules, vm, stateReader, stateWriter)
	}

	// Regular TXs: finalize reads correct coinbase/burnt base from
	// versionMap, adds FeeTipped/FeeBurnt. No delta computation needed.
	return result.finalizeTxSimple(task, txTask, prevReceipt, engine, vm, stateReader,
		rules, balActive, txTrace, tracePrefix)
}

// finalizeSystemTx handles block-end and system TXs (txIndex < 0) via full
// IBS reconstruction. These are infrequent (1 per block) so the overhead is
// acceptable.
func (result *execResult) finalizeSystemTx(
	task *taskVersion,
	txTask *exec.TxTask,
	rules *chain.Rules,
	vm *state.VersionMap,
	stateReader state.StateReader,
	stateWriter state.StateWriter,
) (*types.Receipt, state.ReadSet, state.VersionedWrites, error) {
	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex
	txIncarnation := task.Version().Incarnation

	// Use an empty ReadSet so all reads go through the versionMap (which
	// has all prior TX writes). The execution-phase ReadSet (result.TxIn)
	// may be stale if the system TX ran speculatively before all regular
	// TXs completed — cached reads would return pre-block values instead
	// of the post-block state needed by syscalls (withdrawal/consolidation).
	ibs := state.New(state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader))
	ibs.SetTxContext(blockNum, txIndex)
	ibs.SetVersion(txIncarnation)
	// Use the block's versionMap so the IBS's versionedRead (used by
	// GetState for storage reads) can see writes from prior TXs.
	// The system TX's syscalls read withdrawal/consolidation contract
	// storage which was modified by regular TXs in this block.
	ibs.SetVersionMap(vm)
	if err := ibs.ApplyVersionedWrites(result.TxOut); err != nil {
		return nil, nil, nil, err
	}
	ibs.SetTrace(txTask.Trace)

	if err := ibs.FinalizeTx(rules, stateWriter); err != nil {
		return nil, nil, nil, err
	}
	// Use checkDirty=false because FinalizeTx clears the journal (dirties map).
	// With checkDirty=true, all writes would be deleted from the versionMap
	// since no address appears dirty after the journal reset.
	return nil, ibs.VersionedReads(), ibs.VersionedWrites(false), nil
}

// finalizeTxSimple handles regular TXs with split fee logic:
// the worker debited the sender but did NOT credit coinbase/burnt
// (shouldDelayFeeCalc=true). This function reads the correct coinbase/burnt
// base from the versionMap and adds FeeTipped/FeeBurnt.
func (result *execResult) finalizeTxSimple(
	task *taskVersion,
	txTask *exec.TxTask,
	prevReceipt *types.Receipt,
	engine rules.Engine,
	vm *state.VersionMap,
	stateReader state.StateReader,
	chainRules *chain.Rules,
	balActive bool,
	txTrace bool, tracePrefix string,
) (*types.Receipt, state.ReadSet, state.VersionedWrites, error) {
	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex

	// Read coinbase/burnt base from the versionMap. Use txIndex (NOT
	// txIndex+1) so versionMap.Read(floor(txIndex)) returns the PRIOR
	// tx's cumulative balance — the correct base before this tx's tip.
	// When the block has a BAL sidecar, the versionMap is pre-populated
	// with the BAL's per-tx cumulative values via NewVersionMap →
	// WriteChanges. Reading at txIndex+1 would return THIS tx's BAL
	// entry (already includes its own tip), and adding FeeTipped on top
	// would double-count. The current tx's worker writes (e.g. ETH
	// transfers to coinbase) are picked up below via CollectorWrites.
	vsReader := state.NewVersionedStateReader(txIndex, nil, vm, stateReader)

	// --- Coinbase: base (including execution effects) + FeeTipped ---
	coinbaseAcc, err := vsReader.ReadAccountData(result.Coinbase)
	if err != nil {
		return nil, nil, nil, err
	}
	// Start from the versionMap base (correct accumulated balance from
	// prior finalizes). If the worker's execution also sent ETH to the
	// coinbase (via CALL with value), CollectorWrites has the execution-
	// adjusted balance. We must account for both the execution delta
	// and the fee tip.
	var newCoinbaseBalance uint256.Int
	if coinbaseAcc != nil {
		newCoinbaseBalance = coinbaseAcc.Balance
	}
	// Detect the worker's coinbase Balance write so we can start the
	// finalize from the worker's post-execution value rather than the
	// pre-tx versionMap value. Scan result.TxOut (the raw worker output,
	// where every intermediate write lands) — NOT result.CollectorWrites,
	// which is the IBS's "net change" set and SUPPRESSES the coinbase
	// entry when sender == coinbase and the per-tx net balance change is
	// zero in the IBS's view (e.g. Frontier miner-self-send: gas pre-pay
	// + value-transfer + refund net to zero in the IBS journal, but the
	// worker's intermediate balance under shouldDelayFeeCalc=true still
	// debited the gas-used portion which the finalize must re-credit via
	// the tip).
	//
	// Without this, finalize uses the stale pre-tx versionMap value as
	// base and adds the tip on top — over-crediting the coinbase by one
	// tip per sender==coinbase tx. Observed at mainnet block 218957 with
	// canonical/parallel divergence of exactly one tip (1.05e15 wei).
	// Detect the worker's coinbase write. Two cases produce a coinbase
	// Balance write from the worker's execution:
	//   1. sender == coinbase — the worker's gas-pre-pay debit lands on
	//      the coinbase address. For Frontier-era miner self-sends this
	//      is the load-bearing case (#21017, block 218957) where the
	//      CollectorWrites set suppresses the entry due to per-tx net
	//      balance change == 0.
	//   2. sender != coinbase but the tx body does a balance transfer to
	//      the coinbase (CALL with value, SELFDESTRUCT beneficiary, etc.)
	//      — historically read from CollectorWrites because the IBS's
	//      net-change view captures the explicit transfer correctly.
	//
	// For #1 we MUST scan TxOut (the raw worker output), because that's
	// where the gas debit lives. For #2 we scan CollectorWrites, because
	// TxOut can contain artifact entries (e.g. SELFDESTRUCT bookkeeping
	// that touches the zero address when coinbase == zero address) that
	// would mislead us. Switch source by the sender-vs-coinbase test.
	workerWroteCoinbase := false
	senderIsCoinbase := false
	if msg, err := txTask.TxMessage(); err == nil && msg != nil {
		senderIsCoinbase = (msg.From() == result.Coinbase)
	}
	if senderIsCoinbase {
		for _, w := range result.TxOut {
			if w.Address == result.Coinbase && w.Path == state.BalancePath {
				workerWroteCoinbase = true
				if execBal, ok := w.Val.(uint256.Int); ok {
					newCoinbaseBalance = execBal
				}
				break
			}
		}
	} else if result.CollectorWrites != nil {
		for _, w := range result.CollectorWrites {
			if w.Address == result.Coinbase && w.Path == state.BalancePath {
				workerWroteCoinbase = true
				if execBal, ok := w.Val.(uint256.Int); ok {
					newCoinbaseBalance = execBal
				}
				break
			}
		}
	}
	newCoinbaseBalance.Add(&newCoinbaseBalance, &result.ExecutionResult.FeeTipped)

	// --- Burnt contract: base + FeeBurnt ---
	var newBurntBalance uint256.Int
	var burntAcc *accounts.Account
	burntAddr := result.ExecutionResult.BurntContractAddress
	hasBurnt := !burntAddr.IsNil()
	workerWroteBurnt := false
	if hasBurnt {
		burntAcc, err = vsReader.ReadAccountData(burntAddr)
		if err != nil {
			return nil, nil, nil, err
		}
		if burntAcc != nil {
			newBurntBalance = burntAcc.Balance
		}
		// Mirror the coinbase source-selection rule: scan TxOut only when
		// the burnt contract is the sender (analogous to the
		// sender==coinbase miner-self-send case); otherwise scan
		// CollectorWrites so we don't pick up artifact entries.
		senderIsBurnt := false
		if msg, err := txTask.TxMessage(); err == nil && msg != nil {
			senderIsBurnt = (msg.From() == burntAddr)
		}
		if senderIsBurnt {
			for _, w := range result.TxOut {
				if w.Address == burntAddr && w.Path == state.BalancePath {
					workerWroteBurnt = true
					if execBal, ok := w.Val.(uint256.Int); ok {
						newBurntBalance = execBal
					}
					break
				}
			}
		} else if result.CollectorWrites != nil {
			for _, w := range result.CollectorWrites {
				if w.Address == burntAddr && w.Path == state.BalancePath {
					workerWroteBurnt = true
					if execBal, ok := w.Val.(uint256.Int); ok {
						newBurntBalance = execBal
					}
					break
				}
			}
		}
		if txTask.Config.IsLondon(blockNum) {
			newBurntBalance.Add(&newBurntBalance, &result.ExecutionResult.FeeBurnt)
		}
	}

	// Update CollectorWrites with fee-adjusted balances.
	emptyRemoval := chainRules.IsSpuriousDragon
	coinbaseEmptyPre := coinbaseAcc == nil ||
		(coinbaseAcc.Balance.IsZero() && coinbaseAcc.Nonce == 0 && coinbaseAcc.IsEmptyCodeHash())
	var oldCoinbaseBalance uint256.Int
	if coinbaseAcc != nil {
		oldCoinbaseBalance = coinbaseAcc.Balance
	}
	// Match the serial executor: even when no fee is being credited
	// (tipped == 0 means newBalance == oldBalance) the coinbase must be
	// "touched" so the commitment calculator sees the EIP-161
	// empty-removal delete.
	//
	// Also emit when the worker wrote to the coinbase BalancePath: the
	// worker's incarnation-0 write went to versionMap with the pre-fee
	// value; the finalize-fee write at incarnation+1 must land regardless
	// of value equality so it masks the stale worker entry. Covers the
	// Frontier miner-self-send case where (in IBS terms) net balance
	// change is zero but versionMap carries a stale intermediate value.
	emitCoinbase := newCoinbaseBalance != oldCoinbaseBalance ||
		workerWroteCoinbase ||
		(emptyRemoval && coinbaseEmptyPre && newCoinbaseBalance.IsZero())
	if emitCoinbase {
		result.CollectorWrites = result.CollectorWrites.SetAccountBalanceOrDelete(
			result.Coinbase, coinbaseAcc, newCoinbaseBalance, tracing.BalanceIncreaseRewardTransactionFee, emptyRemoval)
	}
	if hasBurnt {
		var oldBurntBalance uint256.Int
		if burntAcc != nil {
			oldBurntBalance = burntAcc.Balance
		}
		// Same staleness-masking reasoning as coinbase above.
		if newBurntBalance != oldBurntBalance || workerWroteBurnt {
			result.CollectorWrites = result.CollectorWrites.SetAccountBalanceOrDelete(
				burntAddr, burntAcc, newBurntBalance, tracing.BalanceDecreaseGasBuy, emptyRemoval)
		}
	}

	// Build versionMap writes: TxOut (no coinbase/burnt since worker skipped
	// fee credit) plus the fee-adjusted balance writes.
	//
	// When sender == coinbase (or sender == burntAddr) the worker's TxOut
	// already contains a (sender, BalancePath) entry with the *pre-fee*
	// balance, and we're about to append a second (coinbase, BalancePath)
	// entry with the *post-fee* balance. Both entries have identical
	// (Address, Path, Key) — they only differ on Val. Downstream consumers
	// either pass this slice through ibs.ApplyVersionedWrites — which calls
	// sort.Slice (unstable) — or feed it into a MergeVersionedWrites loop
	// where the LAST iterator-visited entry wins. With two equal-key entries
	// the unstable sort orders them arbitrarily, so MergeVersionedWrites
	// flips between picking pre-fee and post-fee per run, and the validator
	// BAL coinbase balance lands pre-fee (missing the priority tip).
	//
	// Drop the worker's TxOut (sender, BalancePath) entry when sender ==
	// coinbase/burntAddr: the fee-adjusted append below is the authoritative
	// post-fee value, and the post-apply IBS reconstruction at the end of
	// this function applies BalancePath via SetBalance which is idempotent
	// w.r.t. the worker's pre-fee write anyway.
	allWrites := make(state.VersionedWrites, 0, len(result.TxOut)+2)
	for _, w := range result.TxOut {
		if w.Path == state.BalancePath && (w.Address == result.Coinbase || (hasBurnt && w.Address == burntAddr)) {
			continue
		}
		allWrites = append(allWrites, w)
	}
	// The coinbase/burnt fee credits are implicit writes: the worker ran
	// with shouldDelayFeeCalc=true and never produced them, so finalize
	// authors them here. Stamp them at incarnation+1 — a distinct version
	// from the worker's own pre-fee coinbase/burnt write. A later tx that
	// speculatively read the coinbase/burnt before this finalize ran
	// recorded the worker's version; the bumped version makes the MapRead
	// checkVersion comparison fail, so that tx is invalidated and
	// re-executed against the post-fee balance. Without the bump finalize
	// reuses the worker's version and the version-only validator cannot
	// tell the stale read apart.
	feeVersion := task.Version()
	feeVersion.Incarnation++
	// Mirror the CollectorWrites emission above: emit the coinbase
	// versionMap write either when balance actually changed OR when
	// EIP-161 empty-removal will turn the empty coinbase into a delete.
	if emitCoinbase {
		if emptyRemoval && coinbaseEmptyPre && newCoinbaseBalance.IsZero() {
			allWrites = append(allWrites, &state.VersionedWrite{
				Address: result.Coinbase,
				Path:    state.SelfDestructPath,
				Val:     true,
				Version: task.Version(),
			})
		} else {
			allWrites = append(allWrites, &state.VersionedWrite{
				Address:             result.Coinbase,
				Path:                state.BalancePath,
				Val:                 newCoinbaseBalance,
				Version:             feeVersion,
				BalanceChangeReason: tracing.BalanceIncreaseRewardTransactionFee,
			})
		}
	}
	if hasBurnt {
		var oldBurntBalance uint256.Int
		if burntAcc != nil {
			oldBurntBalance = burntAcc.Balance
		}
		// Same staleness-masking reasoning as coinbase above.
		if newBurntBalance != oldBurntBalance || workerWroteBurnt {
			allWrites = append(allWrites, &state.VersionedWrite{
				Address:             burntAddr,
				Path:                state.BalancePath,
				Val:                 newBurntBalance,
				Version:             feeVersion,
				BalanceChangeReason: tracing.BalanceDecreaseGasBuy,
			})
		}
	}

	// Engine post-apply message (e.g., AuRa system calls, EIP-7708 burn logs).
	if err := result.runPostApplyMessageOnMinIBS(task, txTask, engine, vm, stateReader, hasBurnt, burntAddr); err != nil {
		return nil, nil, nil, err
	}

	// Compute receipt.
	receipt, err := result.CreateNextReceipt(prevReceipt)
	if err != nil {
		return nil, nil, nil, err
	}

	// The finalize reads (coinbase/burnt base) come from vsReader which
	// doesn't track reads. These reads are implicit — the versionMap
	// entries from the finalize's writes will cause invalidation of
	// subsequent TXs that read stale coinbase/burnt values.

	return receipt, nil, allWrites, nil
}

// runPostApplyMessageOnMinIBS runs the engine's PostApplyMessage callback
// (e.g. AuRa system calls, EIP-7708 burn-log emission via
// LogSelfDestructedAccounts) and appends any resulting logs to result.Logs.
//
// This is the load-bearing IntraBlockState use in finalizeTxSimple's
// post-execution path. It exists to:
//
//  1. Read SD'd accounts and their residual balances
//     (ibs.GetRemovedAccountsWithBalance) so LogSelfDestructedAccounts can
//     emit EIP-7708 burn logs.
//  2. Provide a log buffer (ibs.AddLog → ibs.GetLogs) so logs emitted by
//     postApplyMessageFunc reach the receipt.
//  3. Run AddBalance bookkeeping for the priority-fee credit so the SD'd
//     coinbase carries FeeTipped at the time LogSelfDestructedAccounts
//     inspects it.
//
// All three dependencies are slated for removal under #21138 — once the
// SD-with-balance signal is explicit on ExecutionResult and
// LogSelfDestructedAccounts returns logs as a value, this method becomes
// IBS-free and the minimal IBS construction below disappears.
func (result *execResult) runPostApplyMessageOnMinIBS(
	task *taskVersion,
	txTask *exec.TxTask,
	engine rules.Engine,
	vm *state.VersionMap,
	stateReader state.StateReader,
	hasBurnt bool,
	burntAddr accounts.Address,
) error {
	if engine == nil {
		return nil
	}
	postApplyMessageFunc := engine.GetPostApplyMessageFunc()
	if postApplyMessageFunc == nil {
		return nil
	}

	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex
	chainRules := txTask.EvmBlockContext.Rules(txTask.Config)

	execResult := result.ExecutionResult
	cbReader := state.NewVersionedStateReader(txIndex, nil, vm, stateReader)
	coinbase, err := cbReader.ReadAccountData(result.Coinbase)
	if err != nil {
		return err
	}
	if coinbase != nil {
		execResult.CoinbaseInitBalance = coinbase.Balance
	}
	message, err := task.TxMessage()
	if err != nil {
		return err
	}

	// PostApplyMessage needs an IBS — create a minimal one.
	ibs := state.New(state.NewVersionedStateReader(txIndex, result.TxIn, vm, stateReader))
	ibs.SetTxContext(blockNum, txIndex)
	if err := ibs.ApplyVersionedWrites(result.TxOut); err != nil {
		return err
	}

	// Mirror serial-exec's txn_executor.go post-message fee distribution
	// (AddBalance(coinbase, tip) / AddBalance(burnt, burn)) AFTER applying
	// TxOut. The parallel worker ran with shouldDelayFeeCalc=true, so the
	// fees aren't in TxOut; the finalize accumulates them onto the
	// version-map base separately. But the post-apply IBS handed to
	// postApplyMessageFunc only sees TxOut — without crediting the fees
	// here it underrepresents the coinbase's balance to LogSelfDestructedAccounts.
	//
	// The EIP-7708 case 2 path is the load-bearing one: when the coinbase
	// is itself a contract that SELFDESTRUCTs during the tx, ApplyVersionedWrites
	// has marked it selfdestructed with balance=0; AddBalance leaves the
	// selfdestruct flag intact (Selfdestruct only fires on the addr→clear
	// transition, not on subsequent balance writes) and restores the priority
	// fee as residual balance. LogSelfDestructedAccounts' GetRemovedAccountsWithBalance
	// then reports {coinbase, FeeTipped} and emits the Burn log — matching
	// serial-exec exactly. https://github.com/erigontech/erigon/issues/21136
	if err := ibs.AddBalance(result.Coinbase, result.ExecutionResult.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee); err != nil {
		return err
	}
	if hasBurnt && txTask.Config.IsLondon(blockNum) {
		if err := ibs.AddBalance(burntAddr, result.ExecutionResult.FeeBurnt, tracing.BalanceDecreaseGasBuy); err != nil {
			return err
		}
	}
	postApplyMessageFunc(ibs, message.From(), result.Coinbase, &execResult, chainRules)

	// Capture PostApplyMessage side effects (logs) — e.g. EIP-7708 Burn
	// logs from LogSelfDestructedAccounts. Without this they're stranded
	// on the post-apply ibs and never make it into the receipt, so the
	// validating consumer recomputes a different receipts root and the
	// block is rejected as a BadBlock.
	result.Logs = append(result.Logs, ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), blockNum, txTask.BlockHash())...)
	return nil
}

type taskVersion struct {
	*execTask
	version    state.Version
	versionMap *state.VersionMap
	profile    bool
	stats      map[int]ExecutionStat
	statsMutex *sync.Mutex
}

func (ev *taskVersion) Trace() bool {
	return ev.Task.(*exec.TxTask).Trace
}

func (ev *taskVersion) Execute(evm *vm.EVM,
	engine rules.Engine,
	genesis *types.Genesis,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	chainConfig *chain.Config,
	chainReader rules.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) (result *exec.TxResult) {

	var start time.Time
	if ev.profile {
		start = time.Now()
	}

	// Don't run post apply message during the state transition it is handled in finalize
	postApplyMessage := evm.Context.PostApplyMessage
	evm.Context.PostApplyMessage = nil
	defer func() { evm.Context.PostApplyMessage = postApplyMessage }()

	result = ev.execTask.Execute(evm, engine, genesis, ibs, stateWriter,
		chainConfig, chainReader, dirs, !ev.shouldDelayFeeCalc)

	if ibs.HadInvalidRead() || result.Err != nil {
		if err, ok := result.Err.(protocol.ErrExecAbortError); !ok {
			result.Err = protocol.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex(), OriginError: err}
		}
	}

	if result.Err != nil {
		return result
	}

	if ev.profile {
		ev.statsMutex.Lock()
		ev.stats[ev.version.TxIndex] = ExecutionStat{
			TxIdx:       ev.version.TxIndex,
			Incarnation: ev.version.Incarnation,
			Duration:    time.Since(start),
		}
		ev.statsMutex.Unlock()
	}

	return result
}

func (ev *taskVersion) Reset(evm *vm.EVM, ibs *state.IntraBlockState, callTracer *calltracer.CallTracer) error {
	if err := ev.execTask.Reset(evm, ibs, callTracer); err != nil {
		return err
	}
	ibs.SetVersionMap(ev.versionMap)
	ibs.SetVersion(ev.version.Incarnation)
	return nil
}

func (ev *taskVersion) Version() state.Version {
	return ev.version
}

type blockExecMetrics struct {
	BlockCount atomic.Int64
	Duration   blockDuration
}

func newBlockExecMetrics() *blockExecMetrics {
	return &blockExecMetrics{
		Duration: blockDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
	}
}

type blockDuration struct {
	atomic.Int64
	Ema *metrics.EMA[time.Duration]
}

func (d *blockDuration) Add(i time.Duration) {
	d.Int64.Add(int64(i))
	d.Ema.Update(i)
}

type execRequest struct {
	blockNum      uint64
	blockHash     common.Hash
	gasPool       *protocol.GasPool
	accessList    types.BlockAccessList
	tasks         []exec.Task
	applyResults  chan applyResult
	commitResults chan applyResult
	profile       bool
	exhausted     *ErrLoopExhausted
}

type blockExecutor struct {
	sync.Mutex
	blockNum  uint64
	blockHash common.Hash

	tasks   []*execTask
	results []*execResult

	// settledInput[tx]==true marks a task that was dispatched when every
	// preceding task had already validated — so it executed against fully
	// settled MVCC state, with no lower-indexed worker still in flight.
	//
	// It is set at dispatch time (scheduleExecution), which is the only point
	// the "ran on settled input" property can be asserted: a result-time check
	// would miss that the task may have executed speculatively, earlier, on
	// state a since-validated predecessor has since changed.
	//
	// Used solely to classify a genuine (IsError) execution abort: an error
	// raised against settled input is real invalid-block data, not a
	// speculative-execution artifact, so the block can be rejected on the
	// first such error instead of re-executing to the incarnation limit.
	// It is NEVER consulted by the validator verdict — a result is committed
	// only if validation explicitly passes it (issue #21319).
	settledInput map[int]bool

	// Execution tasks stores the state of each execution task
	execTasks execStatusList

	// Validate tasks stores the state of each validation task
	validateTasks execStatusList

	// Publish tasks stores the state tasks ready for publication
	publishTasks execStatusList

	// Multi-version map
	versionMap *state.VersionMap

	// Stores the inputs and outputs of the last incarnation of all transactions
	blockIO *state.VersionedIO

	// Tracks the incarnation number of each transaction
	txIncarnations []int

	// A map that stores the estimated dependency of a transaction if it is aborted without any known dependency
	estimateDeps map[int][]int

	// A map that records whether a transaction result has been speculatively validated
	preValidated map[int]bool

	// Time records when the parallel execution starts
	begin time.Time

	// Enable profiling
	profile bool

	// Stats for debugging purposes
	cntExec, cntSpecExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail, cntFinalized int

	// finalizedResults stores the finalized execResult snapshot per TX.
	// Prevents the publish loop from seeing a different incarnation's
	// result if be.results[tx] is overwritten between finalize and publish.
	finalizedResults map[int]*execResult

	// cumulative gas for this block.
	// blockRegularGasUsed and blockStateGasUsed are tracked separately so the
	// final blockGasUsed = max(regular, state) matches EIP-8037 / EIP-7778
	// block-level accounting and equals what the builder set in header.GasUsed
	// via protocol.SetGasUsed (= max(cumRegular, cumState)).
	blockRegularGasUsed uint64
	blockStateGasUsed   uint64
	blockGasUsed        uint64
	blobGasUsed         uint64
	gasPool             *protocol.GasPool

	execFailed, execAborted []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	applyResults  chan applyResult
	commitResults chan applyResult // fan-out: same stream goes to commitment calculator

	execStarted time.Time
	result      *blockResult
	applyCount  int
	exhausted   *ErrLoopExhausted

	// blockStateCache provides a stable pre-block snapshot of account data
	// for GetCommittedState reads, unaffected by intra-block ApplyStateWrites.
	blockStateCache *state.BlockStateCache
}

// sendResult fans out an applyResult to both the apply loop and
// the commitment calculator. Blocks if either channel is full.
// Channels may be closed by executeBlocks — recover from panic.
func (be *blockExecutor) sendResult(ctx context.Context, r applyResult) (err error) {
	defer func() {
		// "send on closed channel" panics here are benign — executeBlocks
		// finished and closed applyResults/commitResults during batch
		// shutdown. Re-raise anything else so real bugs still surface.
		if rec := recover(); rec != nil {
			if e, ok := rec.(runtime.Error); ok && strings.Contains(e.Error(), "send on closed channel") {
				err = context.Canceled
				return
			}
			panic(rec)
		}
	}()
	select {
	case be.applyResults <- r:
	case <-ctx.Done():
		return ctx.Err()
	}
	if be.commitResults != nil {
		select {
		case be.commitResults <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newBlockExec(blockNum uint64, blockHash common.Hash, gasPool *protocol.GasPool, accessList types.BlockAccessList, applyResults chan applyResult, commitResults chan applyResult, profile bool, exhausted *ErrLoopExhausted) *blockExecutor {
	return &blockExecutor{
		blockNum:         blockNum,
		blockHash:        blockHash,
		begin:            time.Now(),
		stats:            map[int]ExecutionStat{},
		finalizedResults: map[int]*execResult{},
		settledInput:     map[int]bool{},
		estimateDeps:     map[int][]int{},
		preValidated:     map[int]bool{},
		blockIO:          &state.VersionedIO{},
		versionMap:       state.NewVersionMap(accessList),
		profile:          profile,
		applyResults:     applyResults,
		commitResults:    commitResults,
		gasPool:          gasPool,
		blockStateCache:  state.NewBlockStateCache(),
		exhausted:        exhausted,
	}
}

// invalidBlockResult wraps a block-validity failure (insufficient funds, gas
// overflow, finalize rejection, etc.) as a *blockResult carrying Err. Returning
// this from the worker-result processing path lets the apply loop see that the
// block completed (with a rejection) rather than treating the dangling
// tx-results as a silent miss. The apply loop's case *blockResult fast-paths
// Err != nil at the top: marks the block applied so the channel-close
// completeness check doesn't double-report, and surfaces the error.
func (be *blockExecutor) invalidBlockResult(err error) *blockResult {
	return &blockResult{
		BlockNum:  be.blockNum,
		BlockHash: be.blockHash,
		Err:       err,
	}
}

// tooManyRetries returns an invalid-block result when tx has exceeded its
// retry budget, otherwise nil. origin may be nil (validator-invalid path)
// or carry the worker's underlying error.
func (be *blockExecutor) tooManyRetries(tx, txIndex int, label string, origin error) *blockResult {
	if be.txIncarnations[tx] <= len(be.tasks) {
		return nil
	}
	if origin != nil {
		return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%v]: %w: too many %s retries: %d, expected: %d",
			rules.ErrInvalidBlock, be.blockNum, txIndex, be.tasks[tx].TxHash(), origin, label, be.txIncarnations[tx], len(be.tasks)))
	}
	return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%v]: too many %s retries: %d, expected: %d",
		rules.ErrInvalidBlock, be.blockNum, txIndex, be.tasks[tx].TxHash(), label, be.txIncarnations[tx], len(be.tasks)))
}

func (be *blockExecutor) nextResult(ctx context.Context, pe *parallelExecutor, res *exec.TxResult, applyTx kv.TemporalTx) (result *blockResult, err error) {
	task, ok := res.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", res.Task)
	}

	tx := task.index
	be.results[tx] = &execResult{res, nil}
	if res.Err != nil {
		if execErr, ok := res.Err.(protocol.ErrExecAbortError); ok {
			if res.Version().Incarnation > len(be.tasks) {
				// Parallel scheduler exhausted retries for this tx. Surface
				// through blockResult.Err for the same reason as the other
				// block-validity bailouts above: (nil, err) would race with
				// the apply loop's channel-close completeness check and
				// produce a doubled error chain. The underlying scheduler
				// behaviour (why this test ordering hits the incarnation
				// limit) is a separate investigation — see
				// TestDeleteRecreateSlotsAcrossManyBlocks under
				// GOMAXPROCS=2 -race for a stress repro.
				if execErr.IsError() {
					return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%v]: %w: too many incarnations: %d, expected: %d", rules.ErrInvalidBlock, be.blockNum, res.Version().TxIndex, task.TxHash(), execErr.OriginError, res.Version().Incarnation, len(be.tasks))), nil
				}
				return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%v]: too many incarnations: %d, expected: %d", rules.ErrInvalidBlock, be.blockNum, res.Version().TxIndex, task.TxHash(), res.Version().Incarnation, len(be.tasks))), nil
			}
			if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
				fmt.Println(be.blockNum, "err", execErr)
			}
			be.blockIO.RecordReads(res.Version(), res.TxIn)
			be.blockIO.RecordAccesses(res.Version(), res.AccessedAddresses)

			if execErr.IsError() {
				// Genuine, non-dependency execution error (issue #21319).
				// Classify it only when this task executed against fully
				// settled input — settledInput[tx] means every predecessor had
				// validated when it was dispatched. Validate the held result
				// post-hoc against the current version map: that catches a
				// predecessor re-validated since dispatch, and a clean verdict
				// then confirms the error is genuinely caused by invalid block
				// data — reject the block. Surface it through a blockResult so
				// the apply loop counts the block as completed-as-invalid;
				// returning (nil, err) would race with the channel-close
				// completeness check and double the error chain.
				//
				// The settledInput gate also makes this cascade-safe: a task
				// after a genuine error can never be settledInput, since its
				// erroring predecessor never validates — so out of a cascade of
				// speculative errors only the genuinely-first one is returned.
				if be.settledInput[tx] {
					txVersion := res.Version()
					validity := be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
						func(readVersion, writtenVersion state.Version) state.VersionValidity {
							if readVersion != writtenVersion {
								return state.VersionInvalid
							}
							return state.VersionValid
						}, false, "")
					if validity == state.VersionValid {
						return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%d:%v]: %w", rules.ErrInvalidBlock, be.blockNum, txVersion.TxIndex, txVersion.TxNum, task.TxHash(), execErr.OriginError)), nil
					}
				}
				// Not settled input, or a predecessor changed since dispatch —
				// the error may be speculative. Defer for re-execution; the
				// drain predicate re-dispatches it once every predecessor has
				// validated, so the re-run is itself settledInput and a still-
				// failing error is then returned by the branch above. (Some
				// re-execution is wasted here — bounded and marginal.)
				be.execTasks.clearInProgress(tx)
				be.execTasks.pushDeferred(tx)
				be.execAborted[tx]++
				be.txIncarnations[tx]++
				be.cntAbort++
			} else {
				// Dependency abort: re-execute against the named blocker.
				dependency := execErr.DependencyTxIndex + 1

				l := len(be.estimateDeps[tx])
				for l > 0 && be.estimateDeps[tx][l-1] > dependency {
					be.execTasks.removeDependency(be.estimateDeps[tx][l-1])
					be.estimateDeps[tx] = be.estimateDeps[tx][:l-1]
					l--
				}

				addedDependencies := be.execTasks.addDependency(dependency, tx)
				be.execAborted[tx]++

				if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
					fmt.Println(be.blockNum, "ABORT", tx, be.txIncarnations[tx], be.execFailed[tx], be.execAborted[tx], "dep", dependency, "err", execErr.OriginError)
				}

				be.execTasks.clearInProgress(tx)

				if !addedDependencies {
					// addDependency couldn't register a real wait (named blocker
					// already complete). Defer — scheduleExecution's predicate
					// gates retry on predecessor-validated + no-lower-IP.
					be.execTasks.pushDeferred(tx)
				}
				be.txIncarnations[tx]++
				be.cntAbort++
			}
		} else {
			// Non-ErrExecAbortError from the worker (e.g. raw error from
			// TxTask.Reset: TxMessage rejection, signer rejection, EIP-7702
			// empty authorization list). Surface it as a block-validity
			// failure through blockResult.Err so the apply loop returns
			// ErrInvalidBlock the same way the other invalidBlockResult
			// sites do. Returning (nil, err) instead silently exits the
			// exec loop with no blockResult ever reaching the apply loop;
			// the apply-channel-closed branch then sees blks=0 and
			// fabricates ErrLoopExhausted, which the stage loop reports as
			// "unexpected state step has more work" — engine API
			// mis-categorises that as a state-machine error rather than the
			// real block-validation failure (eest fork_Prague
			// test_empty_authorization_list).
			return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%v]: %w", rules.ErrInvalidBlock, be.blockNum, res.Version().TxIndex, task.TxHash(), res.Err)), nil
		}
	} else {
		txVersion := res.Version()

		be.blockIO.RecordReads(txVersion, res.TxIn)
		be.blockIO.RecordAccesses(txVersion, res.AccessedAddresses)

		if res.Version().Incarnation == 0 {
			be.blockIO.RecordWrites(txVersion, res.TxOut)
		} else {
			prevWrites := be.blockIO.WriteSet(txVersion.TxIndex)
			hasWriteChange := res.TxOut.HasNewWrite(prevWrites)

			// Remove entries that were previously written but are no longer written
			cmpMap := map[accounts.Address]map[state.AccountKey]struct{}{}

			for _, w := range res.TxOut {
				keys, ok := cmpMap[w.Address]
				if !ok {
					keys = map[state.AccountKey]struct{}{}
					cmpMap[w.Address] = keys
				}
				keys[state.AccountKey{Path: w.Path, Key: w.Key}] = struct{}{}
			}

			for _, v := range prevWrites {
				if _, ok := cmpMap[v.Address][state.AccountKey{Path: v.Path, Key: v.Key}]; !ok {
					hasWriteChange = true
					be.versionMap.Delete(v.Address, v.Path, v.Key, txVersion.TxIndex, true)
				}
			}

			be.blockIO.RecordWrites(txVersion, res.TxOut)

			if hasWriteChange {
				be.validateTasks.pushPendingSet(be.execTasks.getRevalidationRange(tx + 1))
			}
		}

		tracePrefix := fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)

		var trace bool
		if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
			fmt.Println(tracePrefix, "RD", be.blockIO.ReadSet(txVersion.TxIndex).Len(), "WRT", len(be.blockIO.WriteSet(txVersion.TxIndex)))
			be.blockIO.ReadSet(txVersion.TxIndex).Scan(func(vr *state.VersionedRead) bool {
				fmt.Println(tracePrefix, "RD", vr.String())
				return true
			})
			for _, vw := range be.blockIO.WriteSet(txVersion.TxIndex) {
				fmt.Println(tracePrefix, "WRT", vw.String())
			}
		}

		be.validateTasks.pushPending(tx)
		be.execTasks.markComplete(tx)
		be.cntSuccess++

		be.execTasks.removeDependency(tx)
	}

	// do validations ...
	maxComplete := be.execTasks.maxComplete()
	toValidate := make(sort.IntSlice, 0, 2)

	for be.validateTasks.minPending() <= maxComplete && be.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, be.validateTasks.takeNextPending())
	}

	cntInvalid := 0
	var stateReader state.StateReader

	for i := 0; i < len(toValidate); i++ {

		be.cntTotalValidations++

		tx := toValidate[i]
		txVersion := be.tasks[tx].Task.Version()

		var trace bool
		var tracePrefix string

		if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)
		}

		validity := be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
			func(readVersion, writtenVersion state.Version) state.VersionValidity {
				vv := state.VersionValid

				if readVersion != writtenVersion {
					vv = state.VersionInvalid
				} else if writtenVersion.TxIndex == state.UnknownDep && tx-1 > be.validateTasks.maxComplete() {
					vv = state.VersionTooEarly
				}

				return vv
			}, trace, tracePrefix)
		be.versionMap.SetTrace(false)

		if validity == state.VersionTooEarly {
			cntInvalid++
			continue
		}

		// The validator verdict is the single source of truth (issue #21319):
		// a result is committed only if validation explicitly passed it.
		valid := validity == state.VersionValid

		be.versionMap.SetTrace(trace)
		writeSet := be.blockIO.WriteSet(txVersion.TxIndex)
		be.versionMap.FlushVersionedWrites(writeSet, cntInvalid == 0, tracePrefix)
		be.versionMap.SetTrace(false)

		if valid {
			if cntInvalid == 0 {
				be.validateTasks.markComplete(tx)

				txResult := be.results[tx]
				be.finalizedResults[tx] = txResult

				var prevReceipt *types.Receipt
				if txVersion.TxIndex > 0 && tx > 0 {
					if prev := be.finalizedResults[tx-1]; prev != nil {
						prevReceipt = prev.Receipt
					}
				}

				txTask := be.tasks[tx].Task

				if txn := txTask.Tx(); txn != nil {
					regularContribution, stateContribution := protocol.InclusionContributions(txn.GetGasLimit(), txResult.ExecutionResult.IntrinsicGas, txTask.Rules().IsAmsterdam)
					if err := protocol.CheckBlockGasInclusion(be.gasPool, regularContribution, stateContribution); err != nil {
						return be.invalidBlockResult(fmt.Errorf("%w: block gas used overflow at block=%d txIdx=%d: %w", rules.ErrInvalidBlock, be.blockNum, txVersion.TxIndex, err)), nil
					}
				}

				if err := be.gasPool.ConsumeRegular(txResult.ExecutionResult.BlockRegularGasUsed); err != nil {
					return be.invalidBlockResult(fmt.Errorf("%w, block=%d: block regular gas overflow", rules.ErrInvalidBlock, be.blockNum)), nil
				}
				if err := be.gasPool.ConsumeState(txResult.ExecutionResult.BlockStateGasUsed); err != nil {
					return be.invalidBlockResult(fmt.Errorf("%w, block=%d: block state gas overflow", rules.ErrInvalidBlock, be.blockNum)), nil
				}

				if txTask.Tx() != nil {
					blobGasUsed := txTask.Tx().GetBlobGas()
					if err := be.gasPool.SubBlobGas(blobGasUsed); err != nil {
						return be.invalidBlockResult(fmt.Errorf("%w, block=%d blob gas used overflow: %w", rules.ErrInvalidBlock, be.blockNum, err)), nil
					}
					be.blobGasUsed += blobGasUsed
				}

				if stateReader == nil {
					if txTask.IsHistoric() {
						// Chain blockCache → sd.mem → applyTx so historic-mode
						// finalize reads see every prior-tx write from the
						// current block. The per-tx finalize path (fee calc,
						// post-apply hooks) uses this reader for base reads
						// not satisfied by the versionMap; omitting blockCache
						// would let a later tx read a pre-block balance that
						// an earlier tx already updated in-batch.
						stateReader = state.NewHistoryReaderV3WithBlockCache(applyTx, pe.rs.Domains(), be.blockStateCache, txTask.Version().TxNum)
					} else {
						// Use CachedReaderV3 with readCurrent=true so the
						// finalize (including system TXs) reads from the
						// BlockStateCache write buffer. This ensures the
						// system TX sees all accumulated state from prior
						// TXs in the block, not stale sd.mem values.
						stateReader = state.NewCurrentCachedReaderV3(pe.rs.Domains().AsGetter(applyTx), be.blockStateCache)
					}
				}

				collector := state.NewVersionedWriteCollector(pe.rs)

				balActive := pe.cfg.experimentalBAL || pe.cfg.chainConfig.IsAmsterdam(txTask.BlockTime())
				_, addReads, addWrites, err := txResult.finalize(prevReceipt, pe.cfg.engine, be.versionMap, stateReader, collector, balActive)

				if err != nil {
					return nil, err
				}

				// Merge any additional reads/writes produced during finalize (fee calc, post apply, etc)
				if addReads != nil {
					mergedReads := MergeReadSets(be.blockIO.ReadSet(txVersion.TxIndex), addReads)
					be.blockIO.RecordReads(txVersion, mergedReads)
				}
				if len(addWrites) > 0 {
					// Merge finalization writes with existing execution writes.
					existingWrites := be.blockIO.WriteSet(txVersion.TxIndex)
					merged := MergeVersionedWrites(existingWrites, addWrites)
					be.blockIO.RecordWrites(txVersion, merged)

					// Flush the merged writes (including fee calc changes)
					// to the version map so that subsequent per-tx
					// finalizations see the full post-tx state (execution
					// + fees) when reading via the version map fallback
					// chain.
					be.versionMap.FlushVersionedWrites(merged, true, "")

					// Update CollectorWrites with fee-adjusted balances (coinbase /
					// burnt) so the BlockStateCache sees the correct accumulated
					// fees. CollectorWrites is a flat slice, so replacing a
					// BalancePath entry by address is a linear scan; doing that per
					// addWrites entry is O(len(addWrites)·len(CollectorWrites)) — when
					// finalize is a full block-end IBS reconstruction both can be ~one
					// entry per account the block touched (a tx that pays ~100k
					// accounts — TestInvalidReceiptHashHighMgas), i.e. ~10^10
					// comparisons. Index CollectorWrites' BalancePath entries by
					// address once instead.
					if len(txResult.CollectorWrites) > 0 {
						balIdx := make(map[accounts.Address]int, len(txResult.CollectorWrites))
						for i, w := range txResult.CollectorWrites {
							if w.Path == state.BalancePath {
								// First match wins — mirrors the old per-entry
								// CollectorWrites.SetBalance(addr) linear scan.
								if _, seen := balIdx[w.Address]; !seen {
									balIdx[w.Address] = i
								}
							}
						}
						for _, w := range addWrites {
							if w.Path != state.BalancePath {
								continue
							}
							bal, ok := w.Val.(uint256.Int)
							if !ok {
								continue
							}
							if i, found := balIdx[w.Address]; found {
								txResult.CollectorWrites[i].Val = bal
								txResult.CollectorWrites[i].BalanceChangeReason = w.BalanceChangeReason
							} else {
								txResult.CollectorWrites = append(txResult.CollectorWrites, &state.VersionedWrite{Address: w.Address, Path: state.BalancePath, Val: bal, BalanceChangeReason: w.BalanceChangeReason})
								balIdx[w.Address] = len(txResult.CollectorWrites) - 1
							}
						}
					}
				}

				{
					// Build clean write set from versionMap WriteSet — not CollectorWrites.
					// The WriteSet has the raw versionWritten output from the validated
					// incarnation. normalizeWriteSet filters no-ops, stale incarnations,
					// and resolves account values from the versionMap.
					resultIncarnation := txResult.Version().Incarnation
					rawWrites := be.blockIO.WriteSet(txVersion.TxIndex)
					// domainStorageKeys: enumerate every storage slot currently
					// committed for addr (sd.mem + domain files), so a self-destruct
					// emits the full StoragePath=0 cascade — covers genesis-allocated
					// and prior-block storage that vm.StorageKeys doesn't see.
					domainStorageKeys := func(addr accounts.Address) []accounts.StorageKey {
						av := addr.Value()
						const addrLen, hashLen = 20, 32 // StorageDomain composite key = addr ++ slotHash
						var keys []accounts.StorageKey
						if iterErr := pe.rs.Domains().IteratePrefix(kv.StorageDomain, av[:], applyTx, func(k, _ []byte) (bool, error) {
							if len(k) >= addrLen+hashLen {
								keys = append(keys, accounts.InternKey(common.BytesToHash(k[addrLen:addrLen+hashLen])))
							}
							return true, nil
						}); iterErr != nil {
							// Non-fatal: fall back to vm.StorageKeys-only. A missed slot
							// surfaces as a wrong-trie-root, not a silent corruption.
							pe.logger.Warn("[parallel] domainStorageKeys iterate failed", "addr", av, "err", iterErr)
						}
						return keys
					}
					txResult.writes = normalizeWriteSet(rawWrites, be.versionMap, txVersion.TxIndex, resultIncarnation, stateReader, domainStorageKeys, pe.cfg.chainConfig.IsSpuriousDragon(be.blockNum))
				}

				// Snapshot the finalized result before pushing — prevents
				// the publish loop from seeing a later incarnation if
				// be.results[tx] is overwritten by a concurrent worker.
				be.finalizedResults[tx] = txResult
				be.publishTasks.pushPending(tx)
			}
		} else {
			cntInvalid++
			be.cntValidationFail++
			be.execFailed[tx]++

			if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
				fmt.Println(be.blockNum, "FAILED", tx, be.txIncarnations[tx], "failed", be.execFailed[tx], "aborted", be.execAborted[tx])
			}

			// 'create validation tasks for all transactions > tx ...'
			be.validateTasks.pushPendingSet(be.execTasks.getRevalidationRange(tx + 1))
			be.validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes
			be.execTasks.clearComplete(tx)
			// Defer: validator-invalid may be race-induced (worker raced an
			// exec-loop flush). Drain predicate in scheduleExecution waits.
			be.execTasks.pushDeferred(tx)
			be.preValidated[tx] = false
			be.txIncarnations[tx]++
			if r := be.tooManyRetries(tx, txVersion.TxIndex, "validator-invalid", nil); r != nil {
				return r, nil
			}
		}
	}

	maxValidated := be.validateTasks.maxComplete()
	be.scheduleExecution(ctx, pe)

	if be.publishTasks.minPending() != -1 {
		toPublish := make(sort.IntSlice, 0, 2)

		for be.publishTasks.minPending() <= maxValidated && be.publishTasks.minPending() >= 0 {
			toPublish = append(toPublish, be.publishTasks.takeNextPending())
		}

		for i := 0; i < len(toPublish); i++ {
			tx := toPublish[i]
			task := be.tasks[tx].Task
			// Use the finalized snapshot — be.results[tx] may have been
			// overwritten by a later incarnation from a concurrent worker.
			result := be.finalizedResults[tx]

			applyResult := txResult{
				blockNum:              be.blockNum,
				traceFroms:            map[accounts.Address]struct{}{},
				traceTos:              map[accounts.Address]struct{}{},
				txNum:                 task.Version().TxNum,
				rules:                 task.Rules(),
				cumulativeBlobGasUsed: be.blobGasUsed,
			}

			if tx := result.Tx(); tx != nil {
				applyResult.blobGasUsed = tx.GetBlobGas()
			}

			if result.Receipt != nil {
				// EIP-8037 / EIP-7778: block-level gas is max(cum regular,
				// cum state) — NOT sum of per-tx receipt gas. Receipt gas
				// accounts for refunds and (post-Amsterdam) carries the
				// FloorGasCost floor; summing it bears no fixed relationship
				// to header.GasUsed, which the builder sets via
				// protocol.SetGasUsed = max(cumBlockRegular, cumBlockState).
				be.blockRegularGasUsed += result.ExecutionResult.BlockRegularGasUsed
				be.blockStateGasUsed += result.ExecutionResult.BlockStateGasUsed
				be.blockGasUsed = max(be.blockRegularGasUsed, be.blockStateGasUsed)
				// applyResult.blockGasUsed is the per-tx contribution used for
				// progress / uncommittedGas tracking; receipt gas is fine here.
				applyResult.blockGasUsed = int64(result.Receipt.GasUsed)

				receipt := *result.Receipt
				applyResult.receipt = &receipt
				applyResult.receipt.Logs = append([]*types.Log{}, result.Receipt.Logs...)
				applyResult.logs = applyResult.receipt.Logs
				pe.executedGas.Add(int64(applyResult.blockGasUsed))
			}

			maps.Copy(applyResult.traceFroms, result.TraceFroms)
			maps.Copy(applyResult.traceTos, result.TraceTos)
			be.cntFinalized++
			be.publishTasks.markComplete(tx)

			pe.lastExecutedTxNum.Store(int64(applyResult.txNum))
			if result.writes != nil {
				applyResult.writes = result.writes
				if len(applyResult.writes) > 0 {
					be.applyCount += len(applyResult.writes)
				}
			}

			// Apply state writes to sd.mem and block cache.
			if err := pe.rs.ApplyStateWrites(ctx, applyTx, applyResult.blockNum, applyResult.txNum, applyResult.writes,
				nil, applyResult.rules, be.blockStateCache); err != nil {
				return nil, err
			}

			// Apply per-tx indexes (logs, traces, receipt cache) here in the
			// exec loop, on the SAME goroutine that owns sd.mem mutations.
			// Doing this in the apply loop instead used to race with the next
			// tx / block-end ApplyStateWrites on SharedDomains.mem.
			if err := pe.rs.ApplyTxIndexes(applyTx, applyResult.txNum, applyResult.receipt, applyResult.blobGasUsed,
				applyResult.logs, applyResult.traceFroms, applyResult.traceTos); err != nil {
				return nil, fmt.Errorf("ApplyTxIndexes block=%d txNum=%d: %w", applyResult.blockNum, applyResult.txNum, err)
			}

			if err := be.sendResult(ctx, &applyResult); err != nil {
				return nil, err
			}
		}
	}

	if be.publishTasks.countComplete() == len(be.tasks) && be.execTasks.countComplete() == len(be.tasks) {
		var allDeps map[int]map[int]bool

		var deps state.DAG

		if be.profile {
			allDeps = state.GetDep(be.blockIO)
			deps = state.BuildDAG(be.blockIO, pe.logger)
		}

		isPartial := len(be.tasks) > 0 && be.tasks[0].Version().TxIndex != -1

		txTask := be.tasks[len(be.tasks)-1].Task

		var receipts types.Receipts
		for _, txResult := range be.results {
			if receipt := txResult.Receipt; receipt != nil {
				receipts = append(receipts, receipt)
			}
		}

		var header *types.Header
		var txs types.Transactions
		if tt, ok := txTask.(*exec.TxTask); ok {
			header = tt.Header
			txs = tt.Txs
		}

		// Block finalize: run engine.Finalize + MakeWriteSet on the producer
		// side so finalize writes go to the BlockStateCache before the Flush.
		var finalizeWrites state.VersionedWrites
		if be.blockNum > 0 {
			lastResult := be.results[len(be.results)-1]
			finalTask := be.tasks[len(be.tasks)-1].Task
			finalVersion := finalTask.Version()

			pe.RLock()
			var reader state.StateReader
			if finalTask.IsHistoric() {
				// Chain blockCache → sd.mem → applyTx so the block-finalize
				// IBS (withdrawals, EIP-7002/7251 system calls) sees every
				// prior-tx write from the current block. Omitting blockCache
				// here was the root cause of the trie-root race at block
				// 24839300: a tip-adjacent historic block's withdrawal read
				// the pre-block balance and stomped tx 28's in-block update.
				reader = state.NewHistoryReaderV3WithBlockCache(applyTx, pe.rs.Domains(), be.blockStateCache, finalVersion.TxNum)
			} else {
				reader = state.NewCurrentCachedReaderV3(pe.rs.Domains().AsGetter(applyTx), be.blockStateCache)
			}
			pe.RUnlock()

			ibs := state.New(reader)
			ibs.SetVersion(finalVersion.Incarnation)
			localVersionMap := state.NewVersionMap(nil)
			ibs.SetVersionMap(localVersionMap)
			ibs.SetTxContext(finalVersion.BlockNum, finalVersion.TxIndex)

			if tt, ok := lastResult.Task.(*taskVersion).Task.(*exec.TxTask); ok {
				// Syscalls share the main ibs so their writes (EIP-7002/7251
				// dequeue, EIP-4788 beacon root) land in ibs.VersionedWrites
				// and then in finalizeWrites via MakeWriteSet. If we instead
				// create a separate syscallIBS in historic mode, the syscall
				// writes land only in BlockStateCache and never reach the
				// commitment calculator's txResult feed — producing a wrong
				// trie root whenever an EIP-7002/7251 SSTORE changes a
				// previously-untouched slot (see the 24839762 race where
				// slots 0x01/0x03 of the EIP-7002 predeploy ended with
				// stale value 0x01 instead of cleared).
				//
				// Main ibs uses HistoryReaderV3WithBlockCache in historic
				// mode (see finalTask.IsHistoric() branch above), so it can
				// still see intra-batch writes from the blockCache.
				syscallIBS := ibs

				syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
					ret, err := protocol.SysCallContract(contract, data, pe.cfg.chainConfig, syscallIBS, tt.Header, pe.cfg.engine, false, *pe.cfg.vmConfig)
					if err != nil {
						return nil, err
					}
					lastResult.Logs = append(lastResult.Logs, syscallIBS.GetRawLogs(tt.TxIndex)...)
					return ret, err
				}

				chainReader := consensuschain.NewReader(pe.cfg.chainConfig, applyTx, pe.cfg.blockReader, pe.logger)
				if _, err := pe.cfg.engine.Finalize(
					pe.cfg.chainConfig, types.CopyHeader(tt.Header), ibs, tt.Uncles, receipts,
					tt.Withdrawals, chainReader, syscall, false, pe.logger); err != nil {
					return be.invalidBlockResult(fmt.Errorf("%w: can't finalize block %d: %v", rules.ErrInvalidBlock, be.blockNum, err)), nil
				}

				// syscallIBS == ibs unconditionally now; no separate write
				// propagation needed — syscall writes flow through
				// ibs.MakeWriteSet into finalizeWrites below.

				be.blockIO.RecordReads(finalVersion, ibs.VersionedReads())
				be.blockIO.RecordAccesses(finalVersion, ibs.AccessedAddresses())

				ivw := ibs.VersionedWrites(true)
				if len(ivw) > 0 {
					be.blockIO.RecordWrites(finalVersion, ivw)
					be.versionMap.FlushVersionedWrites(ivw, true, "")
				}

				collector := state.NewVersionedWriteCollector(pe.rs)
				if err := ibs.MakeWriteSet(tt.EvmBlockContext.Rules(tt.Config), collector); err != nil {
					return nil, err
				}
				finalizeWrites = collector.Writes()
				be.applyCount += len(finalizeWrites)

				// Apply finalize writes to the BlockStateCache.
				if err := pe.rs.ApplyStateWrites(ctx, applyTx, be.blockNum, finalVersion.TxNum,
					finalizeWrites, nil, lastResult.Rules(), be.blockStateCache); err != nil {
					return nil, err
				}
			}
		}

		// Send finalize txResult through the channel for index writes.
		// State writes are already in the BlockStateCache.
		if len(finalizeWrites) > 0 {
			lastResult := be.results[len(be.results)-1]
			if err := be.sendResult(ctx, &txResult{
				blockNum:              be.blockNum,
				txNum:                 txTask.Version().TxNum,
				rules:                 lastResult.Rules(),
				writes:                finalizeWrites,
				logs:                  lastResult.Logs,
				traceFroms:            lastResult.TraceFroms,
				traceTos:              lastResult.TraceTos,
				cumulativeBlobGasUsed: be.blobGasUsed,
				isFinalize:            true,
			}); err != nil {
				return nil, err
			}
		}

		// Flush block state cache to sd.mem — all writes (per-TX + finalize) are now visible.
		if err := be.blockStateCache.Flush(pe.rs.Domains(), applyTx); err != nil {
			return nil, err
		}

		be.result = &blockResult{
			BlockNum:        be.blockNum,
			BlockTime:       txTask.BlockTime(),
			BlockHash:       txTask.BlockHash(),
			ParentHash:      txTask.ParentHash(),
			StateRoot:       txTask.BlockRoot(),
			BlockGasUsed:    be.blockGasUsed,
			BlobGasUsed:     be.blobGasUsed,
			lastTxNum:       txTask.Version().TxNum,
			complete:        true,
			isPartial:       isPartial,
			ApplyCount:      be.applyCount,
			TxIO:            be.blockIO,
			Receipts:        receipts,
			Stats:           be.stats,
			Deps:            &deps,
			AllDeps:         allDeps,
			Exhausted:       be.exhausted,
			Header:          header,
			Txs:             txs,
			blockStateCache: be.blockStateCache,
		}
		return be.result, nil
	}

	// Block not yet complete — return nil. The caller (processResults)
	// only acts on complete blockResults (blockResult.complete == true).
	return nil, nil
}

func (be *blockExecutor) scheduleExecution(ctx context.Context, pe *parallelExecutor) {
	// Drain deferred tx N when its predecessor is validated AND no worker
	// at index < N is in flight. Lower-indexed workers' flushes land at
	// indices visible to N's reads via vm.Read's floor(N-1); higher-indexed
	// ones don't. Non-deferred txs keep dispatching via pending.
	be.execTasks.drainDeferredIfReady(func(tx int) bool {
		if be.validateTasks.maxComplete() < tx-1 {
			return false
		}
		minIP := be.execTasks.minInProgress()
		return minIP < 0 || minIP >= tx
	})

	toExecute := make(sort.IntSlice, 0, 2)

	for be.execTasks.minPending() >= 0 {
		toExecute = append(toExecute, be.execTasks.takeNextPending())
	}

	// Forward-progress safety net: pending empty + no workers in flight
	// means nothing will drive a subsequent maxComplete advance. Force-
	// drain so the exec loop doesn't block on rws.ResultCh forever.
	if len(toExecute) == 0 && be.execTasks.inProgressCount() == 0 {
		be.execTasks.drainDeferred()
		for be.execTasks.minPending() >= 0 {
			toExecute = append(toExecute, be.execTasks.takeNextPending())
		}
	}

	maxValidated := be.validateTasks.maxComplete()
	for i := 0; i < len(toExecute); i++ {
		nextTx := toExecute[i]
		execTask := be.tasks[nextTx]
		isNextValidated := nextTx == maxValidated+1
		if !isNextValidated {
			txIndex := execTask.Version().TxIndex
			if be.txIncarnations[nextTx] > 0 &&
				(be.execTasks.isBlocked(nextTx) || !be.blockIO.HasReads(txIndex) ||
					be.versionMap.ValidateVersion(txIndex, be.blockIO,
						func(_, writtenVersion state.Version) state.VersionValidity {
							wi := writtenVersion.TxIndex + 1
							if wi >= 0 && wi < len(be.txIncarnations) &&
								writtenVersion.TxIndex < maxValidated &&
								writtenVersion.Incarnation == be.txIncarnations[wi] {
								return state.VersionValid
							}
							return state.VersionInvalid
						}, false, "") != state.VersionValid) {
				be.execTasks.pushPending(nextTx)
				continue
			}
		}

		tv := &taskVersion{
			execTask:   execTask,
			versionMap: be.versionMap,
			profile:    be.profile,
			stats:      be.stats,
			statsMutex: &be.Mutex,
		}

		if incarnation := be.txIncarnations[nextTx]; incarnation == 0 {
			tv.version = execTask.Version()
			// Use TryAdd to avoid blocking the execLoop goroutine.
			// If the input queue is full, return remaining tasks to
			// pending — they will be scheduled on the next call to
			// scheduleExecution (triggered by each processed result).
			if !pe.in.TryAdd(tv) {
				be.execTasks.pushPending(nextTx)
				for j := i + 1; j < len(toExecute); j++ {
					be.execTasks.pushPending(toExecute[j])
				}
				return
			}
		} else {
			version := execTask.Version()
			version.Incarnation = incarnation
			tv.version = version
			pe.in.ReTry(tv)
		}

		// Commit side-effects only after successful enqueue. Record whether
		// this dispatch runs against fully settled input (every predecessor
		// already validated) so a genuine error from it can be classified
		// without re-execution — see the settledInput field doc.
		be.settledInput[nextTx] = isNextValidated
		if !isNextValidated {
			be.cntSpecExec++
		}

		if dbg.TraceTransactionIO && be.txIncarnations[nextTx] > 1 {
			fmt.Println(be.blockNum, "EXEC", nextTx, be.txIncarnations[nextTx], "maxValidated", maxValidated, be.blockIO.HasReads(nextTx), "failed", be.execFailed[nextTx], "aborted", be.execAborted[nextTx])
		}

		be.cntExec++
	}
}

func MergeReadSets(a state.ReadSet, b state.ReadSet) state.ReadSet {
	if b == nil {
		return a
	}
	if a == nil {
		return b
	}
	// Merge b into a in-place — a is being replaced by the caller anyway
	b.Scan(func(vr *state.VersionedRead) bool {
		a.Set(*vr)
		return true
	})
	return a
}

func MergeVersionedWrites(prev, next state.VersionedWrites) state.VersionedWrites {
	if len(prev) == 0 {
		return next
	}
	if len(next) == 0 {
		return prev
	}
	// Build merged set using prev as base, overwriting with next
	merged := state.WriteSet{}
	for _, v := range prev {
		merged.Set(*v)
	}
	for _, v := range next {
		merged.Set(*v)
	}
	out := make(state.VersionedWrites, 0, merged.Len())
	merged.Scan(func(v *state.VersionedWrite) bool {
		out = append(out, v)
		return true
	})
	return out
}

// normalizeWriteSet produces a clean write set from the versionMap's WriteSet
// for a given TX. It matches the serial IBS MakeWriteSet behaviour:
//
//  1. Storage no-op filter: removes writes where the value equals the origin
//     (what this TX read at execution start). Matches applyStorageChanges
//     which skips keys where dirty == originStorage.
//
//  2. Incarnation filter: only includes writes from the validated incarnation.
//     Stale entries from prior incarnations are excluded.
//
//  3. Self-destruct: emits DELETE entries for all storage keys of self-destructed
//     accounts (matching DomainDelPrefix behaviour).
//
//  4. Account field resolution: resolves account field values from the versionMap
//     to get the correct accumulated values (not speculative worker values).
//
// The input is blockIO.WriteSet(txIndex) — the raw versionWritten output.
// The output is ready for applyVersionedWrites and TouchUpdates.
//
// domainStorageKeys, when non-nil, must return every storage slot currently
// committed for an address (from sd.mem + domain files) — used to emit the
// full StoragePath=0 cascade when the address self-destructs. vm.StorageKeys
// alone only covers slots written in the current batch; genesis-allocated or
// prior-block storage isn't there, so the calc would never delete those slots
// from the trie (wrong root in TestDeleteRecreateAccount / TestSelfDestructReceive
// / TestEIP161AccountRemoval, all of which SD a contract whose storage predates
// the block). Pass nil in unit tests that don't exercise pre-block storage.
func normalizeWriteSet(writes state.VersionedWrites, vm *state.VersionMap, txIndex int, incarnation int, stateReader state.StateReader, domainStorageKeys func(addr accounts.Address) []accounts.StorageKey, emptyRemoval bool) state.VersionedWrites {
	filtered := make(state.VersionedWrites, 0, len(writes))

	// sdStorageSlots returns the union of vm.StorageKeys (this batch) and
	// domainStorageKeys (committed before this batch), deduped — the complete
	// set of storage slots that must be DELETE'd when addr self-destructs.
	sdStorageSlots := func(addr accounts.Address) []accounts.StorageKey {
		seen := make(map[accounts.StorageKey]struct{})
		var out []accounts.StorageKey
		for _, k := range vm.StorageKeys(addr) {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				out = append(out, k)
			}
		}
		if domainStorageKeys != nil {
			for _, k := range domainStorageKeys(addr) {
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					out = append(out, k)
				}
			}
		}
		return out
	}

	// Pre-scan for SD'd addresses. IBS.Selfdestruct emits 3 writes for the
	// SD'd account (IncarnationPath=preInc, SelfDestructPath=true, BalancePath=0).
	// If we forward all 3 to applyVersionedWrites, it sees d.balance != nil ||
	// d.incarnation != nil and routes into the "cleanup-before-recreate"
	// branch — which writes the account back with {Bal=0, Inc=preInc} encoding
	// instead of taking the pure-delete branch (DomainDel(Accounts)). The
	// account stays in sd.mem with non-zero incarnation, and a subsequent
	// block's CREATE2 at the same address sees a phantom existing account,
	// producing wrong execution / wrong trie root in TestRecreateAndRewind.
	// Drop the BalancePath/NoncePath/IncarnationPath/CodeHashPath writes for
	// SD'd addresses so applyVersionedWrites reaches the pure-delete branch.
	//
	// Two filters applied here:
	//   1. Validated-incarnation: mirror the `w.Version.Incarnation != incarnation`
	//      skip the SelfDestructPath case uses below — a stale SelfDestructPath=true
	//      from a non-validated incarnation must not mark the address as SD'd.
	//   2. Final-state: pre-Cancun a single tx can SELFDESTRUCT an address and then
	//      CREATE2-recreate at the same address; IBS emits SelfDestructPath=true
	//      (from Selfdestruct) followed later by SelfDestructPath=false (from
	//      CreateAccount, since the recreated object's selfdestructed flag is
	//      cleared). The address ends ALIVE, so its recreate-time account-field
	//      writes must survive — only mark sdSet when the LAST SelfDestructPath
	//      entry for the address (in emission order) is true. applyVersionedWrites
	//      already uses last-write-wins for d.selfDestruct, so this keeps the two
	//      in agreement. (EIP-6780 narrows this pattern post-Cancun but doesn't
	//      eliminate it; mainnet-rare, but cheap to get right.)
	sdSet := make(map[accounts.Address]bool)
	for _, w := range writes {
		if w.Path == state.SelfDestructPath && w.Version.Incarnation == incarnation {
			if v, ok := w.Val.(bool); ok {
				sdSet[w.Address] = v
			}
		}
	}
	for addr, sd := range sdSet {
		if !sd {
			delete(sdSet, addr)
		}
	}

	// Track which addresses have account-level writes vs storage-only writes.
	// Serial's MakeWriteSet calls UpdateAccountData for every dirty object,
	// including those with only storage changes. The commitment needs the
	// full account state for trie computation.
	hasAccountWrite := make(map[accounts.Address]bool)
	hasStorageWrite := make(map[accounts.Address]bool)

	for _, w := range writes {
		// Drop account-field writes for SD'd addresses so applyVersionedWrites
		// takes the pure-delete branch instead of cleanup-before-recreate.
		// Also drop StoragePath writes: serial's selfdestruct does
		// DomainDelPrefix(StorageDomain, addr) which wipes ALL slots, so an
		// SSTORE made after a SELFDESTRUCT in the same TX (pre-Cancun the
		// account stays live until end-of-TX, so re-entered code can still
		// SSTORE) is a no-op once the account is removed. The SelfDestructPath
		// case below re-emits an explicit StoragePath=0 delete for every slot
		// (sdStorageSlots), so the trie still sees the wipe. Keeping the raw
		// SSTORE here would race the cascade delete in ApplyWrites' slice order
		// and sometimes leave a phantom slot (TestCVE2020_26265).
		if sdSet[w.Address] {
			switch w.Path {
			case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath, state.CodePath, state.StoragePath:
				continue
			}
		}
		switch w.Path {
		case state.StoragePath:
			// Only include writes from the current (validated) incarnation.
			if w.Version.Incarnation != incarnation {
				continue
			}
			writeVal, _ := w.Val.(uint256.Int)
			// If addr was self-destructed by an earlier TX in this block, its
			// storage was wiped — the effective baseline for any slot not
			// re-written since is 0, regardless of what the versionMap (prior
			// TX) or the domain (pre-block) still holds. The SD's per-slot
			// zeroing is only re-emitted into the calc's writeset below, never
			// flushed back to the versionMap, so without this a resurrect TX
			// that re-writes a slot to its pre-SD value is wrongly dropped as a
			// no-op (TestDeleteRecreateSlotsAcrossManyBlocks).
			sdTxIdx, sdOk := -1, false
			if sd := vm.Read(w.Address, state.SelfDestructPath, accounts.NilKey, txIndex); sd.Status() == state.MVReadResultDone {
				if v, ok := sd.Value().(bool); ok && v {
					sdTxIdx, sdOk = sd.Version().TxIndex, true
				}
			}
			// No-op filter: compare against origin (what this TX would have read).
			// First check versionMap floor (prior TX's write in this block).
			// Then fall back to stateReader (pre-block value from domain).
			origin := vm.Read(w.Address, state.StoragePath, w.Key, txIndex)
			originValid := origin.Status() == state.MVReadResultDone && origin.Value() != nil &&
				!(sdOk && sdTxIdx > origin.Version().TxIndex)
			if originValid {
				originVal := origin.Value().(uint256.Int)
				if writeVal.Eq(&originVal) {
					continue // write-back same as prior TX's value — no-op
				}
			} else if sdOk {
				// SD'd earlier with no re-write since — baseline is 0.
				if writeVal.IsZero() {
					continue
				}
			} else if stateReader != nil {
				// No prior TX wrote this key — compare against pre-block value.
				preVal, found, err := stateReader.ReadAccountStorage(w.Address, w.Key)
				if err == nil {
					if !found && writeVal.IsZero() {
						continue // both zero — no-op
					}
					if found && writeVal.Eq(&preVal) {
						continue // same as pre-block — no-op
					}
				}
			}
			hasStorageWrite[w.Address] = true
		case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath:
			// Account fields: resolve from versionMap to get correct accumulated values.
			rr := vm.Read(w.Address, w.Path, w.Key, txIndex+1)
			if rr.Status() == state.MVReadResultDone && rr.Value() != nil {
				w.Val = rr.Value()
			}
			hasAccountWrite[w.Address] = true
		case state.CodePath:
			if w.Version.Incarnation != incarnation {
				continue
			}
			hasAccountWrite[w.Address] = true
		case state.CreateContractPath:
			if w.Version.Incarnation != incarnation {
				continue
			}
			hasAccountWrite[w.Address] = true
			// Pass through — applyVersionedWrites uses this to call CreateContract
		case state.SelfDestructPath:
			if w.Version.Incarnation != incarnation {
				continue
			}
			// Only emit storage DELETE entries when the account was actually
			// self-destructed (val=true). SelfDestructPath=false means the
			// account was NOT deleted (e.g., contract creation via CREATE2
			// sets selfdestructed=false after createObject).
			destructed, _ := w.Val.(bool)
			if destructed {
				filtered = append(filtered, w)
				for _, slot := range sdStorageSlots(w.Address) {
					filtered = append(filtered, &state.VersionedWrite{
						Address: w.Address,
						Path:    state.StoragePath,
						Key:     slot,
						Val:     uint256.Int{}, // zero = delete
						Version: w.Version,
					})
				}
			}
			continue
		case state.AddressPath:
			// AddressPath is record-level — skip for field-level consumers.
			continue
		}
		filtered = append(filtered, w)
	}

	// For addresses that appear in the raw WriteSet but don't have account-level
	// writes in the output, emit account field entries. Serial's MakeWriteSet
	// always calls UpdateAccountData for every dirty object — the commitment
	// needs the full account state. This covers:
	// - Addresses with only storage writes (no balance/nonce changes)
	// - Addresses whose storage writes were all filtered as no-ops
	//   (the object was still dirty in the IBS)
	//
	// Collect all addresses from the raw input (before filtering).
	allAddresses := make(map[accounts.Address]bool)
	for _, w := range writes {
		if w.Path != state.AddressPath {
			allAddresses[w.Address] = true
		}
	}

	// Track which fields each address already has in the output.
	addrFields := make(map[accounts.Address]map[state.AccountPath]bool)
	for _, w := range filtered {
		switch w.Path {
		case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath:
			if addrFields[w.Address] == nil {
				addrFields[w.Address] = make(map[state.AccountPath]bool)
			}
			addrFields[w.Address][w.Path] = true
		}
	}

	for addr := range allAddresses {
		if sdSet[addr] {
			// Don't fill account fields for SD'd addresses — same rationale as
			// the sdSet drop in the filter loop above. Without this, the
			// stateReader fallback below would round-trip pre-SD account state
			// (Nonce, CodeHash, Incarnation) back into the writeset and undo
			// the SD when applyVersionedWrites picks the cleanup-then-recreate
			// branch.
			continue
		}
		ver := state.Version{TxIndex: txIndex, Incarnation: incarnation}
		fields := addrFields[addr]

		// If addr was self-destructed by an earlier TX in this block and this
		// TX re-creates it (it isn't in sdSet — this TX didn't re-destruct it),
		// missing account fields are the post-destruction defaults, NOT the
		// pre-SD values still sitting in the versionMap. IBS.Selfdestruct only
		// records SelfDestructPath/BalancePath/IncarnationPath, so a re-creation
		// via a plain value transfer (no CREATE) leaves the stale pre-SD nonce
		// and codeHash in the map — reading them back here resurrects a phantom
		// contract (wrong trie root: TestSelfDestructReceive, TestCVE2020_26265).
		// If a later TX between the SD and this one re-created addr via CREATE2,
		// vm.Read returns that recreate's SelfDestructPath=false, so we correctly
		// fall through to the normal versionMap lookup.
		sdEarlier := false
		if sd := vm.Read(addr, state.SelfDestructPath, accounts.NilKey, txIndex); sd.Status() == state.MVReadResultDone {
			if v, ok := sd.Value().(bool); ok && v {
				sdEarlier = true
			}
		}

		// Only emit post-SD defaults when this TX created a new contract
		// (CREATE/CREATE2). A value-transfer resurrect (no CreateContractPath)
		// inherits the pre-SD account fields via the versionMap last-write-wins
		// chain — that matches GenerateChain's accumulate-across-txs behaviour
		// (no per-tx FinalizeTx). Forcing defaults here resets nonce/codeHash
		// against that canonical state (TestSelfDestructReceive).
		hasCreateContract := false
		for _, w := range writes {
			if w.Address == addr && w.Path == state.CreateContractPath {
				if v, ok := w.Val.(bool); ok && v {
					hasCreateContract = true
					break
				}
			}
		}

		// For each missing field, try versionMap then stateReader.
		for _, path := range []state.AccountPath{state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath} {
			if fields != nil && fields[path] {
				continue // already in output
			}
			if sdEarlier && hasCreateContract {
				var val any
				switch path {
				case state.BalancePath:
					val = uint256.Int{}
				case state.NoncePath:
					val = uint64(0)
				case state.IncarnationPath:
					val = uint64(0)
				case state.CodeHashPath:
					val = accounts.EmptyCodeHash
				}
				filtered = append(filtered, &state.VersionedWrite{Address: addr, Path: path, Val: val, Version: ver})
				continue
			}
			rr := vm.Read(addr, path, accounts.NilKey, txIndex+1)
			if rr.Status() == state.MVReadResultDone && rr.Value() != nil {
				filtered = append(filtered, &state.VersionedWrite{
					Address: addr,
					Path:    path,
					Val:     rr.Value(),
					Version: ver,
				})
				continue
			}
			// Fall back to stateReader for pre-block account state.
			if stateReader != nil {
				acc, err := stateReader.ReadAccountData(addr)
				if err == nil {
					var val any
					if acc != nil {
						switch path {
						case state.BalancePath:
							val = acc.Balance
						case state.NoncePath:
							val = acc.Nonce
						case state.IncarnationPath:
							val = acc.Incarnation
						case state.CodeHashPath:
							val = acc.CodeHash
						}
					} else {
						// New account — doesn't exist in domain yet.
						// Emit default values so the commitment sees
						// a full account (not a delete).
						switch path {
						case state.BalancePath:
							val = uint256.Int{}
						case state.NoncePath:
							val = uint64(0)
						case state.IncarnationPath:
							val = uint64(0)
						case state.CodeHashPath:
							val = accounts.EmptyCodeHash
						}
					}
					if val != nil {
						filtered = append(filtered, &state.VersionedWrite{
							Address: addr,
							Path:    path,
							Val:     val,
							Version: ver,
						})
					}
				}
			}
		}
	}

	// EIP-161 empty account removal: if an account has Balance=0, Nonce=0,
	// and empty CodeHash, it should be deleted — not written as a regular
	// account with zero values. Serial's updateAccount checks Empty() and
	// calls DeleteAccount. We must match that behavior.
	//
	// The Nonce==0 check correctly excludes successful CREATE/CREATE2
	// (which sets Nonce to 1 per EIP-161) — including the
	// "constructor returned empty bytecode but wrote storage" case the
	// calc-side 3-way Deleted branch protects via incarnation tracking.
	// OOG-during-CREATE2 leaves Nonce==0 in the writeset, so it
	// correctly falls through to deletion here.
	type acctState struct {
		balance  uint256.Int
		nonce    uint64
		codeHash accounts.CodeHash
		hasBal   bool
		hasNonce bool
		hasCode  bool
	}
	acctStates := make(map[accounts.Address]*acctState)
	for _, w := range filtered {
		switch w.Path {
		case state.BalancePath:
			s := acctStates[w.Address]
			if s == nil {
				s = &acctState{}
				acctStates[w.Address] = s
			}
			s.balance = w.Val.(uint256.Int)
			s.hasBal = true
		case state.NoncePath:
			s := acctStates[w.Address]
			if s == nil {
				s = &acctState{}
				acctStates[w.Address] = s
			}
			s.nonce = w.Val.(uint64)
			s.hasNonce = true
		case state.CodeHashPath:
			s := acctStates[w.Address]
			if s == nil {
				s = &acctState{}
				acctStates[w.Address] = s
			}
			s.codeHash = w.Val.(accounts.CodeHash)
			s.hasCode = true
		}
	}

	// Check for empty accounts and replace with Delete. Only when EIP-161
	// (SpuriousDragon) is active — before that fork an empty account that's
	// merely touched (e.g. a 0-value transfer) is created and persists, so
	// converting it to a delete here would diverge from serial's trie root
	// (TestEIP161AccountRemoval block 1, pre-SpuriousDragon).
	emptyAddrs := make(map[accounts.Address]bool)
	if emptyRemoval {
		for addr, s := range acctStates {
			if s.hasBal && s.hasNonce && s.hasCode &&
				s.balance.IsZero() && s.nonce == 0 && s.codeHash.IsEmpty() {
				emptyAddrs[addr] = true
			}
		}
	}

	if len(emptyAddrs) > 0 {
		var cleaned state.VersionedWrites
		for _, w := range filtered {
			if emptyAddrs[w.Address] {
				// Skip regular account field writes for empty accounts
				if w.Path == state.BalancePath || w.Path == state.NoncePath ||
					w.Path == state.IncarnationPath || w.Path == state.CodeHashPath {
					continue
				}
			}
			cleaned = append(cleaned, w)
		}
		// Add SelfDestructPath=true for each empty account
		for addr := range emptyAddrs {
			cleaned = append(cleaned, &state.VersionedWrite{
				Address: addr,
				Path:    state.SelfDestructPath,
				Val:     true,
				Version: state.Version{TxIndex: txIndex, Incarnation: incarnation},
			})
		}
		filtered = cleaned
	}

	return filtered
}

// resolveStorageWrites produces a clean write set from CollectorWrites:
//  1. Replaces speculative storage values with versionMap resolved values
//  2. Removes storage keys not in the versionMap (speculative writes from
//     code paths that differ under stale state)
//
// The result matches what the serial IBS collector would produce.
// DEPRECATED: use normalizeWriteSet with blockIO.WriteSet(txIndex) instead.
func resolveStorageWrites(writes state.VersionedWrites, vm *state.VersionMap, txIndex int, incarnation int, rs *state.StateV3Buffered) state.VersionedWrites {
	filtered := make(state.VersionedWrites, 0, len(writes))
	for _, w := range writes {
		switch w.Path {
		case state.StoragePath:
			// Check versionMap for this TX's write at this address+slot.
			// Use the resolved value from the versionMap (post-validation correct).
			rr := vm.Read(w.Address, state.StoragePath, w.Key, txIndex+1)
			if rr.Status() == state.MVReadResultDone && rr.Version().TxIndex == txIndex {
				if rr.Incarnation() != incarnation {
					continue // stale incarnation entry
				}
				if rr.Value() != nil {
					w.Val = rr.Value().(uint256.Int)
				}
			} else {
				continue // not written by this TX
			}
			// No-op filter: compare resolved value against origin (what this TX
			// would have read). This matches serial IBS behaviour where
			// applyStorageChanges skips keys where dirty == originStorage.
			// Origin = versionMap floor at txIndex (prior TX's write), or
			// pre-block value from snapshots if no prior TX wrote this key.
			{
				resolved := w.Val.(uint256.Int)
				origin := vm.Read(w.Address, state.StoragePath, w.Key, txIndex)
				if origin.Status() == state.MVReadResultDone && origin.Value() != nil {
					originVal := origin.Value().(uint256.Int)
					if resolved.Eq(&originVal) {
						continue // write-back same as origin — no-op
					}
				} else if rs != nil {
					// No prior TX wrote this key — use pre-block value.
					// At this point sd.mem may have accumulated writes from
					// prior TXs, but for THIS specific key (no prior TX wrote it),
					// GetLatest returns the pre-block value.
					addr := w.Address.Value()
					slot := w.Key.Value()
					composite := append(addr[:], slot[:]...)
					preBlock, _, err := rs.Domains().GetLatest(kv.StorageDomain, nil, composite)
					if err == nil {
						if resolved.IsZero() && len(preBlock) == 0 {
							continue // both zero — no-op
						}
						if len(preBlock) > 0 {
							var preVal uint256.Int
							preVal.SetBytes(preBlock)
							if resolved.Eq(&preVal) {
								continue // same as pre-block — no-op
							}
						}
					}
				}
			}
		case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath:
			// Resolve account field values from the versionMap.
			// CollectorWrites may have stale values from speculative execution.
			rr := vm.Read(w.Address, w.Path, w.Key, txIndex+1)
			if rr.Status() == state.MVReadResultDone && rr.Value() != nil {
				w.Val = rr.Value()
			}
		case state.AddressPath:
			// AddressPath is a record-level write — skip it.
			// The commitment calculator uses individual field paths.
			continue
		case state.SelfDestructPath:
			// When an account self-destructs, emit storage DELETE entries
			// for all storage slots that exist for this address.
			// The IBS path does this via DomainDelPrefix which scans sd.mem
			// and domain files. We reproduce it here.
			filtered = append(filtered, w) // keep the SelfDestructPath entry
			// Emit DELETEs for versionMap storage keys (written this block)
			for _, slot := range vm.StorageKeys(w.Address) {
				filtered = append(filtered, &state.VersionedWrite{
					Address: w.Address,
					Path:    state.StoragePath,
					Key:     slot,
					Val:     uint256.Int{}, // zero = delete
				})
			}
			continue // already appended above
		}
		filtered = append(filtered, w)
	}
	return filtered
}
