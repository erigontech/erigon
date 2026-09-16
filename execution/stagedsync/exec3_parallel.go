package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"runtime/pprof"
	"sort"
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
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/receipts"
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
  - start WorkerContext start workers

When rwLoop has nothing to do - it does Prune, or flush of WAL to RwTx (agg.rotate+agg.Flush)
*/

type parallelExecutor struct {
	txExecutor
	// failedBlock/failedHash record the implicated block when execution fails,
	// so the stage wrapper can target the unwind.
	failedBlock uint64
	failedHash  common.Hash
	execWorkers []*exec.WorkerContext
	stopWorkers func()
	waitWorkers func()
	// prevBlocks lists the finished-but-not-yet-committed blocks' versionMaps. A
	// worker executing block M reads blocks < M from this list in front of its raw
	// sd read, so it sees earlier blocks' writes that apply has not yet committed
	// to the shared domain. Dropped from the tail on commit; nil when the gate is off.
	prevBlocks *state.PrevBlockList
	// mintedWorkers are extra WorkerContexts created when the runSem pool is empty
	// because in-flight workers are parked mid-EVM on a dependency (holding their
	// context). Reused via runSem and reclaimed at teardown. CPU concurrency stays
	// bounded by execSem, so extra contexts add memory, not parallelism.
	mintedWorkers []*exec.WorkerContext
	mintMu        sync.Mutex
	// cancelExecLoop publishes the stopCause on execLoopCtx — the signal the exec
	// loop, calculator and apply loop each read to wind down. Every publish site is
	// ordered after the exec loop has produced everything up to the coalesce block,
	// so it never aborts an in-flight block mid-work.
	cancelExecLoop context.CancelCauseFunc
	// cancelWorkers stops the OCC worker pool via workersCtx (child of execLoopCtx):
	// the explicit ordered halt the exec loop calls once it has produced up to the
	// coalesce block.
	cancelWorkers context.CancelFunc
	// runSem holds the idle worker contexts; runWG tracks in-flight task goroutines
	// for teardown.
	runSem chan *exec.WorkerContext
	// execSem decouples the CPU-parallelism gate (sized to the worker count) from the
	// WorkerContext-object pool (runSem). A worker resting mid-EVM on a dependency
	// keeps its object but releases its execSem slot, so resting does not reduce
	// concurrency — another worker runs on a different object. runSem must hold enough
	// objects for the peak of executing+resting workers; execSem sizes parallelism.
	execSem    chan struct{}
	runWG      sync.WaitGroup
	workersCtx context.Context
	// results carries finished tasks from dispatchRunSelfLoop to the exec loop.
	results        chan *exec.TxResult
	workerCount    int
	blockExecutors map[uint64]*blockExecutor
	// consumers is the fan-out registry (apply loop + commitment calculator), set
	// before execLoop starts. The exec loop closes it on exit to drain the consumers.
	consumers   *resultStream
	maxBlockNum uint64 // set before execLoop; exec loop exits when reached
	accumulator *shards.Accumulator
	// changesetWindowStart is the first block of the batch that must capture a
	// changeset; blocks below it run without one.
	changesetWindowStart uint64
	// currentChangeSet is the block's changeset while it is being built: the exec
	// loop creates it and saves it by hash, then the apply fold and the committer
	// bind it transiently by hash under changesetMu. Deliberately NOT installed as a
	// persistent live accumulator — under splitApply the apply loop is the sole
	// sd.mem writer, so a global install would only race the fold's own transient bind.
	currentChangeSet *changeset.StateChangeSet
	// currentChangeSetBlock is the block currentChangeSet belongs to (0 == none).
	currentChangeSetBlock uint64
}

// stopKind classifies why the executor was asked to stop. It maps directly
// to the stage return: done→nil, more→ErrLoopExhausted, bad→fail.err+unwind.
type stopKind uint8

const (
	stopReachedMax stopKind = iota // all requested work applied — clean batch end
	stopMoreWork                   // size/exhausted cut before maxBlock — resume next cycle
	stopBadBlock                   // wrong trie root — fail the implicated block and unwind
)

func (k stopKind) String() string {
	switch k {
	case stopReachedMax:
		return "reached-max"
	case stopMoreWork:
		return "more-work"
	case stopBadBlock:
		return "bad-block"
	default:
		return fmt.Sprintf("stopKind(%d)", uint8(k))
	}
}

// stopCause is the cancel cause published on the shared executor context. It
// carries the block the batch coalesces to (M) and the kind so every goroutine
// reads the same signal: exec produces state up to M then stops; the calculator
// caps fold-ahead at M and keeps computing to M on its own uncancelled context;
// the apply loop derives the commit boundary. A stopBadBlock cause aborts immediately.
type stopCause struct {
	block uint64
	kind  stopKind
	err   error
}

func (s *stopCause) Error() string {
	if s.err != nil {
		return fmt.Sprintf("parallel executor stop (kind=%s block=%d): %v", s.kind, s.block, s.err)
	}
	return fmt.Sprintf("parallel executor stop (kind=%s block=%d)", s.kind, s.block)
}

// stopCauseOf returns the stopCause published on ctx, if any.
func stopCauseOf(ctx context.Context) (*stopCause, bool) {
	if s, ok := errors.AsType[*stopCause](context.Cause(ctx)); ok {
		return s, true
	}
	return nil, false
}

// ensureChangesetAccumulator makes pe.currentChangeSet point at a fresh,
// block-specific StateChangeSet, to be saved by hash and bound transiently by
// the apply fold and the committer. Idempotent; a no-op outside the changeset
// window. Exec-loop only.
func (pe *parallelExecutor) ensureChangesetAccumulator(blockNum uint64) {
	if blockNum < pe.changesetWindowStart || blockNum == 0 || blockNum > pe.maxBlockNum {
		return
	}
	if pe.currentChangeSet != nil && pe.currentChangeSetBlock == blockNum {
		return
	}
	// A stale changeset for a different block was already saved by hash, so
	// overwriting it here loses nothing.
	pe.currentChangeSet = &changeset.StateChangeSet{}
	pe.currentChangeSetBlock = blockNum
}

// clearChangesetAccumulator detaches the current changeset accumulator after
// its block's changeset has been saved. Exec-loop only.
func (pe *parallelExecutor) clearChangesetAccumulator() {
	pe.currentChangeSet = nil
	pe.currentChangeSetBlock = 0
}

// bindBlockChangesetForFold binds block N's saved changeset (by hash) so the
// apply fold's DomainPuts record account/storage/code diffs into it — letting
// unwind revert state, not just commitment. The bind is transient and
// self-restoring under changesetMu; nothing installs a persistent global.
// Returns a restore closure; a no-op if the block has no saved changeset.
func (pe *parallelExecutor) bindBlockChangesetForFold(blockNum uint64, blockHash common.Hash) (restore func()) {
	pe.domains().LockChangesetAccumulator()
	defer pe.domains().UnlockChangesetAccumulator()
	cs := pe.domains().GetChangesetByHash(blockNum, blockHash)
	if cs == nil {
		return func() {}
	}
	unswap := pe.domains().SwapChangesetAccumulatorLocked(cs)
	return func() {
		pe.domains().LockChangesetAccumulator()
		unswap()
		pe.domains().UnlockChangesetAccumulator()
	}
}

func (pe *parallelExecutor) exec(ctx context.Context,
	startBlockNum uint64, offsetFromBlockBeginning uint64, maxBlockNum uint64, blockLimit uint64,
	initialTxNum uint64, inputTxNum uint64, initialCycle bool, rwTx kv.TemporalRwTx,
	stepsInDb float64, accumulator *shards.Accumulator, readAhead chan uint64, logEvery *time.Ticker) (*types.Header, kv.TemporalRwTx, error) {
	var (
		outHeader *types.Header
		outTx     kv.TemporalRwTx
		outErr    error
	)
	pprof.Do(ctx, pprof.Labels("phase", "pe-exec"), func(lctx context.Context) {
		outHeader, outTx, outErr = pe.execImpl(lctx, startBlockNum, offsetFromBlockBeginning,
			maxBlockNum, blockLimit, initialTxNum, inputTxNum, initialCycle, rwTx, stepsInDb, accumulator, readAhead, logEvery)
	})
	return outHeader, outTx, outErr
}

func (pe *parallelExecutor) execImpl(ctx context.Context,
	startBlockNum uint64, offsetFromBlockBeginning uint64, maxBlockNum uint64, blockLimit uint64,
	initialTxNum uint64, inputTxNum uint64, initialCycle bool, rwTx kv.TemporalRwTx,
	stepsInDb float64, accumulator *shards.Accumulator, readAhead chan uint64, logEvery *time.Ticker) (*types.Header, kv.TemporalRwTx, error) {

	// Do NOT set pe.applyTx to the stageloop's rwTx — the rwTx is thread-bound
	// and cannot be shared with the execLoop goroutine, which opens its own roTx.

	// applyResults feeds the apply goroutine; commitResults feeds the commitment
	// calculator. Both are fed by the fan-out in the execLoop's blockExecutor.
	applyResults := make(chan applyResult, 2_048)
	commitResults := make(chan applyResult, 2_048)
	// Exec-only (DISCARD_COMMITMENT): nil the commit stream so no commitment work
	// runs — fan-out and batch-commit trigger no-op on a nil channel, and the
	// calculator exits immediately and closes rootResults.
	if dbg.DiscardCommitment() {
		commitResults = nil
	}
	// Only wire the BAL fold-ahead pipeline when BAL-driven commitment is on; a nil
	// channel leaves the per-block send and calculator select arm inert.
	var blockRequests chan *blockRequest
	if dbg.BALDrivenCommitment {
		blockRequests = make(chan *blockRequest, 2_048)
	}

	// rootResults receives per-block commitment roots from the calculator.
	rootResults := make(chan commitmentResult, 64)

	if blockLimit > 0 && min(startBlockNum+blockLimit, maxBlockNum) > startBlockNum+16 || maxBlockNum > startBlockNum+16 {
		lastBlock := maxBlockNum
		if blockLimit > 0 {
			lastBlock = min(startBlockNum+blockLimit-1, maxBlockNum)
		}
		log.Info(fmt.Sprintf("[%s] parallel starting", pe.logPrefix),
			"from", startBlockNum, "to", maxBlockNum, "limit", lastBlock, "initialTxNum", initialTxNum,
			"initialBlockTxOffset", offsetFromBlockBeginning, "initialCycle", initialCycle,
			"isForkValidation", pe.isForkValidation, "isApplyingBlocks", pe.isApplyingBlocks)
	}

	// restoreTxNum must run before pe.run() so doms.SetTxNum() completes before any
	// goroutine reads txNum. With an injected block source (ephemeral replay) the
	// caller owns range resolution, so the passed-in inputTxNum is used as-is.
	restoredTxNum := inputTxNum
	if pe.blockSrc == nil {
		var err error
		restoredTxNum, _, _, _, err = restoreTxNum(ctx, &pe.cfg, rwTx, inputTxNum, maxBlockNum)
		if err != nil {
			return nil, rwTx, err
		}
	}

	// Set accumulator before pe.run() so execLoop sees it without a race.
	pe.accumulator = accumulator

	// prevBlocks must exist before pe.run() and resetWorkers: both call
	// EnablePrevBlockReads(pe.prevBlocks), and a nil registry panics on the first
	// per-task SetBlock. run() resets workers on its own goroutine, so setting it
	// after run() is a race.
	pe.prevBlocks = state.NewPrevBlockList()

	executorContext, executorCancel, err := pe.run(ctx)
	defer executorCancel(nil)

	if err != nil {
		return nil, rwTx, err
	}

	if err := pe.resetWorkers(ctx, pe.rs, rwTx); err != nil {
		return nil, rwTx, err
	}

	// Disable inline TouchKey — the commitment calculator accumulates touches
	// via its own Updates buffer.
	pe.rs.Domains().SetDisableInlineTouchKey(true)
	defer pe.rs.Domains().SetDisableInlineTouchKey(false)
	// Restore the caller's InMemHistoryReads on exit; forcing it false breaks
	// post-exec callers (forkchoice GetAsOf, RPC reads) that need in-mem history.
	prevInMemHistoryReads := pe.rs.Domains().InMemHistoryReads()
	pe.rs.Domains().SetInMemHistoryReads(true)
	defer pe.rs.Domains().SetInMemHistoryReads(prevInMemHistoryReads)

	// The calculator installs its own asOfStateReader on the shared commitment
	// context; restore the prior reader on exit so it doesn't leak GetAsOf reads
	// into later foreground commitment reads.
	sdCtx := pe.rs.Domains().GetCommitmentContext()
	prevStateReader := sdCtx.StateReader()
	defer sdCtx.SetStateReader(prevStateReader)

	// Register the fan-out consumers so execLoop can publish + close them.
	// Registration order is publish order; close walks it in reverse (commit before
	// apply). blockRequests is not registered — it is closed by its sole sender (the
	// executeBlocks dispatch goroutine), not by execLoop.
	pe.consumers = newResultStream()
	pe.consumers.register("applyResults", applyResults, true)
	pe.consumers.register("commitResults", commitResults, false)
	pe.maxBlockNum = maxBlockNum

	// Configure changeset capture and seed the initial accumulator BEFORE the exec
	// loop / executeBlocks goroutines start touching sd.mem. The exec loop owns all
	// subsequent accumulator transitions so apply-loop and exec-loop sd.mem writes
	// never race on SharedDomains.mem.
	pe.changesetWindowStart = changesetWindowStart(pe.cfg.syncCfg.AlwaysGenerateChangesets,
		pe.cfg.syncCfg.MaxReorgDepth, pe.cfg.blockReader.FrozenBlocks(), startBlockNum, maxBlockNum)
	pe.ensureChangesetAccumulator(startBlockNum)

	// Start the commitment calculator. Blocks from the changeset window onward must
	// compute per-block — otherwise batch-mode dedupes branch updates across the
	// batch and flushes them all into one block's changeset, which fails on
	// subsequent reorgs. The calculator only publishes results; the apply loop is the
	// sole cancellation authority.
	forcePerBlockCompute := pe.cfg.syncCfg.KeepExecutionProofs
	// ctx runs the calculator's roTx/compute/publish; executorContext carries the
	// stopCause. Separating them lets a clean-stop cancel signal the calculator
	// without aborting an in-flight commitment.
	commitDomainReader := state.NewLayeredDomainReader(pe.rs.Domains(), nil, pe.prevBlocks)
	calculator, err := newCommitmentCalculator(ctx, executorContext, pe.rs.Domains(), pe.cfg.db, pe.cfg.chainConfig, pe.logPrefix, pe.logger, forcePerBlockCompute, pe.changesetWindowStart, commitResults, blockRequests, rootResults, commitDomainReader)
	if err != nil {
		return nil, nil, err
	}
	calculator.Start(ctx)
	defer calculator.Stop()

	if err := pe.executeBlocks(executorContext, startBlockNum, maxBlockNum, blockLimit, initialTxNum, restoredTxNum, readAhead, initialCycle, pe.consumers, blockRequests); err != nil {
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
				pe.logger.Warn("["+pe.logPrefix+"] rw panic", "rec", rec, "stack", dbg.Stack())
				// Surface the panic as the loop's error; otherwise execImpl returns
				// nil with sd.mem partly folded and the caller mistakes an internal
				// invariant failure for success.
				if err == nil {
					err = fmt.Errorf("apply loop panic: %v", rec)
				}
			} else if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, &ErrLoopExhausted{})) {
				pe.logger.Warn("["+pe.logPrefix+"] rw exit", "err", err, "stack", dbg.Stack())
			} else {
				pe.logger.Debug("[" + pe.logPrefix + "] rw exit")
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

		// appliedBlocks tracks blockNums that completed full apply-loop processing
		// (including post-block validation). Compared at exit against txResultBlocks
		// to catch "channel closed cleanly but a block was silently missed" — its
		// blockResult never arrived, so the validator never fired and an invalid
		// block could become canonical.
		appliedBlocks := make(map[uint64]struct{})

		// txResultBlocks tracks every blockNum that had at least one tx-result reach
		// the apply loop. A block here but not in appliedBlocks is a silent failure.
		txResultBlocks := make(map[uint64]struct{})

		// rootResultsClosed records whether rootResults has closed. The select-arm is
		// disabled by setting the local rootResults to nil; the drain-after-close path
		// must then skip its `for cr := range rootResults` (which would hang on nil).
		rootResultsClosed := false

		// fail tracks the earliest block-validity failure across the exec
		// (blockResult.Err) and commit (ErrWrongTrieRoot) streams. Exec verdicts take
		// precedence over trie-root mismatches on the same block. With fold-ahead a
		// commit wrong-root can arrive before the block's exec verdict, so it is
		// recorded and surfaced only after applyResults closes.
		var fail failCandidate
		// finalized flips once the reported failure is decided. Remaining results are
		// then drained without re-validation so a post-cancel block can't mask it.
		finalized := false

		// blockUpdateCount/blockApplyCount count individual VersionedWrite entries;
		// used only for an internal consistency check (blockUpdateCount==ApplyCount)
		// and trace output.
		blockUpdateCount := 0
		blockApplyCount := 0
		// Collect per-tx writes to notify the accumulator AFTER StartChange (which
		// arrives with the blockResult, after all txResults).
		var pendingAccumulatorWrites []state.WriteSetView
		// splitApply: buffer each block's per-tx results and fold them to sd.mem at
		// block end, so sd.mem stays N-1 during exec.
		var splitApplyBuf []*txResult

		// handleCommitResult classifies a commitment result with NO unwind
		// side-effects: a wrong-root is routed through the fail/finalized machinery
		// so the reported failure and its block hash are chosen after exec has had
		// its say. The actual unwind happens at finalization.
		handleCommitResult := func(cr commitmentResult) error {
			if cr.err != nil {
				// Non-wrong-root calculator errors (lazy-load / ComputeCommitment)
				// must not be treated as a wrong-root — that would mark a valid block
				// bad and unwind valid state. Fail fast, preserving the original error.
				if !errors.Is(cr.err, ErrWrongTrieRoot) {
					return fmt.Errorf("[%s] commitment: %w", pe.logPrefix, cr.err)
				}
				pe.logWrongTrieRoot(fmt.Sprintf("[%s] Wrong trie root of block %d: %x (%v)",
					pe.logPrefix, cr.blockNum, cr.rootHash, cr.err))
				return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, cr.blockNum)
			}
			pe.txExecutor.lastCommittedBlockNum.Store(cr.blockNum)
			pe.txExecutor.lastCommittedTxNum.Store(cr.txNum)
			return nil
		}

		// deliberateCancel is the light context-cancel; teardown stays with execImpl's
		// deferred executorCancel so only the main goroutine drives cleanup.
		deliberateCancel := func() {
			pe.cancelExecLoop(&stopCause{block: fail.block, kind: stopBadBlock, err: fail.err})
		}
		// processCommit records a commit failure into `fail`. A wrong-root is deferred
		// so the block's own exec verdict can supersede it — except when exec has
		// already applied the block (an incremental, not fold-ahead, wrong-root): then
		// finalize and cancel eagerly rather than keep building on known-wrong state.
		processCommit := func(cr commitmentResult) error {
			err := handleCommitResult(cr)
			if err == nil {
				return nil
			}
			fail.consider(cr.blockNum, cr.blockHash, false, err)
			if !errors.Is(err, ErrWrongTrieRoot) {
				// Infra fault: do NOT bare-return — that kills the apply loop while the
				// exec loop may be blocked on a mustDeliver send, wedging shutdown.
				// Record + cancel + keep draining; fail.err surfaces at channel close.
				finalized = true
				deliberateCancel()
				return nil
			}
			if _, applied := appliedBlocks[cr.blockNum]; applied {
				finalized = true
				deliberateCancel()
			}
			return nil
		}

		// Apply loop: exits ONLY when applyResults is closed by the exec loop. Do NOT
		// add ctx.Done / executorContext.Done cases — the exec loop owns shutdown
		// sequencing, and exiting early leaves sd.mem inconsistent with the commitment
		// boundary.
		for {
			select {
			case applyResult, ok := <-applyResults:
				if !ok {
					// Exec loop closed the channel — batch complete. Drain calculator
					// results, then exit. Skip the drain if rootResults already closed
					// (ranging a nil channel hangs forever).
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
					// Completeness check: every block whose tx-results arrived must also
					// have produced a blockResult, else the per-block validator never
					// fired for it and an invalid block could become canonical.
					// txResultBlocks minus appliedBlocks is that silent-failure set.
					//
					// Surface the earliest recorded failure ahead of the missing-blocks
					// check: a deliberate cancel manufactures a missing-block condition
					// that would otherwise mask it. A deferred commit wrong-root does its
					// unwind here (not inline) so a !initialCycle reorg marks the bad
					// block with the implicated block's OWN hash.
					if fail.set {
						pe.failedBlock, pe.failedHash = fail.block, fail.blockHash
						return fail.err
					}
					if missing := applyLoopMissingBlocks(txResultBlocks, appliedBlocks); len(missing) > 0 {
						return fmt.Errorf("%w: apply loop exited (lastBlockResult=%d maxBlockNum=%d) but %d block(s) had tx-results without a blockResult: %v",
							rules.ErrInvalidBlock, lastBlockResult.BlockNum, pe.maxBlockNum, len(missing), missing)
					}
					// The stop kind rides in the shared context's cause: stopReachedMax
					// is a clean batch end (nil), stopMoreWork a partial batch to resume
					// (ErrLoopExhausted). stopBadBlock is handled by the fail branch above.
					if sc, ok := stopCauseOf(executorContext); ok {
						switch sc.kind {
						case stopReachedMax:
							return nil
						case stopMoreWork:
							return &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult.BlockNum, Reason: "block batch is full"}
						}
					}
					// Fallback for exit paths that publish no cause (single-block
					// fork-validation, or shutdown via context.Canceled). A fully-applied
					// range — or an empty loop that executed nothing because the range was
					// already applied — is a clean end; otherwise there is more work.
					if applyLoopCloseIsClean(lastBlockResult.BlockNum, pe.maxBlockNum, len(txResultBlocks)) {
						return nil
					}
					return &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult.BlockNum, Reason: "block batch is full"}
				}
				switch applyResult := applyResult.(type) {
				case *txResult:
					txResultBlocks[applyResult.blockNum] = struct{}{}
					uncommittedGas += applyResult.blockGasUsed
					uncommittedTransactions++
					writeCount := applyResult.writes.Count()
					if dbg.TraceApply && dbg.TraceBlock(applyResult.blockNum) {
						pe.rs.SetTrace(true)
						fmt.Println(applyResult.blockNum, "apply", applyResult.txNum, writeCount)
					}
					blockUpdateCount += writeCount
					// The apply loop is the sole sd.mem writer: buffer each tx result
					// and fold the buffer to sd.mem at block end.
					splitApplyBuf = append(splitApplyBuf, applyResult)
					if pe.accumulator != nil {
						pendingAccumulatorWrites = append(pendingAccumulatorWrites, applyResult.writes)
					}
					blockApplyCount += writeCount
					pe.rs.SetTrace(false)
				case *blockResult:
					if finalized {
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						continue
					}
					// Apply loop is the canonical error-emission point for block-validity
					// rejections. Record the exec verdict (it wins its block over a commit
					// wrong-root) and keep draining so an earlier commit wrong-root in
					// rootResults can still supersede it. No cancel: the exec loop
					// self-exits, and cancelling would join context.Canceled onto the error.
					if applyResult.Err != nil {
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						pendingAccumulatorWrites = pendingAccumulatorWrites[:0]
						fail.consider(applyResult.BlockNum, applyResult.BlockHash, true, applyResult.Err)
						finalized = true
						continue
					}
					// failInfra routes an apply-loop infrastructure fault through
					// failCandidate + cancel and keeps the loop draining. Never
					// bare-return while the exec loop may sit in a terminal mustDeliver
					// send on a full applyResults — that strands shutdown and wedges pe.wait.
					failInfra := func(err error) {
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						fail.consider(applyResult.BlockNum, applyResult.BlockHash, true, err)
						finalized = true
						deliberateCancel()
					}
					// Fold the block's per-tx versionMap views to sd.mem at block end in
					// publish order; the versionMap composes each tx's base. The finalize
					// tx skips ApplyTxIndexes, matching the exec loop.
					restoreCS := pe.bindBlockChangesetForFold(applyResult.BlockNum, applyResult.BlockHash)
					var applyErr error
					for _, r := range splitApplyBuf {
						if err := pe.rs.ApplyStateWrites(ctx, rwTx, r.blockNum, r.txNum, r.writes, nil, r.rules); err != nil {
							applyErr = fmt.Errorf("splitApply state block=%d txNum=%d: %w", r.blockNum, r.txNum, err)
							break
						}
						if !r.isFinalize {
							if err := pe.rs.ApplyTxIndexes(rwTx, r.txNum, r.receipt, r.cumulativeBlobGasUsed, r.logs, r.traceFroms, r.traceTos); err != nil {
								applyErr = fmt.Errorf("splitApply index block=%d txNum=%d: %w", r.blockNum, r.txNum, err)
								break
							}
						}
					}
					restoreCS()
					splitApplyBuf = splitApplyBuf[:0]
					if applyErr != nil {
						failInfra(applyErr)
						continue
					}
					// This block's writes are now in the shared domain: drop it from the
					// tail of the prev-block list so later blocks read it from the domain.
					// The tail must be exactly the block just committed; a mismatch is a
					// push/remove desync that would drop the wrong overlay.
					if tb, ok := pe.prevBlocks.TailBlockNum(); ok && tb != applyResult.BlockNum {
						panic(fmt.Sprintf("prevBlocks tail block %d != committed block %d", tb, applyResult.BlockNum))
					}
					pe.prevBlocks.RemoveTail()
					// StartChange + NotifyAccumulator both run in the apply goroutine to
					// keep accumulator access single-threaded. StartChange must precede
					// NotifyAccumulator: it initialises the latestChange entry the notify
					// writes into.
					if pe.accumulator != nil && applyResult.Header != nil {
						rawTxs, marshalErr := types.MarshalTransactionsBinary(applyResult.Txs)
						if marshalErr != nil {
							failInfra(fmt.Errorf("marshal transactions for accumulator, block %d: %w", applyResult.BlockNum, marshalErr))
							continue
						}
						pe.accumulator.StartChange(applyResult.Header, rawTxs, false)
						for _, writes := range pendingAccumulatorWrites {
							state.NotifyAccumulator(pe.accumulator, writes)
						}
						pendingAccumulatorWrites = pendingAccumulatorWrites[:0]
					}

					var blockValidatorWaiter *blockValidator
					if applyResult.BlockNum > 0 && !applyResult.isPartial { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
						checkBloom := !pe.cfg.vmConfig.StatelessExec && !pe.cfg.vmConfig.NoReceipts
						checkReceipts := checkBloom && pe.cfg.chainConfig.IsByzantium(applyResult.BlockNum)

						b, _, err := pe.cfg.blockReader.BlockWithSenders(ctx, rwTx, applyResult.BlockHash, applyResult.BlockNum)

						if err != nil {
							failInfra(fmt.Errorf("can't retrieve block %d: for post validation: %w", applyResult.BlockNum, err))
							continue
						}
						if b == nil {
							failInfra(fmt.Errorf("nil block %d (hash %x)", applyResult.BlockNum, applyResult.BlockHash))
							continue
						}

						lastHeader = b.HeaderNoCopy()

						if lastHeader.Number.Uint64() != applyResult.BlockNum {
							failInfra(fmt.Errorf("block numbers don't match expected: %d: got: %d for hash %x", applyResult.BlockNum, lastHeader.Number.Uint64(), applyResult.BlockHash))
							continue
						}

						if blockUpdateCount != applyResult.ApplyCount {
							failInfra(fmt.Errorf("block %d: applyCount mismatch: got: %d expected %d", applyResult.BlockNum, blockUpdateCount, applyResult.ApplyCount))
							continue
						}

						// Spawn per-block validation concurrently; joined via Wait() below
						// after the other per-result work has run alongside it.
						blockValidatorWaiter = newBlockValidator(pe.cfg.engine, applyResult.BlockGasUsed, applyResult.BlobGasUsed, checkReceipts, checkBloom, applyResult.Receipts,
							lastHeader, b.Transactions(), pe.cfg.chainConfig, pe.logger)

					}

					if applyResult.BlockNum > 0 && applyResult.receiptsComplete && !initialCycle && applyResult.Header != nil &&
						pe.cfg.notifications != nil && pe.cfg.notifications.RecentReceipts != nil {
						pe.cfg.notifications.RecentReceipts.Add(applyResult.Receipts, applyResult.Txs, applyResult.Header)
					}

					if applyResult.BlockNum > lastBlockResult.BlockNum {
						uncommittedBlocks++
						pe.doms.SetTxNum(applyResult.lastTxNum)
						lastBlockResult = *applyResult
					}

					blockUpdateCount = 0
					blockApplyCount = 0

					// Join the per-block validator spawned above (post-execution
					// receipt/BAL checks).
					if err := blockValidatorWaiter.Wait(); err != nil {
						// Block-validity verdict: route through failCandidate and keep
						// draining rather than bare-returning (which would wedge pe.wait).
						// No cancel: mirror the blockResult.Err path.
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						fail.consider(applyResult.BlockNum, applyResult.BlockHash, true, fmt.Errorf("%w, block=%d, %w", rules.ErrInvalidBlock, applyResult.BlockNum, err))
						finalized = true
						continue
					}

					isAmsterdam := pe.cfg.chainConfig.IsAmsterdam(applyResult.BlockTime)
					if isAmsterdam || pe.cfg.experimentalBAL {
						var computedBAL types.BlockAccessList
						computedBAL, err = bal.Process(rwTx, lastHeader, applyResult.TxIO, isAmsterdam, pe.cfg.experimentalBAL, pe.cfg.dirs.DataDir, pe.logger)
						if err != nil {
							failInfra(err)
							continue
						}
						if pe.cfg.balSink != nil {
							pe.cfg.balSink(applyResult.BlockNum, computedBAL)
						}
					}

					appliedBlocks[applyResult.BlockNum] = struct{}{}

					// A commit wrong-root deferred for this (or an earlier) block: exec
					// has now applied it cleanly, so the divergence is real. Finalize on
					// that earliest block and stop dispatching further work.
					if fail.set && !fail.exec && applyResult.BlockNum >= fail.block {
						finalized = true
						deliberateCancel()
					}

					if dbg.StopAfterBlock > 0 && applyResult.BlockNum == dbg.StopAfterBlock {
						pe.logger.Warn(fmt.Sprintf("[%s] STOP_AFTER_BLOCK reached, exiting without commit (debug mode)", pe.logPrefix), "block", applyResult.BlockNum)
						// Intentional os.Exit (debug only): returning would run deferred
						// commit/flush and overwrite the state we want to capture unchanged.
						os.Exit(0)
					}
				}

			case cr, ok := <-rootResults:
				if !ok {
					// rootResults closed by the calculator on Stop. Do NOT return: the
					// apply loop must keep draining applyResults until the exec loop
					// closes it, else we race sendResult and drop the trailing
					// blockResult (invalid block becomes canonical, validator never
					// fired). Nil the arm so it doesn't busy-spin; rootResultsClosed
					// makes the close branch skip its drain (which would hang on nil).
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
					agg := pe.cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
					if agg.HasBackgroundFilesBuild() {
						pe.logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", agg.BackgroundProgress())
					}
				}
			}
		}
	}()

	executorCancel(nil)

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
			return nil, rwTx, execErr
		}
	}

	return lastHeader, rwTx, execErr
}

func (pe *parallelExecutor) LogExecution() {
	pe.progress.LogExecution(pe.rs.StateV3, pe)
	pe.doms.PrintCacheStats()
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

// triggerBatchCommitment sends a commitComputeRequest so the calculator computes
// the batch commitment before the exec loop exits and closes channels. Delivery is
// unconditional: honouring ctx.Done would drop the batch-end commitment when the
// buffer is momentarily full, leaving commitment behind sd.mem. The calculator
// keeps draining until its channel closes, so blocking is safe.
func (pe *parallelExecutor) triggerBatchCommitment(ctx context.Context) {
	if pe.consumers == nil {
		return
	}
	pe.consumers.sendControl("commitResults", &commitComputeRequest{})
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
		_ = worker.ResetState(rs, nil, nil, state.NewLightCollector(), nil)
		worker.EnablePrevBlockReads(pe.prevBlocks)
	}

	return nil
}

// newExecWorker mints an extra WorkerContext when the runSem pool is empty
// because in-flight workers are parked mid-EVM on a dependency (holding their
// context). Reset against the same shared state; tracked for teardown.
func (pe *parallelExecutor) newExecWorker() *exec.WorkerContext {
	w := exec.NewWorkerContext(pe.workersCtx, true, pe.taskExecMetrics, pe.cfg.db,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine, pe.cfg.dirs, pe.logger)
	_ = w.ResetState(pe.rs, nil, nil, state.NewLightCollector(), nil)
	w.EnablePrevBlockReads(pe.prevBlocks)
	pe.mintMu.Lock()
	pe.mintedWorkers = append(pe.mintedWorkers, w)
	pe.mintMu.Unlock()
	return w
}

// prevBlockBase wraps a freshly-built committed-base reader with the prev-block
// layers for blockNum, so those reads also see prior blocks' not-yet-committed
// writes — matching the per-task worker reader. Without this, coinbase
// materialization reads a stale sd.mem base and writes a stale account into the
// current block's versionMap.
func (pe *parallelExecutor) prevBlockBase(raw state.StateReader, blockNum uint64) state.StateReader {
	return state.PrevBlockBase(raw, pe.prevBlocks, blockNum)
}

// acquireWorker takes a context from the pool, or grows the pool when it is
// empty (paused workers hold their contexts). Returns nil on shutdown.
func (pe *parallelExecutor) acquireWorker() *exec.WorkerContext {
	select {
	case w := <-pe.runSem:
		return w
	case <-pe.workersCtx.Done():
		return nil
	default:
	}
	return pe.newExecWorker()
}

// releaseWorker returns a context to the pool for reuse. The pool buffer is sized
// to the elastic peak, so this never blocks; excess is reclaimed at teardown.
func (pe *parallelExecutor) releaseWorker(w *exec.WorkerContext) {
	// Merge this run's reads into sd.metrics before the worker returns to the pool,
	// so the slow-block emitter sees them. Off the critical path (the result is
	// already sent) and safe: RunTxTask has returned, so readMetrics is quiescent.
	w.PublishReadMetrics()
	select {
	case pe.runSem <- w:
	default:
	}
}

func (pe *parallelExecutor) execLoop(ctx context.Context) (err error) {
	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("sub", "exec-loop")))
	// The exec loop owns shutdown sequencing: on exit it closes commitResults then
	// applyResults, draining the calculator and apply loop. It owns the workers'
	// inner context too — whatever exit path it takes, the workers must not outlive it.
	defer pe.cancelWorkers()
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
			// Propagate the panic as the loop's error; otherwise execLoopGroup.Wait
			// returns nil and the apply-loop heuristics can mistake it for a resumable
			// partial batch and spin. A loop panic is an internal invariant failure,
			// not a consensus-invalid block, so surface it as a plain error.
			if err == nil {
				err = fmt.Errorf("exec loop panic: %v", rec)
			}
		} else if err != nil && !errors.Is(err, context.Canceled) {
			pe.logger.Warn("["+pe.logPrefix+"] exec loop error", "err", err)
		} else {
			pe.logger.Debug("[" + pe.logPrefix + "] exec loop exit")
		}
	}()

	pe.RLock()
	applyTx := pe.applyTx
	pe.RUnlock()

	// sizeCutPending: on a size-limit cut, execute one more block so state catches
	// up to any block the fold computed ahead, then stop where state and commitment
	// agree.
	sizeCutPending := false

	// np-phase exec-loop attribution: wall spent waiting for the next in-order
	// result vs serial per-tx processing. Reset per completed block.
	var npWait, npProc time.Duration
	var npWaitStart, npProcStart time.Time

	for {
		if applyTx, err = pe.refreshApplyTx(ctx, applyTx); err != nil {
			return err
		}

		// Bound the blocks pending in pe.blockExecutors: processRequest is
		// non-blocking, so without this execRequests drains instantly and the map
		// grows unbounded, holding all decoded TxTask objects in RAM. A nil pendingCh
		// skips that select case, applying backpressure to executeBlocks.
		const maxPendingBlocks = 32
		pe.RLock()
		pendingBlocks := len(pe.blockExecutors)
		pe.RUnlock()
		var pendingCh chan *execRequest
		if pendingBlocks < maxPendingBlocks {
			pendingCh = pe.execRequests
		}

		if logNpPhases {
			npWaitStart = time.Now()
		}
		var blockResult *blockResult
		select {
		case exec := <-pendingCh:
			if err := pe.processRequest(ctx, exec); err != nil {
				return err
			}
			continue
		case <-ctx.Done():
			for {
				select {
				case txResult, ok := <-pe.results:
					if !ok {
						return pe.execLoopExitCheck(ctx, "ctx-done-drain: results closed")
					}
					br, e := pe.processSingleResult(ctx, applyTx, txResult)
					if e != nil {
						return e
					}
					if br != nil {
						pe.RLock()
						blockExecutor, exists := pe.blockExecutors[br.BlockNum]
						pe.RUnlock()
						if exists {
							pe.lastExecutedBlockNum.Store(int64(br.BlockNum))
							if err := blockExecutor.sendResult(ctx, br, false); err != nil {
								return err
							}
							if br.Err != nil {
								return nil
							}
							pe.Lock()
							delete(pe.blockExecutors, br.BlockNum)
							pe.Unlock()
							pe.scheduleNextPending(ctx)
						}
					}
				default:
					return pe.execLoopExitCheck(ctx, "ctx-done-drain: no more results")
				}
			}
		case txResult, ok := <-pe.results:
			if !ok {
				return pe.execLoopExitCheck(ctx, "main-select: results closed")
			}
			if logNpPhases {
				npWait += time.Since(npWaitStart)
				npProcStart = time.Now()
			}
			blockResult, err = pe.processSingleResult(ctx, applyTx, txResult)
		}
		if logNpPhases {
			npProc += time.Since(npProcStart)
		}

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
				pe.invalidCount.Add(int64(blockExecutor.cntValidationFail))
				pe.readCount.Add(blockExecutor.blockIO.ReadCount())
				pe.writeCount.Add(blockExecutor.blockIO.WriteCount())

				if !blockExecutor.execStarted.IsZero() {
					pe.blockExecMetrics.Duration.Add(time.Since(blockExecutor.execStarted))
					pe.blockExecMetrics.BlockCount.Add(1)
				}
				if logNpPhases {
					busy := time.Duration(blockExecutor.execCpuNanos.Load())
					wall := npWait + npProc
					var occ float64
					if wall > 0 && pe.workerCount > 0 {
						occ = float64(busy) / (float64(pe.workerCount) * float64(wall))
					}
					pe.logger.Info("[np-phase] execloop", "blk", blockResult.BlockNum,
						"wait", npWait, "process", npProc,
						"busy", busy, "workers", pe.workerCount, "occ", fmt.Sprintf("%.2f", occ),
						"tasks", len(blockExecutor.tasks), "exec", blockExecutor.cntExec,
						"spec", blockExecutor.cntSpecExec,
						"valFail", blockExecutor.cntValidationFail,
						"spineUsPerIter", fmt.Sprintf("%.1f", float64(npProc.Nanoseconds())/float64(max(1, blockExecutor.cntExec))/1e3))
					npWait, npProc = 0, 0
				}
				// Save the block's changeset by hash BEFORE sending the blockResult, so
				// the calculator can find it via GetChangesetByHash and record its
				// branch diffs into block N's CS. Saving after sendResult would let the
				// calculator race ahead, look up an unsaved CS, and leak branch deltas
				// into the next block's CS. ensureChangesetAccumulator covers an empty
				// block that created no accumulator via a tx-result.
				pe.ensureChangesetAccumulator(blockResult.BlockNum)
				if pe.currentChangeSet != nil {
					pe.domains().SavePastChangesetAccumulator(blockResult.BlockHash, blockResult.BlockNum, pe.currentChangeSet)
				}

				// Decide the stop BEFORE sending, so a terminal stop publishes the
				// stopCause before blockResult(M) crosses the channel: the calculator
				// then holds the coalesce block M before blockResult(M) opens the fold
				// gate for M+1, otherwise a fold could advance commitment past the state
				// exec stops at (orphan → wrong root on restart).
				terminal, startCatchup := false, false
				if blockResult.Err == nil {
					// AfterCommitment estimate in per-block mode (commitment already
					// computed); BeforeCommitment in batch mode.
					var sizeEst uint64
					if dbg.BatchCommitments {
						sizeEst = pe.rs.SizeEstimateBeforeCommitment()
					} else {
						sizeEst = pe.rs.SizeEstimateAfterCommitment()
					}
					batchLimit := pe.cfg.batchSize.Bytes()
					switch execLoopShouldExit(blockResult, sizeEst, batchLimit, pe.maxBlockNum, dbg.StopAfterBlock) {
					case execLoopExitMaxReached, execLoopExitExhausted, execLoopExitStopAfter:
						terminal = true
					case execLoopExitSizeLimit:
						// Catch-up only matters when a block may have been folded ahead;
						// with BAL-driven commitment off nothing folds, so cut at the budget.
						if dbg.BALDrivenCommitment && !sizeCutPending && blockResult.Exhausted == nil && blockResult.BlockNum < pe.maxBlockNum {
							startCatchup = true
						} else {
							terminal = true
						}
					}
					if terminal {
						kind := stopMoreWork
						if blockResult.BlockNum >= pe.maxBlockNum {
							kind = stopReachedMax
						}
						pe.cancelExecLoop(&stopCause{block: blockResult.BlockNum, kind: kind})
					}
				}

				// mustDeliver: a terminal stop may have just cancelled ctx, but
				// blockResult(M) must still reach the apply loop.
				if err := blockExecutor.sendResult(ctx, blockResult, terminal); err != nil {
					return err
				}
				pe.clearChangesetAccumulator()

				// Block-validity rejection: exit so we don't schedule the next block on
				// discarded state — the apply loop's Err is the canonical signal. No
				// cancel: cancelling would join context.Canceled onto the reported error.
				if blockResult.Err != nil {
					return nil
				}

				pe.Lock()
				delete(pe.blockExecutors, blockResult.BlockNum)
				pe.Unlock()

				if terminal {
					// The calculator drains commitResults on its own uncancelled ctx;
					// trigger the batch commitment, then deferred closeApplyChannels
					// closes commitResults → applyResults.
					pe.triggerBatchCommitment(ctx)
					return nil
				}
				if startCatchup {
					sizeCutPending = true
				}
				pe.scheduleNextPending(ctx)
			}

			// No need to wait for the apply loop before scheduling the next block: its
			// reads layer over the prev-block versionMap overlay until the apply loop
			// (the sole sd.mem writer) folds this block's state.
			pe.RLock()
			blockExecutor, ok = pe.blockExecutors[blockResult.BlockNum+1]
			pe.RUnlock()

			if ok {
				// Fast-path install of the next block's changeset accumulator, still in
				// the exec loop (single-writer); lazily installed on first apply otherwise.
				pe.ensureChangesetAccumulator(blockExecutor.blockNum)
				pe.onBlockStart(ctx, blockExecutor.blockNum, blockExecutor.blockHash)
				blockExecutor.execStarted = time.Now()
				blockExecutor.scheduleExecution(ctx, pe)
			}
		}
	}
}

// refreshApplyTx rolls back a stale local tx handle and (re)opens pe.applyTx
// if it was released, returning the tx this loop iteration should read through.
func (pe *parallelExecutor) refreshApplyTx(ctx context.Context, applyTx kv.TemporalTx) (kv.TemporalTx, error) {
	pe.Lock()
	defer pe.Unlock()
	if applyTx != pe.applyTx && applyTx != nil {
		applyTx.Rollback()
	}
	if pe.applyTx == nil {
		tx, err := pe.cfg.db.BeginTemporalRo(ctx) //nolint
		if err != nil {
			return applyTx, err
		}
		pe.applyTx = tx
		applyTx = tx
	}
	return applyTx, nil
}

func (pe *parallelExecutor) processRequest(ctx context.Context, execRequest *execRequest) (err error) {
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
				executor = newBlockExec(blockNum, execRequest.blockHash, execRequest.gasPool, execRequest.accessList, execRequest.consumers, execRequest.profile, execRequest.exhausted)
				// Set the coinbase once, before any worker runs, so self-loop workers
				// can read it during validation without racing the exec loop.
				if h := txTask.BlockHeader(); h != nil {
					executor.coinbase = accounts.InternAddress(h.Coinbase)
				}
				go executor.selfLoopWatchdog(pe.workersCtx)
			}
		}

		executor.tasks = append(executor.tasks, t)
		executor.results = append(executor.results, nil)
		executor.txIncarnations = append(executor.txIncarnations, 0)
		executor.execFailed = append(executor.execFailed, 0)

		executor.execTasks.pushPending(i)
		executor.validateTasks.pushPending(i)

		switch {
		case len(t.Dependencies()) > 0:
			for _, depTxIndex := range t.Dependencies() {
				// Dependencies() are versionMap TxIndexes; translate to task-index space.
				depTask := executor.taskIndexOf(depTxIndex)
				if depTask >= i {
					panic(fmt.Sprintf("[self-loop] block %d: task %d declares dependency on HIGHER task %d (dep TxIndex %d) — forward dependency",
						executor.blockNum, i, depTask, depTxIndex))
				}
				executor.execTasks.addDependency(depTask, i)
			}
			executor.execTasks.clearPending(i)
		case len(execRequest.accessList) != 0:
			// if we have an access list we can assume that all
			// writes are already in the shared memory map so
			// we can go ahead and schedule all tx jobs
			// optimistically without needing to worry about
			// clashes, this should signifigatly improve tx
			// concurrency
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
		scheduleable.execStarted = time.Now()
		scheduleable.scheduleExecution(ctx, pe)
	}

	return nil
}

// applyLoopMissingBlocks returns the blockNums in txResultBlocks that did not
// produce a corresponding blockResult — the per-block validator never fired for
// them and an invalid block could become canonical. Returns nil when every block
// whose tx-results arrived also produced a blockResult. Does NOT flag a short
// maxBlockNum: a size-limit-cut partial batch legitimately stops short and resumes
// via the stage loop's ErrLoopExhausted handling.
func applyLoopMissingBlocks(txResultBlocks, appliedBlocks map[uint64]struct{}) []uint64 {
	var missing []uint64
	for n := range txResultBlocks {
		if _, ok := appliedBlocks[n]; !ok {
			missing = append(missing, n)
		}
	}
	return missing
}

// failCandidate is the apply loop's running "worst" block-validity failure across
// the exec (blockResult.Err) and commit (ErrWrongTrieRoot) streams. Fold-ahead
// lets a commit failure for block N be observed before N's exec verdict, so the
// loop can no longer assume exec is seen first; the kept failure is chosen by
// block number, with exec outranking commit on the same block.
type failCandidate struct {
	err       error
	block     uint64
	blockHash common.Hash // implicated block's hash — used to mark the bad block on a !initialCycle wrong-root unwind
	exec      bool        // exec verdict (specific, authoritative) vs commit wrong-root (generic)
	set       bool
}

// consider merges a newly observed failure. The reported failure is the one at
// the earliest block; on the same block an exec verdict wins, because it carries
// the specific validation error while the commit side only sees the wrong root.
func (fc *failCandidate) consider(block uint64, blockHash common.Hash, exec bool, err error) {
	if !fc.set || block < fc.block || (block == fc.block && exec && !fc.exec) {
		fc.err, fc.block, fc.blockHash, fc.exec, fc.set = err, block, blockHash, exec, true
	}
}

// execLoopExitDecision is the result of evaluating the exec-loop's
// per-blockResult exit conditions. Values are ordered by precedence:
// later conditions only matter if no earlier one fired.
type execLoopExitDecision int

const (
	// execLoopContinue: keep processing — no exit condition met.
	execLoopContinue execLoopExitDecision = iota
	// execLoopExitSizeLimit: size estimate crossed the batch budget; partial-batch
	// flush path runs.
	execLoopExitSizeLimit
	// execLoopExitMaxReached: blockResult.BlockNum >= maxBlockNum; a stopReachedMax
	// cause makes the apply loop return nil (clean batch end).
	execLoopExitMaxReached
	// execLoopExitExhausted: executeBlocks dispatched its final blockResult with
	// .Exhausted set; without honoring this the exec loop parks forever.
	execLoopExitExhausted
	// execLoopExitStopAfter: dbg.StopAfterBlock crossed (debug only).
	execLoopExitStopAfter
)

// execLoopShouldExit evaluates the exec-loop's per-blockResult exit decision in
// priority order (size-limit > max-reached > exhausted > stop-after). Pure so the
// precedence is unit-testable: reordering silently changes which branch wins when
// two conditions overlap.
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

// applyLoopCloseIsClean reports whether an apply-loop close with no published
// stop cause is a clean end rather than a partial batch to resume. It is clean
// when the requested range was fully applied (lastBlockNum >= maxBlockNum) or
// when the loop executed nothing at all (no tx-results and no blockResult) —
// the range was already applied before this call, so there is no pending work.
func applyLoopCloseIsClean(lastBlockNum, maxBlockNum uint64, txResultCount int) bool {
	if lastBlockNum >= maxBlockNum {
		return true
	}
	return txResultCount == 0 && lastBlockNum == 0
}

// closeApplyChannels closes the apply-loop-bound channels in the required order:
// commitResults FIRST so the calculator drains and closes rootResults, then
// applyResults. The inverse order would let the apply loop exit while the
// calculator is still publishing, landing a trailing commitment write on a closed
// channel. Returns the close order (used by tests).
func (pe *parallelExecutor) closeApplyChannels() (closedOrder []string) {
	if pe.consumers == nil {
		return nil
	}
	return pe.consumers.close()
}

// execLoopExitCheck enforces the completeness invariant for the exec loop's clean
// exit paths: pe.blockExecutors must be drained. A non-empty map means a block was
// scheduled but never produced a blockResult (post-validation never fired, so an
// invalid block could be accepted) — surface it as a loud InvalidBlock error. The
// reason argument tags the call site in the failure log.
func (pe *parallelExecutor) execLoopExitCheck(ctx context.Context, reason string) error {
	// Only a deliberate stopCause exempts the pending-blocks completeness check;
	// an unrelated cancel (shutdown, parent cancel) with blocks still pending is a
	// genuine silent-miss and must surface.
	if _, ok := stopCauseOf(ctx); ok {
		return nil
	}
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

// scheduleNextPending starts the lowest-numbered block still queued in
// pe.blockExecutors. Called after a completed block is removed so a block enqueued
// by processRequest while the slot was busy gets scheduled — processRequest itself
// only schedules when the map was empty at insert time, so without this a block
// enqueued mid-flight would orphan in the map and never be validated.
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
	next.execStarted = time.Now()
	next.scheduleExecution(ctx, pe)
}

// processSingleResult routes one worker result to its block executor's
// nextResult.
func (pe *parallelExecutor) processSingleResult(ctx context.Context, applyTx kv.TemporalTx, txResult *exec.TxResult) (*blockResult, error) {
	if pe.cfg.syncCfg.ChaosMonkey && pe.enableChaosMonkey {
		chaosErr := chaos_monkey.ThrowRandomConsensusError(false, txResult.Version().TxIndex, pe.cfg.badBlockHalt, txResult.Err)
		if chaosErr != nil {
			log.Warn("Monkey in consensus")
			return nil, chaosErr
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

	return blockExecutor.nextResult(ctx, pe, txResult, applyTx)
}

func (pe *parallelExecutor) run(ctx context.Context) (context.Context, context.CancelCauseFunc, error) {
	// execRequests holds one entry per decoded block. A large buffer lets the
	// block-loader race far ahead of the apply loop, accumulating all decoded
	// transaction objects in memory; 128 keeps workers busy without that.
	pe.execRequests = make(chan *execRequest, 128)
	// Clear stale blockExecutors from a previous batch — leftovers after a "batch
	// full" exit would block the new batch's first block from being scheduled
	// (processRequest only schedules when the map is empty).
	pe.blockExecutors = nil

	pe.taskExecMetrics = exec.NewWorkerMetrics()
	pe.blockExecMetrics = newBlockExecMetrics()

	// execLoopCtx (outer) carries the stopCause signal and runs the exec loop.
	// workersCtx (inner, its child) runs the OCC workers, so the exec loop decides
	// when they halt via cancelWorkers rather than a worker sharing the controller's
	// context — the exit path calls cancelWorkers so workers can't outlive it.
	execLoopCtx, execLoopCtxCancel := context.WithCancelCause(ctx)
	pe.execLoopGroup, execLoopCtx = errgroup.WithContext(execLoopCtx)
	pe.cancelExecLoop = execLoopCtxCancel

	workersCtx, cancelWorkers := context.WithCancel(execLoopCtx)
	pe.cancelWorkers = cancelWorkers
	pe.workersCtx = workersCtx

	var err error
	pe.execWorkers, _, pe.stopWorkers, pe.waitWorkers, err = exec.NewWorkersPool(
		workersCtx, nil, true, pe.cfg.db, nil, nil, nil,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine,
		pe.workerCount+1, pe.taskExecMetrics, pe.cfg.dirs, pe.logger)

	if err != nil {
		return execLoopCtx, execLoopCtxCancel, err
	}

	pe.execLoopGroup.Go(func() error {
		_ = pe.resetWorkers(workersCtx, pe.rs, nil)
		// Hand the reset worker contexts to the dispatcher as a semaphore. The buffer
		// is oversized so the pool can grow elastically (acquireWorker mints extras
		// when workers park mid-EVM) and return without blocking.
		pe.runSem = make(chan *exec.WorkerContext, elasticWorkerCap)
		for _, w := range pe.execWorkers {
			pe.runSem <- w
		}
		slots := len(pe.execWorkers)
		pe.execSem = make(chan struct{}, slots)
		for range slots {
			pe.execSem <- struct{}{}
		}
		pe.results = make(chan *exec.TxResult, len(pe.execWorkers)*8)
		return pe.execLoop(execLoopCtx)
	})

	return execLoopCtx, func(cause error) {
		execLoopCtxCancel(cause)
		cancelWorkers()

		// Drain in-flight dispatch goroutines before tearing down worker contexts:
		// cancelWorkers unblocks any waiting on runSem, so no goroutine touches a
		// worker after teardown.
		pe.runWG.Wait()

		pe.stopWorkers()

		// Reclaim elastically-minted contexts (the base pool is torn down by
		// stopWorkers); each may hold its own roTx.
		pe.mintMu.Lock()
		for _, w := range pe.mintedWorkers {
			_ = w.ResetTx(nil)
		}
		pe.mintedWorkers = nil
		pe.mintMu.Unlock()

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
	BlockNum         uint64
	BlockTime        uint64
	BlockHash        common.Hash
	ParentHash       common.Hash
	StateRoot        common.Hash
	Err              error
	BlockGasUsed     uint64
	BlobGasUsed      uint64
	lastTxNum        uint64
	complete         bool
	isPartial        bool
	receiptsComplete bool
	ApplyCount       int
	TxIO             *state.VersionedIO
	Receipts         types.Receipts
	Stats            map[int]ExecutionStat
	Deps             *state.DAG
	AllDeps          map[int]map[int]bool
	Exhausted        *ErrLoopExhausted
	Header           *types.Header      // for accumulator.StartChange in apply loop
	Txs              types.Transactions // for accumulator.StartChange in apply loop

	// Exec window for newPayload wall attribution; the calculator pairs these with
	// its own commit-window timestamps to measure exec/commit overlap.
	execStartedAt time.Time
	execEndedAt   time.Time
}

type txResult struct {
	blockNum              uint64
	blockHash             common.Hash
	txNum                 uint64
	blockGasUsed          int64
	cumulativeBlobGasUsed uint64
	receipt               *types.Receipt
	logs                  []*types.Log
	traceFroms            map[accounts.Address]struct{}
	traceTos              map[accounts.Address]struct{}
	writes                state.WriteSetView
	rules                 *chain.Rules
	isFinalize            bool // block-end finalize writes — apply to sd.mem directly
}

// Block-STM model: workers own execution AND validation. Each worker flushes its
// writes to the versionMap speculatively (as estimate), validates its own read-set,
// and loops — parking on the commit-frontier signal until every read-dependency has
// committed — returning only a stable-valid result. The exec loop is a pure in-order
// commit loop: flush the result's writes as complete, run the coinbase/finalize
// sweep, broadcast the advanced frontier.
//
// Mid-flow dep-pause is the sole dependency mechanism: a read that observes an
// in-flight estimate pauses (via the IBS waitCommit hook) until that writer commits,
// then re-reads the committed value — execution continues in place instead of
// aborting. A paused worker holds its context (mid-EVM), so the context pool grows
// elastically (see acquireWorker) to avoid dependency starvation.

// elasticWorkerCap bounds the runSem buffer so the context pool can grow past the
// base worker count when workers park mid-EVM. A backstop, not a target: CPU
// concurrency is bounded by execSem.
const elasticWorkerCap = 4096

// dispatchRunSelfLoop runs one task on a worker context, looping until its result
// is stable-valid: execute, flush its writes to the versionMap so downstream workers
// read them speculatively, validate its own read-set, and — when valid but a
// read-dependency has not yet committed — park on the commit frontier and
// re-validate. Only a stable-valid result is sent to the exec loop, which commits it
// in order. The context is released while parked so dependencies can run.
func (pe *parallelExecutor) dispatchRunSelfLoop(be *blockExecutor, tv *taskVersion) {
	pe.runWG.Go(func() {
		// The roTx is bound to the execution slot, not the goroutine: opened on slot
		// acquire, rolled back on release. This bounds concurrent roTxs to the slot
		// count rather than the far larger parked-goroutine count, so idle parked
		// workers can't exhaust the MDBX read-tx limiter and deadlock the in-order task.
		var goRoTx kv.TemporalTx
		var w *exec.WorkerContext
		acquire := func() bool {
			w = pe.acquireWorker()
			return w != nil
		}
		// Execution-slot gate: held only while executing, released while resting
		// mid-EVM on a dependency, so a resting worker does not reduce concurrency.
		slotHeld := false
		releaseSlot := func() {
			if !slotHeld {
				return
			}
			if goRoTx != nil {
				goRoTx.Rollback()
				goRoTx = nil
			}
			pe.execSem <- struct{}{}
			slotHeld = false
		}
		// acquireSlot takes an execution slot and opens the slot's roTx. Binding the
		// roTx to the worker is the caller's job because the lock discipline differs:
		// the loop binds via ResetTx (lock-free), the mid-EVM waitCommit rebinds via
		// BindTxHeld (RunTxTask already holds the worker lock).
		acquireSlot := func() bool {
			select {
			case <-pe.execSem:
				slotHeld = true
			case <-pe.workersCtx.Done():
				return false
			}
			tx, err := pe.cfg.db.BeginTemporalRo(pe.workersCtx) //nolint:gocritic // slot-tied rotx: rolled back on slot release (releaseSlot), not a defer here — it outlives this function
			if err != nil {
				pe.execSem <- struct{}{}
				slotHeld = false
				return false
			}
			goRoTx = tx
			return true
		}
		defer releaseSlot()
		// Mid-flow dep-pause hook: a read observing an in-flight (estimate) write
		// releases this worker's slot, waits for that writer's task to commit,
		// reacquires a slot, and retries the read against the final value.
		tv.waitCommit = func(dep int) bool {
			releaseSlot()
			// dep is a versionMap TxIndex; translate to task-index frontier space.
			if !be.waitDep(tv.index, be.taskIndexOf(dep)) {
				return false
			}
			if !acquireSlot() {
				return false
			}
			// RunTxTask holds the worker lock across this mid-EVM rebind.
			return w.BindTxHeld(goRoTx) == nil
		}
		send := func(r *exec.TxResult) {
			select {
			case pe.results <- r:
			case <-pe.workersCtx.Done():
			}
		}
		// A worker must never vanish on a fatal condition (panic, or exhausted
		// incarnation budget) — the exec loop would wait forever for a result that
		// never arrives. Convert both into a fatal result so the block fails.
		sendFatal := func(err error) {
			send(&exec.TxResult{Task: tv, Err: err})
		}
		defer func() {
			if rec := recover(); rec != nil {
				sendFatal(fmt.Errorf("self-loop worker panic (block %d tx %d): %v\n%s",
					be.blockNum, tv.index, rec, dbg.Stack()))
			}
		}()
		if !acquire() {
			return
		}
		bumpInc := func() bool {
			tv.version.Incarnation++
			if tv.version.Incarnation > len(be.tasks)+8 {
				sendFatal(fmt.Errorf("%w: block %d tx %d exceeded the self-loop incarnation limit (%d) without a settled verdict",
					rules.ErrInvalidBlock, be.blockNum, tv.index, len(be.tasks)+8))
				return false
			}
			return true
		}
		// reExec is called holding NO context; it bumps the incarnation and reacquires
		// a context. Returns false on the incarnation limit or shutdown.
		reExec := func() bool {
			return bumpInc() && acquire()
		}
		// waitTo waits for the commit frontier to reach t. Returns false on shutdown.
		waitTo := func(t int) bool {
			return be.waitDep(tv.index, t)
		}
		var prevWrites *state.WriteSet
		for {
			// Whether this run executes against fully-committed state below it. Captured
			// BEFORE execution: the frontier is monotonic and finalized tasks never
			// un-commit, so if every lower task is committed now it stays so for the
			// whole run and an error is authoritative. Capturing after would
			// mis-classify a speculative error (frontier advanced mid-run) as genuine.
			finalExec := be.frontier() >= tv.index-1
			if !acquireSlot() {
				return
			}
			if err := w.ResetTx(goRoTx); err != nil {
				releaseSlot()
				return
			}
			// Invariant: each RUN of a tx uses a strictly ascending incarnation. Two
			// runs sharing one incarnation is the scheduling bug behind stale-flush
			// corruption.
			for {
				last := be.runInc[tv.index].Load()
				if int64(tv.version.Incarnation) <= last {
					panic(fmt.Sprintf("self-loop: tx %d run at incarnation %d <= last-run %d (two runs share an incarnation)",
						tv.index, tv.version.Incarnation, last))
				}
				if be.runInc[tv.index].CompareAndSwap(last, int64(tv.version.Incarnation)) {
					break
				}
			}
			result := w.RunTxTask(tv)
			releaseSlot()
			if result.Err != nil {
				pe.releaseWorker(w)
				if finalExec {
					send(result)
					return
				}
				if !waitTo(tv.index-1) || !reExec() {
					return
				}
				continue
			}
			// Intra-tx read-consistency verdict: the tx read an in-flight or
			// mid-execution-changed value, so it did not run against a settled
			// snapshot. Re-execute once that predecessor commits.
			if result.Dep >= 0 {
				pe.releaseWorker(w)
				if !waitTo(be.taskIndexOf(result.Dep)) || !reExec() {
					return
				}
				continue
			}
			be.selfLoopFlush(tv.version, result, prevWrites)
			prevWrites = result.TxOut
			pe.releaseWorker(w)

			valid, target, blocker := be.selfLoopEvaluate(tv, result)
			if !valid {
				if blocker > be.frontier() && !waitTo(blocker) {
					return
				}
				if !reExec() {
					return
				}
				continue
			}
			// Valid: wait for its dependencies to commit, then re-validate — a
			// dependency may have re-executed to a new value, forcing a re-exec;
			// otherwise the verdict is authoritative and we commit out of order.
			if !waitTo(target) {
				return
			}
			if v, _, _ := be.selfLoopEvaluate(tv, result); v {
				result.WorkerValidated = state.VersionValid
				result.WorkerBlocker = -1
				result.WorkerVerdictSet = true
				send(result)
				// Stay alive rather than exit: the committed-dependent re-check signals
				// slReexec (via a sticky flag, so a dropped wake is recovered on the next
				// one) if a later write invalidates us. slFin closes when we finalize
				// (never un-committed again), slDone on shutdown.
				reexec := false
				for {
					if be.slReexecFlag[tv.index].Swap(false) {
						reexec = true
						break
					}
					done := false
					select {
					case <-be.slReexec[tv.index]:
					case <-be.slFin[tv.index]:
						done = true
					case <-be.slDone:
						done = true
					}
					if done {
						break
					}
				}
				if !reexec {
					return
				}
				if !reExec() {
					return
				}
				continue
			}
			if !reExec() {
				return
			}
		}
	})
}

// selfLoopFlush publishes tv's writes to the versionMap, plus deletion of any key
// the previous incarnation wrote that this one dropped.
func (be *blockExecutor) selfLoopFlush(version state.Version, result *exec.TxResult, prevWrites *state.WriteSet) {
	if prevWrites != nil {
		for h := range prevWrites.AllHeaders() {
			if !result.TxOut.Has(h) {
				be.versionMap.Delete(h.Address, h.Path, h.Key, version.TxIndex, true)
			}
		}
	}
	// Flush as ESTIMATE (complete=false) so a downstream read observes the in-flight
	// dependency and pauses until this tx commits.
	be.versionMap.FlushVersionedWrites(result.TxOut, false, "")
}

// taskIndexOf maps a versionMap block-TxIndex into this block's dense task-list
// index space (what waitDep/committedFrontier use), offsetting by the block's first
// task TxIndex. Without it a dependency's park target lands in versionMap space and
// can exceed the frontier's reach on a partial block, deadlocking the worker.
func (be *blockExecutor) taskIndexOf(versionTxIndex int) int {
	return versionTxIndex - be.tasks[0].Version().TxIndex
}

// selfLoopEvaluate validates tv's read-set against the versionMap and, when valid,
// returns the commit target (task-index space) the tx must reach to be stable — the
// highest task it actually read from, so it can commit as soon as those deps commit,
// out of order rather than behind the whole linear prefix. valid=false forces
// re-exec; blocker is the highest stale writer to wait for.
func (be *blockExecutor) selfLoopEvaluate(tv *taskVersion, result *exec.TxResult) (valid bool, target int, blocker int) {
	blocker = -1
	v := be.versionMap.ValidateReadSet(tv.version.TxIndex, result.TxIn,
		func(rv, wv state.Version) state.VersionValidity {
			if rv != wv {
				if b := be.taskIndexOf(wv.TxIndex); b > blocker {
					// Invariant: a read can only be invalidated by a PREDECESSOR write.
					// A blocker >= this task is a forward dependency (a future write
					// invalidating a past read) — impossible in Block-STM; fail loud.
					if b >= tv.index {
						panic(fmt.Sprintf("[self-loop] block %d: task %d (TxIndex %d) invalidated by HIGHER task %d (writer TxIndex %d) — forward validation dependency",
							be.blockNum, tv.index, tv.version.TxIndex, b, wv.TxIndex))
					}
					blocker = b
				}
				return state.VersionInvalid
			}
			return state.VersionValid
		}, false, "")
	if v != state.VersionValid {
		if blocker == -1 {
			// Invalid but no Done writer named: the invalidation is an in-flight
			// ESTIMATE (MVReadResultDependency), which ValidateReadSet resolves without
			// calling checkVersion. Find the estimate's writer so the caller waits for
			// it — re-executing now would re-read the estimate and busy-loop.
			result.TxIn.RangeFullHeaders(func(a accounts.Address, p state.AccountPath, k accounts.StorageKey, _ state.ReadHeader) bool {
				rr := be.versionMap.ReadStatus(a, p, k, tv.version.TxIndex)
				if rr.Status() == state.MVReadResultDependency {
					if b := be.taskIndexOf(rr.DepIdx()); b > blocker && b < tv.index {
						blocker = b
					}
				}
				return true
			})
		}
		return false, -1, blocker
	}
	target = -1
	result.TxIn.RangeHeaders(func(_ state.AccountPath, hdr state.ReadHeader) bool {
		if hdr.Source != state.MapRead {
			return true
		}
		if t := be.taskIndexOf(hdr.Version.TxIndex); t >= 0 && t < tv.index && t > target {
			target = t
		}
		return true
	})
	return true, target, -1
}

// blockRequest is the commitment calculator's per-block heads-up, sent on its own
// channel ahead of the block's txResult/blockResult stream so it is never trapped
// behind a prior block's results. Carries the block identity and BAL (nil when none).
type blockRequest struct {
	blockNum  uint64
	blockHash common.Hash
	stateRoot common.Hash
	// firstTxNum/lastTxNum bound the block's txNum range. lastTxNum positions
	// asOfReader/ComputeCommitment for the fold; the pair lets the calculator detect
	// a step-boundary-crossing block, which is left to the incremental path (the
	// atomic fold emits no mid-block step-boundary checkpoint).
	firstTxNum uint64
	lastTxNum  uint64
	blockTime  uint64
	bal        types.BlockAccessList
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
	writes                state.WriteSetView
	cumulativeBlobGasUsed uint64
}

func (result *execResult) finalize(cumulativeGasUsed uint64, firstLogIndex uint32, engine rules.Engine, vm *state.VersionMap, stateReader state.StateReader) (*types.Receipt, state.ReadSet, *state.WriteSet, error) {
	task, ok := result.Task.(*taskVersion)

	if !ok {
		return nil, state.ReadSet{}, nil, fmt.Errorf("unexpected task type: %T", result.Task)
	}

	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex
	txIncarnation := task.Version().Incarnation

	txTrace := dbg.TraceTransactionIO &&
		(dbg.TraceTx(blockNum, txIndex) || dbg.TraceAccount(result.Coinbase.Handle()) || dbg.TraceAccount(result.ExecutionResult.BurntContractAddress.Handle()))

	if txTrace {
		tracePrefix := fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation)
		fmt.Println(tracePrefix, "finalize")
		defer fmt.Println(tracePrefix, "done finalize")
	}

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil, state.ReadSet{}, nil, nil
	}

	if txIndex < 0 || task.IsBlockEnd() {
		// System TXs use full IBS reconstruction (no worker path, so no fee split).
		// Strip coinbase/burnt since they may carry stale writes.
		txOut, _, _, _ := result.TxOut.StripBalanceWrite(result.Coinbase, result.TxIn)
		result.TxOut = txOut
		txOut, _, _, _ = result.TxOut.StripBalanceWrite(result.ExecutionResult.BurntContractAddress, result.TxIn)
		result.TxOut = txOut
		result.TxIn.Delete(result.Coinbase)
		result.TxIn.Delete(result.ExecutionResult.BurntContractAddress)
		return result.finalizeSystemTx(task, txTask, vm, stateReader)
	}

	return result.finalizeTx(task, txTask, cumulativeGasUsed, firstLogIndex, engine, vm, stateReader)
}

// finalizeSystemTx handles block-end and system TXs (txIndex < 0) via full
// IBS reconstruction. These are infrequent (1 per block) so the overhead is
// acceptable.
func (result *execResult) finalizeSystemTx(
	task *taskVersion,
	txTask *exec.TxTask,
	vm *state.VersionMap,
	stateReader state.StateReader,
) (*types.Receipt, state.ReadSet, *state.WriteSet, error) {
	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex
	txIncarnation := task.Version().Incarnation

	// Empty ReadSet so all reads go through the versionMap (which holds all prior TX
	// writes). result.TxIn may be stale if the system TX ran speculatively before the
	// regular TXs completed, returning pre-block values instead of the post-block
	// state syscalls (withdrawal/consolidation) need.
	ibs := state.New(state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader, txTask.Rules().IsAmsterdam))
	defer ibs.Close()
	ibs.SetTxContext(blockNum, txIndex)
	ibs.SetVersion(txIncarnation)
	ibs.SetVersionMap(vm)
	if err := ibs.ApplyVersionedWrites(result.TxOut); err != nil {
		return nil, state.ReadSet{}, nil, err
	}
	ibs.SetTrace(txTask.Trace)

	writes := ibs.FinalizedWrites(txTask.Rules())
	return nil, ibs.VersionedReads(), writes, nil
}

func (result *execResult) calcFees(
	task *taskVersion,
	vm *state.VersionMap,
	stateReader state.StateReader,
	chainRules *chain.Rules,
) (*state.WriteSet, error) {
	txIndex := task.Version().TxIndex
	taskVersion := task.Version()

	// Read at txIndex (floor txIndex-1): strictly prior tx, excluding this tx's own
	// prior incarnations that would double-apply the tip on re-execution. Current-tx
	// worker writes are picked up below via TxOut.
	vsReader := state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader, chainRules.IsAmsterdam)

	coinbaseAcc, err := vsReader.ReadAccountData(result.Coinbase)
	if err != nil {
		return nil, err
	}
	// The tip credits only Balance, so seed the coinbase's whole-account origin into
	// the versionMap — else the apply compose has no base and wipes its committed
	// nonce/code/balance.
	state.SeedOrigin(vm, result.Coinbase, coinbaseAcc)
	var newCoinbaseBalance uint256.Int
	if coinbaseAcc != nil {
		newCoinbaseBalance = coinbaseAcc.Balance
	}
	burntAddr := result.ExecutionResult.BurntContractAddress
	hasBurnt := !burntAddr.IsNil()
	var newBurntBalance uint256.Int
	var burntAcc *accounts.Account
	if hasBurnt {
		burntAcc, err = vsReader.ReadAccountData(burntAddr)
		if err != nil {
			return nil, err
		}
		state.SeedOrigin(vm, burntAddr, burntAcc)
		if burntAcc != nil {
			newBurntBalance = burntAcc.Balance
		}
	}
	// The worker writes coinbase/burnt to TxOut when sender matches. Track Nonce /
	// CodeHash alongside Balance so the EIP-161 empty-removal check below sees the
	// worker's post-write coinbase state, not the stale pre-tx snapshot.
	coinbaseNonce := uint64(0)
	coinbaseHasCodeHashWrite := false
	if coinbaseAcc != nil {
		coinbaseNonce = coinbaseAcc.Nonce
	}
	coinbaseEmptyCodeHash := coinbaseAcc == nil || coinbaseAcc.IsEmptyCodeHash()
	coinbaseSelfdestructed := false
	coinbaseCreatedContract := false
	cbOverride := false
	if bw, ok := result.TxOut.GetBalance(result.Coinbase); ok {
		newCoinbaseBalance = bw.Val
		cbOverride = true
	}
	if nw, ok := result.TxOut.GetNonce(result.Coinbase); ok {
		coinbaseNonce = nw.Val
	}
	if _, ok := result.TxOut.GetCodeHash(result.Coinbase); ok {
		coinbaseHasCodeHashWrite = true
	}
	if sw, ok := result.TxOut.GetSelfDestruct(result.Coinbase); ok {
		coinbaseSelfdestructed = sw.Val
	}
	if cw, ok := result.TxOut.GetCreateContract(result.Coinbase); ok {
		coinbaseCreatedContract = cw.Val
	}
	burntOverride := false
	if hasBurnt {
		if bw, ok := result.TxOut.GetBalance(burntAddr); ok {
			newBurntBalance = bw.Val
			burntOverride = true
		}
	}
	oldCoinbaseBalance := newCoinbaseBalance
	// Before EIP-8246, burn the tip only for an actual SELFDESTRUCT of a contract coinbase.
	// DeleteAccount also emits SelfDestructPath=true for EIP-161 empty-removal of
	// a touched EOA coinbase, where the delayed tip must still be credited (it
	// re-creates the account) to match serial.
	coinbaseWasContract := !coinbaseEmptyCodeHash || coinbaseHasCodeHashWrite || coinbaseCreatedContract
	burnCoinbaseTip := !chainRules.IsAmsterdam && coinbaseSelfdestructed && coinbaseWasContract
	// A contract coinbase self-destructed with no same-tx re-create is net-absent:
	// its code/nonce are wiped and a later fee credit revives it balance-only
	// (EmptyCodeHash). coinbaseAcc still carries the stale live code, so the emit
	// below must materialize the revived-codeless account rather than propagate it.
	coinbaseNetDestructed := burnCoinbaseTip && !coinbaseCreatedContract
	if !burnCoinbaseTip {
		newCoinbaseBalance.Add(&newCoinbaseBalance, &result.ExecutionResult.FeeTipped)
	}
	oldBurntBalance := newBurntBalance
	if hasBurnt && chainRules.IsLondon {
		newBurntBalance.Add(&newBurntBalance, &result.ExecutionResult.FeeBurnt)
	}

	// cbOverride: a sender==coinbase tx's worker wrote the coinbase balance directly
	// (the gas debit). calcFees must then re-materialize the coinbase (Balance + its
	// AddressPath sibling) even when the net change is zero, else the worker's raw
	// gas-debit value is left behind and a later whole-account read gets a stale balance.
	emitCoinbase := newCoinbaseBalance != oldCoinbaseBalance || cbOverride

	addWrites := &state.WriteSet{}
	if emitCoinbase {
		{
			addWrites.SetBalance(result.Coinbase, &state.VersionedWrite[uint256.Int]{
				WriteHeader: state.WriteHeader{
					Address: result.Coinbase,
					Path:    state.BalancePath,
					Version: taskVersion,
					Reason:  tracing.BalanceIncreaseRewardTransactionFee,
				},
				Val: newCoinbaseBalance,
			})
			// Emit an AddressPath sibling so downstream txs reading this address see
			// an account record. Serial's AddBalance implicitly creates the account on
			// first credit; without mirroring that, getVersionedAccount returns nil for
			// a freshly-credited coinbase and Empty() returns true — charging a stale
			// CallNewAccountGas for a mid-tx CALL-with-value to the coinbase.
			addrAcc := &accounts.Account{Balance: newCoinbaseBalance}
			if coinbaseAcc != nil && !coinbaseNetDestructed {
				addrAcc.Nonce = coinbaseAcc.Nonce
				addrAcc.Incarnation = coinbaseAcc.Incarnation
				addrAcc.CodeHash = coinbaseAcc.CodeHash
			} else {
				addrAcc.CodeHash = accounts.EmptyCodeHash
				if !coinbaseNetDestructed {
					addrAcc.Nonce = coinbaseNonce
				}
			}
			addWrites.SetAddress(result.Coinbase, &state.VersionedWrite[*accounts.Account]{
				WriteHeader: state.WriteHeader{
					Address: result.Coinbase,
					Path:    state.AddressPath,
					Version: taskVersion,
				},
				Val: addrAcc,
			})
			// When the fee credit revives a coinbase absent from the committed domain,
			// a CodeHashPath read resolves from its own cell, not the AddressPath
			// account — so without this sibling it reads back NilCodeHash instead of
			// the revived account's EmptyCodeHash, and a later EXTCODEHASH is wrong. The
			// per-path revival check needs a CodeHashPath write strictly above the
			// destruct. Gated to revival cases: an unconditional emit would add a
			// spurious block-access-list entry for an ordinary fee credit.
			cbLifecycle, _, _ := vm.AccountLifecycleAt(result.Coinbase, txIndex)
			coinbaseRevivedCodeless := !chainRules.IsAmsterdam && cbLifecycle == state.LifecycleRevived && addrAcc.CodeHash == accounts.EmptyCodeHash
			if coinbaseAcc == nil || coinbaseNetDestructed || coinbaseRevivedCodeless {
				addWrites.SetCodeHash(result.Coinbase, &state.VersionedWrite[accounts.CodeHash]{
					WriteHeader: state.WriteHeader{
						Address: result.Coinbase,
						Path:    state.CodeHashPath,
						Version: taskVersion,
					},
					Val: addrAcc.CodeHash,
				})
			}
		}
	}
	if hasBurnt && (newBurntBalance != oldBurntBalance || burntOverride) {
		addWrites.SetBalance(burntAddr, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{
				Address: burntAddr,
				Path:    state.BalancePath,
				Version: taskVersion,
				Reason:  tracing.BalanceDecreaseGasBuy,
			},
			Val: newBurntBalance,
		})
		// Mirror the AddressPath emission above for the burnt address.
		burntAddrAcc := &accounts.Account{Balance: newBurntBalance}
		if burntAcc != nil {
			burntAddrAcc.Nonce = burntAcc.Nonce
			burntAddrAcc.Incarnation = burntAcc.Incarnation
			burntAddrAcc.CodeHash = burntAcc.CodeHash
		} else {
			burntAddrAcc.CodeHash = accounts.EmptyCodeHash
		}
		addWrites.SetAddress(burntAddr, &state.VersionedWrite[*accounts.Account]{
			WriteHeader: state.WriteHeader{
				Address: burntAddr,
				Path:    state.AddressPath,
				Version: taskVersion,
			},
			Val: burntAddrAcc,
		})
	}

	return addWrites, nil
}

func (result *execResult) finalizeTx(
	task *taskVersion,
	txTask *exec.TxTask,
	cumulativeGasUsed uint64,
	firstLogIndex uint32,
	engine rules.Engine,
	vm *state.VersionMap,
	stateReader state.StateReader,
) (*types.Receipt, state.ReadSet, *state.WriteSet, error) {
	// Engine post-apply message hook.
	if err := result.runPostApplyMessageOnMinIBS(task, txTask, engine, vm, stateReader); err != nil {
		return nil, state.ReadSet{}, nil, err
	}

	receipt, err := result.CreateReceipt(task.Version().TxIndex, cumulativeGasUsed+result.ExecutionResult.ReceiptGasUsed, firstLogIndex)
	if err != nil {
		return nil, state.ReadSet{}, nil, err
	}
	result.Receipt = receipt
	return receipt, state.ReadSet{}, nil, nil
}

// runPostApplyMessageOnMinIBS runs the engine's PostApplyMessage callback on a
// minimal IntraBlockState (the log buffer) and appends the emitted logs to
// result.Logs so they reach the receipt.
func (result *execResult) runPostApplyMessageOnMinIBS(
	task *taskVersion,
	txTask *exec.TxTask,
	engine rules.Engine,
	vm *state.VersionMap,
	stateReader state.StateReader,
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
	cbReader := state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader, chainRules.IsAmsterdam)
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
	ibs := state.New(state.NewVersionedStateReader(txIndex, result.TxIn, vm, stateReader, chainRules.IsAmsterdam))
	defer ibs.Close()
	ibs.SetTxContext(blockNum, txIndex)
	postApplyMessageFunc(ibs, message.From(), result.Coinbase, &execResult, chainRules)
	result.Logs = append(result.Logs, ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), blockNum, txTask.BlockHash())...)
	return nil
}

type taskVersion struct {
	*execTask
	version      state.Version
	versionMap   *state.VersionMap
	profile      bool
	stats        map[int]ExecutionStat
	statsMutex   *sync.Mutex
	execCpuNanos *atomic.Int64
	waitCommit   func(depTxIndex int) bool
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
	if ev.profile || logNpPhases {
		start = time.Now()
	}

	// Don't run post apply message during the state transition it is handled in finalize
	postApplyMessage := evm.Context.PostApplyMessage
	evm.Context.PostApplyMessage = nil
	defer func() { evm.Context.PostApplyMessage = postApplyMessage }()

	result = ev.execTask.Execute(evm, engine, genesis, ibs, stateWriter,
		chainConfig, chainReader, dirs, !ev.shouldDelayFeeCalc)

	// Occupancy accounting: sum every incarnation's exec CPU (including aborts) so
	// occupancy reveals whether workers are starved or compute-bound.
	if logNpPhases && ev.execCpuNanos != nil {
		ev.execCpuNanos.Add(time.Since(start).Nanoseconds())
	}

	// Carry the read-dependency verdict: >= 0 means the tx read an in-flight or
	// mid-execution-changed value and must re-execute once that predecessor commits
	// — a scheduler verdict, not an execution error.
	result.Dep = ibs.DepTxIndex()

	if result.Err != nil {
		return result
	}

	if ev.profile {
		end := time.Now()
		ev.statsMutex.Lock()
		ev.stats[ev.version.TxIndex] = ExecutionStat{
			TxIdx:       ev.version.TxIndex,
			Incarnation: ev.version.Incarnation,
			Duration:    end.Sub(start),
			StartNanos:  start.UnixNano(),
			EndNanos:    end.UnixNano(),
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
	// Point the per-task reader at this task's block: it reads finished-but-uncommitted
	// blocks < this one in front of the raw base.
	if r, ok := ibs.StateReader().(*state.PrevBlockReader); ok {
		r.SetBlock(ev.version.BlockNum)
	}
	ibs.SetNoMaterialize(true)
	ibs.SetVersion(ev.version.Incarnation)
	ibs.SetWaitCommit(ev.waitCommit)
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
	blockNum   uint64
	blockHash  common.Hash
	gasPool    *protocol.GasPool
	accessList types.BlockAccessList
	tasks      []exec.Task
	consumers  *resultStream
	profile    bool
	exhausted  *ErrLoopExhausted
}

type blockExecutor struct {
	sync.Mutex
	blockNum  uint64
	blockHash common.Hash

	tasks   []*execTask
	results []*execResult

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

	// Time records when the parallel execution starts
	begin time.Time

	// Enable profiling
	profile bool

	// Stats for debugging purposes
	cntExec, cntSpecExec, cntTotalValidations, cntValidationFail, cntFinalized int

	// finalizedResults stores the finalized execResult snapshot per TX, so the
	// publish loop can't see a different incarnation if be.results[tx] is overwritten
	// between finalize and publish.
	finalizedResults map[int]*execResult

	// blockExecutionGasUsed and blockStateGasUsed are tracked separately so
	// blockGasUsed = max(execution, state) matches EIP-8037 / EIP-7778 block-level
	// accounting and equals the builder's header.GasUsed via protocol.SetGasUsed.
	blockExecutionGasUsed uint64
	blockStateGasUsed     uint64
	blockGasUsed          uint64
	blobGasUsed           uint64
	gasPool               *protocol.GasPool

	execFailed []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	consumers *resultStream // fan-out to the apply loop + commitment calculator

	execStarted time.Time
	result      *blockResult
	applyCount  int
	exhausted   *ErrLoopExhausted

	finRevalChecks int64
	finRevalFires  int64

	// execCpuNanos sums exec CPU across ALL incarnations (NEWPAYLOAD_PHASES only)
	// for worker-occupancy attribution vs the npWait/npProc wall.
	execCpuNanos atomic.Int64

	// coinbase is the block's fee recipient, cached from the first tx result so
	// dependency-ordered validation can gate coinbase readers.
	coinbase accounts.Address

	// coinbaseFlushedUpTo is the contiguous tx prefix whose fee tips the calcFees
	// sweep has flushed to the versionMap. Coinbase readers gate on it; -1 == none.
	coinbaseFlushedUpTo int

	// writeChangedPrev holds the PREVIOUS write-set of a tx whose re-executed
	// write-set differs from the incarnation its dependents were validated against.
	// The old set is needed alongside the new one so a dependent that read a key the
	// new incarnation DROPPED is still re-checked.
	writeChangedPrev map[int]*state.WriteSet
	// readerIdx: reverse index (read cell -> reader task indices) so
	// revalidateCommittedDependents re-checks only actual readers of a changed tx's
	// keys, not every later committed task.
	readerIdx map[readerKey][]int

	// revalidate[tx]: since tx last passed validation, a write to a cell it read has
	// published, so it must be re-validated at the finalize boundary. A clean tx is
	// provably still valid there — every predecessor is final and any write to one of
	// its read cells went through markReadersDirty. Maintained only on the execLoop
	// (the single result-processing/finalize goroutine), like readerIdx, so no lock.
	revalidate map[int]bool

	// committedFrontier is the highest contiguous finalized task (== coinbaseFlushedUpTo).
	// Fan-out: each worker waits for its actual dependency target, not the linear
	// predecessor. wakeAt maps a frontier value to its waiters; finalizing task F
	// wakes exactly wakeAt[F] via their slWake channels (directed, no broadcast).
	// slDone closes on shutdown to release all.
	committedFrontier atomic.Int64
	slWake            []chan struct{}
	wakeMu            sync.Mutex
	wakeAt            map[int][]int
	// After committing, a self-loop worker stays parked on slReexec rather than
	// exiting: the committed-dependent re-check signals it to re-execute in place
	// (own monotonic incarnation, no respawn); slFin closes when the task finalizes.
	slReexec     []chan struct{}
	slReexecFlag []atomic.Bool
	// runInc[i] is the highest incarnation ever RUN for task i, enforcing that each
	// execution uses a strictly ascending incarnation. Init -1.
	runInc     []atomic.Int64
	slFin      []chan struct{}
	slDone     chan struct{}
	slDoneOnce sync.Once
	// selfLoopDispatched guards against re-dispatching a task the self-loop worker
	// already owns: the worker owns all re-execution, so the scheduler dispatches
	// each task exactly once. Touched only on the exec-loop goroutine.
	selfLoopDispatched map[int]bool
}

// readyForDepOrderValidation decides whether tx may be validated out of contiguous
// order: exec-complete and not already validated. The self-loop worker only sends a
// result once its read-set re-validates, so no dependency/coinbase gate is needed.
func (be *blockExecutor) readyForDepOrderValidation(tx int) bool {
	if !be.execTasks.checkComplete(tx) || be.validateTasks.checkComplete(tx) {
		return false
	}
	return true
}

// sendResult fans out an applyResult to every registered consumer. The
// backpressure, mustDeliver and closed-channel semantics live in the registry.
func (be *blockExecutor) sendResult(ctx context.Context, r applyResult, mustDeliver bool) error {
	return be.consumers.publish(ctx, r, mustDeliver)
}

func newBlockExec(blockNum uint64, blockHash common.Hash, gasPool *protocol.GasPool, accessList types.BlockAccessList, consumers *resultStream, profile bool, exhausted *ErrLoopExhausted) *blockExecutor {
	be := &blockExecutor{
		blockNum:            blockNum,
		blockHash:           blockHash,
		begin:               time.Now(),
		stats:               map[int]ExecutionStat{},
		finalizedResults:    map[int]*execResult{},
		blockIO:             &state.VersionedIO{},
		versionMap:          state.NewVersionMap(accessList),
		profile:             profile,
		consumers:           consumers,
		gasPool:             gasPool,
		exhausted:           exhausted,
		coinbaseFlushedUpTo: -1,
		writeChangedPrev:    map[int]*state.WriteSet{},
		readerIdx:           map[readerKey][]int{},
		revalidate:          map[int]bool{},
		selfLoopDispatched:  map[int]bool{},
		slDone:              make(chan struct{}),
	}
	be.committedFrontier.Store(-1)
	return be
}

// readerKey identifies one versioned read cell (address, path, storage key).
type readerKey struct {
	addr accounts.Address
	path state.AccountPath
	key  accounts.StorageKey
}

// indexReads records taskIdx as a reader of every cell in rs. Append-only across
// incarnations; the HasReadDep re-check filters stale entries, so over-inclusion
// only costs a redundant check.
func (be *blockExecutor) indexReads(taskIdx int, rs state.ReadSet) {
	rs.RangeFullHeaders(func(a accounts.Address, p state.AccountPath, k accounts.StorageKey, _ state.ReadHeader) bool {
		rk := readerKey{a, p, k}
		be.readerIdx[rk] = append(be.readerIdx[rk], taskIdx)
		return true
	})
}

// markReadersDirty flags every successor reader (> writerTx) of a cell writerTx
// just published as needing finalize-boundary re-validation: a value it read may
// have changed. Only successors can be invalidated by writerTx's write. Runs only
// on the execLoop, so the readerIdx read and revalidate write need no lock. A missed
// reader is unsafe; the finalize oracle (ERIGON_ASSERT) guards against that.
func (be *blockExecutor) markReadersDirty(writerTx int, ws *state.WriteSet) {
	if ws == nil {
		return
	}
	for h := range ws.AllHeaders() {
		for _, tx := range be.readerIdx[readerKey{h.Address, h.Path, h.Key}] {
			if tx > writerTx {
				be.revalidate[tx] = true
			}
		}
	}
}

// invalidBlockResult wraps a block-validity failure as a *blockResult carrying Err.
// Returning this (rather than (nil, err)) lets the apply loop see the block
// completed with a rejection rather than treating the dangling tx-results as a
// silent miss.
func (be *blockExecutor) invalidBlockResult(err error) *blockResult {
	return &blockResult{
		BlockNum:  be.blockNum,
		BlockHash: be.blockHash,
		Err:       err,
	}
}

// finalizeValidatedTx runs the in-order finalize tail for a validated tx: receipt
// cumulative-gas offsets, block-gas accounting, finalize, write normalization, and
// queueing for publish. stateReader is the loop-shared reader, lazily created here
// when nil. Returns a non-nil *blockResult (or error) when the block must be
// rejected; (nil, nil) on success.
func (be *blockExecutor) finalizeValidatedTx(pe *parallelExecutor, applyTx kv.TemporalTx, tx int, txTask exec.Task, txResult *execResult, txVersion state.Version, stateReader *state.StateReader) (*blockResult, error) {
	be.finalizedResults[tx] = txResult

	var cumulativeGasUsed uint64
	var firstLogIndex uint32
	// Receipt offsets only exist for real chain txs; other task types (tests) carry
	// no receipt.
	_, isChainTx := txTask.(*exec.TxTask)
	if isChainTx && txVersion.TxIndex > 0 && !txTask.IsBlockEnd() {
		if tx > 0 {
			// In-order finalization guarantees the previous regular tx already has its
			// receipt; a miss would persist corrupted offsets, so fail loudly.
			prevRes := be.finalizedResults[tx-1]
			if prevRes == nil || prevRes.Receipt == nil {
				return nil, fmt.Errorf("parallel exec: missing finalized receipt for tx %d (task %d) in block %d", txVersion.TxIndex-1, tx-1, be.blockNum)
			}
			cumulativeGasUsed = prevRes.Receipt.CumulativeGasUsed
			firstLogIndex = prevRes.Receipt.FirstLogIndexWithinBlock + uint32(len(prevRes.Receipt.Logs))
		} else {
			cumGasUsed, cumBlobGasUsed, logIndexAfterTx, err := rawtemporaldb.ReceiptAsOf(applyTx, txVersion.TxNum)
			if err != nil {
				return nil, err
			}
			cumulativeGasUsed = cumGasUsed
			firstLogIndex = logIndexAfterTx
			be.blobGasUsed = cumBlobGasUsed
		}
	}

	if txn := txTask.Tx(); txn != nil {
		regularContribution, stateContribution := protocol.InclusionContributions(txn.GetGasLimit(), txTask.Rules().IsAmsterdam)
		if err := protocol.CheckBlockGasInclusion(be.gasPool, regularContribution, stateContribution, txn.GetBlobGas()); err != nil {
			return be.invalidBlockResult(fmt.Errorf("%w: block gas used overflow at block=%d txIdx=%d: %w", rules.ErrInvalidBlock, be.blockNum, txVersion.TxIndex, err)), nil
		}
	}

	if err := be.gasPool.ConsumeExecution(txResult.ExecutionResult.BlockExecutionGasUsed); err != nil {
		return be.invalidBlockResult(fmt.Errorf("%w, block=%d: block execution gas overflow", rules.ErrInvalidBlock, be.blockNum)), nil
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

	if *stateReader == nil {
		if txTask.IsHistoric() {
			*stateReader = pe.prevBlockBase(state.NewHistoryReaderV3WithSharedDomains(applyTx, pe.domainsRead(), txTask.Version().TxNum), be.blockNum)
		} else {
			// finalize reads sd.mem for the committed base; the IBS's versionMap
			// composes the intra-block view.
			*stateReader = pe.prevBlockBase(state.NewReaderV3(pe.domainsRead().AsGetterNoMetrics(applyTx)), be.blockNum)
		}
	}

	_, addReads, finalizeWrites, err := txResult.finalize(cumulativeGasUsed, firstLogIndex, pe.cfg.engine, be.versionMap, *stateReader)
	if err != nil {
		return nil, err
	}
	addWrites := finalizeWrites

	// Merge any additional reads/writes produced during finalize (fee calc, post apply, etc)
	if addReads.Len() > 0 {
		existing := be.blockIO.ReadSet(txVersion.TxIndex)
		existing.MergeFrom(addReads)
		be.blockIO.RecordReads(txVersion, existing)
	}
	if !addWrites.IsEmpty() {
		// Merge finalization writes with existing execution writes.
		existingWrites := be.blockIO.WriteSet(txVersion.TxIndex)
		merged := MergeVersionedWrites(existingWrites, addWrites)
		be.blockIO.RecordWrites(txVersion, merged)

		// Flush the merged writes (including fee changes) so subsequent per-tx
		// finalizations see the full post-tx state via the versionMap fallback chain.
		be.versionMap.FlushVersionedWrites(merged, true, "")
		be.markReadersDirty(tx, merged)

		// Update CollectorWrites with fee-adjusted balances so the apply fold records
		// the correct accumulated fees.
		if !txResult.CollectorWrites.IsEmpty() {
			for addr, w := range addWrites.Balances() {
				if existing, ok := txResult.CollectorWrites.GetBalance(addr); ok {
					existing.Val = w.Val
					existing.Reason = w.Reason
				} else {
					txResult.CollectorWrites.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath, Reason: w.Reason}, Val: w.Val})
				}
			}
		}
	}

	{
		// A read-only versionMap-slice view over the tx's raw write-set; apply and
		// the calculator resolve each account's base from the versionMap.
		rawWrites := be.blockIO.WriteSet(txVersion.TxIndex)
		txResult.writes = state.NewVersionMapWriteView(rawWrites, be.versionMap, txVersion.TxIndex)
	}

	// Snapshot before pushing so the publish loop can't see a later incarnation if
	// be.results[tx] is overwritten by a concurrent worker.
	be.finalizedResults[tx] = txResult
	txResult.cumulativeBlobGasUsed = be.blobGasUsed
	be.publishTasks.pushPending(tx)
	return nil, nil
}

// advanceCoinbaseAndFinalize runs the in-order tail over the contiguous validated
// prefix not yet finalized. For each tx it computes the calcFees coinbase credit
// (flushing the tip and advancing coinbaseFlushedUpTo, which coinbase readers gate
// on), then runs the finalize tail. Both need the contiguous validated prefix:
// calcFees needs FeeTipped final, the finalize tail needs the predecessor's receipt.
func (be *blockExecutor) advanceCoinbaseAndFinalize(pe *parallelExecutor, applyTx kv.TemporalTx, stateReader *state.StateReader) (*blockResult, error) {
	maxValidated := be.validateTasks.maxComplete()
	for tx := be.coinbaseFlushedUpTo + 1; tx <= maxValidated; tx++ {
		// Use the validated snapshot, not be.results[tx]: a concurrent worker may
		// have overwritten be.results[tx] with a later incarnation between passes.
		txTask := be.tasks[tx].Task
		txResult := be.finalizedResults[tx]
		txVersion := txResult.Task.Version()

		// Authoritative re-validation at the finalize boundary: every predecessor
		// < tx is now final, so this is the last point a stale read can be caught.
		// Incremental: a tx untouched since it validated (no write to a cell it read
		// has published — tracked via markReadersDirty) is provably still valid here,
		// so only dirty txs are re-checked. Under ERIGON_ASSERT clean txs are checked
		// too and a failure panics, proving the dirty-tracking is complete. If stale,
		// un-commit and stop the sweep; the fixpoint loop re-executes it.
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() && txResult.Err == nil {
			dirty := be.revalidate[tx]
			if dirty || dbg.AssertEnabled {
				be.finRevalChecks++
				if be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
					func(rv, wv state.Version) state.VersionValidity {
						if rv != wv {
							return state.VersionInvalid
						}
						return state.VersionValid
					}, false, "") != state.VersionValid {
					if !dirty {
						panic(fmt.Sprintf("revalidate oracle: clean tx %d failed finalize re-validation", tx))
					}
					be.finRevalFires++
					be.validateTasks.clearComplete(tx)
					be.signalSelfLoopReexec(tx)
					break
				}
				delete(be.revalidate, tx)
			}
		}
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() && txResult.Err == nil {
			taskVer, ok := txResult.Task.(*taskVersion)
			if !ok {
				return nil, fmt.Errorf("apply loop: unexpected task type for tx %d: result.Task=%T", tx, txResult.Task)
			}
			if *stateReader == nil {
				if txTask.IsHistoric() {
					*stateReader = pe.prevBlockBase(state.NewHistoryReaderV3WithSharedDomains(applyTx, pe.domainsRead(), txTask.Version().TxNum), be.blockNum)
				} else {
					*stateReader = pe.prevBlockBase(state.NewReaderV3(pe.domainsRead().AsGetter(applyTx)), be.blockNum)
				}
			}
			tipWrites, err := txResult.calcFees(taskVer, be.versionMap, *stateReader, txTask.Rules())
			if err != nil {
				return nil, err
			}
			if !tipWrites.IsEmpty() {
				existingWrites := be.blockIO.WriteSet(txVersion.TxIndex)
				merged := MergeVersionedWrites(existingWrites, tipWrites)
				be.blockIO.RecordWrites(txVersion, merged)
				// Flush the tip as an Estimate; the whole tx is promoted to Done at the
				// seal point below.
				be.versionMap.FlushVersionedWrites(tipWrites, false, "")
				be.markReadersDirty(tx, tipWrites)
			}
		}
		be.coinbaseFlushedUpTo = tx
		// Promote the whole tx Estimate->Done and seal in one step. Done is granted
		// only at the seal frontier, so no committed-dependent re-exec can downgrade a
		// Done cell a reader already consumed, and a later write to a sealed cell trips
		// assertUnsealed. System/init and block-end txs are seal-exempt (final already).
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() {
			be.versionMap.MarkWritesComplete(be.blockIO.WriteSet(txVersion.TxIndex))
			be.versionMap.SealUpTo(txVersion.TxIndex)
		}

		if r, ferr := be.finalizeValidatedTx(pe, applyTx, tx, txTask, txResult, txVersion, stateReader); ferr != nil || r != nil {
			return r, ferr
		}
		be.signalCommitted(tx)
	}
	return nil, nil
}

// signalCommitted advances the commit frontier to tx and wakes exactly the tasks
// waiting for that frontier value — directed, no broadcast.
func (be *blockExecutor) signalCommitted(tx int) {
	be.wakeMu.Lock()
	be.committedFrontier.Store(int64(tx))
	waiters := be.wakeAt[tx]
	delete(be.wakeAt, tx)
	be.wakeMu.Unlock()
	for _, t := range waiters {
		select {
		case be.slWake[t] <- struct{}{}:
		default:
		}
	}
}

// signalSelfLoopReexec wakes the parked worker of an already-sent tx to re-execute
// in place (it owns its incarnation, so no re-dispatch).
func (be *blockExecutor) signalSelfLoopReexec(tx int) {
	be.slReexecFlag[tx].Store(true)
	select {
	case be.slReexec[tx] <- struct{}{}:
	default:
	}
}

func (be *blockExecutor) frontier() int {
	return int(be.committedFrontier.Load())
}

// waitDep blocks until the commit frontier reaches target (every task tx read from
// has committed), or shutdown. Registers tx under the wake lock so a concurrent
// signalCommitted(target) can't be missed. Returns false on shutdown.
func (be *blockExecutor) waitDep(tx, target int) bool {
	// A target beyond the last task can never be signalled — the worker would park
	// forever. That means a versionMap (block-TxIndex) dependency reached here
	// without a taskIndexOf translation; fail loud at the exact site.
	if target >= len(be.tasks) {
		panic(fmt.Sprintf("[self-loop] block %d: park target %d out of task range [-1,%d) for task %d — "+
			"a dependency reached waitDep in versionMap space (missing taskIndexOf translation); "+
			"block start TxIndex=%d", be.blockNum, target, len(be.tasks), tx, be.tasks[0].Version().TxIndex))
	}
	// Invariant: a task never waits for a HIGHER task — a forward dependency the
	// in-order frontier can never satisfy (deadlock). Fail loud.
	if target > tx {
		panic(fmt.Sprintf("[self-loop] block %d: task %d waiting for HIGHER target %d (forward dependency — invariant violation); "+
			"block start TxIndex=%d", be.blockNum, tx, target, be.tasks[0].Version().TxIndex))
	}
	for {
		be.wakeMu.Lock()
		if int(be.committedFrontier.Load()) >= target {
			be.wakeMu.Unlock()
			return true
		}
		be.wakeAt[target] = append(be.wakeAt[target], tx)
		be.wakeMu.Unlock()
		select {
		case <-be.slWake[tx]:
		case <-be.slDone:
			return be.frontier() >= target
		}
	}
}

// selfLoopWatchdog wakes every parked self-loop worker when the workers' context
// is cancelled, so shutdown/error never strands one in waitFrontier.
func (be *blockExecutor) selfLoopWatchdog(ctx context.Context) {
	done := func() { be.slDoneOnce.Do(func() { close(be.slDone) }) }
	<-ctx.Done()
	done()
}

// revalidateCommittedDependents re-checks changedTx's committed-but-not-published
// dependents against the current versionMap: a still-valid tx stays committed so
// maxValidated does not regress; one that now fails is un-committed and re-queued,
// cascading to its own dependents. Published txs are excluded (already streamed to
// commitment). A dependent is any committed tx reading a key in changedTx's OLD ∪
// NEW write-set — NEW for added/revalued keys, OLD for dropped ones. oldWrites is
// nil for a validation failure (no prior incarnation).
func (be *blockExecutor) revalidateCommittedDependents(changedTx int, oldWrites *state.WriteSet) *blockResult {
	// be.tasks / status lists are keyed by task index; be.blockIO by block-level
	// TxIndex. They differ by the block's leading system tx, so blockIO reads map
	// through be.tasks[i].Task.Version().TxIndex.
	newWrites := be.blockIO.WriteSet(be.tasks[changedTx].Task.Version().TxIndex)
	for _, tx := range be.revalCandidates(changedTx, newWrites, oldWrites) {
		if !be.validateTasks.checkComplete(tx) || be.publishTasks.checkComplete(tx) {
			continue
		}
		rs := be.blockIO.ReadSet(be.tasks[tx].Task.Version().TxIndex)
		hasDep := state.HasReadDep(newWrites, rs) || (oldWrites != nil && state.HasReadDep(oldWrites, rs))
		// A task past the coinbase/finalize frontier is final: un-committing it would
		// leave coinbaseFlushedUpTo ahead of maxValidated and the sweep could never
		// re-finalize it (permanent stall). The finalize→publish window makes this
		// reachable.
		if tx <= be.coinbaseFlushedUpTo {
			continue
		}
		if !hasDep {
			continue
		}
		txResult := be.finalizedResults[tx]
		if txResult == nil {
			continue
		}
		txVersion := txResult.Task.Version()
		if be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
			func(rv, wv state.Version) state.VersionValidity {
				if rv != wv {
					return state.VersionInvalid
				}
				return state.VersionValid
			}, false, "") == state.VersionValid {
			continue
		}
		be.cntValidationFail++
		be.execFailed[tx]++
		be.validateTasks.clearComplete(tx)
		// Signal the parked worker to re-execute in place; leave execTasks complete so
		// the re-sent result re-validates without going through the dispatch path.
		be.slReexecFlag[tx].Store(true)
		select {
		case be.slReexec[tx] <- struct{}{}:
		default:
		}
	}
	return nil
}

// revalCandidates returns the task indices > changedTx to re-check: the readers the
// reverse index records for changedTx's changed keys (new ∪ old write-sets), sorted
// ascending so the cascade order matches the scan.
func (be *blockExecutor) revalCandidates(changedTx int, newWrites, oldWrites *state.WriteSet) []int {
	set := map[int]struct{}{}
	add := func(ws *state.WriteSet) {
		if ws == nil {
			return
		}
		for h := range ws.AllHeaders() {
			for _, tx := range be.readerIdx[readerKey{h.Address, h.Path, h.Key}] {
				if tx > changedTx {
					set[tx] = struct{}{}
				}
			}
		}
	}
	add(newWrites)
	add(oldWrites)
	out := make([]int, 0, len(set))
	for tx := range set {
		out = append(out, tx)
	}
	sort.Ints(out)
	return out
}

// runDepOrderValidation is the dependency-ordered validation pass. It finalizes the
// contiguous validated-but-not-finalized prefix, then validates the dependency-ready
// txs out of order, committing each or cascading a re-validation of successors on
// failure. It does NOT use the contiguous VersionTooEarly gate (inherently in-order,
// would block every out-of-order tx); read stability for base reads is instead
// enforced by the write-change re-validation in nextResult.
func (be *blockExecutor) runDepOrderValidation(pe *parallelExecutor, applyTx kv.TemporalTx, stateReader *state.StateReader) (*blockResult, error) {
	// Run to a fixpoint: advancing the frontier at the end of a pass can unblock more
	// txs, which would otherwise wait for a worker result that may never arrive once
	// the pipeline drains (the dep-order hang). Loop until a pass validates nothing
	// and the frontier is stable.
	for {
		beforeCb := be.coinbaseFlushedUpTo
		if r, ferr := be.advanceCoinbaseAndFinalize(pe, applyTx, stateReader); ferr != nil || r != nil {
			return r, ferr
		}

		toValidate := be.validateTasks.takePendingWhere(func(t int) bool {
			return be.readyForDepOrderValidation(t)
		})

		for _, tx := range toValidate {
			be.cntTotalValidations++
			txResult := be.results[tx]
			txVersion := txResult.Task.Version()

			// Re-validate against the CURRENT versionMap, not the worker's verdict: a
			// predecessor may have changed or sealed since (including an in-flight
			// Validated cell the reader early-broke on), so trusting the stale verdict
			// would commit against a value that has moved.
			txResult.WorkerVerdictSet = false
			valid := be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
				func(rv, wv state.Version) state.VersionValidity {
					if rv != wv {
						return state.VersionInvalid
					}
					return state.VersionValid
				}, false, "") == state.VersionValid
			if dbg.TraceTransactionIO {
				be.versionMap.SetTrace(false)
			}

			if valid {
				// A regular OCC tx's writes go Validated here, not Done: a reader
				// continues on them (early break) but they stay revertible until the
				// in-order seal promotes them to Done. System and block-end txs are
				// seal-exempt and final now.
				if txVersion.TxIndex < 0 || be.tasks[tx].Task.IsBlockEnd() {
					be.versionMap.MarkWritesComplete(be.blockIO.WriteSet(txVersion.TxIndex))
				} else {
					be.versionMap.MarkWritesValidated(be.blockIO.WriteSet(txVersion.TxIndex),
						func(addr accounts.Address) bool { return addr == be.coinbase })
				}
				be.validateTasks.markComplete(tx)
				// Validated against current state; only a later write to one of its
				// read cells (markReadersDirty) re-flags it for finalize re-validation.
				delete(be.revalidate, tx)
				be.finalizedResults[tx] = txResult
				// This tx's writes are now flushed. A committed dependent may have
				// validated earlier against the pre-flush state (reading a key from base,
				// or against an older incarnation carried in writeChangedPrev), so
				// re-check dependents against both the new and old write-sets.
				prev, ok := be.writeChangedPrev[tx]
				if ok {
					delete(be.writeChangedPrev, tx)
				}
				if r := be.revalidateCommittedDependents(tx, prev); r != nil {
					return r, nil
				}
				continue
			}

			be.cntValidationFail++
			be.execFailed[tx]++
			if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
				fmt.Println(be.blockNum, "FAILED", tx, be.txIncarnations[tx], "failed", be.execFailed[tx])
			}
			be.validateTasks.clearInProgress(tx)
			if r := be.revalidateCommittedDependents(tx, nil); r != nil {
				return r, nil
			}
			// Signal the parked worker whose stale verdict we rejected to re-exec in
			// place; leave execTasks complete so the re-sent result re-validates
			// without going through the dispatch path.
			be.signalSelfLoopReexec(tx)
		}

		if r, ferr := be.advanceCoinbaseAndFinalize(pe, applyTx, stateReader); ferr != nil || r != nil {
			return r, ferr
		}
		// Fixpoint reached: this pass validated nothing and the frontier did not
		// advance, so no further validation can proceed without a fresh worker result.
		if len(toValidate) == 0 && be.coinbaseFlushedUpTo == beforeCb {
			return nil, nil
		}
	}
}

func (be *blockExecutor) nextResult(ctx context.Context, pe *parallelExecutor, res *exec.TxResult, applyTx kv.TemporalTx) (result *blockResult, err error) {
	task, ok := res.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", res.Task)
	}

	tx := task.index
	be.results[tx] = &execResult{TxResult: res}
	if res.Err != nil {
		// The worker sends an error only after executing against the full committed
		// prefix, so it is authoritative: the block is invalid. Surface through
		// blockResult.Err, not (nil, err) which would race the channel-close check.
		txVersion := res.Version()
		return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%d:%v]: %w", rules.ErrInvalidBlock, be.blockNum, txVersion.TxIndex, txVersion.TxNum, task.TxHash(), res.Err)), nil
	}

	txVersion := res.Version()

	be.blockIO.RecordReads(txVersion, res.TxIn)
	be.indexReads(tx, res.TxIn)
	// This tx's writes just published; flag any successor that already read one of
	// its cells for finalize-boundary re-validation.
	be.markReadersDirty(tx, res.TxOut)

	if res.Version().Incarnation == 0 {
		be.blockIO.RecordWrites(txVersion, res.TxOut)
	} else {
		prevWrites := be.blockIO.WriteSet(txVersion.TxIndex)
		hasWriteChange := res.TxOut.HasNewWrite(prevWrites)

		// Remove entries the previous incarnation wrote but this one dropped.
		for h := range prevWrites.AllHeaders() {
			if !res.TxOut.Has(h) {
				hasWriteChange = true
				be.versionMap.Delete(h.Address, h.Path, h.Key, txVersion.TxIndex, true)
			}
		}

		be.blockIO.RecordWrites(txVersion, res.TxOut)

		if hasWriteChange {
			// Defer dependent re-validation until this tx's new writes are
			// flushed during validation (they aren't in the versionMap yet).
			if _, ok := be.writeChangedPrev[tx]; !ok {
				be.writeChangedPrev[tx] = prevWrites
			}
		}
	}

	tracePrefix := fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)

	var trace bool
	if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
		fmt.Println(tracePrefix, "RD", be.blockIO.ReadSet(txVersion.TxIndex).Len(), "WRT", be.blockIO.WriteSet(txVersion.TxIndex).Count())
		be.blockIO.ReadSet(txVersion.TxIndex).TraceReads(tracePrefix)
		for h := range be.blockIO.WriteSet(txVersion.TxIndex).AllHeaders() {
			fmt.Println(tracePrefix, "WRT", h.String())
		}
	}

	be.validateTasks.pushPending(tx)
	// A re-executed self-loop result already has execTasks complete (only its
	// validation was cleared); markComplete would panic on the non-in-progress task,
	// so skip it and let the re-validation below re-commit it.
	if !be.execTasks.checkComplete(tx) {
		be.execTasks.markComplete(tx)
		be.execTasks.removeDependency(tx)
	}

	// do validations ...
	var stateReader state.StateReader

	if r, derr := be.runDepOrderValidation(pe, applyTx, &stateReader); derr != nil || r != nil {
		return r, derr
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
				blockHash:             be.blockHash,
				traceFroms:            map[accounts.Address]struct{}{},
				traceTos:              map[accounts.Address]struct{}{},
				txNum:                 task.Version().TxNum,
				rules:                 task.Rules(),
				cumulativeBlobGasUsed: result.cumulativeBlobGasUsed,
			}

			if result.Receipt != nil {
				// EIP-8037 / EIP-7778: block-level gas is max(cum execution, cum state),
				// NOT the sum of per-tx receipt gas — summing receipt gas (which carries
				// refunds and the post-Amsterdam FloorGasCost) bears no fixed relation to
				// header.GasUsed.
				be.blockExecutionGasUsed += result.ExecutionResult.BlockExecutionGasUsed
				be.blockStateGasUsed += result.ExecutionResult.BlockStateGasUsed
				be.blockGasUsed = max(be.blockExecutionGasUsed, be.blockStateGasUsed)
				// Per-tx contribution for progress / uncommittedGas tracking.
				applyResult.blockGasUsed = int64(result.Receipt.GasUsed)

				applyResult.receipt = result.Receipt.Copy()
				applyResult.logs = applyResult.receipt.Logs
				pe.executedGas.Add(int64(applyResult.blockGasUsed))
			}

			maps.Copy(applyResult.traceFroms, result.TraceFroms)
			maps.Copy(applyResult.traceTos, result.TraceTos)
			be.cntFinalized++
			be.publishTasks.markComplete(tx)
			// Published: the re-check no longer considers tx, so its parked worker can
			// never be re-signalled — release it. Closing earlier (at finalize) would
			// let a re-check in the finalize→publish window signal an exited worker.
			if tx >= 0 && tx < len(be.slFin) {
				close(be.slFin[tx])
			}

			pe.lastExecutedTxNum.Store(int64(applyResult.txNum))
			if result.writes != nil {
				applyResult.writes = result.writes
				be.applyCount += applyResult.writes.Count()
			}

			// The apply loop folds these versionMap views to sd.mem at block end,
			// keeping sd.mem at N-1 during exec so seedOrigin reads the committed base.
			if err := be.sendResult(ctx, &applyResult, false); err != nil {
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

		var blockReceipts types.Receipts
		for i := range be.results {
			// Prefer the finalized snapshot: a worker may overwrite be.results[i]
			// with a later receiptless incarnation after finalize set the receipt on
			// finalizedResults[i]. Falls back to be.results[i] when unfinalized.
			txResult := be.finalizedResults[i]
			if txResult == nil {
				txResult = be.results[i]
			}
			if receipt := txResult.Receipt; receipt != nil {
				blockReceipts = append(blockReceipts, receipt)
			}
		}

		var header *types.Header
		var txs types.Transactions
		if tt, ok := txTask.(*exec.TxTask); ok {
			header = tt.Header
			txs = tt.Txs
		}

		receiptsComplete := !isPartial
		if isPartial && be.blockNum > 0 && header != nil {
			startTxIndex := be.tasks[0].Version().TxIndex
			receiptsComplete = startTxIndex == 0
			if startTxIndex > 0 && len(txs) > 0 {
				blockStartTxNum := be.tasks[0].Version().TxNum - uint64(startTxIndex)
				priorReceipts, err := pe.reconstructPriorReceipts(ctx, applyTx, header, txs, startTxIndex, blockStartTxNum)
				if err != nil {
					pe.logger.Warn("["+pe.logPrefix+"] failed to reconstruct prior receipts for partial block",
						"block", be.blockNum, "startTxIndex", startTxIndex, "err", err)
				} else {
					blockReceipts = append(priorReceipts, blockReceipts...)
					receiptsComplete = true
				}
			}
			// The post-exec validator fills receipt blooms for full blocks but skips
			// partial ones — do it here (the suffix receipts still need blooms).
			receipts.DeriveFields(blockReceipts, be.blockHash)
		}

		// Block finalize: run engine.Finalize so finalize writes land in the
		// versionMap and the block writeset.
		var finalizeWrites state.WriteSetView
		if be.blockNum > 0 {
			lastResult := be.results[len(be.results)-1]
			finalTask := be.tasks[len(be.tasks)-1].Task
			finalVersion := finalTask.Version()

			pe.RLock()
			var reader state.StateReader
			if finalTask.IsHistoric() {
				// Historic finalize chains sd.mem → applyTx so withdrawals see prior-tx in-block writes.
				reader = pe.prevBlockBase(state.NewHistoryReaderV3WithSharedDomains(applyTx, pe.domainsRead(), finalVersion.TxNum), be.blockNum)
			} else {
				reader = pe.prevBlockBase(state.NewReaderV3(pe.domainsRead().AsGetterNoMetrics(applyTx)), be.blockNum)
			}
			pe.RUnlock()

			ibs := state.New(reader)
			defer ibs.Close()
			ibs.SetVersion(finalVersion.Incarnation)
			ibs.SetVersionMap(be.versionMap)
			ibs.SetTxContext(finalVersion.BlockNum, finalVersion.TxIndex)
			ibs.StartAccessRecording()

			if tt, ok := lastResult.Task.(*taskVersion).Task.(*exec.TxTask); ok {
				// Syscalls share the main ibs so their writes land in
				// ibs.VersionedWrites and reach finalizeWrites. A separate syscallIBS
				// would strand those writes and never feed them to the commitment
				// calculator, producing a wrong trie root when an EIP-7002/7251 SSTORE
				// changes a previously-untouched slot.
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
					pe.cfg.chainConfig, types.CopyHeader(tt.Header), ibs, tt.Uncles, blockReceipts,
					tt.Withdrawals, chainReader, syscall, false, pe.logger); err != nil {
					return be.invalidBlockResult(fmt.Errorf("%w: can't finalize block %d: %w", rules.ErrInvalidBlock, be.blockNum, err)), nil
				}

				be.blockIO.RecordReads(finalVersion, ibs.VersionedReads())

				ivw := ibs.FinalizedWrites(tt.Rules())
				if !ivw.IsEmpty() {
					be.blockIO.RecordWrites(finalVersion, ivw)
					be.versionMap.FlushVersionedWrites(ivw, true, "")
				}

				// A read-only versionMap-slice view over the finalize write-set.
				finalizeWrites = state.NewVersionMapWriteView(ivw, be.versionMap, finalVersion.TxIndex)
				be.applyCount += finalizeWrites.Count()
			}
		}

		// Send the finalize txResult; the apply loop folds its state writes at block end.
		if finalizeWrites != nil && !finalizeWrites.IsEmpty() {
			lastResult := be.results[len(be.results)-1]
			if err := be.sendResult(ctx, &txResult{
				blockNum:              be.blockNum,
				blockHash:             be.blockHash,
				txNum:                 txTask.Version().TxNum,
				rules:                 lastResult.Rules(),
				writes:                finalizeWrites,
				logs:                  lastResult.Logs,
				traceFroms:            lastResult.TraceFroms,
				traceTos:              lastResult.TraceTos,
				cumulativeBlobGasUsed: be.blobGasUsed,
				isFinalize:            true,
			}, false); err != nil {
				return nil, err
			}
		}

		// Block fully finalized: every tx sealed, block-end writes in the versionMap.
		// Publish it as an overlay so the next block reads its writes before apply
		// drains them to sd.mem; dropped on commit.
		pe.prevBlocks.PushHead(be.blockNum, txTask.Version().TxNum, be.versionMap)

		be.result = &blockResult{
			BlockNum:         be.blockNum,
			BlockTime:        txTask.BlockTime(),
			BlockHash:        txTask.BlockHash(),
			ParentHash:       txTask.ParentHash(),
			StateRoot:        txTask.BlockRoot(),
			BlockGasUsed:     be.blockGasUsed,
			BlobGasUsed:      be.blobGasUsed,
			lastTxNum:        txTask.Version().TxNum,
			complete:         true,
			isPartial:        isPartial,
			ApplyCount:       be.applyCount,
			TxIO:             be.blockIO,
			Receipts:         blockReceipts,
			receiptsComplete: receiptsComplete,
			Stats:            be.stats,
			Deps:             &deps,
			AllDeps:          allDeps,
			Exhausted:        be.exhausted,
			Header:           header,
			Txs:              txs,
			execStartedAt:    be.execStarted,
			execEndedAt:      time.Now(),
		}
		return be.result, nil
	}

	// Block not yet complete — the caller only acts on complete blockResults.
	return nil, nil
}

func (be *blockExecutor) scheduleExecution(ctx context.Context, pe *parallelExecutor) {
	if be.slWake == nil && len(be.tasks) > 0 {
		be.slWake = make([]chan struct{}, len(be.tasks))
		be.slReexec = make([]chan struct{}, len(be.tasks))
		be.slReexecFlag = make([]atomic.Bool, len(be.tasks))
		be.slFin = make([]chan struct{}, len(be.tasks))
		be.runInc = make([]atomic.Int64, len(be.tasks))
		for i := range be.slWake {
			be.slWake[i] = make(chan struct{}, 1)
			be.slReexec[i] = make(chan struct{}, 1)
			be.slFin[i] = make(chan struct{})
			be.runInc[i].Store(-1)
		}
		be.wakeAt = map[int][]int{}
	}
	// Drain deferred tx N when its blockers clear AND no worker at index < N is in
	// flight (whose floor writes must stay visible to N's re-read). Dependency-driven,
	// not the contiguous maxValidated gate, which deadlocks dependency-ordered
	// validation by regressing on a real invalidation.
	drainMinIP := be.execTasks.minInProgress()
	be.execTasks.drainDeferredIfReady(func(tx int) bool {
		return !be.execTasks.isBlocked(tx) && (drainMinIP < 0 || drainMinIP >= tx)
	})

	maxValidated := be.validateTasks.maxComplete()

	// dispatch drains pending, enqueuing each tx. Budget bounds only fresh
	// (incarnation 0) enqueues, which occupy an input-channel slot; retries go to the
	// unbounded retry heap. Txs that can't go now are held aside and re-added after
	// the loop so they aren't re-taken in the same call.
	dispatch := func() (dispatched int) {
		if be.execTasks.minPending() < 0 {
			return 0
		}
		budget := len(be.tasks)
		var holdBack sort.IntSlice
		for {
			nextTx := be.execTasks.minPending()
			if nextTx < 0 {
				break
			}
			incarnation := be.txIncarnations[nextTx]
			// A fresh tx needs a free input-channel slot. If none, leave it in pending
			// (peek, don't take): re-inserting the lowest index would be O(pending) churn.
			if incarnation == 0 && budget <= 0 {
				break
			}
			be.execTasks.takeNextPending()
			execTask := be.tasks[nextTx]
			isNextValidated := nextTx == maxValidated+1

			if !isNextValidated && incarnation > 0 {
				txIndex := execTask.Version().TxIndex
				if be.execTasks.isBlocked(nextTx) || !be.blockIO.HasReads(txIndex) ||
					be.versionMap.ValidateVersion(txIndex, be.blockIO,
						func(_, writtenVersion state.Version) state.VersionValidity {
							wi := writtenVersion.TxIndex + 1
							if wi >= 0 && wi < len(be.txIncarnations) &&
								writtenVersion.TxIndex < maxValidated &&
								writtenVersion.Incarnation == be.txIncarnations[wi] {
								return state.VersionValid
							}
							return state.VersionInvalid
						}, false, "") != state.VersionValid {
					holdBack = append(holdBack, nextTx)
					continue
				}
			}

			tv := &taskVersion{
				execTask:     execTask,
				versionMap:   be.versionMap,
				profile:      be.profile,
				stats:        be.stats,
				statsMutex:   &be.Mutex,
				execCpuNanos: &be.execCpuNanos,
			}

			// The worker owns its own re-execution loop, so the scheduler dispatches
			// each task exactly once; re-execution is signalled in place via slReexec.
			if be.selfLoopDispatched[nextTx] {
				be.cntExec++
				dispatched++
				continue
			}
			be.selfLoopDispatched[nextTx] = true
			version := execTask.Version()
			version.Incarnation = incarnation
			tv.version = version
			pe.dispatchRunSelfLoop(be, tv)
			budget--

			if !isNextValidated {
				be.cntSpecExec++
			}
			if dbg.TraceTransactionIO && be.txIncarnations[nextTx] > 1 {
				fmt.Println(be.blockNum, "EXEC", nextTx, be.txIncarnations[nextTx], "maxValidated", maxValidated, be.blockIO.HasReads(nextTx), "failed", be.execFailed[nextTx])
			}
			be.cntExec++
			dispatched++
		}
		for _, tx := range holdBack {
			be.execTasks.pushPending(tx)
		}
		return dispatched
	}

	// Forward-progress net: force-drain deferred only when nothing dispatched,
	// pending is empty, and nothing is in flight. Guarded on empty pending because
	// non-empty pending is always dispatchable and the net must not drain past it.
	if dispatch() == 0 && be.execTasks.minPending() < 0 && be.execTasks.inProgressCount() == 0 {
		be.execTasks.drainDeferred()
		dispatch()
	}
}

func MergeVersionedWrites(prev, next *state.WriteSet) *state.WriteSet {
	return prev.Merge(next)
}
