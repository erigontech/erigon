package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
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
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/diagnostics/metrics"
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
  - start Worker start workers

When rwLoop has nothing to do - it does Prune, or flush of WAL to RwTx (agg.rotate+agg.Flush)
*/

type parallelExecutor struct {
	txExecutor
	execWorkers []*exec.Worker
	stopWorkers func()
	waitWorkers func()
	// cancelExecLoop publishes the stopCause on the coordination context
	// (execLoopCtx). It is a SIGNAL that the exec loop, calculator and apply loop
	// each read to decide how to wind down. It cancels execLoopCtx and therefore
	// its child workersCtx too, but every publish site is ordered after the exec
	// loop has produced everything up to the coalesce block, so it never aborts an
	// in-flight block mid-work.
	cancelExecLoop context.CancelCauseFunc
	// cancelWorkers stops the OCC worker pool via workersCtx (a child of the
	// coordination context). It is the explicit, ordered halt the exec loop calls
	// once it has produced everything up to the coalesce block.
	cancelWorkers  context.CancelFunc
	in             *exec.QueueWithRetry
	rws            *exec.ResultsQueue
	workerCount    int
	blockExecutors map[uint64]*blockExecutor
	// applyResultsCh and commitResultsCh are set before execLoop starts.
	// The exec loop closes them on exit to signal the apply loop and
	// calculator to drain.
	applyResultsCh  chan applyResult
	commitResultsCh chan applyResult
	maxBlockNum     uint64 // set before execLoop; exec loop exits when reached
	// accumulator for txpool state-diff notifications; set before execLoop
	// starts so that AuRa system-call nonce changes are emitted per block.
	accumulator *shards.Accumulator
	// changesetAccumulator state owned by the exec loop. Accessing or mutating
	// this is the exec loop's responsibility — putting it here (rather than on
	// the apply-loop side) ensures all sd.mem mutations originate from a single
	// goroutine and avoids the data race between SetChangesetAccumulator
	// (apply loop) and ApplyStateWrites (exec loop, via SysCallContract for
	// block-end system calls) on SharedDomains.mem.
	// changesetWindowStart is the first block of the batch that must capture
	// a changeset (see changesetWindowStart in exec3.go); blocks below it run
	// without an accumulator.
	changesetWindowStart uint64
	currentChangeSet     *changeset.StateChangeSet
	// currentChangeSetBlock is the block number currentChangeSet belongs to
	// (0 == none). Tracked so ensureChangesetAccumulator can be a no-op when the
	// accumulator is already installed for the block whose writes are about to
	// be applied — making changeset capture robust against blocks scheduled out
	// of band (e.g. processRequest scheduling the first block of a new request
	// after the blockExecutors map went empty mid-batch, with no preceding
	// blockResult to trigger the install at the rotation site below).
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
// reads the same signal and decides how to wind down: exec produces state up to
// M then stops; the calculator caps fold-ahead at M and keeps computing to M on
// its own (uncancelled) context; the apply loop derives the commit boundary and
// stage return. A stopBadBlock cause aborts immediately.
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
	var s *stopCause
	if errors.As(context.Cause(ctx), &s) {
		return s, true
	}
	return nil, false
}

// ensureChangesetAccumulator makes pe.currentChangeSet point at a fresh,
// block-specific StateChangeSet before any of blockNum's sd.mem writes are
// applied. Idempotent. Exec-loop only — it mutates SharedDomains.mem via
// SetChangesetAccumulator, which must be single-writer (see the comment on
// currentChangeSet).
func (pe *parallelExecutor) ensureChangesetAccumulator(blockNum uint64) {
	if blockNum < pe.changesetWindowStart || blockNum == 0 || blockNum > pe.maxBlockNum {
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
	var (
		outHeader *types.Header
		outTx     kv.TemporalRwTx
		outErr    error
	)
	pprof.Do(ctx, pprof.Labels("phase", "pe-exec"), func(lctx context.Context) {
		outHeader, outTx, outErr = pe.execImpl(lctx, execStage, u, startBlockNum, offsetFromBlockBeginning,
			maxBlockNum, blockLimit, initialTxNum, inputTxNum, initialCycle, rwTx, stepsInDb, accumulator, readAhead, logEvery)
	})
	return outHeader, outTx, outErr
}

func (pe *parallelExecutor) execImpl(ctx context.Context, execStage *StageState, u Unwinder,
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
	// Only wire the BAL fold-ahead pipeline when BAL-driven commitment is on.
	// A nil channel leaves the per-block alloc+send and calculator select arm
	// inert (the receive on nil blocks forever, so the loop stays gated on cc.in).
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
		log.Info(fmt.Sprintf("[%s] parallel starting", execStage.LogPrefix()),
			"from", startBlockNum, "to", maxBlockNum, "limit", lastBlock, "initialTxNum", initialTxNum,
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
	defer executorCancel(nil)

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

	// The calculator installs its own asOfStateReader on the shared commitment
	// context; restore the prior reader on exit so it doesn't leak GetAsOf reads
	// into later foreground commitment reads (which break when the caller runs
	// with in-mem history reads disabled, e.g. offline re-exec).
	sdCtx := pe.rs.Domains().GetCommitmentContext()
	prevStateReader := sdCtx.StateReader()
	defer sdCtx.SetStateReader(prevStateReader)

	// Store channels and limits on pe so execLoop can access them.
	// blockRequests is intentionally not stashed here: it is closed by its
	// sole sender (the executeBlocks dispatch goroutine), not by execLoop —
	// closing it from execLoop would race the dispatch goroutine's send.
	pe.applyResultsCh = applyResults
	pe.commitResultsCh = commitResults
	pe.maxBlockNum = maxBlockNum

	// Configure changeset capture and seed the initial accumulator BEFORE
	// the exec loop / executeBlocks goroutines start touching sd.mem. The
	// exec loop owns all subsequent SetChangesetAccumulator transitions
	// (per-block save/clear/install) so apply-loop and exec-loop sd.mem
	// writes never race on SharedDomains.mem.
	pe.changesetWindowStart = changesetWindowStart(pe.cfg.syncCfg.AlwaysGenerateChangesets,
		pe.cfg.syncCfg.MaxReorgDepth, pe.cfg.blockReader.FrozenBlocks(), startBlockNum, maxBlockNum)
	pe.ensureChangesetAccumulator(startBlockNum)

	// Start the commitment calculator. It mirrors serial's per-block gate
	// (exec3_serial.go: `if !dbg.BatchCommitments || shouldGenerateChangesets
	// || KeepExecutionProofs`): blocks from the changeset window onward must
	// compute per-block — otherwise batch-mode dedupes branch updates across
	// the batch and flushes them all into one block's changeset, which fails
	// on subsequent reorgs. blockRequests feeds it BAL-declared block requests.
	// The calculator only publishes results; the apply loop is the sole
	// cancellation authority (it classifies errors and drives the single unwind).
	forcePerBlockCompute := pe.cfg.syncCfg.KeepExecutionProofs
	// workCtx (ctx) runs the calculator's roTx/compute/publish; signalCtx
	// (executorContext) carries the stopCause. Separating them lets a clean-stop
	// cancel signal the calculator without aborting an in-flight commitment.
	calculator, err := newCommitmentCalculator(ctx, executorContext, pe.rs.Domains(), pe.cfg.db, pe.cfg.chainConfig, pe.logPrefix, pe.logger, forcePerBlockCompute, pe.changesetWindowStart, commitResults, blockRequests, rootResults)
	if err != nil {
		return nil, nil, err
	}
	calculator.Start(ctx)
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

		// pe.changesetWindowStart and pe.currentChangeSet were set up
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

		// fail tracks the earliest block-validity failure across the exec
		// (blockResult.Err) and commit (ErrWrongTrieRoot) streams. Block-
		// validation errors take precedence over trie-root mismatches on the
		// same block: a wrong error category breaks eest's validation taxonomy.
		// With fold-ahead a commit wrong-root can arrive before the block's exec
		// verdict, so it is recorded and surfaced only after applyResults closes
		// (once exec has had its say) — see failCandidate.consider.
		var fail failCandidate
		// finalized flips once the reported failure is decided (an exec verdict,
		// or exec cleanly passing the block a commit wrong-root was deferred on).
		// Remaining results are then drained without re-validation so a post-
		// cancel block can't mask the recorded failure.
		finalized := false

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
		var pendingAccumulatorWrites []*state.WriteSet

		// handleCommitResult processes a single commitment result from the
		// calculator. Defined here so both the blockResult handler and the
		// rootResults case in the main select can use it.
		// handleCommitResult classifies a commitment result. It performs NO
		// unwind side-effects: a wrong-root is only classified here and routed
		// through the fail/finalized machinery, so the reported failure and its
		// block hash are chosen after exec has had its say (under fold-ahead a
		// commit wrong-root can arrive before the block's exec verdict). The
		// actual unwind for a !initialCycle wrong-root happens at finalization
		// with the implicated block's own hash.
		handleCommitResult := func(cr commitmentResult) error {
			if cr.err != nil {
				// Lazy-load / ComputeCommitment errors from the calculator
				// don't wrap ErrWrongTrieRoot. Treating them as a wrong-root
				// would mark a valid block as bad and trigger an unwind that
				// throws away valid state. Fail fast instead and preserve the
				// original error in the message.
				if !errors.Is(cr.err, ErrWrongTrieRoot) {
					return fmt.Errorf("[%s] commitment: %w", pe.logPrefix, cr.err)
				}
				pe.logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x (%v)",
					pe.logPrefix, cr.blockNum, cr.rootHash, cr.err))
				return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, cr.blockNum)
			}
			pe.txExecutor.lastCommittedBlockNum.Store(cr.blockNum)
			pe.txExecutor.lastCommittedTxNum.Store(cr.txNum)
			return nil
		}

		// deliberateCancel is the light context-cancel — teardown (stopWorkers +
		// wait) stays with execImpl's deferred executorCancel so only the main
		// goroutine drives cleanup.
		deliberateCancel := func() {
			pe.cancelExecLoop(&stopCause{block: fail.block, kind: stopBadBlock, err: fail.err})
		}
		// processCommit records a commit failure into `fail`. Non-wrong-root
		// commit errors (lazy-load / compute) are infrastructure faults, so
		// fast-fail. A wrong-root is deferred so the block's own exec verdict can
		// supersede it — EXCEPT when exec has already applied the block: then its
		// verdict is in (this is an incremental, not fold-ahead, wrong-root), so
		// finalize and cancel eagerly rather than keep building on known-wrong
		// state. Fold-ahead wrong-roots arrive before the block is applied and so
		// still defer, with the cancel firing once exec cleanly applies the block.
		processCommit := func(cr commitmentResult) error {
			err := handleCommitResult(cr)
			if err == nil {
				return nil
			}
			fail.consider(cr.blockNum, cr.blockHash, false, err)
			if !errors.Is(err, ErrWrongTrieRoot) {
				// Infra fault (lazy-load / compute), not block-validity: report it
				// but do NOT return here — a bare return kills the apply loop while
				// the exec loop may be blocked on a mustDeliver send, wedging
				// shutdown. Record + cancel + keep draining (which unblocks that
				// send); fail.err surfaces at channel close.
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
					// the exec loop hits its size budget mid-batch it stops with a
					// stopMoreWork cause, the apply loop drops out via the
					// ErrLoopExhausted return below, and the stage loop resumes from
					// lastBlockResult+1 in a follow-up call. Each block still executes
					// exactly once across the two batches, so we deliberately do NOT
					// flag maxBlockNum-not-applied here.
					// Surface the earliest recorded failure ahead of the
					// missing-blocks check: a deliberate cancel manufactures a
					// missing-block condition that would otherwise mask it.
					//
					// A deferred commit wrong-root does its unwind here, not inline
					// at classification time — so a !initialCycle reorg marks the
					// bad block with the implicated block's OWN hash (fail.blockHash),
					// not whatever block exec had last applied when the wrong-root
					// arrived. initialCycle has no reorg: the error is fatal.
					if fail.set {
						if !fail.exec && errors.Is(fail.err, ErrWrongTrieRoot) && !initialCycle {
							if err := handleIncorrectRootHashError(fail.block, fail.blockHash, rwTx, pe.cfg, execStage, pe.logger, u); err != nil {
								return err
							}
							return nil
						}
						return fail.err
					}
					if missing := applyLoopMissingBlocks(txResultBlocks, appliedBlocks); len(missing) > 0 {
						return fmt.Errorf("%w: apply loop exited (lastBlockResult=%d maxBlockNum=%d) but %d block(s) had tx-results without a blockResult: %v",
							rules.ErrInvalidBlock, lastBlockResult.BlockNum, pe.maxBlockNum, len(missing), missing)
					}
					// The stop kind rides in the shared context's cause: stopReachedMax
					// is a clean batch end (nil); stopMoreWork is a partial batch to
					// resume next cycle (ErrLoopExhausted). stopBadBlock is handled by the
					// fail branch above.
					if sc, ok := stopCauseOf(executorContext); ok {
						switch sc.kind {
						case stopReachedMax:
							return nil
						case stopMoreWork:
							return &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult.BlockNum, Reason: "block batch is full"}
						}
					}
					// Fallback for exit paths that publish no cause: a single-block
					// fork-validation batch exits without a stopCause, and real
					// shutdown cancels with context.Canceled. A fully-applied
					// requested range is a clean end; otherwise there is more work.
					if lastBlockResult.BlockNum >= pe.maxBlockNum {
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
					// All ApplyStateWrites + ApplyTxIndexes run in the execLoop
					// (sole sd.mem writer). The apply loop here only collects
					// accumulator notifications and per-tx counters.
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
					// Apply loop is the canonical error-emission point for
					// block-validity rejections (insufficient funds, gas
					// overflow, finalize rejection, scheduler-exhausted
					// incarnations). The worker plumbs the diagnosis through
					// blockResult.Err via nextResult → processResults → the
					// exec loop's sendResult, then exits on its own. Record the
					// exec verdict (it wins its block over a commit wrong-root)
					// and keep draining so an earlier commit wrong-root still in
					// rootResults can supersede it; the earliest recorded failure
					// is returned at channel-close. No cancel here — the exec loop
					// self-exits after an errored block, and cancelling would join
					// context.Canceled onto the reported error.
					if applyResult.Err != nil {
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						pendingAccumulatorWrites = pendingAccumulatorWrites[:0]
						fail.consider(applyResult.BlockNum, applyResult.BlockHash, true, applyResult.Err)
						finalized = true
						continue
					}
					// failInfra routes an apply-loop infrastructure fault through
					// failCandidate (earliest-block-wins) + cancel, and keeps the loop
					// draining. Never bare-return from the apply loop while the exec
					// loop may sit in a terminal mustDeliver send on a full applyResults
					// — that strands closeApplyChannels and wedges pe.wait.
					failInfra := func(err error) {
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						fail.consider(applyResult.BlockNum, applyResult.BlockHash, true, err)
						finalized = true
						deliberateCancel()
					}
					// StartChange + NotifyAccumulator must both run in the apply
					// goroutine — keeps all accumulator access single-threaded
					// (avoids data race with the executor goroutine).
					// StartChange must come BEFORE NotifyAccumulator because it
					// initialises the latestChange entry that ChangeAccount etc. write into.
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

					// Cache flush happens in the execLoop (before blockResult is sent).
					// sd.mem already has all TX writes when we reach here.

					var blockValidatorWaiter *blockValidator
					if applyResult.BlockNum > 0 && !applyResult.isPartial { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
						checkBloom := !pe.cfg.vmConfig.StatelessExec && !pe.cfg.vmConfig.NoReceipts
						checkReceipts := checkBloom && pe.cfg.chainConfig.IsByzantium(applyResult.BlockNum)

						b, err := pe.cfg.blockReader.BlockByHash(ctx, rwTx, applyResult.BlockHash)

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

						// Spawn per-block validation in a goroutine — the result is
						// joined via Wait() below, after the other per-result work
						// has had a chance to run in parallel with validation.
						blockValidatorWaiter = newBlockValidator(pe.cfg.engine, applyResult.BlockGasUsed, applyResult.BlobGasUsed, checkReceipts, checkBloom, applyResult.Receipts,
							lastHeader, b.Transactions(), pe.cfg.chainConfig, pe.logger)

					}

					if applyResult.BlockNum > 0 && applyResult.receiptsComplete && !execStage.CurrentSyncCycle.IsInitialCycle && applyResult.Header != nil {
						pe.cfg.notifications.RecentReceipts.Add(applyResult.Receipts, applyResult.Txs, applyResult.Header)
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
						// Block-validity verdict from post-execution validation. Route it
						// through failCandidate (earliest-block-wins, exec supersedes a
						// commit wrong-root at the same block) and keep draining rather
						// than bare-returning — a bare return here would strand the exec
						// loop in a terminal mustDeliver send on a full applyResults and
						// wedge pe.wait. No cancel: mirror the blockResult.Err path.
						appliedBlocks[applyResult.BlockNum] = struct{}{}
						fail.consider(applyResult.BlockNum, applyResult.BlockHash, true, fmt.Errorf("%w, block=%d, %v", rules.ErrInvalidBlock, applyResult.BlockNum, err))
						finalized = true
						continue
					}

					if pe.cfg.chainConfig.IsAmsterdam(applyResult.BlockTime) || pe.cfg.experimentalBAL {
						if err = ProcessBAL(rwTx, lastHeader, applyResult.TxIO, pe.cfg.chainConfig.IsAmsterdam(applyResult.BlockTime), pe.cfg.experimentalBAL, pe.cfg.dirs.DataDir, pe.logger); err != nil {
							failInfra(err)
							continue
						}
					}

					// Mark this block as fully applied. The exit-completeness
					// check at channel-close compares this set against the
					// expected [startBlockNum, maxBlockNum] range to detect
					// "block silently missed".
					appliedBlocks[applyResult.BlockNum] = struct{}{}

					// If a commit wrong-root was deferred for this (or an earlier)
					// block, exec has now applied it cleanly — exec agrees the
					// block is valid, so the divergence is real. Finalize on that
					// earliest block and stop dispatching further work.
					if fail.set && !fail.exec && applyResult.BlockNum >= fail.block {
						finalized = true
						deliberateCancel()
					}

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
	execErr = reconcileExecAndWaitErr(execErr, pe.wait(ctx))
	execErr = pe.checkBlocksDrained(ctx, execErr)

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
	// Send unconditionally: a terminal stop publishes the stopCause (cancelling
	// ctx) before this runs, but the calculator keeps draining commitResultsCh
	// until it's closed, so blocking is safe (a closed channel is caught by the
	// recover above). Honouring ctx.Done here would drop the batch-end commitment
	// when the buffer is momentarily full — the tail commitment then never
	// computes while stage progress advances, leaving commitment behind sd.mem.
	pe.commitResultsCh <- &commitComputeRequest{}
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
	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("sub", "exec-loop")))
	// The exec loop is the owner of shutdown sequencing. On exit it
	// closes commitResults then applyResults, causing the calculator
	// and apply loop to drain and exit.
	//
	// Note: pe.applyTx is the stageloop's rwTx (externally supplied).
	// Do NOT rollback it here — the stageloop owns its lifecycle.

	// The exec loop owns the workers' inner context: whatever exit path it takes
	// (clean stop, wrong-root drain, error), the workers must not outlive it.
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
	// up to any block the fold computed ahead, then stop at a boundary where state
	// and commitment agree. Under the current C=1 contiguous fold this is
	// scaffolding, not load-bearing: cause-before-send means B+1's fold gate never
	// opens past the terminal block B (blockResult(B) reaches the calculator only
	// after B's stop decision), so nothing is ever folded ahead of the cut and this
	// path only overshoots the batch budget by one block. It is kept for a future
	// explicit C>1 fold-ahead mode, where state would genuinely need to reach the
	// folded-ahead frontier before stopping.
	sizeCutPending := false

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
						return nil
					}
					if closed, err := pe.rws.Drain(ctx, nextResult); err != nil || closed {
						if err != nil {
							return err
						}
						return nil
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
							if err := blockExecutor.sendResult(ctx, blockResult, false); err != nil {
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
						}
					}
				default:
					return nil
				}
			}
		case nextResult, ok := <-pe.rws.ResultCh():
			if !ok {
				return nil
			}
			closed, err := pe.rws.Drain(ctx, nextResult)
			if err != nil {
				return err
			}
			if closed {
				return nil
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
				// In per-block compute mode (changeset window), the
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

				// Decide the stop BEFORE sending. A terminal stop publishes the
				// stopCause on the shared context before blockResult(M) crosses the
				// channel, so the calculator holds the coalesce block M by the time
				// blockResult(M) opens the fold gate for M+1 — otherwise a fold could
				// advance commitment past the state exec stops at (orphan → wrong root
				// on restart). The size cut still catches up: the first over-budget
				// block defers (produce one more so state reaches any block already
				// folded ahead), and its successor is the terminal stop.
				terminal, startCatchup := false, false
				if blockResult.Err == nil {
					// AfterCommitment estimate (2x) in per-block mode since commitment
					// is already computed; BeforeCommitment (4x) in batch mode.
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
						// with BAL-driven commitment off nothing folds, so cut at the
						// budget exactly like main instead of running one extra block.
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

				// mustDeliver: a terminal stop may have just published the stopCause
				// (cancelling ctx); blockResult(M) must still reach the apply loop.
				if err := blockExecutor.sendResult(ctx, blockResult, terminal); err != nil {
					return err
				}
				pe.clearChangesetAccumulator()

				// Block-validity rejection: the apply loop consumes blockResult and
				// returns its Err; the calculator skips the commitment compute. Exit
				// here so we don't schedule the next block on discarded state — the
				// apply loop's Err is the canonical signal. No cancel: exec self-exits
				// and cancelling would join context.Canceled onto the reported error.
				if blockResult.Err != nil {
					return nil
				}

				pe.Lock()
				delete(pe.blockExecutors, blockResult.BlockNum)
				pe.Unlock()

				if terminal {
					// commitResults is drained by the calculator on its own
					// uncancelled ctx; trigger the batch commitment, then the deferred
					// closeApplyChannels closes commitResults → applyResults.
					pe.triggerBatchCommitment(ctx)
					return nil
				}
				if startCatchup {
					sizeCutPending = true
				}
				pe.scheduleNextPending(ctx)
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
	// The state cache is a SharedDomain implementation detail: it is populated
	// only at flush (committed, fork-agnostic state) and invalidated only on
	// unwind (txNum/epoch — see StateCache.Unwind). The executor does not touch
	// it during forward execution.

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
// Does NOT flag a short maxBlockNum: a partial batch
// (size-limit hit) legitimately stops short of maxBlockNum, and the
// stage loop's ErrLoopExhausted handling resumes from the next block
// in a follow-up call. Flagging maxBlockNum here turns that legitimate
// path into a spurious InvalidBlock error — the BenchmarkFeeHistory
// 200-block fixture exhausts the 5MB batch budget at block 114 and
// previously errored despite blocks 1..114 being applied cleanly.
func applyLoopMissingBlocks(txResultBlocks, appliedBlocks map[uint64]struct{}) []uint64 {
	var missing []uint64
	for n := range txResultBlocks {
		if _, ok := appliedBlocks[n]; !ok {
			missing = append(missing, n)
		}
	}
	return missing
}

// applyLoopFlushAsComplete returns the `complete` flag for
// versionMap.FlushVersionedWrites. cntInvalid counts prior VersionTooEarly
// and VersionInvalid txs seen earlier in this iteration but excludes the
// current tx's own verdict, so the `valid` term is required to prevent an
// invalidated tx's writes being flushed as Done and read as committed by
// downstream OCC consumers.
func applyLoopFlushAsComplete(valid bool, cntInvalid int) bool {
	return valid && cntInvalid == 0
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

// wrapAsExecAbort wraps origErr in ErrExecAbortError unless it already is one,
// preserving the real origErr as OriginError instead of the zero-value an
// inline type-assertion would substitute on the failure branch.
func wrapAsExecAbort(origErr error, depTxIndex int) error {
	if _, ok := origErr.(protocol.ErrExecAbortError); ok {
		return origErr
	}
	return protocol.ErrExecAbortError{DependencyTxIndex: depTxIndex, OriginError: origErr}
}

// reconcileExecAndWaitErr combines the apply-loop result with the error from
// pe.wait. A canceled wait never overrides execErr: execImpl cancels the
// executor group on every exit, so a canceled wait is the normal end of a
// batch, not new information. A real wait error has no resumable work, so it
// supersedes ErrLoopExhausted — joining would keep errors.Is(_, ErrLoopExhausted)
// true and let a fatal exec error be retried silently forever.
func reconcileExecAndWaitErr(execErr, waitErr error) error {
	if waitErr == nil || errors.Is(waitErr, context.Canceled) {
		return execErr
	}
	if execErr == nil || errors.Is(execErr, &ErrLoopExhausted{}) {
		return waitErr
	}
	return errors.Join(execErr, waitErr)
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
	// the caller publishes a stopReachedMax cause so the apply loop
	// returns nil (clean batch end) rather than ErrLoopExhausted.
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
// unit-testable; the exec loop calls this and dispatches on the result.
//
// Priority order (matches production):
//  1. sizeEst > batchLimit         (size-limit batch flush — most urgent)
//  2. blockResult.BlockNum >= max  (clean end — stopReachedMax cause)
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
	// blockRequests is closed by its sole sender (the executeBlocks dispatch
	// goroutine), not here — closing it from this goroutine would race that
	// goroutine's send select and panic on "send on closed channel".
	if pe.applyResultsCh != nil {
		safeClose(pe.applyResultsCh, "applyResults")
		pe.applyResultsCh = nil
	}
	return
}

// checkBlocksDrained turns a clean apply-loop exit (execErr == nil) that left
// scheduled blocks undrained in pe.blockExecutors into an ErrInvalidBlock;
// leftover blocks on a resumable (ErrLoopExhausted) or canceled batch are
// expected and pass through.
func (pe *parallelExecutor) checkBlocksDrained(ctx context.Context, execErr error) error {
	if execErr != nil || ctx.Err() != nil {
		return execErr
	}
	pe.RLock()
	pending := slices.Collect(maps.Keys(pe.blockExecutors))
	pe.RUnlock()
	if len(pending) == 0 {
		return nil
	}
	slices.Sort(pending)
	return fmt.Errorf("%w: parallel exec apply loop finished cleanly but %d scheduled block(s) never drained: %v",
		rules.ErrInvalidBlock, len(pending), pending)
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

func (pe *parallelExecutor) run(ctx context.Context) (context.Context, context.CancelCauseFunc, error) {
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

	// execLoopCtx (outer) carries the stopCause signal and is where the exec loop
	// runs. workersCtx (inner) is its child: the OCC workers run on it so the exec
	// loop — the controller — decides when they halt via cancelWorkers, rather
	// than a worker sharing the controller's own context. The exec loop's exit
	// path must call cancelWorkers so the workers can't outlive the controller.
	execLoopCtx, execLoopCtxCancel := context.WithCancelCause(ctx)
	pe.execLoopGroup, execLoopCtx = errgroup.WithContext(execLoopCtx)
	pe.cancelExecLoop = execLoopCtxCancel

	workersCtx, cancelWorkers := context.WithCancel(execLoopCtx)
	pe.cancelWorkers = cancelWorkers

	var err error
	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers, err = exec.NewWorkersPool(
		workersCtx, nil, true, pe.cfg.db, nil, nil, nil, pe.in,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine,
		pe.workerCount+1, pe.taskExecMetrics, pe.cfg.dirs, pe.logger)

	if err != nil {
		return execLoopCtx, execLoopCtxCancel, err
	}

	pe.execLoopGroup.Go(func() error {
		defer pe.rws.Close()
		defer pe.in.Release()
		pe.resetWorkers(workersCtx, pe.rs, nil)
		return pe.execLoop(execLoopCtx)
	})

	return execLoopCtx, func(cause error) {
		execLoopCtxCancel(cause)
		cancelWorkers()

		pe.in.Release()
		pe.stopWorkers()

		_ = pe.wait(ctx)
	}, nil
}

// execWaitShutdownGrace bounds pe.wait once shutdown has begun: long enough
// for canceled goroutines to unwind and report a real failure, short enough
// that a stuck goroutine cannot hang process exit. Variable so tests can
// shorten it.
var execWaitShutdownGrace = 10 * time.Second

// wait joins the executor goroutines and classifies the outcome. A canceled
// group is routine teardown — execImpl cancels the executor on every exit,
// success included — so it is never reported as an error, while a real error
// is returned even when ctx is already shutting down. A canceled ctx only
// bounds the wait: after execWaitShutdownGrace a stuck group is abandoned so
// shutdown cannot hang.
func (pe *parallelExecutor) wait(ctx context.Context) error {
	doneCh := make(chan error, 1)

	go func() {
		if pe.execLoopGroup != nil {
			err := pe.execLoopGroup.Wait()
			pe.waitWorkers()
			if err != nil && !errors.Is(err, context.Canceled) {
				doneCh <- err
				return
			}
		}
		doneCh <- nil
	}()

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		select {
		case err := <-doneCh:
			return err
		case <-time.After(execWaitShutdownGrace):
			pe.logger.Warn(fmt.Sprintf("[%s] executor goroutines still running %s after shutdown; abandoning wait", pe.logPrefix, execWaitShutdownGrace))
			return nil
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
	blockStateCache  *state.BlockStateCache
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
	writes                *state.WriteSet
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
	// firstTxNum/lastTxNum bound the block's txNum range. lastTxNum (the block-end
	// system tx) positions asOfReader/ComputeCommitment for the fold; the pair lets
	// the calculator detect a block that crosses a step boundary — such a block is
	// left to the incremental path, since folding it would need a mid-block
	// step-boundary checkpoint the atomic fold doesn't emit.
	firstTxNum uint64
	lastTxNum  uint64
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
	writes                *state.WriteSet
	cumulativeBlobGasUsed uint64
}

func (result *execResult) finalize(cumulativeGasUsed uint64, firstLogIndex uint32, engine rules.Engine, vm *state.VersionMap, stateReader state.StateReader, stateWriter state.StateWriter) (*types.Receipt, state.ReadSet, *state.WriteSet, error) {
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

	rules := txTask.EvmBlockContext.Rules(txTask.Config)

	if txIndex < 0 || task.IsBlockEnd() {
		// System TXs use full IBS reconstruction — they don't go through
		// the worker execution path so fee splitting doesn't apply.
		// Strip coinbase/burnt for these since they may have stale writes.
		txOut, coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta := result.TxOut.StripBalanceWrite(result.Coinbase, result.TxIn)
		result.TxOut = txOut
		txOut, _, _, _ = result.TxOut.StripBalanceWrite(result.ExecutionResult.BurntContractAddress, result.TxIn)
		result.TxOut = txOut
		result.TxIn.Delete(result.Coinbase)
		result.TxIn.Delete(result.ExecutionResult.BurntContractAddress)
		_, _, _ = coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta
		return result.finalizeSystemTx(task, txTask, rules, vm, stateReader, stateWriter)
	}

	return result.finalizeTx(task, txTask, cumulativeGasUsed, firstLogIndex, engine, vm, stateReader)
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
) (*types.Receipt, state.ReadSet, *state.WriteSet, error) {
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
		return nil, state.ReadSet{}, nil, err
	}
	ibs.SetTrace(txTask.Trace)

	if err := ibs.FinalizeTx(rules, stateWriter); err != nil {
		return nil, state.ReadSet{}, nil, err
	}
	// Use checkDirty=false because FinalizeTx clears the journal (dirties map).
	// With checkDirty=true, all writes would be deleted from the versionMap
	// since no address appears dirty after the journal reset.
	return nil, ibs.VersionedReads(), ibs.VersionedWrites(false), nil
}

func (result *execResult) calcFees(
	task *taskVersion,
	vm *state.VersionMap,
	stateReader state.StateReader,
	chainRules *chain.Rules,
) (*state.WriteSet, error) {
	txIndex := task.Version().TxIndex
	taskVersion := task.Version()

	// Read at txIndex (floor txIndex-1) — strictly prior tx, excluding this tx's
	// own prior incarnations that would double-apply the tip on re-execution.
	// Worker writes for the current tx are picked up below via TxOut.
	vsReader := state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader)

	coinbaseAcc, err := vsReader.ReadAccountData(result.Coinbase)
	if err != nil {
		return nil, err
	}
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
		if burntAcc != nil {
			newBurntBalance = burntAcc.Balance
		}
	}
	// Worker writes coinbase/burnt to TxOut when sender matches (gas-debit
	// applied to sender under shouldDelayFeeCalc=true). Track Nonce / CodeHash
	// alongside Balance so the EIP-161 empty-removal check below sees the
	// worker's post-write coinbase state, not the stale pre-tx snapshot.
	coinbaseNonce := uint64(0)
	coinbaseHasCodeHashWrite := false
	if coinbaseAcc != nil {
		coinbaseNonce = coinbaseAcc.Nonce
	}
	coinbaseEmptyCodeHash := coinbaseAcc == nil || coinbaseAcc.IsEmptyCodeHash()
	coinbaseSelfdestructed := false
	if bw, ok := result.TxOut.GetBalance(result.Coinbase); ok {
		newCoinbaseBalance = bw.Val
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
	if hasBurnt {
		if bw, ok := result.TxOut.GetBalance(burntAddr); ok {
			newBurntBalance = bw.Val
		}
	}
	oldCoinbaseBalance := newCoinbaseBalance
	// Burn the tip only for an actual SELFDESTRUCT of a contract coinbase.
	// DeleteAccount also emits SelfDestructPath=true for EIP-161 empty-removal of
	// a touched EOA coinbase, where the delayed tip must still be credited (it
	// re-creates the account) to match serial.
	coinbaseWasContract := !coinbaseEmptyCodeHash || coinbaseHasCodeHashWrite
	if !(coinbaseSelfdestructed && coinbaseWasContract) {
		newCoinbaseBalance.Add(&newCoinbaseBalance, &result.ExecutionResult.FeeTipped)
	}
	oldBurntBalance := newBurntBalance
	if hasBurnt && chainRules.IsLondon {
		newBurntBalance.Add(&newBurntBalance, &result.ExecutionResult.FeeBurnt)
	}

	// EIP-161 empty-removal: even when the tip is zero (newBal == oldBal)
	// the coinbase must be "touched" so the commitment calculator sees the
	// empty-account delete. Matches serial executor's AddBalance(coinbase, 0)
	// → TouchAccount → MakeWriteSet emits a SelfDestructPath delete.
	//
	// Use the worker's post-write Nonce / CodeHash (not pre-tx coinbaseAcc) so
	// that a sender==coinbase tx whose worker wrote a non-empty Nonce isn't
	// mistakenly treated as empty here when FeeTipped==0.
	coinbaseEmptyRemoval := state.EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, result.Coinbase)
	// nil pre-state must not short-circuit to empty=true: a worker may
	// have already bumped Nonce or set CodeHash, and EIP-161 emptiness
	// must respect those writes — otherwise SelfDestructPath is emitted
	// and normalizeWriteSet's sdSet filter drops them.
	coinbaseEmptyPre := (coinbaseAcc == nil || coinbaseAcc.Balance.IsZero()) &&
		coinbaseNonce == 0 && coinbaseEmptyCodeHash && !coinbaseHasCodeHashWrite
	emitCoinbase := newCoinbaseBalance != oldCoinbaseBalance ||
		(coinbaseEmptyRemoval && coinbaseEmptyPre && newCoinbaseBalance.IsZero())

	addWrites := &state.WriteSet{}
	if emitCoinbase {
		result.CollectorWrites = result.CollectorWrites.SetAccountBalanceOrDelete(
			result.Coinbase, coinbaseAcc, newCoinbaseBalance,
			tracing.BalanceIncreaseRewardTransactionFee, coinbaseEmptyRemoval)
		if coinbaseEmptyRemoval && coinbaseEmptyPre && newCoinbaseBalance.IsZero() {
			addWrites.SetSelfDestruct(result.Coinbase, &state.VersionedWrite[bool]{
				WriteHeader: state.WriteHeader{
					Address: result.Coinbase,
					Path:    state.SelfDestructPath,
					Version: taskVersion,
				},
				Val: true,
			})
		} else {
			addWrites.SetBalance(result.Coinbase, &state.VersionedWrite[uint256.Int]{
				WriteHeader: state.WriteHeader{
					Address: result.Coinbase,
					Path:    state.BalancePath,
					Version: taskVersion,
					Reason:  tracing.BalanceIncreaseRewardTransactionFee,
				},
				Val: newCoinbaseBalance,
			})
			// Emit an AddressPath sibling write so downstream parallel txs
			// reading this address see an account record. Serial's AddBalance
			// implicitly creates the account on first credit; parallel calcFees
			// must mirror that, otherwise getVersionedAccount returns nil for
			// a freshly-credited coinbase (no pre-block storage entry, no
			// versionMap AddressPath) and Empty() returns true — charging the
			// stale CallNewAccountGas (+25000) for a CALL-with-value to the
			// coinbase mid-tx. Mainnet block 25151825 tx 31's SD+CREATE2-on-
			// coinbase MEV pattern surfaced this divergence.
			addrAcc := &accounts.Account{Balance: newCoinbaseBalance}
			if coinbaseAcc != nil {
				addrAcc.Nonce = coinbaseAcc.Nonce
				addrAcc.Incarnation = coinbaseAcc.Incarnation
				addrAcc.CodeHash = coinbaseAcc.CodeHash
			} else {
				addrAcc.Nonce = coinbaseNonce
				addrAcc.CodeHash = accounts.EmptyCodeHash
			}
			addWrites.SetAddress(result.Coinbase, &state.VersionedWrite[*accounts.Account]{
				WriteHeader: state.WriteHeader{
					Address: result.Coinbase,
					Path:    state.AddressPath,
					Version: taskVersion,
				},
				Val: addrAcc,
			})
		}
	}
	if hasBurnt && newBurntBalance != oldBurntBalance {
		result.CollectorWrites = result.CollectorWrites.SetAccountBalanceOrDelete(
			burntAddr, burntAcc, newBurntBalance,
			tracing.BalanceDecreaseGasBuy, state.EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, burntAddr))
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
	burntAddr := result.ExecutionResult.BurntContractAddress
	hasBurnt := !burntAddr.IsNil()

	// Engine post-apply message (e.g., AuRa system calls, EIP-7708 burn logs).
	if err := result.runPostApplyMessageOnMinIBS(task, txTask, engine, vm, stateReader, hasBurnt, burntAddr); err != nil {
		return nil, state.ReadSet{}, nil, err
	}

	receipt, err := result.CreateReceipt(task.Version().TxIndex, cumulativeGasUsed+result.ExecutionResult.ReceiptGasUsed, firstLogIndex)
	if err != nil {
		return nil, state.ReadSet{}, nil, err
	}
	result.Receipt = receipt
	return receipt, state.ReadSet{}, nil, nil
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
	cbReader := state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader)
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
		result.Err = wrapAsExecAbort(result.Err, ibs.DepTxIndex())
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
func (be *blockExecutor) sendResult(ctx context.Context, r applyResult, mustDeliver bool) (err error) {
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
	// mustDeliver (the terminal stop): the coordination ctx is already cancelled
	// by the stopCause published just before this send, but blockResult(M) MUST
	// still reach the apply loop (validation + progress) and the calculator — a
	// dropped M surfaces as a spurious ErrInvalidBlock. Both consumers are alive
	// and draining, so block unconditionally rather than honour ctx.Done; a
	// closed channel (batch shutdown) is caught by the recover above.
	if mustDeliver {
		be.applyResults <- r
		if be.commitResults != nil {
			be.commitResults <- r
		}
		return nil
	}
	// Data-arm-first on both channels: deliver while the buffer has room; only
	// honour ctx.Done if the buffer is full (avoids a deadlock when the consumer
	// is truly gone).
	select {
	case be.applyResults <- r:
	default:
		select {
		case be.applyResults <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if be.commitResults != nil {
		select {
		case be.commitResults <- r:
		default:
			select {
			case be.commitResults <- r:
			case <-ctx.Done():
				return ctx.Err()
			}
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
	be.results[tx] = &execResult{TxResult: res}
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

			// Remove entries that were previously written but are no longer
			// written — res.TxOut.Has answers membership directly, no cmp map.
			for h := range prevWrites.AllHeaders() {
				if !res.TxOut.Has(h) {
					hasWriteChange = true
					be.versionMap.Delete(h.Address, h.Path, h.Key, txVersion.TxIndex, true)
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
			fmt.Println(tracePrefix, "RD", be.blockIO.ReadSet(txVersion.TxIndex).Len(), "WRT", be.blockIO.WriteSet(txVersion.TxIndex).Count())
			be.blockIO.ReadSet(txVersion.TxIndex).TraceReads(tracePrefix)
			for h := range be.blockIO.WriteSet(txVersion.TxIndex).AllHeaders() {
				fmt.Println(tracePrefix, "WRT", h.String())
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
		txTask := be.tasks[tx].Task
		txResult := be.results[tx]
		// txResult.Task is the *taskVersion wrapper from this scheduled run,
		// carrying the current Incarnation. be.tasks[tx].Task is the bare
		// TxTask whose Version().Incarnation never advances past 0.
		txVersion := txResult.Task.Version()

		var trace bool
		var tracePrefix string

		if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)
		}

		// Credit tip pre-validate for regular TXs so the validator sees the
		// post-tip coinbase write. Caveat: the tip write is stamped at the
		// same (TxIndex, Incarnation) as the worker's coinbase write, so a
		// downstream tx that read coinbase via versionMap between the worker
		// write and the tip write records the same Version the validator
		// observes — the version-only validator will NOT catch that case.
		// In practice this is unusual: only sender==coinbase produces a
		// worker coinbase write, and downstream BALANCE(coinbase) reads
		// across this window are rare. Value-aware validation would close
		// the gap if it surfaces.
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() && txResult != nil && txResult.Err == nil {
			taskVer, ok := txResult.Task.(*taskVersion)
			if !ok {
				return nil, fmt.Errorf("apply loop: unexpected task type for tx %d: result.Task=%T", tx, txResult.Task)
			}
			if stateReader == nil {
				if txTask.IsHistoric() {
					stateReader = state.NewHistoryReaderV3WithBlockCache(applyTx, pe.rs.Domains(), be.blockStateCache, txTask.Version().TxNum)
				} else {
					stateReader = state.NewCurrentCachedReaderV3(pe.rs.Domains().AsGetter(applyTx), be.blockStateCache)
				}
			}
			tipWrites, err := txResult.calcFees(taskVer, be.versionMap, stateReader, txTask.Rules())
			if err != nil {
				return nil, err
			}
			if !tipWrites.IsEmpty() {
				existingWrites := be.blockIO.WriteSet(txVersion.TxIndex)
				merged := MergeVersionedWrites(existingWrites, tipWrites)
				be.blockIO.RecordWrites(txVersion, merged)
			}
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
		be.versionMap.FlushVersionedWrites(writeSet, applyLoopFlushAsComplete(valid, cntInvalid), tracePrefix)
		be.versionMap.SetTrace(false)

		if valid {
			if cntInvalid == 0 {
				be.validateTasks.markComplete(tx)

				be.finalizedResults[tx] = txResult

				var cumulativeGasUsed uint64
				var firstLogIndex uint32
				// Receipt offsets only exist for real chain txs — finalize()
				// skips receipt creation for other task types (tests), whose
				// results legitimately carry no receipt.
				_, isChainTx := txTask.(*exec.TxTask)
				if isChainTx && txVersion.TxIndex > 0 && !txTask.IsBlockEnd() {
					if tx > 0 {
						// In-order finalization guarantees the previous regular tx
						// already has its receipt; a miss means corrupted offsets
						// would be persisted, so fail loudly instead.
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
						stateReader = state.NewHistoryReaderV3WithBlockCache(applyTx, pe.rs.Domains(), be.blockStateCache, txTask.Version().TxNum)
					} else {
						// Use CachedReaderV3 with readCurrent=true so the
						// finalize (including system TXs) reads from the
						// BlockStateCache write buffer. This ensures the
						// system TX sees all accumulated state from prior
						// TXs in the block, not stale sd.mem values.
						stateReader = state.NewCurrentCachedReaderV3(pe.rs.Domains().AsGetterNoMetrics(applyTx), be.blockStateCache)
					}
				}

				collector := state.NewVersionedWriteCollector(pe.rs)

				_, addReads, finalizeWrites, err := txResult.finalize(cumulativeGasUsed, firstLogIndex, pe.cfg.engine, be.versionMap, stateReader, collector)
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

					// Flush the merged writes (including fee calc changes)
					// to the version map so that subsequent per-tx
					// finalizations see the full post-tx state (execution
					// + fees) when reading via the version map fallback
					// chain.
					be.versionMap.FlushVersionedWrites(merged, true, "")

					// Update CollectorWrites with fee-adjusted balances (coinbase /
					// burnt) so the BlockStateCache sees the correct accumulated fees.
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
					// Mirror txtask.go's genesis rules-clobber so empty allocs (AuRa ZeroAddress) survive.
					emptyRemoval := be.blockNum != 0 && pe.cfg.chainConfig.IsEIP161Enabled(be.blockNum)
					txResult.writes = normalizeWriteSet(rawWrites, be.versionMap, txVersion.TxIndex, resultIncarnation, stateReader, domainStorageKeys, emptyRemoval, pe.cfg.chainConfig.Aura != nil)
				}

				// Snapshot the finalized result before pushing — prevents
				// the publish loop from seeing a later incarnation if
				// be.results[tx] is overwritten by a concurrent worker.
				be.finalizedResults[tx] = txResult
				txResult.cumulativeBlobGasUsed = be.blobGasUsed
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
				blockHash:             be.blockHash,
				traceFroms:            map[accounts.Address]struct{}{},
				traceTos:              map[accounts.Address]struct{}{},
				txNum:                 task.Version().TxNum,
				rules:                 task.Rules(),
				cumulativeBlobGasUsed: result.cumulativeBlobGasUsed,
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
				be.applyCount += applyResult.writes.Count()
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
			if err := pe.rs.ApplyTxIndexes(applyTx, applyResult.txNum, applyResult.receipt, applyResult.cumulativeBlobGasUsed,
				applyResult.logs, applyResult.traceFroms, applyResult.traceTos); err != nil {
				return nil, fmt.Errorf("ApplyTxIndexes block=%d txNum=%d: %w", applyResult.blockNum, applyResult.txNum, err)
			}

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
		for _, txResult := range be.results {
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
			// The post-exec validator, which fills receipt blooms for full
			// blocks, skips partial ones — do it here, even when prior receipts
			// couldn't be reconstructed (the suffix receipts still need blooms).
			receipts.DeriveFields(blockReceipts, be.blockHash)
		}

		// Block finalize: run engine.Finalize + MakeWriteSet on the producer
		// side so finalize writes go to the BlockStateCache before the Flush.
		var finalizeWrites *state.WriteSet
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
				reader = state.NewCurrentCachedReaderV3(pe.rs.Domains().AsGetterNoMetrics(applyTx), be.blockStateCache)
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
					pe.cfg.chainConfig, types.CopyHeader(tt.Header), ibs, tt.Uncles, blockReceipts,
					tt.Withdrawals, chainReader, syscall, false, pe.logger); err != nil {
					return be.invalidBlockResult(fmt.Errorf("%w: can't finalize block %d: %v", rules.ErrInvalidBlock, be.blockNum, err)), nil
				}

				// syscallIBS == ibs unconditionally now; no separate write
				// propagation needed — syscall writes flow through
				// ibs.MakeWriteSet into finalizeWrites below.

				be.blockIO.RecordReads(finalVersion, ibs.VersionedReads())
				be.blockIO.RecordAccesses(finalVersion, ibs.AccessedAddresses())

				ivw := ibs.VersionedWrites(true)
				if !ivw.IsEmpty() {
					be.blockIO.RecordWrites(finalVersion, ivw)
					be.versionMap.FlushVersionedWrites(ivw, true, "")
				}

				collector := state.NewVersionedWriteCollector(pe.rs)
				if err := ibs.MakeWriteSet(tt.EvmBlockContext.Rules(tt.Config), collector); err != nil {
					return nil, err
				}
				finalizeWrites = collector.Writes()
				be.applyCount += finalizeWrites.Count()

				// Apply finalize writes to the BlockStateCache.
				if err := pe.rs.ApplyStateWrites(ctx, applyTx, be.blockNum, finalVersion.TxNum,
					finalizeWrites, nil, lastResult.Rules(), be.blockStateCache); err != nil {
					return nil, err
				}
			}
		}

		// Send finalize txResult through the channel for index writes.
		// State writes are already in the BlockStateCache.
		if !finalizeWrites.IsEmpty() {
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

		// Flush block state cache to sd.mem — all writes (per-TX + finalize) are now visible.
		if err := be.blockStateCache.Flush(pe.rs.Domains(), applyTx); err != nil {
			return nil, err
		}

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
			blockStateCache:  be.blockStateCache,
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

func MergeVersionedWrites(prev, next *state.WriteSet) *state.WriteSet {
	return prev.Merge(next)
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
// codePathRecoveryHashMismatch counts BAL codePath recoveries skipped because
// the recovered bytes didn't hash to the emitted codeHash; surfaced so the skip
// isn't silent.
var codePathRecoveryHashMismatch = metrics.GetOrCreateCounter("exec3_codepath_recovery_hash_mismatch")

func normalizeWriteSet(writes *state.WriteSet, vm *state.VersionMap, txIndex int, incarnation int, stateReader state.StateReader, domainStorageKeys func(addr accounts.Address) []accounts.StorageKey, emptyRemoval bool, isAura bool) *state.WriteSet {
	filtered := &state.WriteSet{}
	if writes == nil {
		return filtered
	}

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
	for addr, vw := range writes.SelfDestructs() {
		if vw.Version.Incarnation == incarnation && vw.Val {
			sdSet[addr] = true
		}
	}

	for h := range writes.AllHeaders() {
		// Drop account-field writes for SD'd addresses so applyVersionedWrites
		// takes the pure-delete branch instead of cleanup-before-recreate; drop
		// raw StoragePath writes too (the SelfDestructPath case re-emits an
		// explicit StoragePath=0 delete for every slot via sdStorageSlots).
		if sdSet[h.Address] {
			switch h.Path {
			case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath, state.CodePath, state.StoragePath:
				continue
			}
		}
		switch h.Path {
		case state.StoragePath:
			// Only include writes from the current (validated) incarnation.
			if h.Version.Incarnation != incarnation {
				continue
			}
			sw, ok := writes.GetStorage(h.Address, h.Key)
			if !ok {
				continue
			}
			writeVal := sw.Val
			// If addr was self-destructed by an earlier TX in this block, its
			// storage was wiped — the effective baseline for any slot not
			// re-written since is 0, regardless of what the versionMap (prior
			// TX) or the domain (pre-block) still holds. The SD's per-slot
			// zeroing is only re-emitted into the calc's writeset below, never
			// flushed back to the versionMap, so without this a resurrect TX
			// that re-writes a slot to its pre-SD value is wrongly dropped as a
			// no-op (TestDeleteRecreateSlotsAcrossManyBlocks).
			sdTxIdx, sdOk := -1, false
			if v, sd, _ := vm.ReadSelfDestruct(h.Address, txIndex); sd.Status() == state.MVReadResultDone && v {
				sdTxIdx, sdOk = sd.Version().TxIndex, true
			}
			// No-op filter: compare against origin (what this TX would have read).
			// First check versionMap floor (prior TX's write in this block).
			// Then fall back to stateReader (pre-block value from domain).
			originVal, origin, originOK := vm.ReadStorage(h.Address, h.Key, txIndex)
			originValid := originOK && origin.Status() == state.MVReadResultDone &&
				!(sdOk && sdTxIdx > origin.Version().TxIndex)
			if originValid {
				if writeVal.Eq(&originVal) {
					continue // write-back same as prior TX's value — no-op
				}
			} else if sdOk {
				// SD'd earlier with no re-write since — baseline is 0.
				if writeVal.IsZero() {
					continue
				}
			} else if stateReader != nil {
				// SD-then-revival: latest SelfDestructPath may be false (a
				// later TxIdx revived), but the SD's per-slot DELETE cascade
				// already fixed the baseline at zero for any post-SD write.
				// History scan catches that; the sdOk latest-value read above
				// misses it. Narrower than an IncarnationPath probe: pure
				// CREATE (no prior SD=true) doesn't wipe pre-existing storage,
				// so its same-value SSTOREs still no-op against pre-block.
				if vm.AnyDoneSelfDestructEquals(h.Address, txIndex-1, true) {
					if writeVal.IsZero() {
						continue
					}
				} else {
					preVal, found, err := stateReader.ReadAccountStorage(h.Address, h.Key)
					if err == nil {
						if !found && writeVal.IsZero() {
							continue
						}
						if found && writeVal.Eq(&preVal) {
							continue
						}
					}
				}
			}
			filtered.SetStorage(h.Address, h.Key, sw)
		case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath:
			// Account fields: prefer the versionMap's accumulated value; fall
			// back to the raw write when the map has none.
			if !state.SetAccountFieldFromMap(filtered, vm, h.Address, h.Path, h.Version, txIndex+1) {
				switch h.Path {
				case state.BalancePath:
					if vw, ok := writes.GetBalance(h.Address); ok {
						filtered.SetBalance(h.Address, vw)
					}
				case state.NoncePath:
					if vw, ok := writes.GetNonce(h.Address); ok {
						filtered.SetNonce(h.Address, vw)
					}
				case state.IncarnationPath:
					if vw, ok := writes.GetIncarnation(h.Address); ok {
						filtered.SetIncarnation(h.Address, vw)
					}
				case state.CodeHashPath:
					if vw, ok := writes.GetCodeHash(h.Address); ok {
						filtered.SetCodeHash(h.Address, vw)
					}
				}
			}
		case state.CodePath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			if vw, ok := writes.GetCode(h.Address); ok {
				filtered.SetCode(h.Address, vw)
			}
		case state.CreateContractPath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			if vw, ok := writes.GetCreateContract(h.Address); ok {
				filtered.SetCreateContract(h.Address, vw)
			}
		case state.SelfDestructPath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			// Only emit storage DELETE entries when the account was actually
			// self-destructed (val=true).
			sdw, ok := writes.GetSelfDestruct(h.Address)
			if !ok || !sdw.Val {
				continue
			}
			filtered.SetSelfDestruct(h.Address, sdw)
			for _, slot := range sdStorageSlots(h.Address) {
				filtered.SetStorage(h.Address, slot, &state.VersionedWrite[uint256.Int]{
					WriteHeader: state.WriteHeader{
						Address: h.Address,
						Path:    state.StoragePath,
						Key:     slot,
						Version: h.Version,
					},
				})
			}
		case state.AddressPath:
			// AddressPath is record-level — skip for field-level consumers.
		case state.CodeSizePath:
			// Code size is derived from the code bytes and isn't a domain field,
			// so it's intentionally not carried into the calc/apply write set (as
			// on serial). Cross-tx ReadCodeSize is served from the versionMap,
			// which the worker populates directly — independent of this pass.
		}
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
	for h := range writes.AllHeaders() {
		if h.Path != state.AddressPath {
			allAddresses[h.Address] = true
		}
	}

	// Track which fields each address already has in the output.
	addrFields := make(map[accounts.Address]map[state.AccountPath]bool)
	for h := range filtered.AllHeaders() {
		switch h.Path {
		case state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath:
			if addrFields[h.Address] == nil {
				addrFields[h.Address] = make(map[state.AccountPath]bool)
			}
			addrFields[h.Address][h.Path] = true
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
		if v, sd, _ := vm.ReadSelfDestruct(addr, txIndex); sd.Status() == state.MVReadResultDone && v {
			sdEarlier = true
		}

		// Only emit post-SD defaults when this TX created a new contract
		// (CREATE/CREATE2). A value-transfer resurrect (no CreateContractPath)
		// inherits the pre-SD account fields via the versionMap last-write-wins
		// chain — that matches GenerateChain's accumulate-across-txs behaviour
		// (no per-tx FinalizeTx). Forcing defaults here resets nonce/codeHash
		// against that canonical state (TestSelfDestructReceive).
		hasCreateContract := false
		if vw, ok := writes.GetCreateContract(addr); ok && vw.Val {
			hasCreateContract = true
		}

		// For each missing field, try versionMap then stateReader.
		for _, path := range []state.AccountPath{state.BalancePath, state.NoncePath, state.IncarnationPath, state.CodeHashPath} {
			if fields != nil && fields[path] {
				continue // already in output
			}
			if sdEarlier && hasCreateContract {
				state.SetAccountFieldZero(filtered, addr, path, ver)
				continue
			}
			if state.SetAccountFieldFromMap(filtered, vm, addr, path, ver, txIndex+1) {
				continue
			}
			// Fall back to stateReader for pre-block account state.
			if stateReader != nil {
				acc, err := stateReader.ReadAccountData(addr)
				if err == nil {
					// New account (acc == nil) — doesn't exist in domain yet.
					// SetAccountFieldFromAccount emits default values so the
					// commitment sees a full account (not a delete).
					state.SetAccountFieldFromAccount(filtered, addr, path, ver, acc)
				}
			}
		}
	}

	// CodePath must travel with CodeHashPath: the case above and the fill loop
	// recover an account's codeHash but not its code, so a validated writeset
	// lacking a fresh CodePath (e.g. a re-executing 7702 tx whose SetCode
	// short-circuited) would persist a codeHash with no code. Recover the code
	// (versionMap, else stateReader post-state) and re-emit CodePath, bounded to
	// 7702 designators so unchanged contract code isn't re-emitted. Forward-only:
	// it can't repair codeHash-no-code already collated into snapshots.
	codeInOutput := make(map[accounts.Address]bool)
	for addr := range filtered.Codes() {
		codeInOutput[addr] = true
	}
	codeHashInOutput := make(map[accounts.Address]accounts.CodeHash)
	for addr, vw := range filtered.CodeHashes() {
		codeHashInOutput[addr] = vw.Val
	}
	for addr, h := range codeHashInOutput {
		if h.IsEmpty() || codeInOutput[addr] || sdSet[addr] {
			continue
		}
		// Recover the code whose hash this tx emitted. Prefer the versionMap
		// (this batch's writes); on the SetCode short-circuit path — a
		// re-executing 7702 delegation whose code equals the already-committed
		// designator, so the validated incarnation writes no CodePath and the
		// prior incarnation's versionMap entry was invalidated on re-exec — the
		// versionMap holds nothing for this tx, so fall back to the post-state
		// via stateReader.
		var code []byte
		if c, _, ok := vm.ReadCode(addr, txIndex+1); ok {
			code = c.Bytes
		}
		if len(code) == 0 && stateReader != nil {
			if c, err := stateReader.ReadAccountCode(addr); err == nil {
				code = c
			}
		}
		// Gate recovery to 7702 designators: that SetCode short-circuit is the only
		// one that leaves uncommitted code without a CodePath. A regular deploy
		// writes CodePath with CodeHashPath; a CREATE2/unchanged redeploy already
		// has its code in CodeDomain. (Gating also never misattributes callee code.)
		if _, ok := types.ParseDelegation(code); !ok {
			continue
		}
		// The recovered bytes (ceiling versionMap read or stateReader post-state)
		// can race and disagree with the codeHash this tx emitted; only re-emit
		// when they hash to it, else we'd persist code that mismatches its hash.
		recovered := accounts.NewCode(code)
		if recovered.Hash.Value() != h.Value() {
			// Cannot repair: re-emitting would persist code mismatching its hash.
			// Skipping leaves codeHash-without-code, so signal rather than hide it.
			codePathRecoveryHashMismatch.Inc()
			log.Warn("[exec3] BAL codePath recovery skipped: recovered bytes do not hash to emitted codeHash",
				"addr", addr, "txIndex", txIndex, "emittedHash", h.Value(), "recoveredHash", recovered.Hash.Value())
			continue
		}
		filtered.SetCode(addr, &state.VersionedWrite[accounts.Code]{
			WriteHeader: state.WriteHeader{
				Address: addr,
				Path:    state.CodePath,
				Version: state.Version{TxIndex: txIndex, Incarnation: incarnation},
			},
			Val: recovered,
		})
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
	ensureAcctState := func(addr accounts.Address) *acctState {
		s := acctStates[addr]
		if s == nil {
			s = &acctState{}
			acctStates[addr] = s
		}
		return s
	}
	for addr, vw := range filtered.Balances() {
		s := ensureAcctState(addr)
		s.balance = vw.Val
		s.hasBal = true
	}
	for addr, vw := range filtered.Nonces() {
		s := ensureAcctState(addr)
		s.nonce = vw.Val
		s.hasNonce = true
	}
	for addr, vw := range filtered.CodeHashes() {
		s := ensureAcctState(addr)
		s.codeHash = vw.Val
		s.hasCode = true
	}

	// Check for empty accounts and replace with Delete. Only when EIP-161
	// (SpuriousDragon) is active — before that fork an empty account that's
	// merely touched (e.g. a 0-value transfer) is created and persists, so
	// converting it to a delete here would diverge from serial's trie root
	// (TestEIP161AccountRemoval block 1, pre-SpuriousDragon).
	emptyAddrs := make(map[accounts.Address]bool)
	for addr, s := range acctStates {
		if state.EIP161EmptyRemoval(emptyRemoval, isAura, addr) &&
			s.hasBal && s.hasNonce && s.hasCode &&
			s.balance.IsZero() && s.nonce == 0 && s.codeHash.IsEmpty() {
			emptyAddrs[addr] = true
		}
	}

	for addr := range emptyAddrs {
		filtered.DeleteAccountFields(addr)
		filtered.SetSelfDestruct(addr, &state.VersionedWrite[bool]{
			WriteHeader: state.WriteHeader{
				Address: addr,
				Path:    state.SelfDestructPath,
				Version: state.Version{TxIndex: txIndex, Incarnation: incarnation},
			},
			Val: true,
		})
	}

	return filtered
}
