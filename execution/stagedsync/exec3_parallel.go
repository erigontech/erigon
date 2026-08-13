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
	// failedBlock/failedHash record the implicated block when execution fails
	// (wrong-root or invalid block), so the stage wrapper can target the unwind.
	failedBlock uint64
	failedHash  common.Hash
	execWorkers []*exec.WorkerContext
	stopWorkers func()
	waitWorkers func()
	// prevBlocks lists the finished-but-not-yet-committed blocks' versionMaps
	// (PREV_BLOCK_READS). A worker executing block M reads blocks < M from this
	// list in front of its raw sd read, so it sees earlier blocks' writes that
	// apply has not yet committed to the shared domain. Bounded to the exec-ahead
	// window (dropped from the tail on commit). Nil when the gate is off.
	prevBlocks *state.PrevBlockList
	// mintedWorkers are extra WorkerContexts created on demand when the runSem
	// pool is empty because in-flight workers are parked mid-EVM on a dependency
	// (they hold their context and cannot release it). The pool grows elastically;
	// minted contexts are reused via runSem and reclaimed at teardown (delayed
	// shrink — no per-release create/destroy churn). CPU concurrency stays bounded
	// by execSem, so extra contexts add memory, not parallelism.
	mintedWorkers []*exec.WorkerContext
	mintMu        sync.Mutex
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
	cancelWorkers context.CancelFunc
	// dispatch runs a task on a semaphore-held worker context (dispatchRunSelfLoop) and
	// pushes the result to the plain results channel. runSem holds the idle worker
	// contexts; runWG tracks in-flight task goroutines for teardown.
	runSem chan *exec.WorkerContext
	// execSem decouples the execution-concurrency gate (the CPU tuning variable)
	// from the WorkerContext-object pool (runSem, memory). Under selfLoopPause a worker
	// resting mid-EVM on a dependency keeps its object but releases its execSem
	// slot, so resting does not reduce the available concurrency — another worker
	// runs on a different object. runSem must hold enough objects for the peak of
	// executing+resting workers; execSem sizes actual parallelism.
	execSem    chan struct{}
	runWG      sync.WaitGroup
	workersCtx context.Context
	// results is the plain results channel: dispatchRunSelfLoop pushes finished tasks here
	// (as-arrive) and the exec loop consumes them directly via processSingleResult.
	results        chan *exec.TxResult
	workerCount    int
	blockExecutors map[uint64]*blockExecutor
	// consumers is the fan-out registry (apply loop + commitment calculator),
	// set before execLoop starts. The exec loop closes it on exit to signal the
	// consumers to drain.
	consumers   *resultStream
	maxBlockNum uint64 // set before execLoop; exec loop exits when reached
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

// bindBlockChangesetForFold routes the apply loop's block-end state fold into
// block N's own changeset so unwind can revert account/storage/code, not just
// commitment. Under splitApply the apply loop — not the exec loop — is the sole
// sd.mem state writer, so the exec loop's accumulator (the CS it saved by hash)
// stays empty unless the fold's DomainPuts record into it here. The committer
// finds the same CS by hash and adds commitment; each DomainPut serializes on
// changesetMu (held throughout by the committer's compute), so the accumulator
// stays this block's CS across the fold. Returns a restore closure; a no-op if
// the block has no saved changeset (outside the changeset window). Mirrors the
// committer's computeWithBlockAccumulator.
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
	// and cannot be shared with the execLoop goroutine. The execLoop creates
	// its own roTx at line 571. executeBlocks uses its own roTx too.

	// applyResults receives completed block/tx results from execLoop for the apply goroutine.
	// commitResults receives the same stream for the commitment calculator.
	// Both are fed by the fan-out in the execLoop's blockExecutor.
	applyResults := make(chan applyResult, 2_048)
	commitResults := make(chan applyResult, 2_048)
	// Exec-only (DISCARD_COMMITMENT): nil the commit stream. The exec loop's
	// fan-out (sendResult) and the batch-commit trigger both no-op on a nil
	// channel, so no commitment work runs; the calculator sees a nil input,
	// exits immediately, and closes rootResults, which the apply loop's normal
	// close-handling absorbs. Used by ephemeral single-block replay over a flat
	// witness (no trie). Real staged sync leaves this non-nil.
	if dbg.DiscardCommitment() {
		commitResults = nil
	}
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
		log.Info(fmt.Sprintf("[%s] parallel starting", pe.logPrefix),
			"from", startBlockNum, "to", maxBlockNum, "limit", lastBlock, "initialTxNum", initialTxNum,
			"initialBlockTxOffset", offsetFromBlockBeginning, "initialCycle", initialCycle,
			"isForkValidation", pe.isForkValidation, "isApplyingBlocks", pe.isApplyingBlocks)
	}

	// restoreTxNum must run before pe.run() so that doms.SetTxNum() completes
	// before any goroutine reads txNum (via AsGetter/GetLatest). With an injected
	// block source (ephemeral replay) the caller owns range resolution and there
	// is no TxNums index to consult, so the passed-in inputTxNum is used as-is.
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

	// prevBlocks must exist before pe.run() and resetWorkers: both reset paths call
	// EnablePrevBlockReads(pe.prevBlocks), and a nil registry panics on the first
	// per-task SetBlock. run() resets workers on its own goroutine, so setting this
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

	// Register the fan-out consumers so execLoop can publish + close them.
	// applyResults feeds the read base (it is the domain writer today); the
	// commit sink is a pure sink. Registration order is publish order; close
	// walks it in reverse (commit before apply). blockRequests is intentionally
	// not registered: it is closed by its sole sender (the executeBlocks
	// dispatch goroutine), not by execLoop.
	pe.consumers = newResultStream()
	pe.consumers.register("applyResults", applyResults, true)
	pe.consumers.register("commitResults", commitResults, false)
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
		var pendingAccumulatorWrites []state.WriteSetView
		// splitApply: buffer each block's per-tx results (versionMap views) and fold
		// them to sd.mem at block end, so sd.mem stays N-1 during exec.
		var splitApplyBuf []*txResult

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
						// Unwind handling is hoisted to SpawnExecuteBlocksStage. Record the
						// implicated block (not the last-applied one) so the stage can target
						// the unwind correctly, then surface the failure.
						pe.failedBlock, pe.failedHash = fail.block, fail.blockHash
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
					// fork-validation batch exits via execLoopExitCheck (no cause), and
					// real shutdown cancels with context.Canceled. A fully-applied range
					// — or an empty loop that executed nothing because the range was
					// already applied (async background commit advanced progress) — is a
					// clean end; otherwise there is more work.
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
					// The apply loop is the sole sd.mem writer: buffer each tx's
					// per-tx result and fold the buffer to sd.mem at block end.
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
					// Apply the block's per-tx versionMap views to sd.mem, folding the
					// buffer at block end in publish order. blockCache=nil writes domains
					// directly; the versionMap composes each tx's base. Finalize skips
					// ApplyTxIndexes, matching the exec loop.
					restoreCS := pe.bindBlockChangesetForFold(applyResult.BlockNum, applyResult.BlockHash)
					var applyErr error
					for _, r := range splitApplyBuf {
						if err := pe.rs.ApplyStateWrites(ctx, rwTx, r.blockNum, r.txNum, r.writes, nil, r.rules, nil); err != nil {
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
					// This block's writes are now in the shared domain: drop it from
					// the tail of the prev-block list so later blocks read it from the
					// domain (fire-and-forget — readers never block on this).
					pe.prevBlocks.RemoveTail()
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

						// Spawn per-block validation in a goroutine — the result is
						// joined via Wait() below, after the other per-result work
						// has had a chance to run in parallel with validation.
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

// triggerBatchCommitment sends a commitComputeRequest to the calculator so it
// computes the batch commitment before the exec loop exits and closes channels.
// Delivery is unconditional: a terminal stop publishes the stopCause (cancelling
// ctx) before this runs, but the calculator keeps draining until its channel is
// closed, so blocking is safe. Honouring ctx.Done here would drop the batch-end
// commitment when the buffer is momentarily full, leaving commitment behind
// sd.mem. A closed target during shutdown drops the request (harmless).
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
		worker.ResetState(rs, nil, nil, state.NewLightCollector(), nil)
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

// prevBlockBase wraps a freshly-built committed-base reader (the finalize /
// calcFees readers, which build a fresh reader per block) with the prev-block
// layers for blockNum, so those reads also see prior blocks' not-yet-committed
// writes — matching the per-task worker reader. Without this the coinbase
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

// releaseWorker returns a context to the pool for reuse. The pool buffer is
// sized to hold the elastic peak, so this never blocks; excess is reclaimed at
// teardown (delayed shrink — no per-release churn).
func (pe *parallelExecutor) releaseWorker(w *exec.WorkerContext) {
	select {
	case pe.runSem <- w:
	default:
	}
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

	// np-phase exec-loop attribution: wall spent waiting for the next in-order
	// result vs doing serial per-tx processing (Drain + processResults:
	// validate + blockStateCache apply + ApplyTxIndexes). Reset per completed block.
	var npWait, npProc time.Duration
	var npWaitStart, npProcStart time.Time

	for {
		if applyTx, err = pe.refreshApplyTx(ctx, applyTx); err != nil {
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

		if logNpPhases {
			npWaitStart = time.Now()
		}
		var blockResult *blockResult
		// Plain-channel path: results arrive as-arrive on pe.results (no heap
		// reorder); process each directly. Order is imposed at publish.
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
				pe.abortCount.Add(int64(blockExecutor.cntAbort))
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
						"spec", blockExecutor.cntSpecExec, "abort", blockExecutor.cntAbort,
						"valFail", blockExecutor.cntValidationFail,
						"blockEndApplyMs", fmt.Sprintf("%.1f", float64(blockResult.flushDur.Nanoseconds())/1e6),
						"spineUsPerIter", fmt.Sprintf("%.1f", float64(npProc.Nanoseconds())/float64(max(1, blockExecutor.cntExec))/1e3))
					npWait, npProc = 0, 0
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
				executor = newBlockExec(blockNum, execRequest.blockHash, execRequest.gasPool, execRequest.accessList, execRequest.consumers, execRequest.profile, execRequest.exhausted)
				// Set the coinbase once, before any worker runs, so self-loop
				// workers can read it during validation without racing the exec
				// loop (which otherwise sets it from the first result).
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
		executor.execAborted = append(executor.execAborted, 0)

		executor.estimateDeps[len(executor.tasks)-1] = []int{}

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
	if pe.consumers == nil {
		return nil
	}
	return pe.consumers.close()
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
	// execRequests holds one entry per decoded block (each containing all its TxTasks).
	// A large buffer causes the block-loader goroutine to race far ahead of the apply
	// loop, accumulating all decoded transaction objects in memory simultaneously.
	// 128 blocks (~25 k txns on mainnet) is sufficient to keep workers busy.
	pe.execRequests = make(chan *execRequest, 128)
	// Clear stale blockExecutors from previous batch — unprocessed blocks left
	// in the map after a "batch full" exit would prevent the first block of the
	// new batch from being scheduled (processRequest only schedules when map is empty).
	pe.blockExecutors = nil

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
		pe.resetWorkers(workersCtx, pe.rs, nil)
		// Hand the reset worker contexts to the dispatcher as a semaphore
		// (see dispatchRunSelfLoop). The buffer is oversized so the pool can grow
		// elastically (acquireWorker mints extras when workers park mid-EVM) and
		// return them without blocking; excess is reclaimed at teardown.
		pe.runSem = make(chan *exec.WorkerContext, elasticWorkerCap)
		for _, w := range pe.execWorkers {
			pe.runSem <- w
		}
		slots := selfLoopSlots
		if slots <= 0 || slots > len(pe.execWorkers) {
			slots = len(pe.execWorkers)
		}
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

		// Drain in-flight dispatch goroutines before tearing down worker
		// contexts: cancelWorkers unblocks any waiting on runSem, so this returns
		// promptly and no goroutine touches a worker after teardown.
		pe.runWG.Wait()

		pe.stopWorkers()

		// Reclaim contexts minted elastically during the run (the base pool is
		// torn down by stopWorkers); each may hold its own roTx.
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
	blockStateCache  *state.BlockStateCache

	// Exec window for additive newPayload wall attribution: stamped when the
	// block's execution completes (result built). The calculator pairs these
	// with its own commit-window timestamps to measure exec/commit overlap
	// directly rather than inferring it.
	execStartedAt time.Time
	execEndedAt   time.Time
	flushDur      time.Duration
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
	commitWrites          state.WriteSetView // VMAP_COMMIT_VIEW A/B: versionMap-slice view fed to the calculator instead of the normalized writes
	rules                 *chain.Rules
	isFinalize            bool // block-end finalize writes — apply to sd.mem directly
}



// The executor is the true Block-STM model: workers own execution AND validation.
// Each worker flushes its writes to the versionMap speculatively (as estimate),
// validates its own read-set, and loops — parking on the commit-frontier signal
// until every read-dependency has committed — returning only a stable-valid
// result. The exec loop is then a pure in-order commit loop: flush the result's
// writes as complete, run the coinbase/finalize sweep, and broadcast the advanced
// frontier. No exec-loop ValidateVersion walk, no committed-dependent
// re-validation, no re-dispatch.

// Mid-flow dep-pause is the sole dependency mechanism: workers flush their writes
// as ESTIMATE, and a read that observes an in-flight estimate pauses (via the IBS
// waitCommit hook) until that writer commits, then re-reads the committed value —
// execution continues in place instead of aborting/re-executing. Reads then always
// see final values. A paused worker holds its context (it is mid-EVM), so the
// context pool grows elastically (see acquireWorker) to avoid dependency starvation.

// selfLoopSlots is the execution-concurrency tuning variable (the CPU parallelism
// target), independent of the WorkerContext-object pool size (EPHEMERAL_WORKERS).
// 0 → default to the object-pool size (no extra gating).
var selfLoopSlots = dbg.EnvInt("SELF_LOOP_SLOTS", 0)

// elasticWorkerCap bounds the runSem buffer so the context pool can grow past
// the base worker count when workers park mid-EVM on a dependency. It is a
// backstop, not a target: real CPU concurrency is bounded by execSem, and the
// live context count tracks the peak number of simultaneously-parked workers,
// which is bounded by the block's task count.
const elasticWorkerCap = 4096


// dispatchRunSelfLoop runs one task on a worker context, looping until its
// result is stable-valid (SELF_LOOP true Block-STM): execute, flush its writes
// to the versionMap so downstream workers read them speculatively, validate its
// own read-set, and — when valid but a read-dependency has not yet committed —
// park on the commit frontier and re-validate. Only a stable-valid result is
// sent to the exec loop, which commits it in order. The context is released
// while parked so dependencies can run; a fresh one is taken to re-execute.
func (pe *parallelExecutor) dispatchRunSelfLoop(be *blockExecutor, tv *taskVersion) {
	pe.runWG.Add(1)
	go func() {
		defer pe.runWG.Done()
		// The roTx is bound to the execution slot, not the goroutine: opened when a
		// slot is acquired and rolled back when it is released — across a mid-EVM
		// dependency wait and while parked committed-valid awaiting re-exec. This
		// bounds concurrent roTxs to the slot count rather than the far larger
		// parked-goroutine count, so idle parked workers can't exhaust the MDBX
		// read-tx limiter and starve the in-order task into a deadlock.
		var goRoTx kv.TemporalTx
		var w *exec.WorkerContext
		acquire := func() bool {
			w = pe.acquireWorker()
			return w != nil
		}
		// Execution-slot gate (selfLoopPause): held only while actually executing,
		// released while resting mid-EVM on a dependency, so a resting worker does
		// not reduce available concurrency.
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
		// the loop binds via ResetTx (lock free), while the mid-EVM waitCommit rebinds
		// via BindTxHeld (RunTxTask already holds the worker lock).
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
		// by block-TxIndex dep releases this worker's execution slot, waits until
		// that writer's task (dep+1) commits, reacquires a slot, and the read
		// retries to see the final value. This is the sole dependency mechanism —
		// a read never aborts/re-executes on an in-flight predecessor.
		tv.waitCommit = func(dep int) bool {
			releaseSlot()
			// dep is a versionMap TxIndex (predecessor writer); translate to the
			// scheduler's task-index frontier space before parking.
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
		if !acquire() {
			return
		}
		bumpInc := func() bool {
			tv.version.Incarnation++
			if tv.version.Incarnation > len(be.tasks)+8 {
				pe.logger.Warn("[self-loop] incarnation limit exceeded", "block", be.blockNum, "tx", tv.index, "inc", tv.version.Incarnation)
				return false
			}
			return true
		}
		// reExec is called holding NO context (it was released before the wait);
		// it bumps the incarnation and reacquires a context to re-execute. Returns
		// false on the incarnation limit or shutdown.
		reExec := func() bool {
			return bumpInc() && acquire()
		}
		// waitTo releases the context, waits for the commit frontier to reach t.
		// Returns false on shutdown.
		waitTo := func(t int) bool {
			return be.waitDep(tv.index, t)
		}
		var prevWrites *state.WriteSet
		for {
			// Whether this execution runs against fully-committed state below it:
			// captured BEFORE execution, since the run reads the versionMap as it
			// goes. The frontier is monotonic and finalized tasks never un-commit, so
			// if every lower task is committed now it stays so for the whole run — an
			// error is then authoritative. Capturing after would mis-classify a
			// speculative error (read stale, frontier advanced mid-run) as genuine.
			finalExec := be.frontier() >= tv.index-1
			if !acquireSlot() {
				return
			}
			if err := w.ResetTx(goRoTx); err != nil {
				releaseSlot()
				return
			}
			// Invariant: each RUN of a tx uses a strictly ascending incarnation. Two
			// runs sharing one incarnation (an in-place bumpInc racing a fresh
			// dispatch) is the scheduling bug behind the stale-flush corruption.
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
			// mid-execution-changed value, so it did not run against a single
			// settled snapshot. Re-execute once that predecessor commits rather
			// than publish a result the version-set validator can't catch.
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
			// dependency may have re-executed to a new value, which forces a re-exec;
			// otherwise the verdict is authoritative and we commit out of order.
			if !waitTo(target) {
				return
			}
			if v, _, _ := be.selfLoopEvaluate(tv, result); v {
				result.WorkerValidated = state.VersionValid
				result.WorkerBlocker = -1
				result.WorkerVerdictSet = true
				send(result)
				// Stay alive rather than exit: if the committed-dependent re-check
				// finds a later write invalidated us, it signals slReexec and we
				// re-execute in place. slFin closes when we finalize (never un-
				// committed again), slDone on shutdown.
				// Lossless wait: the re-exec request is a sticky flag, so a dropped
				// wake (buffered channel full, or set just before we parked) is
				// recovered by re-checking the flag on the next wake. slFin/slDone
				// only fire once the tx can never be re-checked again.
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
	}()
}

// selfLoopFlush publishes tv's writes to the versionMap (SELF_LOOP): its writes
// as complete so downstream workers read them speculatively, plus deletion of
// any key the previous incarnation wrote that this one dropped.
func (be *blockExecutor) selfLoopFlush(version state.Version, result *exec.TxResult, prevWrites *state.WriteSet) {
	if prevWrites != nil {
		for h := range prevWrites.AllHeaders() {
			if !result.TxOut.Has(h) {
				be.versionMap.Delete(h.Address, h.Path, h.Key, version.TxIndex, true)
			}
		}
	}
	// Flush the tx's writes as ESTIMATE (complete=false) so a downstream read
	// observes the in-flight dependency and pauses until this tx commits, rather
	// than reading the speculative value.
	be.versionMap.FlushVersionedWrites(result.TxOut, false, "")
}

// taskIndexOf maps a versionMap block-TxIndex into this block's dense task-list
// index space (what waitDep/committedFrontier use). For a full block task 0
// is the block-init sys tx (TxIndex -1), so the offset is +1; for a resumed
// (partial) block whose leading committed txs were skipped, task 0 starts at a
// higher TxIndex and the offset shrinks accordingly. Without this, a dependency's
// park target lands in versionMap space and can exceed the frontier's reach on a
// partial block — a worker then parks forever and the block deadlocks.
func (be *blockExecutor) taskIndexOf(versionTxIndex int) int {
	return versionTxIndex - be.tasks[0].Version().TxIndex
}

// selfLoopEvaluate validates tv's read-set against the versionMap and, when
// valid, returns the commit frontier (task-index space) the tx must reach to be
// stable — the highest committed read-dependency, mapped from versionMap
// (block-TxIndex) space into task-list-index space via taskIndexOf.
// Fan-out: a valid result's target is the highest task it actually read from (its
// real dependency), so it can commit as soon as those deps commit — out of order,
// not behind the whole linear prefix. A coinbase reader implicitly depends on
// every prior fee tip, so it gates on the full prefix (tv.index-1). Base reads of
// a key a lower task writes carry no dependency here; that hazard is caught by the
// exec loop's committed-dependent re-validation once the lower write lands.
// valid=false forces re-exec; blocker is the highest stale writer to wait for.
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
	if !be.coinbase.IsNil() && result.TxIn.ReadsAccount(be.coinbase) {
		target = tv.index - 1
	}
	return true, target, -1
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

	// Use an empty ReadSet so all reads go through the versionMap (which
	// has all prior TX writes). The execution-phase ReadSet (result.TxIn)
	// may be stale if the system TX ran speculatively before all regular
	// TXs completed — cached reads would return pre-block values instead
	// of the post-block state needed by syscalls (withdrawal/consolidation).
	ibs := state.New(state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader))
	defer ibs.Close()
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

	// Read at txIndex (floor txIndex-1) — strictly prior tx, excluding this tx's
	// own prior incarnations that would double-apply the tip on re-execution.
	// WorkerContext writes for the current tx are picked up below via TxOut.
	vsReader := state.NewVersionedStateReader(txIndex, state.ReadSet{}, vm, stateReader)

	coinbaseAcc, err := vsReader.ReadAccountData(result.Coinbase)
	if err != nil {
		return nil, err
	}
	// The tip credits only the coinbase's Balance via vsReader (no seeding read),
	// so seed its whole-account origin into the versionMap — else the apply compose
	// has no base and wipes the coinbase's committed nonce/code/balance.
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
	// WorkerContext writes coinbase/burnt to TxOut when sender matches (gas-debit
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
	coinbaseCreatedContract := false
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
	if cw, ok := result.TxOut.GetCreateContract(result.Coinbase); ok {
		coinbaseCreatedContract = cw.Val
	}
	if hasBurnt {
		if bw, ok := result.TxOut.GetBalance(burntAddr); ok {
			newBurntBalance = bw.Val
		}
	}
	oldCoinbaseBalance := newCoinbaseBalance
	// Before EIP-8246, burn the tip only for an actual SELFDESTRUCT of a contract coinbase.
	// DeleteAccount also emits SelfDestructPath=true for EIP-161 empty-removal of
	// a touched EOA coinbase, where the delayed tip must still be credited (it
	// re-creates the account) to match serial.
	coinbaseWasContract := !coinbaseEmptyCodeHash || coinbaseHasCodeHashWrite || coinbaseCreatedContract
	burnCoinbaseTip := !chainRules.IsAmsterdam && coinbaseSelfdestructed && coinbaseWasContract
	if !burnCoinbaseTip {
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
	// and Normalize's sdSet filter drops them.
	coinbaseEmptyPre := (coinbaseAcc == nil || coinbaseAcc.Balance.IsZero()) &&
		coinbaseNonce == 0 && coinbaseEmptyCodeHash && !coinbaseHasCodeHashWrite
	emitCoinbase := newCoinbaseBalance != oldCoinbaseBalance ||
		(coinbaseEmptyRemoval && coinbaseEmptyPre && newCoinbaseBalance.IsZero())

	addWrites := &state.WriteSet{}
	if emitCoinbase {
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
	// Engine post-apply message (e.g. Bor fee-transfer logs).
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

// runPostApplyMessageOnMinIBS runs the engine's PostApplyMessage callback
// (e.g. Bor's AddFeeTransferLog) on a minimal IntraBlockState that serves as
// the log buffer, and appends the emitted logs to result.Logs so they reach
// the receipt.
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
	ibs := state.New(state.NewVersionedStateReader(txIndex, result.TxIn, vm, stateReader))
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

	// Occupancy accounting: sum every incarnation's exec CPU (including aborts)
	// so occupancy = execCpuNanos / (workerCount × wall) reveals whether workers
	// are starved (idle → dispatch/order artifact) or saturated (compute-bound).
	if logNpPhases && ev.execCpuNanos != nil {
		ev.execCpuNanos.Add(time.Since(start).Nanoseconds())
	}

	// Carry the read-dependency verdict: >= 0 means the tx read an in-flight or
	// mid-execution-changed value (intra-tx read inconsistency) and must
	// re-execute once that predecessor commits — a validation verdict, handled by
	// the scheduler, not an execution error.
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
	// Point the per-task reader at this task's block: it reads blocks < this one
	// (finished, not yet committed to the shared domain) in front of the raw base.
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
	// blockExecutionGasUsed and blockStateGasUsed are tracked separately so the
	// final blockGasUsed = max(execution, state) matches EIP-8037 / EIP-7778
	// block-level accounting and equals what the builder set in header.GasUsed
	// via protocol.SetGasUsed (= max(cumExecution, cumState)).
	blockExecutionGasUsed uint64
	blockStateGasUsed     uint64
	blockGasUsed          uint64
	blobGasUsed           uint64
	gasPool               *protocol.GasPool

	execFailed, execAborted []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	consumers *resultStream // fan-out to the apply loop + commitment calculator

	execStarted time.Time
	result      *blockResult
	applyCount  int
	exhausted   *ErrLoopExhausted

	finRevalChecks int64
	finRevalFires  int64

	// blockStateCache provides a stable pre-block snapshot of account data
	// for GetCommittedState reads, unaffected by intra-block ApplyStateWrites.
	blockStateCache *state.BlockStateCache

	// execCpuNanos sums exec CPU across ALL incarnations (NEWPAYLOAD_PHASES only)
	// for worker-occupancy attribution vs the npWait/npProc wall.
	execCpuNanos atomic.Int64

	// coinbase is the block's fee recipient, cached from the first tx result;
	// dependency-ordered validation needs it to gate coinbase readers.
	coinbase accounts.Address

	// coinbaseFlushedUpTo is the contiguous tx prefix whose fee tips the
	// calcFees sweep has flushed to the versionMap (DEP_ORDER_VAL). Coinbase
	// readers gate on it; -1 means none flushed yet.
	coinbaseFlushedUpTo int

	// writeChangedPrev holds the PREVIOUS write-set of a tx whose re-executed
	// write-set differs from the incarnation its dependents were validated
	// against (DEP_ORDER_VAL). Set at result arrival; consumed once the tx's new
	// writes are flushed during validation, when its committed dependents are
	// re-validated. The old write-set is needed alongside the new one so a
	// dependent that read a key the new incarnation DROPPED is still re-checked.
	writeChangedPrev map[int]*state.WriteSet
	// readerIdx: reverse index (read cell -> task indices that read it), used by
	// revalidateCommittedDependents to re-check only actual readers of a changed
	// tx's keys instead of scanning all later committed tasks.
	readerIdx map[readerKey][]int

	// commit-frontier signal (SELF_LOOP). committedFrontier is the highest
	// contiguous finalized task (== coinbaseFlushedUpTo). Fan-out: each worker
	// waits for its actual dependency target (the highest task it read from), not
	// the linear predecessor. wakeAt maps a frontier value to the tasks waiting for
	// it; finalizing task F wakes exactly wakeAt[F] via their slWake channels — a
	// directed wakeup, no broadcast. slDone closes on shutdown to release all.
	committedFrontier atomic.Int64
	slWake            []chan struct{}
	wakeMu            sync.Mutex
	wakeAt            map[int][]int
	// After committing, a self-loop worker stays alive parked on slReexec rather
	// than exiting — the committed-dependent re-check signals it to re-execute in
	// place (no goroutine respawn, its own monotonic incarnation), and slFin closes
	// when the task finalizes so it exits promptly.
	slReexec     []chan struct{}
	slReexecFlag []atomic.Bool
	// runInc[i] is the highest incarnation ever RUN for task i, enforcing the
	// invariant that each execution of a tx uses a strictly ascending incarnation
	// (two concurrent runs sharing one incarnation is a scheduling bug). Init -1.
	runInc           []atomic.Int64
	slFin            []chan struct{}
	slDone           chan struct{}
	slDoneOnce       sync.Once
	// selfLoopDispatched guards against re-dispatching a task the self-loop
	// worker already owns: under SELF_LOOP the worker owns all re-execution, so
	// the exec-loop scheduler must dispatch each task exactly once. Touched only
	// on the exec-loop goroutine (dispatch()).
	selfLoopDispatched map[int]bool
}

// readyForDepOrderValidation decides whether tx may be validated out of
// contiguous order: exec-complete and not already validated. The self-loop
// worker only sends a result once every read-dependency has committed and its
// read-set re-validates, so an exec-complete result is ready to commit — no
// dependency or coinbase-frontier gate is needed here.
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
		settledInput:        map[int]bool{},
		estimateDeps:        map[int][]int{},
		preValidated:        map[int]bool{},
		blockIO:             &state.VersionedIO{},
		versionMap:          state.NewVersionMap(accessList),
		profile:             profile,
		consumers:           consumers,
		gasPool:             gasPool,
		blockStateCache:     state.NewBlockStateCache(),
		exhausted:           exhausted,
		coinbaseFlushedUpTo: -1,
		writeChangedPrev:    map[int]*state.WriteSet{},
		readerIdx:           map[readerKey][]int{},
		selfLoopDispatched:  map[int]bool{},
		slDone:              make(chan struct{}),
	}
	be.committedFrontier.Store(-1)
	return be
}

// readerKey identifies one versioned read cell (address, path, storage key).
// The reverse index readerIdx maps each such cell to the task indices that read
// it, so revalidateCommittedDependents re-checks only the actual readers of a
// changed tx's keys instead of scanning every later committed task (O(n²)).
type readerKey struct {
	addr accounts.Address
	path state.AccountPath
	key  accounts.StorageKey
}

// indexReads records taskIdx as a reader of every cell in rs. Append-only across
// incarnations; the precise HasReadDep re-check in revalidateCommittedDependents
// filters any stale entry, so over-inclusion only costs a redundant check.
func (be *blockExecutor) indexReads(taskIdx int, rs state.ReadSet) {
	rs.RangeFullHeaders(func(a accounts.Address, p state.AccountPath, k accounts.StorageKey, _ state.ReadHeader) bool {
		rk := readerKey{a, p, k}
		be.readerIdx[rk] = append(be.readerIdx[rk], taskIdx)
		return true
	})
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

// finalizeValidatedTx runs the in-order finalize tail for a validated tx:
// receipt cumulative-gas offsets, block-gas accounting, finalize (receipt +
// any system-tx writes), write normalization, and queueing for publish. The
// tx's worker writes and (for regular txs) the calcFees coinbase credit are
// already flushed to the versionMap. stateReader is the loop-shared reader,
// lazily created here when nil. Returns a non-nil *blockResult (or error) when
// the block must be rejected; (nil, nil) on success.
func (be *blockExecutor) finalizeValidatedTx(pe *parallelExecutor, applyTx kv.TemporalTx, tx int, txTask exec.Task, txResult *execResult, txVersion state.Version, stateReader *state.StateReader) (*blockResult, error) {
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
		regularContribution, stateContribution := protocol.InclusionContributions(txn.GetGasLimit(), txTask.Rules().IsAmsterdam)
		if err := protocol.CheckBlockGasInclusion(be.gasPool, regularContribution, stateContribution); err != nil {
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
			*stateReader = pe.prevBlockBase(state.NewHistoryReaderV3WithBlockCache(applyTx, pe.domainsRead(), be.blockStateCache, txTask.Version().TxNum), be.blockNum)
		} else {
			// Use CachedReaderV3 with readCurrent=true so the
			// finalize (including system TXs) reads from the
			// BlockStateCache write buffer. This ensures the
			// system TX sees all accumulated state from prior
			// TXs in the block, not stale sd.mem values.
			*stateReader = pe.prevBlockBase(state.NewCurrentCachedReaderV3(pe.domainsRead().AsGetterNoMetrics(applyTx), be.blockStateCache), be.blockNum)
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
		// The write set is a read-only versionMap-slice view over the tx's raw
		// write-set (touched keys + vm-floor values); apply and the calculator
		// resolve each account's base from the versionMap.
		rawWrites := be.blockIO.WriteSet(txVersion.TxIndex)
		txResult.writes = state.NewVersionMapWriteView(rawWrites, be.versionMap, txVersion.TxIndex)
	}

	// Snapshot the finalized result before pushing — prevents
	// the publish loop from seeing a later incarnation if
	// be.results[tx] is overwritten by a concurrent worker.
	be.finalizedResults[tx] = txResult
	txResult.cumulativeBlobGasUsed = be.blobGasUsed
	be.publishTasks.pushPending(tx)
	return nil, nil
}

// advanceCoinbaseAndFinalize runs the in-order tail for dependency-ordered
// validation over the contiguous validated prefix that has not yet been
// finalized. For each tx it computes the calcFees coinbase credit (flushing the
// tip to the versionMap and advancing coinbaseFlushedUpTo — coinbase readers
// gate on this), then runs the finalize tail (receipt cumulative-gas, gas
// accounting, normalization, publish queue). calcFees needs the tx validated so
// its FeeTipped is final; the finalize tail needs its predecessor's receipt —
// both hold on the contiguous validated prefix.
func (be *blockExecutor) advanceCoinbaseAndFinalize(pe *parallelExecutor, applyTx kv.TemporalTx, stateReader *state.StateReader) (*blockResult, error) {
	maxValidated := be.validateTasks.maxComplete()
	for tx := be.coinbaseFlushedUpTo + 1; tx <= maxValidated; tx++ {
		// Use the validated snapshot, not be.results[tx]: validate and finalize
		// are separate passes under dependency-ordered validation, so a
		// concurrent worker may have overwritten be.results[tx] with a later
		// incarnation in between.
		txTask := be.tasks[tx].Task
		txResult := be.finalizedResults[tx]
		txVersion := txResult.Task.Version()

		// Authoritative re-validation at the finalize boundary. Every predecessor
		// < tx is now final (contiguous prefix), so this is the last point a stale
		// read can be caught. The write-change dependent re-check in nextResult can
		// miss the pre-publish window: a predecessor's late FIRST write over a key
		// this tx read from base lands after this tx is marked complete, and the
		// re-check skips already-published txs — so the dependent finalizes stale.
		// If stale here, un-commit and stop the sweep; the fixpoint loop re-executes
		// it against the now-final predecessors before it can finalize.
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() && txResult.Err == nil {
			be.finRevalChecks++
			if be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
				func(rv, wv state.Version) state.VersionValidity {
					if rv != wv {
						return state.VersionInvalid
					}
					return state.VersionValid
				}, false, "") != state.VersionValid {
				be.finRevalFires++
				be.validateTasks.clearComplete(tx)
				be.preValidated[tx] = false
				be.signalSelfLoopReexec(tx)
				break
			}
		}
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() && txResult.Err == nil {
			taskVer, ok := txResult.Task.(*taskVersion)
			if !ok {
				return nil, fmt.Errorf("apply loop: unexpected task type for tx %d: result.Task=%T", tx, txResult.Task)
			}
			if *stateReader == nil {
				if txTask.IsHistoric() {
					*stateReader = pe.prevBlockBase(state.NewHistoryReaderV3WithBlockCache(applyTx, pe.domainsRead(), be.blockStateCache, txTask.Version().TxNum), be.blockNum)
				} else {
					*stateReader = pe.prevBlockBase(state.NewCurrentCachedReaderV3(pe.domainsRead().AsGetter(applyTx), be.blockStateCache), be.blockNum)
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
				// Flush the tip so coinbase readers gated on coinbaseFlushedUpTo
				// see the accumulated fee credit in the versionMap.
				be.versionMap.FlushVersionedWrites(tipWrites, true, "")
			}
		}
		be.coinbaseFlushedUpTo = tx
		// Seal this tx: its versionMap cells are now immutable. Any later write to
		// them (a stale speculative incarnation flushing after finalization) trips
		// the assertUnsealed invariant rather than silently corrupting the prefix.
		// Only regular OCC txs participate; negative-index system/init txs and the
		// block-end finalize tx (whose reward/withdrawal writes flush at block end)
		// are exempt.
		if txVersion.TxIndex >= 0 && !txTask.IsBlockEnd() {
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
// waiting for that frontier value (fan-out: their dependency target is now met).
// Directed — no broadcast — so finalizing one task never stampedes the rest.
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

// signalSelfLoopReexec wakes the parked worker of an already-sent tx to
// re-execute in place (it owns its incarnation, so no re-dispatch / no
// txIncarnations sync). Mirrors revalidateCommittedDependents' re-exec signal.
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

// waitDep blocks until the commit frontier reaches target (tx's dependency target
// — every task it read from has committed), or shutdown. Registers tx under the
// wake lock so a concurrent signalCommitted(target) cannot be missed. Returns
// false on shutdown before the target was reached.
func (be *blockExecutor) waitDep(tx, target int) bool {
	// The frontier only ever reaches len(be.tasks)-1 (the last task). A target
	// beyond that can never be signalled, so the worker would park forever and the
	// block deadlocks silently. That happens when a dependency in versionMap
	// (block-TxIndex) space reaches a park site without being mapped through
	// taskIndexOf — a mistake reintroduced almost every time the processing model
	// changes. Fail loud at the exact site instead.
	if target >= len(be.tasks) {
		panic(fmt.Sprintf("[self-loop] block %d: park target %d out of task range [-1,%d) for task %d — "+
			"a dependency reached waitDep in versionMap space (missing taskIndexOf translation); "+
			"block start TxIndex=%d", be.blockNum, target, len(be.tasks), tx, be.tasks[0].Version().TxIndex))
	}
	// Invariant: a task never waits for a HIGHER task to commit — that is a forward
	// dependency the in-order frontier can never satisfy (deadlock). Fail loud at
	// the exact site so the stack shows which caller produced the forward target.
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
// is cancelled, so shutdown/error never strands one in waitFrontier. Under
// SELF_LOOP_DEBUG it also periodically dumps the frontier/park state to expose a
// stall.
func (be *blockExecutor) selfLoopWatchdog(ctx context.Context) {
	done := func() { be.slDoneOnce.Do(func() { close(be.slDone) }) }
	<-ctx.Done()
	done()
}

// revalidateCommittedDependents re-checks the committed-but-not-published
// dependents of changedTx (whose write-set just changed via re-execution or a
// validation failure) against the current versionMap, in place: a still-valid tx
// stays committed so maxValidated does not regress; one that now fails is
// un-committed and re-queued for re-execution, propagating the cascade to its
// dependents on the next result. Published txs are excluded — final and already
// streamed to commitment.
//
// A dependent is any committed tx that reads a key in changedTx's OLD ∪ NEW
// write-set: NEW covers keys the new incarnation added or revalued; OLD covers
// keys it dropped (a reader of a dropped key would otherwise be missed). This
// keeps the ValidateVersion cost off txs changedTx can't affect. oldWrites is
// nil for a validation failure (no prior incarnation to compare).
func (be *blockExecutor) revalidateCommittedDependents(changedTx int, oldWrites *state.WriteSet) *blockResult {
	// be.tasks / the status lists are keyed by task index; be.blockIO is keyed by
	// the block-level TxIndex. They differ by the block's leading system tx, so
	// blockIO reads must map through be.tasks[i].Task.Version().TxIndex.
	newWrites := be.blockIO.WriteSet(be.tasks[changedTx].Task.Version().TxIndex)
	for _, tx := range be.revalCandidates(changedTx, newWrites, oldWrites) {
		if !be.validateTasks.checkComplete(tx) || be.publishTasks.checkComplete(tx) {
			continue
		}
		rs := be.blockIO.ReadSet(be.tasks[tx].Task.Version().TxIndex)
		hasDep := state.HasReadDep(newWrites, rs) || (oldWrites != nil && state.HasReadDep(oldWrites, rs))
		// A task already past the coinbase/finalize frontier is final: un-committing
		// it would leave coinbaseFlushedUpTo ahead of maxValidated, and the finalize
		// sweep (which starts at coinbaseFlushedUpTo+1) could never re-finalize it —
		// a permanent stall. Under selfLoop the finalize→publish window makes this
		// reachable (publishTasks lags the frontier).
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
		be.preValidated[tx] = false
		// The committed worker is still alive, parked on slReexec. Signal it to
		// re-execute in place (it owns its incarnation, so no re-dispatch and no
		// be.txIncarnations sync); leave execTasks complete so the re-sent result
		// re-validates without going through the dispatch path.
		be.slReexecFlag[tx].Store(true)
		select {
		case be.slReexec[tx] <- struct{}{}:
		default:
		}
	}
	return nil
}

// revalCandidates returns the task indices > changedTx to re-check. Default:
// every later task (the caller filters by HasReadDep). Under REVAL_INDEX: only
// the tasks the reverse index records as readers of changedTx's changed keys
// (new ∪ old write-sets) — the exact set for which HasReadDep can be true —
// sorted ascending so the cascade order matches the scan.
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

// runDepOrderValidation is the DEP_ORDER_VAL validation pass. It first finalizes
// the contiguous validated-but-not-yet-finalized prefix (calcFees coinbase sweep
// + finalize tail), then selects the dependency-ready txs — read-deps validated,
// coinbase readers gated on the coinbase-flush frontier — and validates them out
// of contiguous order, committing each (flush + markComplete) or cascading a
// re-validation of successors on failure. A final finalize pass picks up the
// prefix just extended by these validations.
//
// Unlike the contiguous path it does NOT use the VersionTooEarly gate (tx-1 >
// maxValidated): that gate is inherently in-order and would block every
// out-of-order tx. Read stability for base/UnknownDep reads is instead enforced
// by the write-change re-validation in nextResult — a predecessor whose writes
// change re-queues its successors for validation.
func (be *blockExecutor) runDepOrderValidation(pe *parallelExecutor, applyTx kv.TemporalTx, stateReader *state.StateReader) (*blockResult, error) {
	// Run to a fixpoint: each pass advances the coinbase/finalize frontier, then
	// validates the txs that frontier newly unblocked. Advancing the frontier at
	// the end of a pass can unblock more; without looping here those wait for the
	// next worker result, which may never arrive once the pipeline drains — the
	// dep-order hang. Loop until a pass validates nothing and the frontier is stable.
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

			var trace bool
			var tracePrefix string
			if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
				tracePrefix = fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)
			}

			// The worker's read-set verdict is a parallel pre-filter, not
			// authoritative: a lower tx can write a key this tx read AFTER the worker
			// validated and parked (its target is the version it read, not the later
			// writer). Re-validate against the versionMap at the serial commit point —
			// where every earlier-processed write is visible — so a stale read is
			// caught. Consume the verdict so a re-execution supplies a fresh one.
			txResult.WorkerVerdictSet = false
			validity := be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
				func(readVersion, writtenVersion state.Version) state.VersionValidity {
					if readVersion != writtenVersion {
						return state.VersionInvalid
					}
					return state.VersionValid
				}, trace, tracePrefix)
			// SetTrace mutates the shared versionMap; under selfLoop workers validate it
			// concurrently, so only touch the flag when tracing is actually on.
			if dbg.TraceTransactionIO {
				be.versionMap.SetTrace(false)
			}

			valid := validity == state.VersionValid

			if dbg.TraceTransactionIO {
				be.versionMap.SetTrace(trace)
			}
			writeSet := be.blockIO.WriteSet(txVersion.TxIndex)
			be.versionMap.FlushVersionedWrites(writeSet, valid, tracePrefix)
			if dbg.TraceTransactionIO {
				be.versionMap.SetTrace(false)
			}

			if valid {
				be.validateTasks.markComplete(tx)
				be.finalizedResults[tx] = txResult
				// This tx's writes are now flushed to the versionMap. A committed
				// dependent may have validated earlier against the pre-flush state:
				// reading one of these keys from base (missing this tx's first write)
				// or against an older incarnation (writeChangedPrev carries that set).
				// Re-check every committed dependent against the new write-set, and the
				// old one when a prior incarnation existed.
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
				fmt.Println(be.blockNum, "FAILED", tx, be.txIncarnations[tx], "failed", be.execFailed[tx], "aborted", be.execAborted[tx])
			}
			be.validateTasks.clearInProgress(tx)
			if r := be.revalidateCommittedDependents(tx, nil); r != nil {
				return r, nil
			}
			// The worker whose stale verdict we just rejected is alive and parked on
			// slReexec. Signal an in-place re-exec (it owns its incarnation) and leave
			// execTasks complete so the re-sent result re-validates without going
			// through the dispatch path — same as revalidateCommittedDependents.
			be.preValidated[tx] = false
			be.signalSelfLoopReexec(tx)
		}

		if r, ferr := be.advanceCoinbaseAndFinalize(pe, applyTx, stateReader); ferr != nil || r != nil {
			return r, ferr
		}
		// Fixpoint reached: this pass validated nothing and the coinbase frontier
		// did not advance, so no further validation can proceed without a fresh
		// worker result. Return and let the exec loop dispatch/await re-executions.
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
		if res.Version().Incarnation > len(be.tasks) {
			// Re-execution (read-consistency / validation churn) exhausted the
			// incarnation budget. Surface through blockResult.Err (not (nil, err),
			// which would race the apply loop's channel-close completeness check).
			return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%v]: %w: too many incarnations: %d, expected: %d", rules.ErrInvalidBlock, be.blockNum, res.Version().TxIndex, task.TxHash(), res.Err, res.Version().Incarnation, len(be.tasks))), nil
		}
		be.blockIO.RecordReads(res.Version(), res.TxIn)
		be.indexReads(tx, res.TxIn)
		// A sent error is genuine: a tx that read an in-flight or mid-execution
		// changed value re-executes via result.Dep and is never sent. Reject the
		// block only when this task ran against settled input and a post-hoc
		// re-validation confirms its read-set is still current; otherwise the
		// error may stem from a predecessor re-validated since dispatch — defer
		// for re-execution.
		if be.settledInput[tx] || be.frontier() >= tx-1 {
			txVersion := res.Version()
			validity := be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
				func(readVersion, writtenVersion state.Version) state.VersionValidity {
					if readVersion != writtenVersion {
						return state.VersionInvalid
					}
					return state.VersionValid
				}, false, "")
			if validity == state.VersionValid {
				return be.invalidBlockResult(fmt.Errorf("%w: could not apply tx %d:%d [%d:%v]: %w", rules.ErrInvalidBlock, be.blockNum, txVersion.TxIndex, txVersion.TxNum, task.TxHash(), res.Err)), nil
			}
		}
		be.execTasks.clearInProgress(tx)
		be.execTasks.pushDeferred(tx)
		be.execAborted[tx]++
		be.txIncarnations[tx]++
		be.cntAbort++
	} else {
		txVersion := res.Version()

		be.blockIO.RecordReads(txVersion, res.TxIn)
		be.indexReads(tx, res.TxIn)

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
		// A re-executed self-loop result (its worker signalled to re-run by the
		// committed-dependent re-check) already has execTasks complete — only its
		// validation was cleared. markComplete would panic on the non-in-progress
		// task, so skip it; the re-validation below re-commits it.
		if !be.execTasks.checkComplete(tx) {
			be.execTasks.markComplete(tx)
			be.execTasks.removeDependency(tx)
		}
		be.cntSuccess++
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
				// EIP-8037 / EIP-7778: block-level gas is max(cum execution,
				// cum state) — NOT sum of per-tx receipt gas. Receipt gas
				// accounts for refunds and (post-Amsterdam) carries the
				// FloorGasCost floor; summing it bears no fixed relationship
				// to header.GasUsed, which the builder sets via
				// protocol.SetGasUsed = max(cumBlockExecution, cumBlockState).
				be.blockExecutionGasUsed += result.ExecutionResult.BlockExecutionGasUsed
				be.blockStateGasUsed += result.ExecutionResult.BlockStateGasUsed
				be.blockGasUsed = max(be.blockExecutionGasUsed, be.blockStateGasUsed)
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
			// Published: the committed-dependent re-check no longer considers tx, so
			// its still-parked self-loop worker can never be signalled to re-execute
			// again — release it. Closing earlier (at finalize) would let a re-check
			// in the finalize→publish window signal an already-exited worker.
			if tx >= 0 && tx < len(be.slFin) {
				close(be.slFin[tx])
			}

			pe.lastExecutedTxNum.Store(int64(applyResult.txNum))
			if result.writes != nil {
				applyResult.writes = result.writes
				be.applyCount += applyResult.writes.Count()
			}

			// The apply loop folds the versionMap views the txResults carry to sd.mem
			// at block end, keeping sd.mem at N-1 during exec so seedOrigin reads the
			// committed base.
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
			// Prefer the finalized snapshot: under dependency-ordered validation a
			// worker may overwrite be.results[i] with a later speculative
			// incarnation (no receipt) after finalize set the receipt on
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
			// The post-exec validator, which fills receipt blooms for full
			// blocks, skips partial ones — do it here, even when prior receipts
			// couldn't be reconstructed (the suffix receipts still need blooms).
			receipts.DeriveFields(blockReceipts, be.blockHash)
		}

		// Block finalize: run engine.Finalize + MakeWriteSet on the producer
		// side so finalize writes go to the BlockStateCache before the Flush.
		var finalizeWrites state.WriteSetView
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
				reader = pe.prevBlockBase(state.NewHistoryReaderV3WithBlockCache(applyTx, pe.domainsRead(), be.blockStateCache, finalVersion.TxNum), be.blockNum)
			} else {
				reader = pe.prevBlockBase(state.NewCurrentCachedReaderV3(pe.domainsRead().AsGetterNoMetrics(applyTx), be.blockStateCache), be.blockNum)
			}
			pe.RUnlock()

			ibs := state.New(reader)
			defer ibs.Close()
			ibs.SetVersion(finalVersion.Incarnation)
			ibs.SetVersionMap(be.versionMap)
			ibs.SetTxContext(finalVersion.BlockNum, finalVersion.TxIndex)
			ibs.StartAccessRecording()

			if tt, ok := lastResult.Task.(*taskVersion).Task.(*exec.TxTask); ok {
				// Syscalls share the main ibs so their writes (EIP-7002/7251
				// dequeue, EIP-4788 beacon root) land in ibs.VersionedWrites
				// and then in finalizeWrites via Normalize below. If we instead
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

				be.blockIO.RecordReads(finalVersion, ibs.VersionedReads())

				ivw := ibs.VersionedWrites()
				if !ivw.IsEmpty() {
					be.blockIO.RecordWrites(finalVersion, ivw)
					be.versionMap.FlushVersionedWrites(ivw, true, "")
				}

				// Commit finalize writes as a read-only versionMap-slice view over
				// the finalize write-set, sourcing the parallel commit solely from
				// versionedWrites so the write-path stateObject is redundant.
				finalizeWrites = state.NewVersionMapWriteView(ivw, be.versionMap, finalVersion.TxIndex)
				be.applyCount += finalizeWrites.Count()
				// The apply loop folds the finalize writes (via the isFinalize
				// txResult below) at block end.
			}
		}

		// Send finalize txResult through the channel for index writes.
		// State writes are already in the BlockStateCache.
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

		// The apply loop folds the block's writes to sd.mem at block end (off the
		// exec spine); nothing populates blockStateCache here.
		var flushDur time.Duration

		// The block is fully finalized here: every tx sealed, block-end writes in
		// the versionMap. Publish it as an overlay so the next block reads its
		// writes before apply drains them to sd.mem; dropped on commit.
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
			blockStateCache:  be.blockStateCache,
			execStartedAt:    be.execStarted,
			execEndedAt:      time.Now(),
			flushDur:         flushDur,
		}
		return be.result, nil
	}

	// Block not yet complete — return nil. The caller (processResults)
	// only acts on complete blockResults (blockResult.complete == true).
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
	// Drain deferred tx N when its predecessor is validated AND no worker
	// at index < N is in flight. Lower-indexed workers' flushes land at
	// indices visible to N's reads via vm.Read's floor(N-1); higher-indexed
	// ones don't. Non-deferred txs keep dispatching via pending.
	drainMinIP := be.execTasks.minInProgress()
	be.execTasks.drainDeferredIfReady(func(tx int) bool {
		// Dependency-driven drain: re-queue as soon as the tx's actual blockers
		// clear, not when the contiguous prefix reaches tx-1. The contiguous
		// maxValidated gate deadlocks dependency-ordered validation — a real
		// invalidation regresses it and permanently strands deferred txs whose
		// true dependencies are already satisfied.
		// The in-flight guard keeps a lower-indexed worker's floor writes
		// visible on re-read — but it serializes independent re-runs by index.
		return !be.execTasks.isBlocked(tx) && (drainMinIP < 0 || drainMinIP >= tx)
	})

	maxValidated := be.validateTasks.maxComplete()

	// dispatch drains pending, enqueuing each tx. Budget bounds only fresh
	// (incarnation 0) enqueues, which occupy an input-channel slot; retries go
	// to the retry heap (unbounded) and don't consume budget. Txs that can't go
	// now (gate-rejected retry, or fresh with no free slot) are held aside and
	// re-added after the loop so they aren't re-taken in the same call.
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
			// A fresh tx needs a free input-channel slot. If none, leave it in
			// pending (peek, don't take): taking then re-inserting the lowest
			// index at the front would be O(pending) shift churn per call.
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

			// The worker owns its own re-execution loop; the scheduler dispatches a
			// task once, then again only when the committed-dependent re-check
			// un-commits it (guard cleared, incarnation bumped) — start that fresh
			// worker at the bumped incarnation so its versionMap flush stays
			// monotonic over the one the exited worker left.
			if be.selfLoopDispatched[nextTx] {
				be.settledInput[nextTx] = isNextValidated
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
			dispatched++
		}
		for _, tx := range holdBack {
			be.execTasks.pushPending(tx)
		}
		return dispatched
	}

	// Forward-progress net: release deferred (past its predicate) only when nothing
	// dispatched, pending is empty, and nothing is in flight. Guarded on empty
	// pending because the next-to-validate tx is never gate-rejected, so non-empty
	// pending is always dispatchable — the net must not force-drain past it.
	if dispatch() == 0 && be.execTasks.minPending() < 0 && be.execTasks.inProgressCount() == 0 {
		be.execTasks.drainDeferred()
		dispatch()
	}
}

func MergeVersionedWrites(prev, next *state.WriteSet) *state.WriteSet {
	return prev.Merge(next)
}
