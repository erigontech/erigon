package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/state"
)

// commitmentResult is the outcome of a single commitment computation.
type commitmentResult struct {
	blockNum  uint64
	blockHash common.Hash
	txNum     uint64
	rootHash  []byte
	err       error
}

// commitComputeRequest tells the calculator to compute commitment now. It flows
// through the same channel as txResult/blockResult, so by the time it arrives
// all prior touches have been accumulated.
type commitComputeRequest struct{}

// pendingBlock is a blockRequest the calculator has received but not yet
// computed, together with the mode selected for it.
type pendingBlock struct {
	req  *blockRequest
	mode calcMode
}

// computedAheadCount counts BAL compute-ahead computations across all calculators;
// only tests read it (to assert compute-ahead actually engaged rather than
// silently degrading to incremental).
var computedAheadCount atomic.Int64

// logNpPhases emits the per-block additive newPayload phase breakdown
// (stage/exec/commit/overlap/gap/residual) for perf attribution.
var logNpPhases = dbg.EnvBool("NEWPAYLOAD_PHASES", false)

// ComputedAheadCountForTest reports the number of BAL compute-ahead computations
// performed so far; ResetComputedAheadForTest zeroes it. Test-only observability.
func ComputedAheadCountForTest() int64 { return computedAheadCount.Load() }
func ResetComputedAheadForTest()       { computedAheadCount.Store(0) }

// commitmentCalculator receives the same txResult/blockResult stream as the
// apply loop (via a fan-out channel), accumulates key touches, and decides when
// to compute: in BatchCommitments mode it accumulates across blocks and computes
// only on a commitComputeRequest at batch boundaries; otherwise it computes at
// every blockResult. It owns its own commitment.Updates buffer and reads
// block-boundary state from sd.mem (the execLoop flushes before each blockResult).
type commitmentCalculator struct {
	doms        *execctx.SharedDomains
	db          kv.TemporalRoDB
	chainConfig *chain.Config
	logPrefix   string
	logger      log.Logger

	// updates is the calculator's OWN buffer — only this goroutine touches it.
	updates *commitment.Updates

	// spare is the other half of the compute buffer ring. handOffUpdates gives
	// updates to the compute for it to drain, and rotates this one in; the compute
	// drains synchronously, so by the next rotation the handed-off buffer is idle.
	spare *commitment.Updates

	// balUpdates is the per-block BAL fold buffer, Reset and reused across blocks
	// to avoid reallocating a fresh prefix-trie arena each block.
	balUpdates *commitment.Updates

	// state accumulates account/storage values across a block's TX writes, then
	// flushes them to updates at the block boundary. Values are lazy-loaded from
	// the domain on first touch via asOfReader.
	state *calcState

	// asOfReader is shared between calcState (lazy-load) and compute methods
	// (fold/unfold sibling reads); its txNum advances at each block boundary.
	asOfReader *asOfStateReader

	// roTx is a persistent read-only transaction living for the calculator's lifetime.
	roTx kv.TemporalTx

	// lastBlockResult is the most recent block boundary, so computeAndPublish
	// knows which block to compute for.
	lastBlockResult *blockResult

	// lastComputedBlock is the last computed block, avoiding duplicate
	// computation when a commitComputeRequest arrives after a per-block compute
	// already covered it.
	lastComputedBlock uint64

	// hasComputed disambiguates lastComputedBlock=0 (see forcePerBlockCompute).
	hasComputed bool

	// in receives the same applyResult stream as the apply loop, plus
	// commitComputeRequest messages.
	in chan applyResult
	// blockRequests is the per-block heads-up channel — separate from `in`
	// so a blockRequest is never trapped behind a block's txResults.
	blockRequests chan *blockRequest
	// out publishes commitment roots.
	out chan commitmentResult

	// pending records the per-block mode from each blockRequest, keyed by block
	// number; cleared on the matching blockResult.
	pending map[uint64]*pendingBlock

	// firstBlockNum is the batch's first block, whose baseline the prior cycle
	// already committed, so its compute-ahead gate is open at once.
	firstBlockNum uint64
	hasFirstBlock bool

	// lastBlockResultSeen is the highest block whose blockResult has arrived. The
	// compute-ahead gate for block N is lastBlockResultSeen >= N-1: N-1's state
	// must be flushed to sd.mem before N's baseline reads.
	lastBlockResultSeen uint64
	hasSeenBlockResult  bool

	// computedAhead marks blocks already computed by computeBlockFromBAL so a
	// later blockResult(N) does not recompute them. balRoots holds each
	// BAL-driven root for the shadow-mode cross-check.
	computedAhead map[uint64]bool
	balRoots      map[uint64][]byte

	// lastComputedAheadBlock is the highest block computed ahead so far.
	// Compute-ahead reads the commitment domain for its baseline, so it may only
	// run when the preceding block advanced the domain (was itself computed ahead,
	// or is the batch's first block) — otherwise it would read a stale trie
	// across a block whose BAL sidecar was missing.
	lastComputedAheadBlock uint64
	hasComputedAhead       bool

	// signalCtx carries the stopCause; the calculator reads it (never its own
	// compute ctx) to cap compute-ahead at the batch's coalesce block, so a
	// clean-stop cancel never aborts an in-flight commitment.
	signalCtx context.Context

	// forcePerBlockCompute forces a ComputeCommitment at every block boundary,
	// mirroring serial. Per-block computation is required when changesets must be
	// generated (reorg support) or execution proofs kept, so each block's
	// changeset records only its own branch deltas; batch mode folds blocks
	// together and flushes merged updates into the last block's changeset, which
	// is wrong for per-block unwind. The genesis case (block 0) relies on the
	// separate hasComputed flag to distinguish "never computed" from "computed
	// genesis", else the first batch's genesis commitment is never written to sd.
	forcePerBlockCompute bool

	// perBlockFrom is the changeset window start: blocks >= it compute per-block
	// (their changesets must record per-block branch deltas), blocks below it
	// accumulate in batch mode. The last pre-window block triggers a transition
	// compute so no pre-window deltas leak into a window block's changeset.
	perBlockFrom uint64

	wg   sync.WaitGroup
	done chan struct{}
}

func newCommitmentCalculator(
	workCtx context.Context,
	signalCtx context.Context,
	doms *execctx.SharedDomains,
	db kv.TemporalRoDB,
	chainConfig *chain.Config,
	logPrefix string,
	logger log.Logger,
	forcePerBlockCompute bool,
	perBlockFrom uint64,
	in chan applyResult,
	blockRequests chan *blockRequest,
	out chan commitmentResult,
	drTemplate *state.LayeredDomainReader,
) (*commitmentCalculator, error) {
	// ModeUpdate carries values in its btree for the trie to read; the parallel
	// trie reads leaf values from the as-of reader, so keep its ModeParallel buffer.
	sdCtxUpdates := doms.GetCommitmentContext().GetUpdates()
	calcUpdates := sdCtxUpdates.NewEmpty()
	spareUpdates := sdCtxUpdates.NewEmpty()
	if sdCtxUpdates.Mode() != commitment.ModeParallel {
		calcUpdates.SetMode(commitment.ModeUpdate)
		spareUpdates.SetMode(commitment.ModeUpdate)
	}

	// Persistent read-only tx for lazy-loading state, living for the calculator's
	// lifetime. Rolled back in Stop() (not deferred here); it never spans a prune,
	// since Stop() runs before the stageloop commit and prune only fires between
	// batches.
	roTx, err := db.BeginTemporalRo(workCtx) //nolint:gocritic
	if err != nil {
		return nil, fmt.Errorf("commitmentCalculator: open roTx: %w", err)
	}

	// Single asOfStateReader shared by calcState (lazy-load) and compute methods.
	asOfReader := &asOfStateReader{sd: doms, roTx: roTx, txNum: 0, dr: drTemplate.CloneWithTx(roTx)}

	return &commitmentCalculator{
		doms:                 doms,
		db:                   db,
		chainConfig:          chainConfig,
		logPrefix:            logPrefix,
		logger:               logger,
		updates:              calcUpdates,
		spare:                spareUpdates,
		state:                newCalcState(asOfReader, logger, logPrefix),
		asOfReader:           asOfReader,
		roTx:                 roTx,
		signalCtx:            signalCtx,
		in:                   in,
		blockRequests:        blockRequests,
		out:                  out,
		pending:              map[uint64]*pendingBlock{},
		computedAhead:        map[uint64]bool{},
		balRoots:             map[uint64][]byte{},
		forcePerBlockCompute: forcePerBlockCompute,
		perBlockFrom:         perBlockFrom,
		done:                 make(chan struct{}),
	}, nil
}

func (cc *commitmentCalculator) Start(ctx context.Context) {
	cc.wg.Go(func() {
		cc.loop(ctx)
	})
}

func (cc *commitmentCalculator) Stop() {
	close(cc.done)
	cc.wg.Wait()
	// balUpdates isn't closed here: the shared commitment context may still reference it post-exec.
	if cc.roTx != nil {
		cc.roTx.Rollback()
	}
}

func (cc *commitmentCalculator) loop(ctx context.Context) {
	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("sub", "calculator")))
	defer close(cc.out) // Signal apply loop that no more results will come.

	// Exit ONLY when cc.in is closed (by the exec loop, which owns shutdown
	// sequencing). Do NOT add ctx.Done/cc.done checks — exiting early leaves
	// commitment behind sd.mem, causing nonce mismatches on batch restart; all
	// buffered items must be processed. blockRequests is only a compute-ahead
	// heads-up, not a gate, so it is set to nil once closed rather than drained.
	in, reqs := cc.in, cc.blockRequests
	for in != nil {
		select {
		case result, ok := <-in:
			if !ok {
				in = nil
				continue
			}
			cc.handleMessage(ctx, result)
		case req, ok := <-reqs:
			if !ok {
				reqs = nil
				continue
			}
			cc.handleBlockRequest(ctx, req)
		}
	}
}

// perBlockCompute reports whether the given block computes commitment at its
// own boundary (vs accumulating into a batch).

func (cc *commitmentCalculator) perBlockCompute(blockNum uint64) bool {
	return !dbg.BatchCommitments || cc.forcePerBlockCompute || blockNum >= cc.perBlockFrom
}

// ownsChangeset reports whether block n gets its own changeset: genesis excluded,
// window starts at perBlockFrom. A block owning none must compute isolated.

func (cc *commitmentCalculator) ownsChangeset(n uint64) bool {
	return n != 0 && n >= cc.perBlockFrom
}

// handleMessage contains the break logic — decides what to do with each
// message in the stream.

func (cc *commitmentCalculator) handleMessage(ctx context.Context, msg applyResult) {
	switch r := msg.(type) {
	case *txResult:
		// Pin asOfReader at this tx's txNum before ApplyWrites so a first-touch
		// lazy-load reads the canonical pre-tx state via GetAsOf(r.txNum).
		// Otherwise the initial txNum=0 leaks into the first lazy-load and crashes
		// with seekInFiles(txNum=0) on datadirs whose history window starts past
		// genesis. computeAndPublish resets this to lastTxNum+1 before compute, so
		// it only affects the lazy-load path, never the trie fold.
		if r.writes != nil && !r.writes.IsEmpty() {
			cc.asOfReader.txNum = r.txNum
			cc.state.ApplyWrites(r.writes, r.rules.IsAmsterdam)
		}

		// A computed-ahead block already emitted its interior step checkpoints from
		// the BAL while the domain sat at each edge; re-checkpointing here on an
		// already-advanced domain would let the last writer win and leave the step's
		// commitment .kv inconsistent. If a late-consumed blockRequest leaves
		// computedAhead[n] unset and both paths checkpoint the same edge, that is
		// benign — both emit identical values at the same txNum.
		if !cc.computedAhead[r.blockNum] && cc.doms.IsUnfrozenStepEdge(cc.roTx, r.txNum) {
			cc.computeStepBoundary(ctx, &blockResult{BlockNum: r.blockNum, BlockHash: r.blockHash, lastTxNum: r.txNum})
		}

	case *blockResult:
		// A rejected block: skip commitment. sd.mem may hold partial-tx writes
		// from txs that succeeded before the failing one (root would be
		// non-canonical), and emitting an ErrWrongTrieRoot here would race the
		// apply loop's Err return and mask the original validation diagnostic.
		if r.Err != nil {
			return
		}

		// lastBlockResultSeen opens the compute-ahead gate for the next block
		// (its baseline is now in sd.mem).
		cc.lastBlockResult = r
		cc.lastBlockResultSeen = r.BlockNum
		cc.hasSeenBlockResult = true

		commitStart := time.Now()
		switch {
		case cc.computedAhead[r.BlockNum]:
			// Already computed ahead from its BAL. In shadow mode, recompute
			// incrementally and cross-check; otherwise the verified root was already
			// published. Either way clear the per-block dirty flags so a later
			// transition compute doesn't re-flush them.
			if dbg.BALShadowCompute {
				cc.shadowCrossCheck(ctx, r)
			} else {
				cc.state.ResetBlockFlags()
			}
		case cc.perBlockCompute(r.BlockNum):
			if dbg.EnvBool("PARTIAL_TRACE", false) {
				cc.logger.Warn("[partial-trace] perBlockCompute", "blk", r.BlockNum, "isPartial", r.isPartial, "lastComputedBlock", cc.lastComputedBlock, "lastTxNum", r.lastTxNum)
			}
			if cc.lastComputedBlock == 0 && r.isPartial {
				// First block resumed mid-block: compute (as serial does) to save
				// trie state without checking the root.
				cc.computeWithoutCheck(ctx, r)
			} else {
				cc.computeAndCheck(ctx, r)
			}
			if r.BlockNum+1 == cc.perBlockFrom {
				// Flush the boundary block's pending update outside any changeset
				// before the first window block's compute routes into its saved CS.
				cc.flushPendingUpdatesWithoutChangeset(ctx, r)
			}
		case r.BlockNum+1 == cc.perBlockFrom:
			// Last pre-window block: fold everything accumulated in batch mode now,
			// so the first window block's compute covers only its own deltas.
			cc.computeTransition(ctx, r)
		}
		if logNpPhases && !r.execStartedAt.IsZero() {
			commitEnd := time.Now()
			execActive := r.execEndedAt.Sub(r.execStartedAt)
			commitActive := commitEnd.Sub(commitStart)
			stageWall := commitEnd.Sub(r.execStartedAt)
			// Overlap and gap between the exec and commit windows; exactly one is
			// non-zero, and residual proves full accounting.
			var overlap, gap time.Duration
			if d := r.execEndedAt.Sub(commitStart); d > 0 {
				overlap = d
			} else {
				gap = -d
			}
			residual := stageWall - execActive - commitActive + overlap - gap
			cc.logger.Info("[np-phase] stage", "blk", r.BlockNum,
				"stage", stageWall, "exec", execActive, "commit", commitActive,
				"overlap", overlap, "gap", gap, "residual", residual)
		}
		delete(cc.pending, r.BlockNum)
		delete(cc.computedAhead, r.BlockNum)
		delete(cc.balRoots, r.BlockNum)
		cc.maybeComputeAhead(ctx, r.BlockNum+1)

	case *commitComputeRequest:
		if cc.shouldComputeOnRequest() {
			cc.computeAndPublish(ctx, cc.lastBlockResult)
		} else {
			// Publish empty result so drainBeforeExit doesn't block forever.
			cc.publish(ctx, commitmentResult{blockNum: cc.lastComputedBlock})
		}
	}
}

// handleBlockRequest records the per-block mode from a blockRequest —
// BAL-driven when the block carries a BAL, BAL I/O is enabled and
// BALDrivenCommitment is set, else incremental — then tries to compute the
// block ahead of its result stream (maybeComputeAhead).
func (cc *commitmentCalculator) handleBlockRequest(ctx context.Context, req *blockRequest) {
	// Record the batch's first block before the drop-guard: if blockResult(n)
	// arrives before blockRequest(n), a dropped first request must still set
	// firstBlockNum to n, else n+1 claims it and computes ahead on a baseline
	// missing n.
	if !cc.hasFirstBlock {
		cc.firstBlockNum = req.blockNum
		cc.hasFirstBlock = true
	}
	// Drop a request whose block result was already processed: re-inserting
	// pending[n] would let a late compute-ahead read a since-advanced trie and
	// leak pending/computedAhead/balRoots.
	if cc.hasSeenBlockResult && req.blockNum <= cc.lastBlockResultSeen {
		return
	}
	mode := calcModeIncremental
	if len(req.bal) > 0 && !dbg.IgnoreBAL && dbg.BALDrivenCommitment {
		mode = calcModeBALDriven
	}
	cc.pending[req.blockNum] = &pendingBlock{req: req, mode: mode}
	cc.maybeComputeAhead(ctx, req.blockNum)
}

// computeAheadGateOpen reports whether block n's BAL compute-ahead can run:
// block n-1's committed state must be in sd.mem. blockResult(n-1) signals
// that; the batch's first block has it already from the prior cycle.
func (cc *commitmentCalculator) computeAheadGateOpen(n uint64) bool {
	if cc.hasFirstBlock && n == cc.firstBlockNum {
		return true
	}
	return cc.hasSeenBlockResult && cc.lastBlockResultSeen+1 >= n
}

// maybeComputeAhead computes block n from its BAL when it is BAL-driven, the
// compute-ahead gate is open, and doing so is safe — overlapping with block n's
// execution. Idempotent (computedAhead guard).
//
// Two safety restrictions:
//   - Only pre-window blocks (!ownsChangeset) compute ahead, under computeIsolated
//     (nil accumulator), so nothing leaks into a later window block's changeset.
//     A changeset-owning block computes incrementally at its own boundary.
//   - Compute ahead only contiguously from the batch's first block: n's baseline
//     comes from the commitment domain, which only a prior compute-ahead (or the
//     prior cycle) advanced. Computing across a missing-BAL block would read a
//     stale trie.
func (cc *commitmentCalculator) maybeComputeAhead(ctx context.Context, n uint64) {
	pb, ok := cc.pending[n]
	if !ok || pb.mode != calcModeBALDriven || cc.computedAhead[n] {
		return
	}
	// Cap compute-ahead at the batch's coalesce block so commitment cannot outrun
	// the state exec will stop at. Read the signal context, not the compute ctx.
	if sc, stopping := stopCauseOf(cc.signalCtx); stopping && n > sc.block {
		return
	}
	if cc.ownsChangeset(n) {
		return
	}
	if !cc.computeAheadGateOpen(n) {
		return
	}
	if n != cc.firstBlockNum && !(cc.hasComputedAhead && cc.lastComputedAheadBlock == n-1) {
		return
	}
	cc.computeBlockFromBAL(ctx, pb)
}

// computeBlockFromBAL computes block pb's commitment from its BAL, ahead of the
// per-tx result stream. The root is verified against the block header's
// stateRoot — a mismatch fails the block. The fresh calcState is used because
// a BAL-driven block's changed-key set comes wholly from the BAL, never from
// the cross-block incremental accumulator.
func (cc *commitmentCalculator) computeBlockFromBAL(ctx context.Context, pb *pendingBlock) {
	req := pb.req
	br := &blockResult{
		BlockNum:  req.blockNum,
		BlockHash: req.blockHash,
		StateRoot: req.stateRoot,
		lastTxNum: req.lastTxNum,
	}
	// IsEIP161Enabled (not IsSpuriousDragon) so a chain with EIP-161 in
	// disabledEIPs keeps empty leaves exactly as exec does.
	emptyRemoval := req.blockNum != 0 && cc.chainConfig.IsEIP161Enabled(req.blockNum)
	eip8246 := cc.chainConfig.IsAmsterdam(req.blockTime)
	// A block straddling an unfrozen step edge must checkpoint at that edge, else
	// the step's commitment .kv lags its account/storage .kv. The per-tx BAL lets
	// compute-ahead do this mid-block.
	if err := cc.checkpointStepsFromBAL(ctx, req, emptyRemoval, eip8246); err != nil {
		cc.fail(ctx, br, err)
		return
	}
	rh, flushOwn, err := cc.computeRootFromBAL(ctx, req, math.MaxUint32, emptyRemoval, eip8246, targetOf(br))
	if err != nil {
		cc.fail(ctx, br, fmt.Errorf("BAL-driven compute-ahead block %d: %w", req.blockNum, err))
		return
	}
	if !bytes.Equal(rh, req.stateRoot[:]) {
		cc.doms.GetCommitmentContext().ResetPendingUpdates()
		cc.fail(ctx, br, fmt.Errorf("%w: BAL-driven block %d root %x expected %x",
			ErrWrongTrieRoot, req.blockNum, rh, req.stateRoot))
		return
	}
	if flushOwn != nil {
		if err := flushOwn(); err != nil {
			cc.fail(ctx, br, fmt.Errorf("BAL-driven compute-ahead block %d flush: %w", req.blockNum, err))
			return
		}
	}
	cc.computedAhead[req.blockNum] = true
	cc.balRoots[req.blockNum] = rh
	cc.lastComputedAheadBlock = req.blockNum
	cc.hasComputedAhead = true
	computedAheadCount.Add(1)
	cc.lastComputedBlock = req.blockNum
	cc.hasComputed = true
	// Shadow mode defers publish to the incremental cross-check; otherwise publish now.
	if !dbg.BALShadowCompute {
		cc.publish(ctx, commitmentResult{blockNum: req.blockNum, blockHash: req.blockHash, txNum: req.lastTxNum, rootHash: rh})
	}
}

// checkpointStepsFromBAL emits a commitment checkpoint at each unfrozen step
// edge interior to the block, applying the per-tx BAL up to that edge. A txNum's
// BAL index is txNum-firstTxNum, so applying changes at index <= edge-firstTxNum
// gives the state as of the edge. The returned root is discarded (no header to
// verify mid-block). Runs before the block-end compute so that builds on it.
func (cc *commitmentCalculator) checkpointStepsFromBAL(ctx context.Context, req *blockRequest, emptyRemoval bool, eip8246 bool) error {
	ss := cc.doms.StepSize()
	if ss == 0 {
		return nil
	}
	for edge := ((req.firstTxNum/ss)+1)*ss - 1; edge < req.lastTxNum; edge += ss {
		if !cc.doms.IsUnfrozenStepEdge(cc.roTx, edge) {
			continue
		}
		stepBr := &blockResult{BlockNum: req.blockNum, BlockHash: req.blockHash, lastTxNum: edge}
		_, flushOwn, err := cc.computeRootFromBAL(ctx, req, uint32(edge-req.firstTxNum), emptyRemoval, eip8246, targetOf(stepBr))
		if err != nil {
			return fmt.Errorf("BAL-driven step-checkpoint at txNum %d: %w", edge, err)
		}
		if flushOwn != nil {
			if err := flushOwn(); err != nil {
				return fmt.Errorf("BAL-driven step-checkpoint flush at txNum %d: %w", edge, err)
			}
		}
	}
	return nil
}

// computeRootFromBAL builds a calcState from the BAL restricted to maxTxIndex,
// flushes it to a fresh updates buffer, and computes the root at t. Shared by
// the block-end compute-ahead and the mid-block step checkpoints so the two can't drift.
func (cc *commitmentCalculator) computeRootFromBAL(ctx context.Context, req *blockRequest, maxTxIndex uint32, emptyRemoval bool, eip8246 bool, t commitTarget) ([]byte, func() error, error) {
	reader := &asOfStateReader{sd: cc.doms, roTx: cc.roTx, txNum: t.lastTxNum + 1, dr: cc.asOfReader.dr}
	balState := newCalcState(reader, cc.logger, cc.logPrefix)
	balState.LoadFromBALUpTo(req.bal, maxTxIndex, emptyRemoval, cc.chainConfig.Aura != nil, eip8246)
	if err := balState.LazyLoadErr(); err != nil {
		return nil, nil, fmt.Errorf("lazy-load: %w", err)
	}
	if cc.balUpdates == nil {
		cc.balUpdates = cc.updates.NewEmpty()
		// ModeDirect must upgrade to ModeUpdate to carry the BAL values.
		if cc.balUpdates.Mode() != commitment.ModeParallel {
			cc.balUpdates.SetMode(commitment.ModeUpdate)
		}
	} else {
		cc.balUpdates.Reset()
	}
	balUpdates := cc.balUpdates
	balState.FlushToUpdates(balUpdates)
	return cc.computeRootFromUpdates(ctx, t, balUpdates, reader)
}

// computeRootFromUpdates installs an explicit updates buffer + reader on the
// commitment context and computes the root, routed by ownsChangeset like
// compute(). Used by BAL compute-ahead, which supplies its own balState-derived
// updates rather than cc.state.
func (cc *commitmentCalculator) computeRootFromUpdates(ctx context.Context, t commitTarget, updates *commitment.Updates, reader *asOfStateReader) ([]byte, func() error, error) {
	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(updates)
	reader.txNum = t.lastTxNum + 1
	sdCtx.SetStateReader(reader)
	if !cc.ownsChangeset(t.blockNum) {
		return cc.computeIsolated(ctx, t)
	}
	rh, err := cc.computeWithBlockAccumulator(ctx, t)
	return rh, nil, err
}

// shadowCrossCheck recomputes block N incrementally and asserts the root matches
// the BAL-driven root computed ahead; divergence fails the block. Publishes the
// incremental root. BALShadowCompute only.
func (cc *commitmentCalculator) shadowCrossCheck(ctx context.Context, r *blockResult) {
	balRoot := cc.balRoots[r.BlockNum]
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.fail(ctx, r, fmt.Errorf("shadow incremental lazy-load: %w", err))
		return
	}
	// Mirror compute(): the raw versionMap view carries no EIP-161 deletion
	// marker, so a touched-empty account must be removed before the flush or the
	// recompute keeps a spurious leaf the BAL-driven root dropped.
	emptyRemoval := r.BlockNum != 0 && cc.chainConfig.IsEIP161Enabled(r.BlockNum)
	cc.state.ApplyEIP161Removal(emptyRemoval, cc.chainConfig.Aura != nil)
	cc.state.flushToUpdates(cc.updates)
	cc.state.ResetBlockFlags()
	incUpdates := cc.handOffUpdates()
	rh, flushOwn, err := cc.computeRootFromUpdates(ctx, targetOf(r), incUpdates, cc.asOfReader)
	if err != nil {
		cc.fail(ctx, r, fmt.Errorf("shadow incremental compute: %w", err))
		return
	}
	if !bytes.Equal(rh, balRoot) {
		cc.doms.GetCommitmentContext().ResetPendingUpdates()
		cc.fail(ctx, r, fmt.Errorf("%w: shadow mismatch block %d incremental %x BAL-driven %x",
			ErrWrongTrieRoot, r.BlockNum, rh, balRoot))
		return
	}
	if flushOwn != nil {
		if err := flushOwn(); err != nil {
			cc.fail(ctx, r, fmt.Errorf("shadow incremental flush: %w", err))
			return
		}
	}
	cc.publish(ctx, commitmentResult{blockNum: r.BlockNum, blockHash: r.BlockHash, txNum: r.lastTxNum, rootHash: rh})
}

// fail publishes a calculator error. It does NOT cancel execution: the apply
// loop is the sole cancellation authority.
func (cc *commitmentCalculator) fail(ctx context.Context, br *blockResult, err error) {
	if cc.logger != nil {
		cc.logger.Error("["+cc.logPrefix+"] commitmentCalculator: reporting failure", "block", br.BlockNum, "err", err)
	}
	cc.publish(ctx, commitmentResult{blockNum: br.BlockNum, blockHash: br.BlockHash, txNum: br.lastTxNum, err: err})
}

// shouldComputeOnRequest returns true when there's a blockResult and either
// nothing has been computed yet (covers a genesis-only first batch) or a new
// boundary advanced past the last computed one.
func (cc *commitmentCalculator) shouldComputeOnRequest() bool {
	if cc.lastBlockResult == nil {
		return false
	}
	if !cc.hasComputed {
		return true
	}
	return cc.lastBlockResult.BlockNum > cc.lastComputedBlock
}

// commitTarget is the block identity a compute needs; a mid-block checkpoint has
// no StateRoot, so it carries a zero one and never sets checkRoot.
type commitTarget struct {
	blockNum  uint64
	blockHash common.Hash
	lastTxNum uint64
	stateRoot common.Hash
}

func targetOf(br *blockResult) commitTarget {
	return commitTarget{blockNum: br.BlockNum, blockHash: br.BlockHash, lastTxNum: br.lastTxNum, stateRoot: br.StateRoot}
}

// computeMode selects compute's per-call behaviour; isolation is otherwise
// decided by ownsChangeset.
type computeMode struct {
	label       string // error-message context, e.g. "step-boundary "
	midBlock    bool   // mid-block checkpoint: keep block flags dirty and don't advance lastComputedBlock (block-end otherwise)
	checkRoot   bool   // compare the computed root against target.stateRoot
	publishRoot bool   // with checkRoot, publish the successful root too (batch-boundary request), not just mismatches
}

// handOffUpdates returns the filled buffer for the caller to compute against and
// rotates the spare into cc.updates. The compute drains the returned buffer
// synchronously, so it is idle again by the next rotation.
func (cc *commitmentCalculator) handOffUpdates() *commitment.Updates {
	filled := cc.updates
	cc.updates, cc.spare = cc.spare, filled
	cc.updates.Reset()
	return filled
}

// compute is the shared prologue/compute/footer for every calculator commitment
// path; the per-call differences live in m.

func (cc *commitmentCalculator) compute(ctx context.Context, t commitTarget, m computeMode) {
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.publish(ctx, commitmentResult{blockNum: t.blockNum, txNum: t.lastTxNum,
			err: fmt.Errorf("commitmentCalculator: %slazy-load failed: %w", m.label, err)})
		return
	}
	// The raw versionMap view carries no EIP-161 deletion marker; synthesize the
	// empty→removal here.
	emptyRemoval := t.blockNum != 0 && cc.chainConfig.IsEIP161Enabled(t.blockNum)
	cc.state.ApplyEIP161Removal(emptyRemoval, cc.chainConfig.Aura != nil)
	cc.state.flushToUpdates(cc.updates)
	if !m.midBlock {
		cc.state.ResetBlockFlags()
	}

	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(cc.handOffUpdates())

	cc.asOfReader.txNum = t.lastTxNum + 1
	sdCtx.SetStateReader(cc.asOfReader)

	var rh []byte
	var flushOwn func() error
	var err error
	if !cc.ownsChangeset(t.blockNum) {
		rh, flushOwn, err = cc.computeIsolated(ctx, t)
	} else {
		rh, err = cc.computeWithBlockAccumulator(ctx, t)
	}
	if err != nil {
		cc.publish(ctx, commitmentResult{blockNum: t.blockNum, txNum: t.lastTxNum,
			err: fmt.Errorf("commitmentCalculator: %scompute failed: %w", m.label, err)})
		return
	}

	mismatch := m.checkRoot && !bytes.Equal(rh, t.stateRoot[:])
	if flushOwn != nil {
		if mismatch {
			cc.doms.GetCommitmentContext().ResetPendingUpdates()
		} else if ferr := flushOwn(); ferr != nil {
			cc.publish(ctx, commitmentResult{blockNum: t.blockNum, txNum: t.lastTxNum,
				err: fmt.Errorf("commitmentCalculator: %sflush failed: %w", m.label, ferr)})
			return
		}
	}

	if !m.midBlock {
		cc.lastComputedBlock = t.blockNum
		cc.hasComputed = true
	}

	if !m.checkRoot {
		return
	}
	if !m.publishRoot && !mismatch {
		return
	}
	r := commitmentResult{blockNum: t.blockNum, blockHash: t.blockHash, txNum: t.lastTxNum, rootHash: rh}
	if mismatch {
		r.err = fmt.Errorf("%w: block %d root %x expected %x", ErrWrongTrieRoot, t.blockNum, rh, t.stateRoot)
	}
	cc.publish(ctx, r)
}

// computeIsolated computes under a nil changeset accumulator, so a block that
// owns no changeset records into none. It returns the root and the flush of its
// own deferred updates, which the caller runs only once the root is accepted.
func (cc *commitmentCalculator) computeIsolated(ctx context.Context, t commitTarget) ([]byte, func() error, error) {
	// Flush the previous block's own pending update, hash-routed. The commitment
	// diff is swapped to nil for the flush so that when that block owns no saved
	// changeset the flush does not leak its branch deltas into the live
	// accumulator (a later window block's changeset).
	if err := func() error {
		cc.doms.LockChangesetAccumulator()
		defer cc.doms.UnlockChangesetAccumulator()
		defer cc.doms.SwapCommitmentDiffLocked(nil)()
		return cc.doms.FlushPendingUpdatesLocked(ctx, cc.roTx)
	}(); err != nil {
		return nil, nil, err
	}

	rh, err := cc.doms.GetCommitmentContext().ComputeCommitmentWithDiff(ctx, cc.roTx, true, t.blockNum, t.lastTxNum, cc.logPrefix, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	return rh, func() error { return cc.doms.FlushPendingUpdatesWithoutChangeset(cc.roTx) }, nil
}

func (cc *commitmentCalculator) computeAndPublish(ctx context.Context, br *blockResult) {
	cc.compute(ctx, targetOf(br), computeMode{checkRoot: true, publishRoot: true})
}

// computeWithoutCheck computes the first partial block's commitment without
// verifying the root (its trie state doesn't match the header).

func (cc *commitmentCalculator) computeWithoutCheck(ctx context.Context, br *blockResult) {
	cc.compute(ctx, targetOf(br), computeMode{label: "partial-block "})
}

// computeStepBoundary checkpoints commitment at a mid-block step edge without
// advancing lastComputedBlock or resetting block flags — the block-end fold
// still needs the pre-edge dirty keys.

func (cc *commitmentCalculator) computeStepBoundary(ctx context.Context, br *blockResult) {
	cc.compute(ctx, targetOf(br), computeMode{label: "step-boundary ", midBlock: true})
}

// computeAndCheck computes per-block commitment and validates the root,
// publishing only on mismatch (silent success keeps the bounded output channel
// from deadlocking).

func (cc *commitmentCalculator) computeAndCheck(ctx context.Context, br *blockResult) {
	cc.compute(ctx, targetOf(br), computeMode{checkRoot: true})
}

// flushPendingUpdatesWithoutChangeset eagerly applies the pending deferred
// update under a nil accumulator — a pre-window block's branch deltas must
// not pend into the first window block's changeset-routed compute.

func (cc *commitmentCalculator) flushPendingUpdatesWithoutChangeset(ctx context.Context, br *blockResult) {
	// The closure bounds the locked window: publish must stay outside it, since
	// its send can block on the apply loop, which contends on changesetMu.
	err := func() error {
		cc.doms.LockChangesetAccumulator()
		defer cc.doms.UnlockChangesetAccumulator()
		defer cc.doms.SwapCommitmentDiffLocked(nil)()
		return cc.doms.FlushPendingUpdatesLocked(ctx, cc.roTx)
	}()
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: %w", err),
		})
	}
}

// computeTransition folds all accumulated batch-mode blocks at the last
// pre-window block, isolated so their deltas don't leak into the first window
// block's changeset.

func (cc *commitmentCalculator) computeTransition(ctx context.Context, br *blockResult) {
	cc.compute(ctx, targetOf(br), computeMode{checkRoot: true})
}

func (cc *commitmentCalculator) publish(ctx context.Context, r commitmentResult) {
	// Best-effort send; log only genuine errors (wrong-root and shutdown cancels
	// are expected). The apply loop surfaces the authoritative error.
	if r.err != nil && cc.logger != nil &&
		!errors.Is(r.err, ErrWrongTrieRoot) &&
		!errors.Is(r.err, context.Canceled) &&
		!errors.Is(r.err, context.DeadlineExceeded) {
		cc.logger.Warn("["+cc.logPrefix+"] commitment compute failed", "block", r.blockNum, "txNum", r.txNum, "err", r.err)
	}
	select {
	case cc.out <- r:
	case <-ctx.Done():
	case <-cc.done:
	}
}

// computeWithBlockAccumulator runs ComputeCommitment with block N's own writes
// (branch nodes and the [state] marker) routed into an explicit diff — N's saved
// changeset when present (looked up by hash, since multiple changesets can exist
// per block number after a fork-bounce), else the live accumulator's. Routing
// through the diff needs no changesetMu against a concurrent SetChangesetAccumulator,
// so the apply loop's DomainPut is never blocked by the fold. It also stamps the
// pending deferred update with the block hash so the next call's flush uses the
// same hash-aware routing.
func (cc *commitmentCalculator) computeWithBlockAccumulator(ctx context.Context, t commitTarget) ([]byte, error) {
	defer func() {
		if upd := cc.doms.GetCommitmentContext().PeekPendingUpdate(); upd != nil && upd.BlockNum == t.blockNum {
			upd.BlockHash = t.blockHash
		}
	}()

	// Flush block N-1's own pending update (hash-routed to N-1's saved changeset,
	// independent of N's diff below) — the one remaining changesetMu window, brief
	// rather than spanning the fold that follows.
	if err := func() error {
		cc.doms.LockChangesetAccumulator()
		defer cc.doms.UnlockChangesetAccumulator()
		return cc.doms.FlushPendingUpdatesLocked(ctx, cc.roTx)
	}(); err != nil {
		return nil, err
	}

	// Read live before the saved lookup: the exec loop saves N strictly before it
	// rotates away from N, so whichever read lands after a rotation, the other
	// still identifies N's changeset. A mid-block step-boundary finds no saved cs
	// (the exec loop saves once the block is done) and falls back to live.
	live := cc.doms.GetChangesetAccumulator()
	cs := cc.doms.GetChangesetByHash(t.blockNum, t.blockHash)
	var diff *kv.DomainDiff
	if cs != nil {
		diff = &cs.Diffs[kv.CommitmentDomain]
	} else if live != nil {
		diff = &live.Diffs[kv.CommitmentDomain]
	}
	return cc.doms.GetCommitmentContext().ComputeCommitmentWithDiff(ctx, cc.roTx, true, t.blockNum, t.lastTxNum, cc.logPrefix, nil, diff)
}

// asOfStateReader reads account/storage/code at a specific txNum via
// sd.GetAsOf (which checks sd.mem first, then falls through to files).
// Commitment domain reads use GetLatest since branches are only written
// by the calculator sequentially.
type asOfStateReader struct {
	sd    *execctx.SharedDomains
	roTx  kv.TemporalTx
	txNum uint64
	// workerCtx, when non-nil, carries a worker's lock-free metrics accumulator so
	// the CommitmentDomain read routes through GetLatestContext instead of racing
	// the shared main accumulator. Nil on the main reader.
	workerCtx context.Context

	// dr is the shared account/storage/code reader (the multi-block versionMap
	// window over sd.mem + files). It carries its own files-fallback tx, so
	// concurrent users never share one.
	dr *state.LayeredDomainReader
}

func (r *asOfStateReader) WithHistory() bool { return false }

func (r *asOfStateReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	return nil
}

func (r *asOfStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	if d == kv.CommitmentDomain {
		// Branches: use GetLatest — written only by this calculator, sequential.
		if r.workerCtx != nil {
			return r.sd.GetLatestContext(r.workerCtx, d, r.roTx, plainKey)
		}
		return r.sd.GetLatest(d, r.roTx, plainKey)
	}
	// Account/storage/code: the window layered over sd.mem + files.
	enc, ok, err := r.dr.ReadDomain(d, plainKey, r.txNum)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		enc = nil
	}
	if stepSize > 0 {
		step = kv.Step(r.txNum / stepSize)
	}
	return enc, step, nil
}

func (r *asOfStateReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	return &asOfStateReader{sd: r.sd, roTx: tx, txNum: r.txNum, dr: r.dr.CloneWithTx(tx)}
}

// CloneForWorker meters the worker's CommitmentDomain reads into the per-worker
// accumulator carried by workerCtx, so a clone used during block assembly (where
// trie-warmup runs concurrently) doesn't write the shared main accumulator.
func (r *asOfStateReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) commitmentdb.StateReader {
	return &asOfStateReader{sd: r.sd, roTx: tx, txNum: r.txNum, workerCtx: workerCtx, dr: r.dr.CloneWithTx(tx)}
}
