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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// commitmentResult is the outcome of a single commitment computation.
type commitmentResult struct {
	blockNum  uint64
	blockHash common.Hash
	txNum     uint64
	rootHash  []byte
	err       error
}

// commitComputeRequest is sent through the commitResults channel to tell
// the calculator to compute commitment NOW. It flows through the same
// channel as txResult/blockResult so ordering is preserved — by the time
// the calculator sees it, all prior touches have been accumulated.
type commitComputeRequest struct{}

// pendingBlock is a blockRequest the calculator has received but not yet
// computed, together with the mode selected for it.
type pendingBlock struct {
	req  *blockRequest
	mode calcMode
}

// foldsAheadPerformed counts BAL fold-ahead computations across all calculators;
// only tests read it (to assert the fold path actually engaged rather than
// silently degrading to incremental).
var foldsAheadPerformed atomic.Int64

// FoldsAheadPerformedForTest reports the number of BAL fold-ahead computations
// performed so far; ResetFoldsAheadForTest zeroes it. Test-only observability.
func FoldsAheadPerformedForTest() int64 { return foldsAheadPerformed.Load() }
func ResetFoldsAheadForTest()           { foldsAheadPerformed.Store(0) }

// commitmentCalculator receives the same txResult/blockResult stream as the
// apply loop (via a fan-out channel). For each txResult it accumulates key
// touches from the writes. It contains the break logic that decides when
// to compute:
//
//   - BatchCommitments mode (default, initial sync): accumulates across many
//     blocks, computes only when the apply loop sends a commitComputeRequest
//     at batch boundaries (flushPending, maxBlockNum, exhausted).
//
//   - Per-block mode (!BatchCommitments): computes at every blockResult.
//     Used at chain tip or when shouldGenerateChangesets is true.
//
// The calculator owns its own commitment.Updates buffer, fed one aggregated
// update per dirty key by calcState.FlushToUpdates — no serialization round-trip.
//
// It reads values from sd.mem (shared with the execLoop) via GetLatest.
// The execLoop's Flush writes to sd.mem before sending the blockResult,
// so by the time the calculator receives the blockResult, sd.mem has the
// correct block-boundary state.
type commitmentCalculator struct {
	doms        *execctx.SharedDomains
	db          kv.TemporalRoDB
	chainConfig *chain.Config
	logPrefix   string
	logger      log.Logger

	// updates is the calculator's OWN buffer — never shared with the
	// execLoop or apply loop. Only this goroutine reads/writes it.
	updates *commitment.Updates

	// state accumulates account/storage values across TX writes.
	// At block boundary, the accumulated state is flushed to updates.
	// Values are lazy-loaded from the domain on first touch via asOfReader.
	state *calcState

	// asOfReader is shared between calcState (lazy-load) and compute
	// methods (fold/unfold sibling reads). Its txNum is updated at each
	// block boundary.
	asOfReader *asOfStateReader

	// roTx is a persistent read-only transaction for domain reads.
	// Opened at start, lives for the calculator's lifetime.
	roTx kv.TemporalTx

	// lastBlockResult tracks the most recent block boundary so that
	// computeAndPublish knows which block to compute for.
	lastBlockResult *blockResult

	// lastComputedBlock tracks the block number of the last computed
	// commitment to avoid duplicate computation when commitComputeRequest
	// arrives after a per-block computation already covered this block.
	lastComputedBlock uint64

	// hasComputed disambiguates lastComputedBlock=0: without this flag,
	// "never computed" and "computed block 0 (genesis)" both look like
	// lastComputedBlock=0. The commitComputeRequest dedup check would
	// then skip computing the very first batch when its lastBlockResult
	// is block 0 — leaving the genesis commitment unwritten to sd, which
	// breaks SeekCommitment for the next exec3 cycle (it falls back to
	// stage progress instead of finding a commitment state).
	hasComputed bool

	// in receives the same applyResult stream as the apply loop,
	// plus commitComputeRequest messages for explicit compute triggers.
	in chan applyResult
	// blockRequests is the per-block heads-up channel — separate from `in`
	// so a blockRequest is never trapped behind a block's txResults.
	blockRequests chan *blockRequest
	// out publishes commitment roots.
	out chan commitmentResult

	// pending records the per-block mode selected from each blockRequest,
	// keyed by block number — populated from blockRequests, cleared on the
	// matching blockResult.
	pending map[uint64]*pendingBlock

	// firstBlockNum is the block number of the first blockRequest this
	// calculator sees — the batch's first block, whose baseline is already
	// committed by the prior cycle, so its BAL fold gate is open at once.
	firstBlockNum uint64
	hasFirstBlock bool

	// lastBlockResultSeen is the highest block whose blockResult has arrived
	// on `in`. The BAL fold gate for block N is lastBlockResultSeen >= N-1:
	// block N-1's state must be flushed to sd.mem (blockResult signals it)
	// before N's baseline reads.
	lastBlockResultSeen uint64
	hasSeenBlockResult  bool

	// foldedAhead marks blocks already computed by foldBlockFromBAL, so the
	// later blockResult(N) does not recompute them. balRoots holds each
	// BAL-driven root for the shadow-mode cross-check.
	foldedAhead map[uint64]bool
	balRoots    map[uint64][]byte

	// lastFoldedBlock is the highest block folded so far. A fold reads the
	// commitment domain for its baseline, so it may only run when the
	// preceding block advanced the domain — i.e. was itself folded (or is the
	// batch's first block, whose baseline the prior cycle committed). This
	// stops a fold from reading a stale trie across a block whose BAL sidecar
	// was missing (eth/71 backfill is best-effort) and so never advanced it.
	lastFoldedBlock uint64
	hasFolded       bool

	// signalCtx is the shared executor context carrying the stopCause. The
	// calculator reads it (never its own compute ctx) to cap fold-ahead at the
	// batch's coalesce block M — compute/publish run on the separate uncancelled
	// workCtx so a clean-stop cancel never aborts an in-flight commitment.
	signalCtx context.Context

	// forcePerBlockCompute overrides dbg.BatchCommitments and triggers a
	// ComputeCommitment at every block boundary. Mirrors serial's
	// `!dbg.BatchCommitments || shouldGenerateChangesets || KeepExecutionProofs`
	// gate in [exec3_serial.go]: when changesets must be generated (reorg
	// support) or execution proofs must be kept, per-block computation is
	// required so that each block's changeset records the branch deltas
	// attributable to that block alone. In batch mode, the trie folds
	// multiple blocks together and the deferred buffer dedupes branch
	// prefixes across the batch — those merged updates flush into the
	// LAST block's changeset, which is wrong for per-block unwind.
	forcePerBlockCompute bool

	// perBlockFrom is the batch's changeset window start: blocks >= it
	// compute per-block (their changesets must record per-block branch
	// deltas); blocks below it accumulate in batch mode. The last
	// pre-window block triggers a transition compute (computeTransition)
	// so no pre-window branch deltas leak into a window block's changeset.
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
) (*commitmentCalculator, error) {
	// ModeUpdate carries values in its btree for the trie to read; the parallel
	// trie reads leaf values from the as-of reader, so keep its ModeParallel buffer.
	sdCtxUpdates := doms.GetCommitmentContext().GetUpdates()
	calcUpdates := sdCtxUpdates.NewEmpty()
	if sdCtxUpdates.Mode() != commitment.ModeParallel {
		calcUpdates.SetMode(commitment.ModeUpdate)
	}

	// Open a persistent read-only TX for lazy-loading state from the domain.
	// This lives for the calculator's lifetime, like worker TX handles.
	roTx, err := db.BeginTemporalRo(workCtx) //nolint:gocritic
	if err != nil {
		return nil, fmt.Errorf("commitmentCalculator: open roTx: %w", err)
	}
	// roTx lives for the calculator's lifetime — rolled back in Stop(), not
	// deferred here. Safe across collate/prune cycles because the calculator
	// is constructed in pe.exec() and its `defer Stop()` runs *before* the
	// stageloop's rwTx.Commit(), and CollateAndPrune only fires
	// between batches via FCU. So this roTx never spans a prune — by the
	// time prune holds commitGate.Lock(), Stop() has already rolled this tx
	// back and the calculator goroutine is gone.

	// Single asOfStateReader shared by calcState (lazy-load) and compute
	// methods (fold/unfold sibling reads). Uses GetAsOf for account/storage
	// (avoids future sd.mem state) and GetLatest for commitment branches
	// (written sequentially by this calculator).
	asOfReader := &asOfStateReader{sd: doms, roTx: roTx, txNum: 0}

	return &commitmentCalculator{
		doms:                 doms,
		db:                   db,
		chainConfig:          chainConfig,
		logPrefix:            logPrefix,
		logger:               logger,
		updates:              calcUpdates,
		state:                newCalcState(asOfReader, logger, logPrefix),
		asOfReader:           asOfReader,
		roTx:                 roTx,
		signalCtx:            signalCtx,
		in:                   in,
		blockRequests:        blockRequests,
		out:                  out,
		pending:              map[uint64]*pendingBlock{},
		foldedAhead:          map[uint64]bool{},
		balRoots:             map[uint64][]byte{},
		forcePerBlockCompute: forcePerBlockCompute,
		perBlockFrom:         perBlockFrom,
		done:                 make(chan struct{}),
	}, nil
}

func (cc *commitmentCalculator) Start(ctx context.Context) {
	cc.wg.Add(1)
	go cc.loop(ctx)
}

func (cc *commitmentCalculator) Stop() {
	close(cc.done)
	cc.wg.Wait()
	if cc.roTx != nil {
		cc.roTx.Rollback()
	}
}

func (cc *commitmentCalculator) loop(ctx context.Context) {
	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("sub", "calculator")))
	defer cc.wg.Done()
	defer close(cc.out) // Signal apply loop that no more results will come.

	// The calculator exits ONLY when cc.in is closed (by the exec loop).
	// Do NOT add ctx.Done or cc.done checks here — the exec loop owns
	// shutdown sequencing. Exiting early would leave commitment behind
	// sd.mem, causing nonce mismatches on batch restart. The calculator
	// must process ALL buffered items before exiting.
	// Context cancellation is handled by the exec loop which closes
	// cc.in after stopping.
	//
	// blockRequests is multiplexed but not a gate: it is only a fold-ahead
	// heads-up, and draining leftovers after cc.in closes would re-fold
	// already-finalized blocks. It is set to nil once closed.
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
		// Accumulate writes in the calculator's local state.
		// Values are lazy-loaded from domain on first touch, then
		// overwritten by each TX's writes. At block boundary,
		// the accumulated final values are flushed to the trie.
		//
		// Pin asOfReader at this tx's txNum BEFORE ApplyWrites so any
		// first-touch lazy-load reads the canonical "pre-tx" state via
		// GetAsOf(r.txNum). Without this the calculator's initial
		// asOfReader.txNum=0 leaks into the first lazy-load and crashes
		// with seekInFiles(txNum=0) on snapshot-loaded datadirs whose
		// visible history window starts well past genesis (e.g. devnets
		// loaded from a chunked snapshot). On synced-from-genesis
		// datadirs txNum=0 is in-window so the bug is invisible.
		//
		// computeAndPublish overwrites this back to lastTxNum+1 right
		// before ComputeCommitment, so the per-tx setting only affects
		// the lazy-load path and never leaks into the trie fold path.
		if !r.writes.IsEmpty() {
			cc.asOfReader.txNum = r.txNum
			cc.state.ApplyWrites(r.writes)
		}

		// A folded-ahead block already emitted its interior step checkpoints from
		// the BAL (foldStepCheckpoints), computed while the domain sat exactly at
		// each edge. Re-checkpointing here from the partially-accumulated cc.state
		// on an already-advanced domain would let the last writer win and leave the
		// step's commitment .kv inconsistent — so only the incremental path (which
		// owns the checkpoint for non-folded blocks) runs the hook.
		if !cc.foldedAhead[r.blockNum] && cc.doms.IsUnfrozenStepEdge(cc.roTx, r.txNum) {
			cc.computeStepBoundary(ctx, &blockResult{BlockNum: r.blockNum, BlockHash: r.blockHash, lastTxNum: r.txNum})
		}

	case *blockResult:
		// Block-validity rejection (set by the worker-result path in
		// nextResult — insufficient funds, gas overflow, finalize error,
		// scheduler-exhausted incarnations). The apply loop's case
		// *blockResult fast-paths Err != nil and returns it directly;
		// we must NOT compute commitment for this block because (a)
		// sd.mem may contain partial-tx writes from txs that succeeded
		// before the failing one, so the computed root would be
		// non-canonical, and (b) computing here would emit an
		// ErrWrongTrieRoot through rootResults that races with the apply
		// loop's Err return — the wrong-trie-root error wins and masks
		// the original validation diagnostic (EEST assertions on the
		// underlying exception class then fail). Skip silently and let
		// the apply loop surface the worker's diagnosis.
		if r.Err != nil {
			return
		}

		// Track the latest block boundary. lastBlockResultSeen opens the BAL
		// fold gate for the next block (its baseline is now in sd.mem).
		cc.lastBlockResult = r
		cc.lastBlockResultSeen = r.BlockNum
		cc.hasSeenBlockResult = true

		// Break logic: in per-block mode, compute at every block boundary.
		// Skip the first block if it's a partial block (resumed mid-block).
		// `forcePerBlockCompute` overrides dbg.BatchCommitments to mirror
		// serial's gate (exec3_serial.go around the `if !dbg.BatchCommitments
		// || shouldGenerateChangesets || ...` check) — per-block compute is
		// required when changesets must record per-block branch deltas
		// (reorg support, KeepExecutionProofs). Blocks from the changeset
		// window (perBlockFrom) onward compute per-block for the same reason.
		switch {
		case cc.foldedAhead[r.BlockNum]:
			// Folded ahead from its BAL (pre-window: computeIsolated already
			// advanced the domain and flushed its deferred update under a nil
			// accumulator, so nothing leaks into a later block's changeset). In
			// shadow mode, recompute incrementally and cross-check the roots;
			// otherwise the verified root was already published. Either way clear
			// the per-block dirty flags the block's txResults set in cc.state so
			// they are not re-flushed by a later transition compute.
			if dbg.BALShadowCompute {
				cc.shadowCrossCheck(ctx, r)
			} else {
				cc.state.ResetBlockFlags()
			}
		case cc.perBlockCompute(r.BlockNum):
			if cc.lastComputedBlock == 0 && r.isPartial {
				// First block is partial (resumed mid-block).
				// Compute it (like serial does) to save trie state, then
				// restore that state so the next full block starts from
				// the same trie state as serial's batch 2 start.
				cc.computeWithoutCheck(ctx, r)
			} else {
				cc.computeAndCheck(ctx, r)
			}
			if r.BlockNum+1 == cc.perBlockFrom {
				// Pre-window per-block computes (BatchCommitments off) defer
				// branch writes too — flush the boundary block's pending update
				// outside any changeset before the first window block's compute
				// routes into its saved CS.
				cc.flushPendingUpdatesWithoutChangeset(ctx, r)
			}
		case r.BlockNum+1 == cc.perBlockFrom:
			// Last pre-window block: fold everything accumulated in batch
			// mode now, so the first window block's per-block compute (and
			// changeset) covers only its own deltas.
			cc.computeTransition(ctx, r)
		}
		// In BatchCommitments mode (without forcePerBlockCompute): just
		// accumulate — compute only on explicit commitComputeRequest from
		// the apply loop.

		// Block N is done; its boundary opens the fold gate for N+1.
		delete(cc.pending, r.BlockNum)
		delete(cc.foldedAhead, r.BlockNum)
		delete(cc.balRoots, r.BlockNum)
		cc.maybeFoldAhead(ctx, r.BlockNum+1)

	case *commitComputeRequest:
		// Explicit compute signal from the apply loop at batch boundary.
		if cc.shouldComputeOnRequest() {
			cc.computeAndPublish(ctx, cc.lastBlockResult)
		} else {
			// Publish empty result so drainBeforeExit doesn't block forever
			cc.publish(ctx, commitmentResult{blockNum: cc.lastComputedBlock})
		}
	}
}

// handleBlockRequest records the per-block mode from a blockRequest —
// BAL-driven when the block carries a BAL, BAL I/O is enabled and
// BALDrivenCommitment is set, else incremental — then tries to fold the
// block ahead of its result stream (maybeFoldAhead).
func (cc *commitmentCalculator) handleBlockRequest(ctx context.Context, req *blockRequest) {
	// Record the batch's first block before the drop-guard: if blockResult(n) is
	// consumed before blockRequest(n) (the in/reqs select has no cross-channel
	// ordering), a dropped first request must still set firstBlockNum to n rather
	// than leave n+1 to claim it — else foldGateOpen and the contiguity guard are
	// both bypassed via n == firstBlockNum and n+1 folds on a baseline missing n.
	if !cc.hasFirstBlock {
		cc.firstBlockNum = req.blockNum
		cc.hasFirstBlock = true
	}
	// Drop a request whose block result was already processed: re-inserting
	// pending[n] would let a late fold read a trie that has since advanced (batch
	// boundary, step checkpoint, per-block compute), poisoning a valid block, and
	// would leak pending/foldedAhead/balRoots (retaining the decoded BAL).
	if cc.hasSeenBlockResult && req.blockNum <= cc.lastBlockResultSeen {
		return
	}
	mode := calcModeIncremental
	if len(req.bal) > 0 && !dbg.IgnoreBAL && dbg.BALDrivenCommitment {
		mode = calcModeBALDriven
	}
	cc.pending[req.blockNum] = &pendingBlock{req: req, mode: mode}
	cc.maybeFoldAhead(ctx, req.blockNum)
}

// foldGateOpen reports whether block n's BAL fold can run: block n-1's
// committed state must be in sd.mem. blockResult(n-1) signals that; the
// batch's first block has it already from the prior cycle.
func (cc *commitmentCalculator) foldGateOpen(n uint64) bool {
	if cc.hasFirstBlock && n == cc.firstBlockNum {
		return true
	}
	return cc.hasSeenBlockResult && cc.lastBlockResultSeen+1 >= n
}

// maybeFoldAhead folds block n from its BAL when it is BAL-driven, the fold
// gate is open, and folding it is safe. Folding overlaps the execution of
// block n — the parallel-commitment win. Idempotent (foldedAhead guard).
//
// Two safety restrictions, both preserving the well-tested incremental path:
//   - Only pre-window blocks (!ownsChangeset) fold. A block that owns a
//     changeset computes incrementally at its own boundary so its per-block
//     branch deltas are captured; folding it ahead would race the exec loop's
//     changeset-accumulator install. Pre-window folds run under computeIsolated
//     (nil accumulator), so nothing leaks into a later window block's changeset.
//   - Fold only contiguously from the batch's first block: n's baseline is read
//     from the commitment domain, which only a prior fold (or the prior cycle,
//     for the first block) advanced. A missing-BAL block accumulates without
//     advancing the domain, so folding across it would read a stale trie.
func (cc *commitmentCalculator) maybeFoldAhead(ctx context.Context, n uint64) {
	pb, ok := cc.pending[n]
	if !ok || pb.mode != calcModeBALDriven || cc.foldedAhead[n] {
		return
	}
	// Batch cut: the shared executor context carries the coalesce block M. Fold
	// no further than M so commitment cannot outrun the state exec will stop at
	// (an orphan → wrong root on restart). Read the signal context, never the
	// compute ctx — compute must still finish blocks up to M.
	if sc, stopping := stopCauseOf(cc.signalCtx); stopping && n > sc.block {
		return
	}
	if cc.ownsChangeset(n) {
		return
	}
	if !cc.foldGateOpen(n) {
		return
	}
	if n != cc.firstBlockNum && !(cc.hasFolded && cc.lastFoldedBlock == n-1) {
		return
	}
	cc.foldBlockFromBAL(ctx, pb)
}

// foldBlockFromBAL computes block pb's commitment from its BAL, ahead of the
// per-tx result stream. The root is verified against the block header's
// stateRoot — a mismatch fails the block. The fresh calcState is used because
// a BAL-driven block's changed-key set comes wholly from the BAL, never from
// the cross-block incremental accumulator.
func (cc *commitmentCalculator) foldBlockFromBAL(ctx context.Context, pb *pendingBlock) {
	req := pb.req
	br := &blockResult{
		BlockNum:  req.blockNum,
		BlockHash: req.blockHash,
		StateRoot: req.stateRoot,
		lastTxNum: req.lastTxNum,
	}
	// EIP-161 empty-removal inputs, matching normalizeWriteSet on the exec path.
	// IsEIP161Enabled (not IsSpuriousDragon) so a chain with EIP-161 in disabledEIPs
	// keeps empty leaves in the fold exactly as exec does.
	emptyRemoval := req.blockNum != 0 && cc.chainConfig.IsEIP161Enabled(req.blockNum)
	// A block straddling an unfrozen step edge must leave a commitment checkpoint
	// at that edge (else the step's commitment .kv lags its account/storage .kv).
	// The per-tx BAL lets the fold checkpoint mid-block, so a straddling block
	// stays on the fold path instead of dropping to the incremental one.
	if err := cc.foldStepCheckpoints(ctx, req, emptyRemoval); err != nil {
		cc.fail(ctx, br, err)
		return
	}
	rh, err := cc.foldBALToRoot(ctx, req, math.MaxUint32, emptyRemoval, targetOf(br))
	if err != nil {
		cc.fail(ctx, br, fmt.Errorf("BAL-driven fold block %d: %w", req.blockNum, err))
		return
	}
	if !bytes.Equal(rh, req.stateRoot[:]) {
		cc.fail(ctx, br, fmt.Errorf("%w: BAL-driven block %d root %x expected %x",
			ErrWrongTrieRoot, req.blockNum, rh, req.stateRoot))
		return
	}
	cc.foldedAhead[req.blockNum] = true
	cc.balRoots[req.blockNum] = rh
	cc.lastFoldedBlock = req.blockNum
	cc.hasFolded = true
	foldsAheadPerformed.Add(1)
	cc.lastComputedBlock = req.blockNum
	cc.hasComputed = true
	// Shadow mode defers publish to the incremental cross-check at
	// blockResult(N); otherwise publish the verified root now.
	if !dbg.BALShadowCompute {
		cc.publish(ctx, commitmentResult{blockNum: req.blockNum, blockHash: req.blockHash, txNum: req.lastTxNum, rootHash: rh})
	}
}

// foldStepCheckpoints emits a commitment checkpoint at each unfrozen step edge
// interior to the block (edge < block-end txNum), folding the per-tx BAL up to
// that edge. Without it a block straddling a step edge would leave the step's
// commitment .kv lagging its account/storage .kv. The BAL index of a txNum is
// txNum-firstTxNum (blockAccessIndex == TxIndex+1 and txNum == firstTxNum+
// TxIndex+1), so folding changes at index ≤ edge-firstTxNum is the state as of
// the edge. computeRootFromUpdates saves the checkpoint (ComputeCommitmentLocked
// with saveStateAfter); the returned root is discarded — there is no header to
// verify mid-block. Runs before the block-end fold so that builds on it.
func (cc *commitmentCalculator) foldStepCheckpoints(ctx context.Context, req *blockRequest, emptyRemoval bool) error {
	ss := cc.doms.StepSize()
	if ss == 0 {
		return nil
	}
	for edge := ((req.firstTxNum/ss)+1)*ss - 1; edge < req.lastTxNum; edge += ss {
		if !cc.doms.IsUnfrozenStepEdge(cc.roTx, edge) {
			continue
		}
		stepBr := &blockResult{BlockNum: req.blockNum, BlockHash: req.blockHash, lastTxNum: edge}
		if _, err := cc.foldBALToRoot(ctx, req, uint32(edge-req.firstTxNum), emptyRemoval, targetOf(stepBr)); err != nil {
			return fmt.Errorf("BAL-driven step-checkpoint at txNum %d: %w", edge, err)
		}
	}
	return nil
}

// foldBALToRoot builds a calcState from the BAL restricted to maxTxIndex,
// flushes it to a fresh updates buffer, and computes the root at t. Shared by
// the block-end fold and the mid-block step checkpoints so the two can't drift.
func (cc *commitmentCalculator) foldBALToRoot(ctx context.Context, req *blockRequest, maxTxIndex uint32, emptyRemoval bool, t commitTarget) ([]byte, error) {
	reader := &asOfStateReader{sd: cc.doms, roTx: cc.roTx, txNum: t.lastTxNum + 1}
	balState := newCalcState(reader, cc.logger, cc.logPrefix)
	balState.LoadFromBALUpTo(req.bal, maxTxIndex, emptyRemoval, cc.chainConfig.Aura != nil)
	if err := balState.LazyLoadErr(); err != nil {
		return nil, fmt.Errorf("lazy-load: %w", err)
	}
	balUpdates := cc.updates.NewEmpty()
	// Match the calculator's constructor: only ModeDirect needs upgrading to
	// ModeUpdate to carry the fold's BAL-sourced values; ModeParallel already
	// carries them (and ParallelPatriciaHashed rejects any other mode).
	if balUpdates.Mode() != commitment.ModeParallel {
		balUpdates.SetMode(commitment.ModeUpdate)
	}
	balState.FlushToUpdates(balUpdates)
	return cc.computeRootFromUpdates(ctx, t, balUpdates, reader)
}

// computeRootFromUpdates installs an explicit updates buffer + reader on the
// commitment context and computes the root, routed by ownsChangeset exactly
// like compute(): a pre-window block computes isolated (nil accumulator,
// flushing its own deferred update) so its branch deltas never pend into a
// later window block's changeset. Used by the BAL fold, which supplies its
// own balState-derived updates rather than cc.state.
func (cc *commitmentCalculator) computeRootFromUpdates(ctx context.Context, t commitTarget, updates *commitment.Updates, reader *asOfStateReader) ([]byte, error) {
	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(updates)
	reader.txNum = t.lastTxNum + 1
	sdCtx.SetStateReader(reader)
	if !cc.ownsChangeset(t.blockNum) {
		return cc.computeIsolated(ctx, t)
	}
	return cc.computeWithBlockAccumulator(ctx, t)
}

// shadowCrossCheck recomputes block N the incremental way and asserts the
// root matches the BAL-driven root folded ahead by foldBlockFromBAL — the
// dual-compute consistency net. Divergence fails the block. Publishes the
// incremental root (the proven oracle). BALShadowCompute only.
func (cc *commitmentCalculator) shadowCrossCheck(ctx context.Context, r *blockResult) {
	balRoot := cc.balRoots[r.BlockNum]
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.fail(ctx, r, fmt.Errorf("shadow incremental lazy-load: %w", err))
		return
	}
	cc.state.FlushToUpdates(cc.updates)
	cc.state.ResetBlockFlags()
	incUpdates := cc.updates
	cc.updates = cc.updates.NewEmpty()
	rh, err := cc.computeRootFromUpdates(ctx, targetOf(r), incUpdates, cc.asOfReader)
	if err != nil {
		cc.fail(ctx, r, fmt.Errorf("shadow incremental compute: %w", err))
		return
	}
	if !bytes.Equal(rh, balRoot) {
		cc.fail(ctx, r, fmt.Errorf("%w: shadow mismatch block %d incremental %x BAL-driven %x",
			ErrWrongTrieRoot, r.BlockNum, rh, balRoot))
		return
	}
	cc.publish(ctx, commitmentResult{blockNum: r.BlockNum, blockHash: r.BlockHash, txNum: r.lastTxNum, rootHash: rh})
}

// fail publishes a calculator error. It does NOT cancel execution: the apply
// loop is the sole cancellation authority — it classifies the published error
// (deferring a fold-ahead wrong-root until the block's own exec verdict) and
// drives the single UnwindTo.
func (cc *commitmentCalculator) fail(ctx context.Context, br *blockResult, err error) {
	if cc.logger != nil {
		cc.logger.Error("["+cc.logPrefix+"] commitmentCalculator: reporting failure", "block", br.BlockNum, "err", err)
	}
	cc.publish(ctx, commitmentResult{blockNum: br.BlockNum, blockHash: br.BlockHash, txNum: br.lastTxNum, err: err})
}

// shouldComputeOnRequest decides whether a commitComputeRequest should
// trigger a fresh ComputeCommitment vs. publish an empty result. Returns
// true when there is a blockResult to compute against AND either:
//
//	(a) we haven't computed at all yet — covers the very first batch
//	    which may be just genesis at blockNum=0; without this, the
//	    genesis commitment would never be written to sd because
//	    lastBlockResult.BlockNum=0 fails the > lastComputedBlock=0 check
//	    (the same blockNum=0 ambiguity as SeekCommitment's stageProgress
//	    fallback), leaving SeekCommitment in a subsequent exec3 cycle
//	    to fall back to stage progress instead of finding a commitment
//	    state — which then forces re-execution of block 0 and corrupts
//	    the next batch's commitment.
//
//	(b) a new block boundary advanced past the last computed one.
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

// compute is the shared prologue/compute/footer for every calculator commitment
// path; the per-call differences live in m.
func (cc *commitmentCalculator) compute(ctx context.Context, t commitTarget, m computeMode) {
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.publish(ctx, commitmentResult{blockNum: t.blockNum, txNum: t.lastTxNum,
			err: fmt.Errorf("commitmentCalculator: %slazy-load failed: %w", m.label, err)})
		return
	}
	cc.state.FlushToUpdates(cc.updates)
	if !m.midBlock {
		cc.state.ResetBlockFlags()
	}

	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(cc.updates)
	cc.updates = cc.updates.NewEmpty()

	cc.asOfReader.txNum = t.lastTxNum + 1
	sdCtx.SetStateReader(cc.asOfReader)

	var rh []byte
	var err error
	if !cc.ownsChangeset(t.blockNum) {
		rh, err = cc.computeIsolated(ctx, t)
	} else {
		rh, err = cc.computeWithBlockAccumulator(ctx, t)
	}
	if err != nil {
		cc.publish(ctx, commitmentResult{blockNum: t.blockNum, txNum: t.lastTxNum,
			err: fmt.Errorf("commitmentCalculator: %scompute failed: %w", m.label, err)})
		return
	}

	if !m.midBlock {
		cc.lastComputedBlock = t.blockNum
		cc.hasComputed = true
	}

	if !m.checkRoot {
		return
	}
	mismatch := !bytes.Equal(rh, t.stateRoot[:])
	if !m.publishRoot && !mismatch {
		return
	}
	r := commitmentResult{blockNum: t.blockNum, blockHash: t.blockHash, txNum: t.lastTxNum, rootHash: rh}
	if mismatch {
		r.err = fmt.Errorf("%w: block %d root %x expected %x", ErrWrongTrieRoot, t.blockNum, rh, t.stateRoot)
	}
	cc.publish(ctx, r)
}

// computeIsolated computes and flushes its own deferred updates under a nil
// changeset accumulator, so a block that owns no changeset records into none.
func (cc *commitmentCalculator) computeIsolated(ctx context.Context, t commitTarget) ([]byte, error) {
	cc.doms.LockChangesetAccumulator()
	defer cc.doms.UnlockChangesetAccumulator()
	prev := cc.doms.GetChangesetAccumulatorLocked()
	cc.doms.SetChangesetAccumulatorLocked(nil)
	defer cc.doms.SetChangesetAccumulatorLocked(prev)

	rh, err := cc.doms.ComputeCommitmentLocked(ctx, cc.roTx, true, t.blockNum, t.lastTxNum, cc.logPrefix, nil)
	if err != nil {
		return nil, err
	}
	if err := cc.doms.FlushPendingUpdatesLocked(ctx, cc.roTx); err != nil {
		return nil, err
	}
	return rh, nil
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
	cc.doms.LockChangesetAccumulator()
	prev := cc.doms.GetChangesetAccumulatorLocked()
	cc.doms.SetChangesetAccumulatorLocked(nil)
	err := cc.doms.FlushPendingUpdatesLocked(ctx, cc.roTx)
	cc.doms.SetChangesetAccumulatorLocked(prev)
	cc.doms.UnlockChangesetAccumulator()
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
	// Best-effort send; log only genuine errors as a breadcrumb (the apply loop
	// surfaces the authoritative one). Wrong-root and shutdown cancels are expected.
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

// computeWithBlockAccumulator runs ComputeCommitment with the changeset
// accumulator switched to block N's saved changeset (looked up by hash) so
// that any branch writes during compute (mid-process inline flushes from
// `pendingPrefixes` collisions, plus the [state] write at end via
// encodeAndStoreCommitmentState) land in block N's CS rather than whatever
// the exec loop has installed as current.
//
// IMPORTANT: hash-aware lookup is mandatory here. pastChangesAccumulator
// can hold multiple changesets per block number after a fork-bounce
// (canonical block 1 + forks[i] block 1 with different hashes), and a
// number-only GetChangesetByBlockNum returns the first match in
// non-deterministic map iteration order. That non-determinism caused the
// calculator's [state] write for canonical block 1 to land in the fork's
// block 1 CS during the TestBlockchainHeaderchainReorgConsistency
// reproducer, leaving canonical block 1's CS without [state] and producing
// off-by-one wrong-trie-root chains on the next iteration's re-execution.
//
// If block N's CS hasn't been saved yet it falls through to the live
// accumulator, which — because the lookup is under changesetMu — is still N's
// own (the apply loop can't rotate it while the lock is held).
//
// Also annotates the pending deferred update (set inside ComputeCommitment
// when defer mode is on) with the block's hash, so the next call's
// FlushPendingUpdates uses the same hash-aware routing.
func (cc *commitmentCalculator) computeWithBlockAccumulator(ctx context.Context, t commitTarget) ([]byte, error) {
	defer func() {
		// Stamp the pending update (if any was set during ComputeCommitment)
		// with this block's hash so FlushPendingUpdates on the next call
		// routes to the exact (BlockNum, BlockHash) entry rather than
		// guessing among ambiguous block-number-only matches.
		if upd := cc.doms.GetCommitmentContext().PeekPendingUpdate(); upd != nil && upd.BlockNum == t.blockNum {
			upd.BlockHash = t.blockHash
		}
	}()

	// Look up cs AND compute under changesetMu: reading cs before the lock races
	// the apply loop's SavePastChangesetAccumulator + accumulator rotation, which
	// would route this block's [state] write into the next block's changeset. The
	// lock is required even on the cs==nil path — the internal FlushPendingUpdates
	// mutates the same global accumulator pointer.
	cc.doms.LockChangesetAccumulator()
	defer cc.doms.UnlockChangesetAccumulator()
	cs := cc.doms.GetChangesetByHash(t.blockNum, t.blockHash)
	if cs == nil {
		return cc.doms.ComputeCommitmentLocked(ctx, cc.roTx, true, t.blockNum, t.lastTxNum, cc.logPrefix, nil)
	}
	// LOAD-BEARING swap under the outer lock (already taken above). The
	// Set/restore dance below mutates the global current-accumulator
	// pointer; the deferred branch writes from block N-1 (flushed inside
	// ComputeCommitmentLocked → FlushPendingUpdatesLocked) AND the [state]
	// marker write at end of compute also touch that same global pointer
	// and the per-domain diff fields. Holding changesetMu through all of
	// it serializes against the apply goroutine's DomainPut/DomainDel.
	//
	// Inside the lock we must use the *Locked variants of Get/Set/Compute
	// — the public counterparts re-acquire the same Mutex and would
	// self-deadlock.
	prev := cc.doms.GetChangesetAccumulatorLocked()
	cc.doms.SetChangesetAccumulatorLocked(cs)
	defer cc.doms.SetChangesetAccumulatorLocked(prev)
	return cc.doms.ComputeCommitmentLocked(ctx, cc.roTx, true, t.blockNum, t.lastTxNum, cc.logPrefix, nil)
}

// asOfStateReader reads account/storage/code at a specific txNum via
// sd.GetAsOf (which checks sd.mem first, then falls through to files).
// Commitment domain reads use GetLatest since branches are only written
// by the calculator sequentially.
type asOfStateReader struct {
	sd    *execctx.SharedDomains
	roTx  kv.TemporalTx
	txNum uint64
	// workerCtx, when non-nil, carries this worker's lock-free metrics
	// accumulator; the CommitmentDomain read routes through GetLatestContext so
	// a concurrent trie-warmup worker doesn't write the shared main accumulator
	// (a race) or take the global metrics lock. Nil on the main reader.
	workerCtx context.Context
}

func (r *asOfStateReader) WithHistory() bool { return false }

func (r *asOfStateReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	return nil
}

func (r *asOfStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	if d == kv.CommitmentDomain {
		// Branches: use GetLatest — written only by this calculator, sequential.
		if r.workerCtx != nil {
			enc, step, err = r.sd.GetLatestContext(r.workerCtx, d, r.roTx, plainKey)
		} else {
			enc, step, err = r.sd.GetLatest(d, r.roTx, plainKey)
		}
	} else {
		// Account/storage/code: use GetAsOf to avoid reading future state.
		// Check sd.mem first (in-memory data from current batch), then
		// fall through to DB files for data not in the batch.
		var ok bool
		enc, ok, err = r.sd.GetAsOf(d, plainKey, r.txNum)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			// Not in sd.mem — read from DB files
			enc, ok, err = r.roTx.GetAsOf(d, plainKey, r.txNum)
			if err != nil {
				return nil, 0, err
			}
			if !ok {
				enc = nil
			}
		}
		if stepSize > 0 {
			step = kv.Step(r.txNum / stepSize)
		}
	}
	return enc, step, err
}

func (r *asOfStateReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	return &asOfStateReader{sd: r.sd, roTx: tx, txNum: r.txNum}
}

// CloneForWorker meters the worker's CommitmentDomain reads into the per-worker
// accumulator carried by workerCtx (this reader is used as the commitment
// reader during block assembly, where trie-warmup runs concurrently — so it
// must not write the shared main accumulator).
func (r *asOfStateReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) commitmentdb.StateReader {
	return &asOfStateReader{sd: r.sd, roTx: tx, txNum: r.txNum, workerCtx: workerCtx}
}

// asOfStorageEnumerator lists the persisted storage slots under an address via
// the calculator's stable roTx snapshot (the pre-cycle baseline the trie was
// built from), so a self-destruct deletes the whole subtree. The exec loop's
// DomainDelPrefix runs with inline TouchKey disabled in parallel mode, so this
// is the parallel path's equivalent of serial's per-slot delete touches.
type asOfStorageEnumerator struct {
	reader *asOfStateReader
}

func (e *asOfStorageEnumerator) EachStorageSlot(addr accounts.Address, fn func(key accounts.StorageKey) error) error {
	addrVal := addr.Value()
	toKey, _ := kv.NextSubtree(addrVal[:])
	it, err := e.reader.roTx.RangeAsOf(kv.StorageDomain, addrVal[:], toKey, e.reader.txNum, order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if len(v) == 0 || len(k) != 52 || !bytes.HasPrefix(k, addrVal[:]) {
			continue
		}
		var h common.Hash
		copy(h[:], k[20:])
		if err := fn(accounts.InternKey(h)); err != nil {
			return err
		}
	}
	return nil
}

// Keep imports used.
var _ = commitment.CommitProgress{}
