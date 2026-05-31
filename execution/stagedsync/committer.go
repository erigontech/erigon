package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// commitmentResult is the outcome of a single commitment computation.
type commitmentResult struct {
	blockNum uint64
	txNum    uint64
	rootHash []byte
	err      error
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
// The calculator owns its own commitment.Updates buffer. Writes from the
// execLoop flow through VersionedWrites.TouchUpdates() which calls
// TouchPlainKeyDirect() — no serialization round-trip.
//
// It reads values from sd.mem (shared with the execLoop) via GetLatest.
// The execLoop's Flush writes to sd.mem before sending the blockResult,
// so by the time the calculator receives the blockResult, sd.mem has the
// correct block-boundary state.
type commitmentCalculator struct {
	doms      *execctx.SharedDomains
	db        kv.TemporalRoDB
	logPrefix string
	logger    log.Logger

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

	// cancelExec stops execution when the calculator fails (BAL-driven fold
	// mismatch / wrong root). Calling it makes the exec loop's ctx.Done
	// branches fire so execution halts eagerly instead of running ahead
	// behind the result buffer. Stage 4.
	cancelExec context.CancelFunc

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

	wg   sync.WaitGroup
	done chan struct{}
}

func newCommitmentCalculator(
	ctx context.Context,
	doms *execctx.SharedDomains,
	db kv.TemporalRoDB,
	logPrefix string,
	logger log.Logger,
	forcePerBlockCompute bool,
	in chan applyResult,
	blockRequests chan *blockRequest,
	out chan commitmentResult,
	cancelExec context.CancelFunc,
) (*commitmentCalculator, error) {
	// Create the calculator's own Updates buffer in ModeUpdate.
	// ModeUpdate stores actual values (balance, nonce, storage) in the btree,
	// so ComputeCommitment reads values from the Updates rather than sd.mem.
	sdCtxUpdates := doms.GetCommitmentContext().GetUpdates()
	calcUpdates := sdCtxUpdates.NewEmpty()
	calcUpdates.SetMode(commitment.ModeUpdate)

	// Open a persistent read-only TX for lazy-loading state from the domain.
	// This lives for the calculator's lifetime, like worker TX handles.
	roTx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, fmt.Errorf("commitmentCalculator: open roTx: %w", err)
	}
	// roTx lives for the calculator's lifetime — rolled back in Stop(), not
	// deferred here. Safe across collate/prune cycles because the calculator
	// is constructed in pe.exec() and its `defer Stop()` runs *before* the
	// stageloop's rwTx.Commit(), and CollateAndPruneIfNeeded only fires
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
		logPrefix:            logPrefix,
		logger:               logger,
		updates:              calcUpdates,
		state:                newCalcState(asOfReader, logger, logPrefix),
		asOfReader:           asOfReader,
		roTx:                 roTx,
		in:                   in,
		blockRequests:        blockRequests,
		out:                  out,
		pending:              map[uint64]*pendingBlock{},
		cancelExec:           cancelExec,
		foldedAhead:          map[uint64]bool{},
		balRoots:             map[uint64][]byte{},
		forcePerBlockCompute: forcePerBlockCompute,
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
	// blockRequests is multiplexed alongside cc.in so a blockRequest never
	// waits behind a block's txResults. When it closes (dispatch done) it
	// is set to nil so the select ignores it; the loop still runs until
	// cc.in closes and drains.
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

// handleBlockRequest records the per-block mode from a blockRequest —
// BAL-driven when the block carries a BAL, BAL I/O is enabled and
// BALDrivenCommitment is set, else incremental — then tries to fold the
// block ahead of its result stream (maybeFoldAhead).
func (cc *commitmentCalculator) handleBlockRequest(ctx context.Context, req *blockRequest) {
	if !cc.hasFirstBlock {
		cc.firstBlockNum = req.blockNum
		cc.hasFirstBlock = true
	}
	mode := calcModeIncremental
	if req.bal != nil && !dbg.IgnoreBAL && dbg.BALDrivenCommitment {
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

// maybeFoldAhead folds block n from its BAL when it is in BAL-driven mode
// and the fold gate is open. The fold overlaps the execution of block n —
// this is the parallel-commitment win. Idempotent (foldedAhead guard).
func (cc *commitmentCalculator) maybeFoldAhead(ctx context.Context, n uint64) {
	pb, ok := cc.pending[n]
	if !ok || pb.mode != calcModeBALDriven || cc.foldedAhead[n] {
		return
	}
	if !cc.foldGateOpen(n) {
		return
	}
	cc.foldBlockFromBAL(ctx, pb)
}

// foldBlockFromBAL computes block pb's commitment from its BAL, ahead of the
// per-tx result stream. The root is verified against the block header's
// stateRoot — a mismatch fails the block and (via cancelExec) halts
// execution. The fresh calcState is used because a BAL-driven block's
// changed-key set comes wholly from the BAL, never from the cross-block
// incremental accumulator. Stage 3.
func (cc *commitmentCalculator) foldBlockFromBAL(ctx context.Context, pb *pendingBlock) {
	req := pb.req
	br := &blockResult{
		BlockNum:  req.blockNum,
		BlockHash: req.blockHash,
		StateRoot: req.stateRoot,
		lastTxNum: req.lastTxNum,
	}
	reader := &asOfStateReader{sd: cc.doms, roTx: cc.roTx, txNum: req.lastTxNum + 1}
	balState := newCalcState(reader, cc.logger, cc.logPrefix)
	balState.LoadFromBAL(req.bal)
	if err := balState.LazyLoadErr(); err != nil {
		cc.fail(ctx, br, fmt.Errorf("BAL-driven lazy-load: %w", err))
		return
	}
	balUpdates := cc.updates.NewEmpty()
	balUpdates.SetMode(commitment.ModeUpdate)
	balState.FlushToUpdates(balUpdates)

	rh, err := cc.computeRoot(ctx, br, balUpdates, reader)
	if err != nil {
		cc.fail(ctx, br, fmt.Errorf("BAL-driven compute: %w", err))
		return
	}
	if !bytes.Equal(rh, req.stateRoot[:]) {
		cc.fail(ctx, br, fmt.Errorf("%w: BAL-driven block %d root %x expected %x",
			ErrWrongTrieRoot, req.blockNum, rh, req.stateRoot))
		return
	}
	cc.foldedAhead[req.blockNum] = true
	cc.balRoots[req.blockNum] = rh
	cc.lastComputedBlock = req.blockNum
	cc.hasComputed = true
	// Shadow mode defers publish to the incremental cross-check at
	// blockResult(N); otherwise publish the verified root now.
	if !dbg.BALShadowCompute {
		cc.publish(ctx, commitmentResult{blockNum: req.blockNum, txNum: req.lastTxNum, rootHash: rh})
	}
}

// computeRoot installs updates + reader on the commitment context and runs
// ComputeCommitment wrapped in block N's changeset accumulator. Shared by
// the BAL-driven (foldBlockFromBAL) and incremental (shadowCrossCheck)
// paths.
func (cc *commitmentCalculator) computeRoot(ctx context.Context, br *blockResult, updates *commitment.Updates, reader *asOfStateReader) ([]byte, error) {
	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(updates)
	reader.txNum = br.lastTxNum + 1
	sdCtx.SetStateReader(reader)
	return cc.computeWithBlockAccumulator(ctx, br)
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
	rh, err := cc.computeRoot(ctx, r, incUpdates, cc.asOfReader)
	if err != nil {
		cc.fail(ctx, r, fmt.Errorf("shadow incremental compute: %w", err))
		return
	}
	if !bytes.Equal(rh, balRoot) {
		cc.fail(ctx, r, fmt.Errorf("%w: shadow mismatch block %d incremental %x BAL-driven %x",
			ErrWrongTrieRoot, r.BlockNum, rh, balRoot))
		return
	}
	cc.publish(ctx, commitmentResult{blockNum: r.BlockNum, txNum: r.lastTxNum, rootHash: rh})
}

// fail publishes a calculator error and cancels execution so the exec loop
// stops eagerly instead of running ahead behind the result buffer. Stage 4.
func (cc *commitmentCalculator) fail(ctx context.Context, br *blockResult, err error) {
	if cc.logger != nil {
		cc.logger.Error("["+cc.logPrefix+"] commitmentCalculator: stopping execution", "block", br.BlockNum, "err", err)
	}
	if cc.cancelExec != nil {
		cc.cancelExec()
	}
	cc.publish(ctx, commitmentResult{blockNum: br.BlockNum, txNum: br.lastTxNum, err: err})
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
		if len(r.writes) > 0 {
			cc.asOfReader.txNum = r.txNum
			cc.state.ApplyWrites(r.writes)
		}

	case *blockResult:
		// Track the latest block boundary.
		cc.lastBlockResult = r
		cc.lastBlockResultSeen = r.BlockNum
		cc.hasSeenBlockResult = true

		switch {
		case cc.foldedAhead[r.BlockNum]:
			// Block N was already folded from its BAL ahead of this
			// boundary. In shadow mode, recompute it the incremental way
			// and cross-check the roots; otherwise the verified BAL-driven
			// result was already published by foldBlockFromBAL.
			if dbg.BALShadowCompute {
				cc.shadowCrossCheck(ctx, r)
			}
		case !dbg.BatchCommitments || cc.forcePerBlockCompute:
			// Per-block mode. `forcePerBlockCompute` overrides
			// dbg.BatchCommitments to mirror serial's gate (exec3_serial.go
			// `if !dbg.BatchCommitments || shouldGenerateChangesets || ...`)
			// — per-block compute is required when changesets must record
			// per-block branch deltas (reorg support, KeepExecutionProofs).
			if cc.lastComputedBlock == 0 && r.isPartial {
				// First block is partial (resumed mid-block). Compute it
				// (like serial does) to save trie state.
				cc.computeWithoutCheck(ctx, r)
			} else {
				cc.computeAndCheck(ctx, r)
			}
		}
		// In BatchCommitments mode (without forcePerBlockCompute, not
		// folded): just accumulate — compute only on commitComputeRequest.

		// Block N is done; its boundary opens the BAL fold gate for N+1.
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

func (cc *commitmentCalculator) computeAndPublish(ctx context.Context, br *blockResult) {
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: lazy-load failed: %w", err),
		})
		return
	}
	cc.state.FlushToUpdates(cc.updates)
	cc.state.ResetBlockFlags()

	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(cc.updates)
	cc.updates = cc.updates.NewEmpty()

	cc.asOfReader.txNum = br.lastTxNum + 1
	sdCtx.SetStateReader(cc.asOfReader)

	// Use hash-aware accumulator wrap — see computeWithBlockAccumulator
	// docstring for why this is mandatory in reorg scenarios.
	rh, err := cc.computeWithBlockAccumulator(ctx, br)
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: %w", err),
		})
		return
	}

	r := commitmentResult{
		blockNum: br.BlockNum,
		txNum:    br.lastTxNum,
		rootHash: rh,
	}

	// Check against expected root from the block header.
	if !bytes.Equal(rh, br.StateRoot[:]) {
		r.err = fmt.Errorf("%w: block %d root %x expected %x", ErrWrongTrieRoot, br.BlockNum, rh, br.StateRoot)
	}

	cc.lastComputedBlock = br.BlockNum
	cc.hasComputed = true
	cc.publish(ctx, r)
}

// computeWithoutCheck computes commitment but doesn't verify the root.
// Used for the first partial block where the trie state doesn't match the header.
func (cc *commitmentCalculator) computeWithoutCheck(ctx context.Context, br *blockResult) {
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: lazy-load failed: %w", err),
		})
		return
	}
	cc.state.FlushToUpdates(cc.updates)
	cc.state.ResetBlockFlags()

	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(cc.updates)
	cc.updates = cc.updates.NewEmpty()

	cc.asOfReader.txNum = br.lastTxNum + 1
	sdCtx.SetStateReader(cc.asOfReader)

	// Use the same hash-aware accumulator wrap as computeAndCheck — without
	// it, the [state] write inside ComputeCommitment can land in a stale
	// past-changeset entry chosen non-deterministically by GetChangesetByBlockNum
	// when pastChangesAccumulator holds multiple changesets per block number
	// (canonical + fork during reorg-bounce tests).
	if _, err := cc.computeWithBlockAccumulator(ctx, br); err != nil {
		// Partial-block compute is intentionally not verified (no header root
		// to compare against), but a real ComputeCommitment failure leaves
		// later trie state suspect — log so the failure isn't silent.
		if cc.logger != nil {
			cc.logger.Warn("["+cc.logPrefix+"] commitmentCalculator: computeWithoutCheck failed", "block", br.BlockNum, "txNum", br.lastTxNum, "err", err)
		}
	}

	cc.lastComputedBlock = br.BlockNum
	cc.hasComputed = true
}

// computeAndCheck computes per-block commitment and validates the root.
// Only publishes errors to the output channel — successful results are
// tracked internally. This avoids filling the output channel buffer
// (which would deadlock the pipeline when batch size > buffer size).
// Uses the SharedDomains' roTx (not a separate DB connection) to ensure
// consistency between sd.mem and the trie node reads.
func (cc *commitmentCalculator) computeAndCheck(ctx context.Context, br *blockResult) {
	if err := cc.state.LazyLoadErr(); err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: lazy-load failed: %w", err),
		})
		return
	}
	// Flush accumulated local state to the trie's Updates buffer.
	// This produces one Update per dirty account/slot with the final
	// block-end values — not intermediate per-TX values.
	cc.state.FlushToUpdates(cc.updates)
	cc.state.ResetBlockFlags()

	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(cc.updates)
	cc.updates = cc.updates.NewEmpty()

	// Install asOfStateReader so fold/unfold sibling reads see state at
	// this block's txNum, not the apply loop's future state in sd.mem.
	cc.asOfReader.txNum = br.lastTxNum + 1
	sdCtx.SetStateReader(cc.asOfReader)

	// In per-block compute mode, the exec loop has (or is about to)
	// swap the changeset accumulator to block N+1 by the time this runs.
	// Wrap ComputeCommitment so any branch writes (mid-process inline
	// flushes from `pendingPrefixes` collisions, plus any writes via
	// putBranch) go into block N's saved changeset, not whatever the
	// exec loop has set as current. Without this wrap, block N's
	// branch deltas leak into block N+1's CS, producing a wrong-trie-root
	// chain on subsequent blocks (see TestTxLookupUnwind reproducer).
	rh, err := cc.computeWithBlockAccumulator(ctx, br)
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: %w", err),
		})
		return
	}

	cc.lastComputedBlock = br.BlockNum
	cc.hasComputed = true

	// Only publish on mismatch — success is silent.
	if mismatch := !bytes.Equal(rh, br.StateRoot[:]); mismatch {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			rootHash: rh,
			err:      fmt.Errorf("%w: block %d root %x expected %x", ErrWrongTrieRoot, br.BlockNum, rh, br.StateRoot),
		})
	}
}

func (cc *commitmentCalculator) publish(ctx context.Context, r commitmentResult) {
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
// If block N's CS hasn't been saved yet (rare race with the exec loop's
// SavePastChangesetAccumulator), falls through to whatever current is
// installed — same as the pre-fix behavior.
//
// Also annotates the pending deferred update (set inside ComputeCommitment
// when defer mode is on) with the block's hash, so the next call's
// FlushPendingUpdates uses the same hash-aware routing.
func (cc *commitmentCalculator) computeWithBlockAccumulator(ctx context.Context, br *blockResult) ([]byte, error) {
	defer func() {
		// Stamp the pending update (if any was set during ComputeCommitment)
		// with this block's hash so FlushPendingUpdates on the next call
		// routes to the exact (BlockNum, BlockHash) entry rather than
		// guessing among ambiguous block-number-only matches.
		if upd := cc.doms.GetCommitmentContext().PeekPendingUpdate(); upd != nil && upd.BlockNum == br.BlockNum {
			upd.BlockHash = br.BlockHash
		}
	}()

	cs := cc.doms.GetChangesetByHash(br.BlockNum, br.BlockHash)
	// Always take the lock around ComputeCommitment, even on the cs==nil
	// fast path: the FlushPendingUpdates that ComputeCommitment runs
	// internally still mutates the global accumulator pointer + per-domain
	// diff fields, racing with the apply loop's SetChangesetAccumulator if
	// we don't serialize. Without this, race detector flags ~73 SetDiff vs
	// PutWithPrev hits on the cs==nil path (genesis, missing-CS edge cases).
	cc.doms.LockChangesetAccumulator()
	defer cc.doms.UnlockChangesetAccumulator()
	if cs == nil {
		return cc.doms.ComputeCommitmentLocked(ctx, cc.roTx, true, br.BlockNum, br.lastTxNum, cc.logPrefix, nil)
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
	return cc.doms.ComputeCommitmentLocked(ctx, cc.roTx, true, br.BlockNum, br.lastTxNum, cc.logPrefix, nil)
}

// asOfStateReader reads account/storage/code at a specific txNum via
// sd.GetAsOf (which checks sd.mem first, then falls through to files).
// Commitment domain reads use GetLatest since branches are only written
// by the calculator sequentially.
type asOfStateReader struct {
	sd    *execctx.SharedDomains
	roTx  kv.TemporalTx
	txNum uint64
}

func (r *asOfStateReader) WithHistory() bool { return false }

func (r *asOfStateReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	return nil
}

func (r *asOfStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	if d == kv.CommitmentDomain {
		// Branches: use GetLatest — written only by this calculator, sequential.
		enc, step, err = r.sd.GetLatest(d, r.roTx, plainKey)
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

// Keep imports used.
var _ = commitment.CommitProgress{}
