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
	// out publishes commitment roots.
	out chan commitmentResult

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
	out chan commitmentResult,
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
		logPrefix:            logPrefix,
		logger:               logger,
		updates:              calcUpdates,
		state:                newCalcState(asOfReader, logger, logPrefix),
		asOfReader:           asOfReader,
		roTx:                 roTx,
		in:                   in,
		out:                  out,
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
	for result := range cc.in {
		cc.handleMessage(ctx, result)
	}
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

		// Track the latest block boundary.
		cc.lastBlockResult = r

		// Break logic: in per-block mode, compute at every block boundary.
		// Skip the first block if it's a partial block (resumed mid-block).
		// `forcePerBlockCompute` overrides dbg.BatchCommitments to mirror
		// serial's gate (exec3_serial.go around the `if !dbg.BatchCommitments
		// || shouldGenerateChangesets || ...` check) — per-block compute is
		// required when changesets must record per-block branch deltas
		// (reorg support, KeepExecutionProofs).
		if !dbg.BatchCommitments || cc.forcePerBlockCompute {
			if cc.lastComputedBlock == 0 && r.isPartial {
				// First block is partial (resumed mid-block).
				// Compute it (like serial does) to save trie state, then
				// restore that state so the next full block starts from
				// the same trie state as serial's batch 2 start.
				cc.computeWithoutCheck(ctx, r)
			} else {
				cc.computeAndCheck(ctx, r)
			}
		}
		// In BatchCommitments mode (without forcePerBlockCompute): just
		// accumulate — compute only on explicit commitComputeRequest from
		// the apply loop.

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
