// Copyright 2022 The Erigon Authors
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

package execmodule

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
)

type BlockTimings [2]time.Duration

const (
	BlockTimingsValidationIndex    = 0
	BlockTimingsFlushExtendingFork = 1
)

const timingsCacheSize = 16

type ForkValidator struct {
	// current memory batch containing chain head that extend canonical fork.
	sharedDom *execctx.SharedDomains
	// notifications accumulated for the extending fork
	extendingForkNotifications *shards.Notifications
	// hash of chain head that extend canonical fork.
	extendingForkHeadHash common.Hash
	extendingForkNumber   uint64
	maxReorgDepth         uint64
	// preExecStack is an AUXILIARY chain of older, not-yet-canonical pre-executed SDs
	// (frontier SD chain, [[venue_exec_on_round_plan]]). It sits ALONGSIDE the single
	// sharedDom above — sharedDom stays the ACTIVE in-progress block exactly as before,
	// and every existing method keeps operating on it unchanged. The stack holds only the
	// PREVIOUS blocks' SDs that were pushed at OPEN (instead of being closed) so they
	// survive as read-through parents while their FCU is still pending. Under load the
	// pre-exec frontier outruns FCU; the stack lets block N+1 read N's carried-forward
	// state instead of stale canonical. Ordered oldest→newest (tail = frontierParent's
	// oldest, next to canonicalise). Popped+released on flush (FCU). Empty in steady state
	// (depth 1 = just sharedDom). See preExecStack methods below.
	preExecStack []*preExecGen
	// frontierMode arms the decoupled-boundary lifecycle (DAG-L2 producer, set via SetFrontierMode from
	// ExecModule.SetBlockAssembler). When true, MergeExtendingFork PARKS the canonicalised block's SD
	// (keep-alive) so the successor block reads its live commitment through the parent chain, and retires
	// parked gens STRICTLY below the canonicalised number. When false (normal sync/reorg), the merged SD
	// is dropped on canonicalisation exactly as before. See [[consensus_advance_untested_regression]].
	frontierMode bool
	// pipeline executor used for fork validation (ValidateBlock).
	executor    *PipelineExecutor
	blockReader services.FullBlockReader
	// this is the current point where we processed the chain so far.
	currentHeight uint64
	// committedHeight is the last block whose merged state is FLUSHED+COMMITTED to the durable DB
	// (set by NotifyCommitted after the FCU commit). Parked pre-exec gens are retired only once
	// committed — see retireParkedUpTo. Guarded by fv.lock.
	committedHeight uint64
	// block hashes that are deemed valid
	validHashes *lru.Cache[common.Hash, bool]

	ctx context.Context

	// we want fork validator to be thread safe so let
	lock sync.Mutex

	timingsCache *lru.Cache[common.Hash, BlockTimings]

	// Flashblock state: tracks tx hashes already executed for the
	// in-progress block so we can detect prefix-extension updates.
	flashblockTxHashes []common.Hash

	// promotedHash/promotedNumber record the LAST block canonicalised by PromoteBlock (the prefetch
	// "promoted" lifecycle state). A prefetched gen is kept live after promotion (it stays the read-through
	// parent of its successor until the successor canonicalises), so WITHOUT this record a re-FCU of the
	// same head — routine for an L2 CL that re-sends FCU every slot — would re-pass HasLiveGen and
	// re-promote (re-merge) the same block, republishing notifications and tearing down the eager-opened
	// successor frontier. IsPromotedHead lets updateForkChoice treat a re-FCU of the already-promoted head
	// as a true no-op. Promote-ONCE is a prefetch-lifecycle invariant (prefetched → promoted → retired).
	promotedHash   common.Hash
	promotedNumber uint64
}

// preExecGen is one older, not-yet-canonical pre-executed SharedDomains parked on the
// auxiliary preExecStack. It carries only what FCU needs to flush+retire it in order:
// the SD itself and the block it belongs to. It does NOT duplicate the active-block
// bookkeeping (flashblockTxHashes/notifications) — those stay on the ForkValidator for
// the single active sharedDom.
type preExecGen struct {
	sd       *execctx.SharedDomains
	headHash common.Hash
	number   uint64
}

// pushPreExec parks an outgoing in-progress SD on the auxiliary stack at block OPEN, so
// it survives as the read-through parent of the next block instead of being closed.
// Caller must hold fv.lock.
func (fv *ForkValidator) pushPreExec(sd *execctx.SharedDomains, headHash common.Hash, number uint64) {
	if sd == nil {
		return
	}
	fv.preExecStack = append(fv.preExecStack, &preExecGen{sd: sd, headHash: headHash, number: number})
}

// NewestFrontierSD returns the SD of the most-recently-parked pre-exec generation (the
// parent a newly-opening block should chain to), or nil if the stack is empty. Takes the
// lock — safe to call from PreExecute (which does not hold fv.lock).
func (fv *ForkValidator) NewestFrontierSD() *execctx.SharedDomains {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if len(fv.preExecStack) == 0 {
		return nil
	}
	return fv.preExecStack[len(fv.preExecStack)-1].sd
}

func newForkValidator(ctx context.Context, currentHeight uint64, executor *PipelineExecutor, blockReader services.FullBlockReader, maxReorgDepth uint64) *ForkValidator {
	validHashes, err := lru.New[common.Hash, bool]("validHashes", int(maxReorgDepth)*8)
	if err != nil {
		panic(err)
	}

	timingsCache, err := lru.New[common.Hash, BlockTimings]("timingsCache", timingsCacheSize)
	if err != nil {
		panic(err)
	}
	return &ForkValidator{
		executor:      executor,
		currentHeight: currentHeight,
		blockReader:   blockReader,
		ctx:           ctx,
		validHashes:   validHashes,
		timingsCache:  timingsCache,
		maxReorgDepth: maxReorgDepth,
	}
}

// ExtendingForkHeadHash return the fork head hash of the fork that extends the canonical chain.
// SetFrontierMode arms/disarms the decoupled-boundary keep-alive lifecycle (see frontierMode field).
func (fv *ForkValidator) SetFrontierMode(on bool) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.frontierMode = on
}

func (fv *ForkValidator) ExtendingForkHeadHash() common.Hash {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash
}

// FrontierMode reports whether the decoupled-boundary keep-alive lifecycle is armed (the marker-driven
// run-ahead flow). forkchoice uses it to gate parked-gen promotion (PromoteBlock) so non-frontier flows keep
// the legacy single-active-fork merge path exactly as before.
func (fv *ForkValidator) FrontierMode() bool {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.frontierMode
}

// NotifyCommitted records the last block whose merged state has been FLUSHED + COMMITTED to the durable DB
// (called after runForkchoiceFlushCommit). A parked pre-exec gen may only be retired once its own merged state
// is committed here — until then its commitment branches live only in its SD mem (no later merge carries them),
// so closing it early makes a successor's commitment read them empty ("empty branch data read during unfold").
// See retireParkedUpTo. Caller must NOT hold fv.lock.
func (fv *ForkValidator) NotifyCommitted(height uint64) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if height > fv.committedHeight {
		fv.committedHeight = height
	}
}

// CommittedHeight is the last block whose merged state is flushed+committed to the durable DB. ValidateChain
// uses it to short-circuit re-validation of already-applied (lagging) blocks WITHOUT building a fresh
// SharedDomains that would displace the DAG frontier SD — see the ALREADY-COMMITTED fast-path.
func (fv *ForkValidator) CommittedHeight() uint64 {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.committedHeight
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and represent the last processed block.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.currentHeight == currentHeight {
		return
	}
	fv.currentHeight = currentHeight
	// FRONTIER (user 2026-08-26): the active extending fork is a block AHEAD of the head (the frontier runs
	// ahead of FCU), so the head advancing must NOT tear it down — FCU is a status update, not a block
	// operation. Closing it here made block N+1's marker seal find "no extending fork" (guard1) the moment
	// FCU advanced the head to N. Only the legacy sync/reorg flow invalidates the extending fork on head change.
	if fv.frontierMode {
		return
	}
	// If the head changed, previous assumptions on head are incorrect now.
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
	fv.extendingForkNotifications = nil
	fv.extendingForkNumber = 0
	fv.extendingForkHeadHash = common.Hash{}
}

// AbandonExtendingFork closes and clears the ACTIVE extending-fork SD without touching the parked pre-exec
// generations (which are prior blocks' read-through parents). It is used by the DAG producer to DISCARD a
// PROVISIONALLY pre-executed in-progress block so the next open re-executes it from a fresh SD: the marker-
// driven atomic open eager-opens block N+1 BEFORE its CL attributes are known (stamping placeholder attrs +
// a placeholder block-start), and when N+1 stays empty until its own marker those attrs must be corrected.
// A flashblock re-run alone cannot fix it — CheckFlashblockUpdate would REUSE the maintained SD and skip the
// block-start system tx (SetPreExecStart), leaving the old ParentBeaconBlockRoot/PrevRandao state baked in.
// Abandoning the SD forces PreExecute down its fresh-SD path, which re-runs block-start under the real attrs
// and re-parents to the still-parked predecessor. Off the FCU path; safe because the abandoned block was never
// canonicalised. Caller must NOT hold fv.lock.
func (fv *ForkValidator) AbandonExtendingFork() {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
	fv.extendingForkNotifications = nil
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	fv.flashblockTxHashes = nil
}

// MergeExtendingFork merges the shared domains of the current extending fork into the current shared domains if fcu chooses its head hash as the fork choice.
func (fv *ForkValidator) MergeExtendingFork(ctx context.Context, tx kv.TemporalTx, sd *execctx.SharedDomains, target *Accumulation) error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	start := time.Now()
	if fv.sharedDom != nil {
		if err := fv.sharedDom.FlushPendingUpdates(ctx, tx); err != nil {
			return err
		}
		sdTxNum, _, err := sd.SeekCommitment(ctx, tx)
		if err != nil {
			return err
		}
		otherTxNum, _, err := fv.sharedDom.SeekCommitment(ctx, tx)
		if err != nil {
			return err
		}
		// closeOther = !frontierMode: in the frontier flow keep block N's SD alive+readable as its
		// successor's read-through parent (parenting), rather than closing it in the merge (which broke
		// parenting past the first hop). Canonical/normal flow closes it as before.
		err = sd.Merge(ctx, sdTxNum, fv.sharedDom, otherTxNum, !fv.frontierMode)
		if err != nil {
			return err
		}
	}
	timings, _ := fv.timingsCache.Get(fv.extendingForkHeadHash)
	timings[BlockTimingsFlushExtendingFork] = time.Since(start)
	fv.timingsCache.Add(fv.extendingForkHeadHash, timings)
	fv.extendingForkNotifications.Accumulator.CopyAndReset(target.Accumulator)
	fv.extendingForkNotifications.RecentReceipts.CopyAndReset(target.RecentReceipts)
	// Retire any parked pre-exec generations that are at/below the block just canonicalised
	// (their state is now durable in the DB via the merge above / prior FCUs). Frontier SD
	// chain retire step: flush+release TAIL-FIRST so a still-live child never reads a closed
	// parent. Under the current serialized boundary the stack is empty and this is a no-op;
	// under the decoupled boundary it bounds memory (the OOM guard). See
	// [[venue_exec_on_round_plan]].
	// Retire parked gens. In frontierMode retire STRICTLY BELOW the block just canonicalised (its own SD
	// is kept parked as the successor's read-through frontier); otherwise retire ≤ (legacy behaviour).
	retireUpTo := fv.extendingForkNumber
	if fv.frontierMode {
		fv.retireParkedUpTo(ctx, tx, retireUpTo, false /* strictlyBelow → keep the just-canonicalised gen */)
		// FRONTIER KEEP-ALIVE ([[consensus_advance_untested_regression]], user 2026-08-24): do NOT drop this
		// block's SD on canonicalisation. The successor block N+1 reads N's commitment trie THROUGH this SD
		// (read-through parent) — the just-committed DB can be INCOMPLETE for that read (a near-root
		// commitment branch is empty/absent until N is fully durable → "empty branch data read during
		// unfold" when N+1 closes). Park N's SD so N+1 reads N's LIVE, complete commitment; it is retired one
		// step later, when N+1 canonicalises, by which point N is fully durable. This is the "each preexec
		// block moves preexec→currentContext in turn" lifecycle: the FCU promotes N into the canonical state
		// but keeps N's preexec SD alive until the frontier moves past it.
		if fv.sharedDom != nil {
			fv.pushPreExec(fv.sharedDom, fv.extendingForkHeadHash, fv.extendingForkNumber)
		}
	} else {
		fv.retireParkedUpTo(ctx, tx, retireUpTo, true /* inclusive ≤ — legacy */)
	}
	// Clean the ACTIVE extending-fork slot (its SD now lives on the park stack in frontierMode; legacy just
	// clears the reference exactly as before — its state was transferred into the canonical SD by the merge).
	fv.sharedDom = nil
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	fv.extendingForkNotifications = nil
	return nil
}

// PromoteBlock canonicalises the specific block the FCU chose (blockHash/blockNumber) into the canonical
// sd — which, when the frontier runs AHEAD (the marker-driven atomic open opened N+1 before N's FCU), may be
// a PARKED pre-exec generation BELOW the active extending-fork head, not the head itself. It merges that
// block's SD into canonical with keep-alive (closeOther=false, so the block stays readable as its successor's
// read-through parent), retires parked gens strictly below it, and LEAVES the active run-ahead frontier
// intact. Returns (true,nil) when it promoted a live pre-exec gen (active head or parked); (false,nil) when
// the target is not a live gen (the caller falls back to the normal RunLoop canonicalisation — unchanged for
// non-run-ahead flows). Caller must NOT hold fv.lock.
func (fv *ForkValidator) PromoteBlock(ctx context.Context, tx kv.TemporalTx, sd *execctx.SharedDomains, target *Accumulation, blockHash common.Hash, blockNumber uint64) (bool, error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()

	activeHead := fv.sharedDom != nil && fv.extendingForkHeadHash == blockHash && fv.extendingForkNumber == blockNumber
	var promoteSD *execctx.SharedDomains
	if activeHead {
		promoteSD = fv.sharedDom
	} else {
		for _, g := range fv.preExecStack {
			if g.sd != nil && g.headHash == blockHash && g.number == blockNumber {
				promoteSD = g.sd
				break
			}
		}
	}
	if promoteSD == nil {
		return false, nil // not a live pre-exec block → caller uses the normal canonical path
	}

	start := time.Now()
	// Merge the chosen block's state into the canonical sd. closeOther follows the legacy rule: in the
	// frontier flow keep the block's SD alive (closeOther=false) so its successor keeps reading it as the
	// read-through parent; the normal/non-frontier flow closes it as before. This makes the ACTIVE-HEAD case
	// byte-for-byte identical to MergeExtendingFork in both modes (so non-frontier flows are unchanged).
	if err := promoteSD.FlushPendingUpdates(ctx, tx); err != nil {
		return false, err
	}
	sdTxNum, _, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return false, err
	}
	otherTxNum, _, err := promoteSD.SeekCommitment(ctx, tx)
	if err != nil {
		return false, err
	}
	// Re-key canonical[N] in the PROMOTED gen's OWN overlay to its sealed head hash before the merge flushes
	// that overlay into the canonical sd. During pre-exec StateStep wrote canonical[N]=deferred-placeholder
	// there so exec3 could locate the in-progress block; copyFrontierChainTables then copied that placeholder
	// forward through the whole frontier chain, and it has no stored header after the seal. promoteSD is the
	// PARKED gen at the front of the queue (the sealed block N) — NOT the running frontier — so re-keying it
	// here corrects the value sd.Merge propagates to the FCU sd (and thence durable) WITHOUT touching the live
	// frontier's exec or a successor's SeekCommitment (the RunAheadEmptyTailDrain corruption Fix A caused by
	// writing fv.sharedDom, the wrong SD). blockHash is the gen's sealed head. Without this the placeholder
	// rides the merge to durable, and the first safe-checkpoint that lands on block N reads a header-less
	// canonical hash → verifyForkchoiceHashes false → InvalidForkchoice → FCU skips its commit (the stall).
	if ov := promoteSD.BlockOverlay(); ov != nil {
		if err := rawdb.WriteCanonicalHash(ov, blockHash, blockNumber); err != nil {
			return false, err
		}
	}
	if err := sd.Merge(ctx, sdTxNum, promoteSD, otherTxNum, !fv.frontierMode); err != nil {
		return false, err
	}
	timings, _ := fv.timingsCache.Get(blockHash)
	timings[BlockTimingsFlushExtendingFork] = time.Since(start)
	fv.timingsCache.Add(blockHash, timings)
	if fv.extendingForkNotifications != nil {
		fv.extendingForkNotifications.Accumulator.CopyAndReset(target.Accumulator)
		fv.extendingForkNotifications.RecentReceipts.CopyAndReset(target.RecentReceipts)
	}

	if fv.frontierMode {
		// Frontier flow: retire parked gens STRICTLY BELOW the promoted block (their state is now durable);
		// keep the promoted block itself parked as its successor's read-through frontier.
		fv.retireParkedUpTo(ctx, tx, blockNumber, false)
		if activeHead {
			// Caught-up: promoted the active head — park it (keep-alive) then clear the active slot.
			fv.pushPreExec(fv.sharedDom, fv.extendingForkHeadHash, fv.extendingForkNumber)
			fv.sharedDom = nil
			fv.extendingForkHeadHash = common.Hash{}
			fv.extendingForkNumber = 0
			fv.extendingForkNotifications = nil
		} else {
			// Run-ahead: promoted a parked gen below the active head — kept parked by retireParkedUpTo above;
			// the active extending fork (the frontier running ahead) is untouched and keeps accumulating.
			if err := fv.ensureParked(promoteSD, blockHash, blockNumber); err != nil {
				return false, err
			}
		}
	} else {
		// Legacy non-frontier flow: identical to MergeExtendingFork — retire ≤ inclusive, clear the active slot
		// (its state was transferred into the canonical SD by the merge, which also closed it).
		fv.retireParkedUpTo(ctx, tx, blockNumber, true)
		fv.sharedDom = nil
		fv.extendingForkHeadHash = common.Hash{}
		fv.extendingForkNumber = 0
		fv.extendingForkNotifications = nil
	}
	// Record the promoted head so a re-FCU of the same block is a true no-op (promote-once). ONLY in
	// frontier mode: there a promoted gen is kept live (re-FCU would re-promote) AND canonicalHash(N) can
	// diverge from the head so the stock duplicate-FCU short-circuit misses. In non-frontier mode the stock
	// short-circuit already handles a duplicate FCU, and recording here would wrongly no-op a re-FCU that
	// must still recover (e.g. state-ahead-of-txNums) — so leave it untouched.
	if fv.frontierMode {
		fv.promotedHash = blockHash
		fv.promotedNumber = blockNumber
	}
	return true, nil
}

// IsPromotedHead reports whether (hash, number) is exactly the last block PromoteBlock canonicalised,
// i.e. a re-FCU of the current promoted head. updateForkChoice uses this to no-op such a re-FCU instead
// of re-promoting (which would re-merge the block and tear down the eager-opened successor frontier).
// Caller must NOT hold fv.lock.
func (fv *ForkValidator) IsPromotedHead(hash common.Hash, number uint64) bool {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.promotedHash != (common.Hash{}) && fv.promotedHash == hash && fv.promotedNumber == number
}

// ensureParked guarantees the given SD is present on the park stack (idempotent) — used after promoting a
// parked gen, whose retire step keeps it but whose bookkeeping we normalise here. Caller must hold fv.lock.
func (fv *ForkValidator) ensureParked(sd *execctx.SharedDomains, headHash common.Hash, number uint64) error {
	for _, g := range fv.preExecStack {
		if g.sd == sd {
			return nil
		}
	}
	fv.pushPreExec(sd, headHash, number)
	return nil
}

// retireParkedUpTo flushes+releases parked pre-exec generations whose block number is ≤
// upTo, oldest-first (tail-first). Called from the FCU merge once a block canonicalises:
// the parked predecessors' state is now durable in the DB, so their SDs can be released and
// the auxiliary stack shrinks back toward empty. Best-effort on flush error (logged) — a
// parked gen that fails to flush is still removed to avoid an unbounded stack, since its
// state is either already durable (canonicalised) or will be re-derived on the next open.
// Caller must hold fv.lock.
func (fv *ForkValidator) retireParkedUpTo(ctx context.Context, tx kv.TemporalTx, upTo uint64, inclusive bool) {
	// Retire parked pre-exec generations so the live-SD count stays FLAT. In frontierMode the caller passes
	// inclusive=false (retire STRICTLY BELOW the just-canonicalised block, keeping that one block's SD parked as
	// the successor's read-through parent); legacy passes inclusive=true. (This was temporarily disabled as a
	// diagnostic in 2026-08 to rule out "SD closed too early"; re-enabled now that the consensus/txpool/L1
	// issues are fixed — a growing preExecStack is a real SD leak.)
	kept := fv.preExecStack[:0]
	for _, g := range fv.preExecStack {
		// inclusive=true → retire ≤ upTo (legacy). inclusive=false → retire STRICTLY BELOW upTo: the
		// just-canonicalised block's own SD (g.number == upTo) is kept parked as the read-through frontier
		// for its successor; it retires when the NEXT block canonicalises.
		retire := g.number < upTo || (inclusive && g.number == upTo)
		if retire {
			if g.sd != nil {
				// Best-effort flush: a parked gen at/below the canonicalised block either has
				// its state already durable (via the active-block merge / a prior FCU) or it
				// will be re-derived on the next open, so a flush error must not wedge FCU nor
				// leak the stack — release regardless.
				_ = g.sd.FlushPendingUpdates(ctx, tx)
				g.sd.Close()
			}
			continue
		}
		kept = append(kept, g)
	}
	fv.preExecStack = kept
}

type HasDiff interface {
	Diff() (*membatchwithdb.MemoryDiff, error)
}

// isPreExecutedLive reports whether (hash, number) is a block this validator has already PRE-EXECUTED
// and still holds LIVE in memory — the active extending fork, or a parked frontier generation. Such a
// block is a valid BASE for its successor: its full post-execution state lives in the SharedDomains the
// successor chains to (preexecute SetParent), so ValidatePayload must NOT re-assemble+re-execute it as an
// unvalidated side fork (which replays its txs against its own already-applied state → "nonce too low").
// Caller must hold fv.lock.
func (fv *ForkValidator) isPreExecutedLive(hash common.Hash, number uint64) bool {
	if fv.sharedDom != nil && fv.extendingForkHeadHash == hash && fv.extendingForkNumber == number {
		return true
	}
	for _, g := range fv.preExecStack {
		if g.sd != nil && g.headHash == hash && g.number == number {
			return true
		}
	}
	return false
}

// HasLiveGen is the exported, locked form of isPreExecutedLive: reports whether (hash, number) is a live
// pre-exec generation (the active extending fork or a parked frontier gen). forkchoice uses it to decide
// whether an FCU target can be promoted from the frontier (PromoteBlock) — including a parked gen below the
// run-ahead head — rather than re-executed via the normal canonical path.
func (fv *ForkValidator) HasLiveGen(hash common.Hash, number uint64) bool {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.isPreExecutedLive(hash, number)
}

// ValidatePayload returns whether a payload is valid or invalid, or if cannot be determined, it will be accepted.
// if the payload extends the canonical chain, then we stack it in extendingFork without any unwind.
// if the payload is a fork then we unwind to the point where the fork meets the canonical chain, and there we check whether it is valid.
// if for any reason none of the actions above can be performed due to lack of information, we accept the payload and avoid validation.
func (fv *ForkValidator) ValidatePayload(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody, logger log.Logger) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.executor == nil {
		status = engine_types.AcceptedStatus
		return
	}
	hash := header.Hash()
	number := header.Number.Uint64()

	// A flashblock CLOSE reuses the maintained accumulating SD (sd == fv.sharedDom) with accumulation
	// now off. The accumulation rounds already ran the block's body through this same ValidatePayload
	// path, so the block hash is BOTH in validHashes and marked canonical — but block-end (systemEnd/
	// Finalize) has NOT run yet (it belongs to the close). The "already validated / already canonical"
	// short-circuits below would skip the close's block-end seal entirely. Force the close to execute.
	flashblockClose := sd != nil && sd == fv.sharedDom && !sd.FlashblockAccumulating()

	// If the block is stored within the side fork it means it was already validated.
	if _, ok := fv.validHashes.Get(hash); ok && !flashblockClose {
		status = engine_types.ValidStatus
		latestValidHash = hash
		return
	}

	// if the block is not in range of maxReorgDepth from head then we do not validate it.
	if math.AbsoluteDifference(fv.currentHeight, header.Number.Uint64()) > fv.maxReorgDepth {
		status = engine_types.AcceptedStatus
		return
	}
	var foundCanonical bool
	foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, hash, number)
	if criticalError != nil {
		return
	}
	if foundCanonical && !flashblockClose {
		status = engine_types.ValidStatus
		latestValidHash = header.Hash()
		return
	}
	// Let's assemble the side fork backwards
	currentHash := header.ParentHash
	unwindPoint := number - 1
	baseIsPreExecLive := false // true when the side-fork base is a live in-memory frontier block, not canonical
	foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, currentHash, unwindPoint)
	if criticalError != nil {
		return
	}
	// A live in-memory frontier ancestor takes PRECEDENCE over a canonical marking. The seal re-keys a
	// sealed frontier block as canonical so it can be FOUND, but its STATE still lives only in the frontier
	// SD (FCU lags). Chain to that live SD rather than treating it as a committed-canonical base — the
	// latter reads a lagging/absent DB base and makes the commitment root diverge. A genuinely committed
	// (non-live) canonical ancestor is unaffected: isPreExecutedLive is false there.
	if fv.isPreExecutedLive(currentHash, unwindPoint) {
		foundCanonical = true
		baseIsPreExecLive = true
	}

	logger.Debug("Execution ForkValidator.ValidatePayload", "foundCanonical", foundCanonical, "currentHash", currentHash, "unwindPoint", unwindPoint)

	var bodiesChain []*types.RawBody
	var headersChain []*types.Header
	for !foundCanonical {
		var (
			header *types.Header
			body   *types.Body
		)
		header, criticalError = fv.blockReader.Header(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if header == nil {
			// We miss some components so we did not check validity.
			status = engine_types.AcceptedStatus
			return
		}
		body, criticalError = fv.blockReader.BodyWithTransactions(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		if body == nil {
			criticalError = fmt.Errorf("found chain gap in block body at hash %s, number %d", currentHash, unwindPoint)
			return
		}

		headersChain = append([]*types.Header{header}, headersChain...)
		bodiesChain = append([]*types.RawBody{body.RawBody()}, bodiesChain...)

		currentHash = header.ParentHash
		unwindPoint = header.Number.Uint64() - 1
		foundCanonical, criticalError = fv.blockReader.IsCanonical(fv.ctx, tx, currentHash, unwindPoint)
		if criticalError != nil {
			return
		}
		// Stop at any pre-executed LIVE ancestor on the extending chain (FCU may canonicalise several
		// blocks at once, so a run of extending blocks can still be un-canonicalised here). Its state is
		// already in memory on the frontier stack — do not re-execute it. Takes PRECEDENCE over a canonical
		// marking (a re-keyed-canonical-but-still-live frontier block must chain to its live SD, not the DB).
		if fv.isPreExecutedLive(currentHash, unwindPoint) {
			foundCanonical = true
			baseIsPreExecLive = true
		}
		logger.Debug("Execution ForkValidator.ValidatePayload", "foundCanonical", foundCanonical, "currentHash", currentHash, "unwindPoint", unwindPoint)
	}
	// A pre-executed LIVE base is chained in memory (SetParent) — it is NOT a canonical ancestor to unwind
	// to, and trying would read its (non-existent) canonical hash. There is nothing to unwind: the SD
	// already carries the base's state via the parent chain. Skip the unwind.
	if baseIsPreExecLive {
		unwindPoint = 0
	}
	// Do not set an unwind point if we are already there.
	if unwindPoint == fv.currentHeight {
		unwindPoint = 0
	}
	// Do NOT close the SD we are about to reuse. PreExecute (the flashblock
	// pre-execution path) passes fv.sharedDom straight back in to carry the
	// accumulated execution state forward across rounds — closing it here would
	// destroy the very state being extended.
	//
	// When sd is a FRESH SD (a new block opening) the old sharedDom is being displaced.
	// Two cases:
	//  - SUCCESSOR opening (new block number > old): under the decoupled boundary the old
	//    block may not have canonicalised yet, and the fresh block's SD is parented to it
	//    (preexecute SetParent). Closing it would destroy the read-through parent, so PARK
	//    it on the auxiliary preExecStack; FCU flushes+releases it in order later.
	//  - Same/lower number (a fork replacement at this height, or a normal single-block
	//    flow where the previous block already canonicalised): the old SD is obsolete →
	//    close it, exactly as before.
	if fv.sharedDom != nil && fv.sharedDom != sd {
		if number > fv.extendingForkNumber && fv.extendingForkNumber != 0 {
			fv.pushPreExec(fv.sharedDom, fv.extendingForkHeadHash, fv.extendingForkNumber)
		} else {
			fv.sharedDom.Close()
		}
	}
	fv.sharedDom = sd
	// Use the validation pipeline's own notifications object so that state
	// changes accumulated by exec3 during ValidateBlock are visible here.
	// Reset accumulator and receipts to match main's behaviour of creating
	// a fresh Sync (and thus fresh notifications) per validation call.
	fv.extendingForkNotifications = fv.executor.ValidationNotifications()
	fv.extendingForkNotifications.Accumulator.Reset(0)
	fv.extendingForkNotifications.RecentReceipts.Clear()
	status, latestValidHash, validationError, criticalError =
		fv.validateAndStorePayload(fv.ctx, fv.sharedDom, tx, header, body, unwindPoint, headersChain, bodiesChain)

	if fv.sharedDom != nil &&
		(criticalError != nil || status == engine_types.InvalidStatus) {
		fv.sharedDom.Close()
		fv.sharedDom = nil
	}

	return
}

// clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosen forks are
// to be considered obsolete.
func (fv *ForkValidator) clear() {
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingForkNumber = 0
	if fv.sharedDom != nil {
		fv.sharedDom.Close()
	}
	fv.sharedDom = nil
	fv.flashblockTxHashes = nil
}

// InFlashblock reports whether an in-progress flashblock is being built at blockNumber. Used by
// InsertBlocks to AVOID clearing the extending-fork state when a flashblock update (a re-insert of
// the same in-progress block number with a grown body) arrives — clearing would destroy the
// accumulated PreExecute SharedDomains that the update carries forward.
func (fv *ForkValidator) InFlashblock(blockNumber uint64) bool {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkNumber != 0 && fv.extendingForkNumber == blockNumber && len(fv.flashblockTxHashes) > 0
}

// ClearWithUnwind wipes out current extending fork data.
func (fv *ForkValidator) ClearWithUnwind() {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.clear()
}

// FlashblockUpdate describes the result of checking whether an incoming
// block is a flashblock update to the in-progress block.
type FlashblockUpdate struct {
	// IsUpdate is true when the incoming block extends the in-progress flashblock.
	IsUpdate bool
	// PrefixLen is the number of already-executed transactions that match.
	// The caller should execute only transactions from PrefixLen onward.
	PrefixLen int
	// SD is the SharedDomains from the previous flashblock, carrying
	// accumulated VersionedIO state. Nil when IsUpdate is false.
	SD *execctx.SharedDomains
}

// CheckFlashblockUpdate checks whether the given block is a flashblock
// update (prefix-extension) of the in-progress block.
//
// Returns IsUpdate=true when:
//  1. The block number matches the in-progress extending fork number
//  2. The in-progress block has flashblock tx hashes recorded
//  3. Every previously-executed tx hash appears at the same position
//     in the new block's transaction list (prefix match)
//
// When the prefix does NOT match (reordered transactions), the caller
// should treat this as a restart: clear the old state and re-execute.
func (fv *ForkValidator) CheckFlashblockUpdate(blockNumber uint64, txs []types.Transaction) FlashblockUpdate {
	fv.lock.Lock()
	defer fv.lock.Unlock()

	// Reuse the maintained SD whenever it is already open for THIS block number. An EMPTY (0-tx)
	// in-progress block IS reusable: the marker-driven atomic open creates N+1's SD (parented to N's live
	// SD) as an empty block at close, then the first content round carries forward into it with PrefixLen=0.
	// (Previously a `len(flashblockTxHashes) == 0` guard forced a fresh SD here, discarding the eager-opened
	// one — the reuse conflict that made the atomic open create a second SD.)
	if fv.extendingForkNumber != blockNumber || fv.sharedDom == nil {
		return FlashblockUpdate{}
	}

	prevHashes := fv.flashblockTxHashes
	if len(txs) < len(prevHashes) {
		return FlashblockUpdate{}
	}

	for i, h := range prevHashes {
		if txs[i].Hash() != h {
			return FlashblockUpdate{}
		}
	}

	return FlashblockUpdate{
		IsUpdate:  true,
		PrefixLen: len(prevHashes),
		SD:        fv.sharedDom,
	}
}

// RecordFlashblockTxHashes records the transaction hashes executed for
// the current in-progress flashblock so that subsequent updates can
// detect prefix matches.
func (fv *ForkValidator) RecordFlashblockTxHashes(txs []types.Transaction) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	fv.flashblockTxHashes = make([]common.Hash, len(txs))
	for i, tx := range txs {
		fv.flashblockTxHashes[i] = tx.Hash()
	}
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ForkValidator) validateAndStorePayload(ctx context.Context, sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
) (status engine_types.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	start := time.Now()
	headersChain = append(headersChain, header)
	bodiesChain = append(bodiesChain, body)
	hash := header.Hash()
	number := header.Number.Uint64()
	if err := fv.executor.ValidateBlock(ctx, sd, tx, unwindPoint, headersChain, bodiesChain); err != nil {
		if errors.Is(err, rules.ErrInvalidBlock) {
			validationError = err
		} else {
			criticalError = fmt.Errorf("validateAndStorePayload: %w", err)
			return
		}
	}
	fv.timingsCache.Add(hash, BlockTimings{time.Since(start), 0})

	latestValidHash = hash
	fv.extendingForkHeadHash = hash
	fv.extendingForkNumber = number
	if validationError != nil {
		var latestValidNumber uint64
		latestValidNumber, criticalError = stages.GetStageProgress(tx, stages.Execution)

		if criticalError != nil {
			return
		}
		var ok bool
		latestValidHash, ok, criticalError = fv.blockReader.CanonicalHash(fv.ctx, tx, latestValidNumber)
		if criticalError != nil {
			return
		}
		if !ok {
			criticalError = fmt.Errorf("canonical hash not found: %d", latestValidNumber)
			return
		}
		status = engine_types.InvalidStatus
		fv.extendingForkHeadHash = common.Hash{}
		fv.extendingForkNumber = 0
		return
	}
	// A flashblock PRE-EXECUTE accumulation round must NOT mark the block hash validated: block-end
	// (systemEnd/Finalize) has not run yet, so the block is not fully sealed. The final round's hash is
	// identical to the CLOSE's hash (same body), so adding it here would make the close's ValidatePayload
	// short-circuit on the validHashes cache and skip the block-end seal entirely. Only the CLOSE
	// (FlashblockAccumulating unset) records the fully-validated hash.
	if !sd.FlashblockAccumulating() {
		fv.validHashes.Add(hash, true)
	}

	_, criticalError = rawdb.WriteRawBodyIfNotExists(tx, hash, number, body)
	if criticalError != nil {
		return //nolint:nilnesserr
	}

	status = engine_types.ValidStatus
	return
}

// SealInPlace re-keys a freshly-closed flashblock from its deferred-output in-progress hash to the
// SEALED header hash. After the CLOSE, validateAndStorePayload set extendingForkHeadHash and validHashes
// to the in-progress (zero-output) hash; the exec module has now materialised the real-root header H1 in
// the block overlay. Point the extending fork at H1 and mark it valid, so a normal FCU(H1) takes the
// merge-extending-fork fast path (no re-execution) and canonicalises the correct header. The maintained
// sharedDom (already holding the sealed state) and extendingForkNumber are unchanged.
func (fv *ForkValidator) SealInPlace(oldHash, newHash common.Hash, number uint64) error {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if fv.extendingForkHeadHash == oldHash {
		fv.extendingForkHeadHash = newHash
	}
	fv.validHashes.Add(newHash, true)
	// The extending fork's SD block-overlay recorded HeadHeaderKey = oldHash (the executed in-progress
	// header) during fork-validation StateStep. On the merge-extending-fork FCU, MergeExtendingFork
	// flushes THIS overlay into the FCU tx (domain_shared.go), which would clobber the FCU's own
	// HeadHeaderKey=newHash with the stale oldHash — and stage_finish then copies that into the head
	// block hash, yielding a head/blockHash mismatch. Rewrite it to the sealed newHash here so the merge
	// replays the correct head. (After this, the merged FCU state is identical to a normal newPayload's.)
	if fv.sharedDom != nil {
		if ov := fv.sharedDom.BlockOverlay(); ov != nil {
			if err := rawdb.WriteHeadHeaderHash(ov, newHash); err != nil {
				return err
			}
			// StateStep wrote kv.HeaderCanonical[number] = the deferred (root-0) placeholder hash during
			// pre-exec so exec3 could locate the in-progress block; the seal above materialised the real
			// sealed header but left that canonical row pointing at the placeholder. copyFrontierChainTables
			// copies this SD's overlay canonical rows FORWARD into successor blocks' overlays, so the stale
			// placeholder propagates and a later ValidateChain of this block reads a header-less canonical
			// hash (senders "can't find header") and rebuilds+leaks a SharedDomains ([[shared_domain_lifecycle_leak]]).
			// Re-key the canonical row to the sealed hash here, alongside HeadHeaderKey, so the forward copy
			// carries the sealed hash and every later read resolves the real header.
			if err := rawdb.WriteCanonicalHash(ov, newHash, number); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetTimings returns the timings of the last block validation.
func (fv *ForkValidator) GetTimings(hash common.Hash) BlockTimings {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	if timings, ok := fv.timingsCache.Get(hash); ok {
		return timings
	}
	return BlockTimings{}
}

func (fv *ForkValidator) ExtendingFork() (common.Hash, uint64, *execctx.SharedDomains) {
	fv.lock.Lock()
	defer fv.lock.Unlock()
	return fv.extendingForkHeadHash, fv.extendingForkNumber, fv.sharedDom
}
