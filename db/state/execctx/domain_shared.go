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

package execctx

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	mxFlushTook = metrics.GetOrCreateSummary("domain_flush_took")
)

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys []string
	Vals [][]byte
}

func (l *KvList) Push(key string, val []byte) {
	l.Keys = append(l.Keys, key)
	l.Vals = append(l.Vals, val)
}

func (l *KvList) Len() int {
	return len(l.Keys)
}

func (l *KvList) Less(i, j int) bool {
	return l.Keys[i] < l.Keys[j]
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
}

type accHolder interface {
	SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet)
	SetChangesetAccumulator(acc *changeset.StateChangeSet)
}

func IsDomainAheadOfBlocks(ctx context.Context, tx kv.TemporalRwTx, logger log.Logger) bool {
	doms, err := NewSharedDomains(ctx, tx, logger)
	if doms != nil {
		defer doms.Close()
	}
	if err != nil {
		logger.Debug("domain ahead of blocks", "err", err, "stack", dbg.Stack())
		return errors.Is(err, commitmentdb.ErrBehindCommitment)
	}
	return false
}

type SharedDomains struct {
	sdCtx *commitmentdb.SharedDomainsCommitmentContext

	stepSize uint64

	logger log.Logger

	txNum             uint64
	currentStep       kv.Step
	trace             bool //nolint
	commitmentCapture bool
	// disableInlineTouchKey when true, DomainPut skips the TouchKey call.
	// Used when the commitment calculator goroutine owns the Updates buffer
	// and feeds touches via TouchPlainKeyDirect from the fan-out channel.
	disableInlineTouchKey bool
	mem                   kv.TemporalMemBatch
	metrics               changeset.DomainMetrics

	// blockOverlay is an in-memory overlay for block-level metadata writes (headers, bodies,
	// canonical hashes, TD, stage progress, forkchoice markers). It allows execution to
	// operate without holding an RwTx — writes accumulate here and are flushed atomically
	// alongside domain state via Flush().
	// Atomic because concurrent readers (RPC via LatestSD) may call BlockOverlay()
	// while Close() nils the pointer.
	blockOverlay atomic.Pointer[membatchwithdb.MemoryMutation]

	// parent is an optional parent SD for read-through chaining. When set,
	// domain reads that miss in the local mem batch fall through to the parent's
	// mem batch before consulting the underlying tx. Used by the block builder
	// to read from the FCU's published SD without writing to it.
	parent *SharedDomains

	// stateCache is an optional cache for state data (accounts, storage, code)
	stateCache *cache.StateCache

	// changesetMu guards the global current-changeset-accumulator pointer
	// against concurrent mutation while other writers are recording into it.
	//
	// Why this exists (the layering violation we are NOT fixing here):
	//
	// The "current accumulator" is unwind-side machinery: a sidecar that
	// records per-block prev-value diffs so a later unwind can reconstruct
	// the pre-block state. Execution should be forward-only and not be
	// concerned with it at all. The proper architecture is to ignore the
	// accumulator during execution and derive the per-block changesets
	// post-hoc from sd entries (which are now tx-granular) at sd.Flush time.
	// That decoupling is a larger refactor than this PR is taking on.
	//
	// The acute symptom that forces this band-aid: the parallel commitment
	// calculator briefly swaps the global accumulator pointer to route its
	// own per-block branch writes into block N's saved changeset (see
	// committer.go computeWithBlockAccumulator). During that swap window,
	// the apply goroutine continues calling DomainPut for block N+1's
	// account/storage writes, and those land in block N's CS instead of
	// block N+1's. On a later unwind, block N+1's CS lacks the prev-value
	// for those writes and the executor reads stale state, producing wrong
	// trie roots in reorg/fork tests (TestBlockchainHeaderchainReorgConsistency
	// + the off-by-one cluster).
	//
	// Until the architectural fix lands, serialize the swap window: the
	// calculator takes Lock around its swap+compute+restore, and DomainPut
	// / DomainDel take Lock during the brief window they record into the
	// accumulator. Functionally correct; perf-suboptimal.
	//
	// PERF FOLLOW-UP DRIVER: this lock is the concrete reason to move the
	// accumulator out of the execution path. The goal is lock-free
	// execution: derive per-block changesets post-hoc from sd entries
	// (now tx-granular) at sd.Flush time, and delete this Mutex + the
	// SetChangesetAccumulator/GetChangesetAccumulator API entirely.
	changesetMu sync.Mutex

	// branchCache is the aggregator-scope commitment-branch cache. It sits
	// behind sd.mem and sd.parent.mem in the read chain (consulted only after
	// both miss, before the aggTx files/MDBX read), so writers' in-flight
	// bytes always mask the cache and cross-SD pollution is impossible.
	// May be nil for test setups whose AggTx doesn't implement
	// commitment.BranchCacheProvider.
	branchCache *commitment.BranchCache

	// adaptivePinController owns the policy that decides which
	// contracts get pinned based on observed miss pressure. nil when
	// branchCache is nil or when the adaptive layer is disabled. The
	// controller's miss callback is wired onto branchCache via Bind
	// during EnableParaTrieDB; OnBlockComplete fires from sd.Flush.
	// Uses sd.txNum as the "block tick" for cooldown semantics — Flush
	// is per-batch so the controller's tick maps to batches not blocks,
	// but cooldown thresholds are tunable to compensate.
	adaptivePinController *commitment.AdaptivePinController
}

func NewSharedDomains(ctx context.Context, tx kv.TemporalTx, logger log.Logger) (*SharedDomains, error) {
	tv := commitment.VariantHexPatriciaTrie
	if statecfg.ExperimentalConcurrentCommitment {
		tv = commitment.VariantConcurrentHexPatricia
	}
	return NewSharedDomainsWithTrieVariant(ctx, tx, logger, tv)
}

// NewSharedDomainsWithTrieVariant is like NewSharedDomains but accepts an
// explicit trie variant instead of reading the global statecfg flag. Use this
// when the caller needs a specific variant without mutating process-wide state.
func NewSharedDomainsWithTrieVariant(ctx context.Context, tx kv.TemporalTx, logger log.Logger, tv commitment.TrieVariant) (*SharedDomains, error) {
	sd := &SharedDomains{
		logger: logger,
		//trace:   true,
		metrics:  changeset.DomainMetrics{Domains: map[kv.Domain]*changeset.DomainIOMetrics{}},
		stepSize: tx.Debug().StepSize(),
	}

	sd.mem = tx.Debug().NewMemBatch(&sd.metrics)

	// Fetch the aggregator-scope branch cache (lives on the commitment
	// Domain, shared across all SharedDomains derived from this
	// aggregator). The duck-typed BranchCacheProvider lookup avoids
	// importing db/state directly — db/state already imports execctx, so
	// the reverse import would create a cycle.
	var branchCache *commitment.BranchCache
	if p, ok := tx.AggTx().(commitment.BranchCacheProvider); ok {
		branchCache = p.BranchCache()
	}
	sd.branchCache = branchCache
	sd.sdCtx = commitmentdb.NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, tv, tx.Debug().Dirs().Tmp, branchCache)

	// Construct the adaptive pin controller alongside the cache. The
	// controller observes per-contract miss pressure and decides
	// promotions/extensions/demotions; without binding (deferred to
	// EnableParaTrieDB so the db is available for the reader path)
	// the controller is inert.
	if branchCache != nil && !dbg.EnvBool("DISABLE_ADAPTIVE_PIN", false) {
		sd.adaptivePinController = commitment.NewAdaptivePinController(
			branchCache,
			commitment.DefaultAdaptivePinControllerConfig(),
			logger,
		)
	}

	_, blockNum, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return sd, err
	}

	// ErrBehindCommitment is an environmental signal; sd is fully initialized.
	if blockNum > 0 {
		lastBn, _, err := rawdbv3.TxNums.Last(tx)
		if err != nil {
			return sd, err
		}
		if lastBn < blockNum {
			return sd, fmt.Errorf("%w: TxNums index is at block %d and behind commitment %d", commitmentdb.ErrBehindCommitment, lastBn, blockNum)
		}
	}

	// PIN_CONTRACT_TRUNKS preload runs from EnableParaTrieDB (called by
	// the staged sync exec stage) NOT here in NewSharedDomains — at SD
	// construction time we'd block the engine HTTP handler for ~3-4s
	// while preload reads, causing the bench's first NewPayload to be
	// dropped. EnableParaTrieDB runs in the staged sync init goroutine,
	// not the request handler, AND has access to the kv.TemporalRoDB
	// needed to spawn an own-tx preload goroutine that doesn't share
	// cursors with the main pipeline (the cause of the earlier
	// cgo-pointer panic).

	return sd, nil
}

type temporalPutDel struct {
	sd *SharedDomains
	tx kv.TemporalTx
}

func (pd *temporalPutDel) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte) error {
	return pd.sd.DomainPut(domain, pd.tx, k, v, txNum, prevVal)
}

func (pd *temporalPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte) error {
	return pd.sd.DomainDel(domain, pd.tx, k, txNum, prevVal)
}

func (pd *temporalPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	return pd.sd.DomainDelPrefix(domain, pd.tx, prefix, txNum)
}

func (sd *SharedDomains) AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel {
	return &temporalPutDel{sd, tx}
}

// changesetSwitcher is implemented by TemporalMemBatch to get/set changesets for deferred writes.
type changesetSwitcher interface {
	// GetChangesetByBlockNum returns the changeset for a given block number and
	// the block hash it is keyed under.
	GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet)
	// GetChangesetByHash returns the changeset saved under (blockNumber, blockHash).
	// Use in preference to GetChangesetByBlockNum when both are known —
	// pastChangesAccumulator can hold multiple changesets per block number after
	// a fork-bounce reorg, and number-only lookups are non-deterministic in that
	// scenario.
	GetChangesetByHash(blockNumber uint64, blockHash common.Hash) *changeset.StateChangeSet
	GetChangesetAccumulator() *changeset.StateChangeSet
	SetChangesetAccumulator(acc *changeset.StateChangeSet)
	SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet)
}

func (sd *SharedDomains) Merge(ctx context.Context, sdTxNum uint64, other *SharedDomains, otherTxNum uint64) error {
	if sdTxNum > otherTxNum {
		return fmt.Errorf("can't merge backwards: txnum: %d > %d", sdTxNum, otherTxNum)
	}

	if err := sd.mem.Merge(other.mem); err != nil {
		return err
	}

	// Merge block-level metadata from other's overlay into ours by flushing
	// other's overlay writes directly into our overlay (which implements kv.RwTx).
	if otherOverlay, sdOverlay := other.blockOverlay.Load(), sd.blockOverlay.Load(); otherOverlay != nil && sdOverlay != nil {
		if err := otherOverlay.Flush(ctx, sdOverlay); err != nil {
			return fmt.Errorf("blockOverlay merge: %w", err)
		}
	}

	// Transfer pending commitment update from other to sd (other's mem is invalidated after merge)
	if otherUpd := other.sdCtx.TakePendingUpdate(); otherUpd != nil {
		sd.sdCtx.SetPendingUpdate(otherUpd)
	}

	sd.txNum = otherTxNum
	sd.currentStep = kv.Step(otherTxNum / sd.stepSize)
	return nil
}

// ResetPendingUpdates clears all pending commitment updates.
func (sd *SharedDomains) ResetPendingUpdates() {
	if sd != nil && sd.sdCtx != nil {
		sd.sdCtx.ResetPendingUpdates()
	}
}

// FlushPendingUpdates applies the pending deferred commitment update.
// It sets the corresponding block's changeset as the accumulator
// so writes go directly to the correct changeset.
//
// Concurrency contract: the inner swap (set cs_N → apply → restore prev)
// mutates the global accumulator pointer and per-domain diff fields that
// the apply goroutine's DomainPut/DomainDel writes through. Calls from
// inside the calculator's outer LockChangesetAccumulator window must hold
// that same Mutex; calls from end-of-stage Flush are single-threaded
// against apply but still need the lock for race-detector happens-before
// against any concurrent reads via DomainPut. Caller passes
// `lockHeld=true` when it already holds changesetMu (calc path);
// `false` when FlushPendingUpdates should acquire it itself
// (Flush / standalone callers).
func (sd *SharedDomains) FlushPendingUpdates(ctx context.Context, tx kv.TemporalTx) error {
	return sd.flushPendingUpdates(ctx, tx, false)
}

// FlushPendingUpdatesLocked is the variant for callers that already hold
// changesetMu via LockChangesetAccumulator (the parallel calculator's
// per-block compute window). The public FlushPendingUpdates above
// acquires the lock itself.
func (sd *SharedDomains) FlushPendingUpdatesLocked(ctx context.Context, tx kv.TemporalTx) error {
	return sd.flushPendingUpdates(ctx, tx, true)
}

func (sd *SharedDomains) flushPendingUpdates(ctx context.Context, tx kv.TemporalTx, lockHeld bool) error {
	upd := sd.sdCtx.TakePendingUpdate()
	if upd == nil {
		return nil
	}
	defer upd.Clear()

	putBranch := func(prefix, data, prevData []byte) error {
		// Use the unlocked variant — we either hold the lock externally
		// (lockHeld=true) or inside this function (locked below). Using
		// the public DomainPut would re-acquire and self-deadlock for
		// commitment-domain writes if the lock is held externally.
		return sd.domainPutNoLock(kv.CommitmentDomain, tx, prefix, data, upd.TxNum, prevData)
	}

	if !lockHeld {
		sd.changesetMu.Lock()
		defer sd.changesetMu.Unlock()
	}

	switcher, ok := sd.mem.(changesetSwitcher)
	if !ok {
		_, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch)
		return err
	}

	// Hash-aware lookup when the pending update carries a BlockHash. This
	// disambiguates pastChangesAccumulator entries when multiple changesets
	// exist for the same block number (canonical + fork during a reorg-bounce).
	// Falls back to the legacy number-only lookup if the hash isn't set
	// (zero hash) — preserves behavior for callers that don't yet thread
	// the hash through.
	var blockHash common.Hash
	var cs *changeset.StateChangeSet
	if upd.BlockHash != (common.Hash{}) {
		blockHash = upd.BlockHash
		cs = switcher.GetChangesetByHash(upd.BlockNum, blockHash)
	} else {
		blockHash, cs = switcher.GetChangesetByBlockNum(upd.BlockNum)
	}
	if cs != nil {
		// Save current accumulator, switch to the pending update's block
		// changeset, apply deferred branch writes, save it back, then
		// restore the original accumulator. All accesses under
		// changesetMu — see concurrency contract on the wrappers above.
		prev := switcher.GetChangesetAccumulator()
		switcher.SetChangesetAccumulator(cs)

		if _, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch); err != nil {
			switcher.SetChangesetAccumulator(prev)
			return err
		}

		switcher.SavePastChangesetAccumulator(blockHash, upd.BlockNum, cs)
		switcher.SetChangesetAccumulator(prev)
		return nil
	}

	// No past changeset found — write into whatever is current
	_, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch)
	return err
}

// domainPutNoLock is the lock-held variant of DomainPut for callers
// (FlushPendingUpdates) that already hold changesetMu externally. It
// shares DomainPut's body via domainPut(..., lockHeld=true).
//
// Today DomainPut(kv.CommitmentDomain, ...) happens to skip the lock
// anyway (see the CommitmentDomain exemption in domainPut), so calling
// DomainPut directly from FlushPendingUpdates wouldn't deadlock on the
// current code path. This variant is defensive: it stays correct even if
// a future change removes that exemption (e.g. the lock-free refactor in
// #21106 reshapes how CommitmentDomain writes are routed).
func (sd *SharedDomains) domainPutNoLock(domain kv.Domain, roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
	return sd.domainPut(domain, roTx, k, v, txNum, prevVal, true)
}

type temporalGetter struct {
	sd *SharedDomains
	tx kv.TemporalTx
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return gt.sd.GetLatest(name, gt.tx, k)
}

// GetCodeSize returns the length of the code at addr without loading the
// bytes. Returns (size, true, nil) on size-cache hit, (size, true, nil)
// after a full-bytes load+populate, or (0, false, nil) when the account
// has no code. Errors propagate normally.
//
// Callers (ReaderV3.ReadAccountCodeSize, etc.) type-assert on this method
// so the existing kv.TemporalGetter interface is unchanged.
func (gt *temporalGetter) GetCodeSize(addr []byte) (int, bool, error) {
	return gt.sd.GetCodeSize(gt.tx, addr)
}

func (gt *temporalGetter) HasPrefix(name kv.Domain, prefix []byte) (firstKey []byte, firstVal []byte, ok bool, err error) {
	return gt.sd.HasPrefix(name, prefix, gt.tx)
}

func (gt *temporalGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return gt.tx.StepsInFiles(entitySet...)
}

type unmarkedPutter struct {
	sd         *SharedDomains
	forkableId kv.ForkableId
}

func (sd *SharedDomains) AsUnmarkedPutter(id kv.ForkableId) kv.UnmarkedPutter {
	return &unmarkedPutter{sd, id}
}

func (up *unmarkedPutter) Put(num kv.Num, v []byte) error {
	return up.sd.mem.PutForkable(up.forkableId, num, v)
}

func (sd *SharedDomains) AsGetter(tx kv.TemporalTx) kv.TemporalGetter {
	return &temporalGetter{sd, tx}
}

// LockChangesetAccumulator and UnlockChangesetAccumulator bracket a
// swap+use+restore sequence on the global accumulator pointer (see
// changesetMu doc on the SharedDomains struct for the layering rationale).
// Apply-side DomainPut/DomainDel take the same lock briefly so they
// cannot record into a swapped accumulator that does not belong to the
// block they are writing for.
//
// Holders MUST pair Lock with Unlock and MUST keep the critical section
// short — currently the calculator's per-block ComputeCommitment runs
// inside this lock, which serializes apply-side writes for the duration
// of compute. That cost goes away once the post-hoc-from-sd-entries
// derivation lands and this lock + the swap dance can both be deleted.
//
// Inside the locked window callers must use the *Locked variants
// (Set/GetChangesetAccumulatorLocked) — the public Set/Get acquire the
// same Mutex and would self-deadlock.
func (sd *SharedDomains) LockChangesetAccumulator()   { sd.changesetMu.Lock() }
func (sd *SharedDomains) UnlockChangesetAccumulator() { sd.changesetMu.Unlock() }

// SetChangesetAccumulator installs the given accumulator as the global
// "current" target for DomainPut/DomainDel diff recording. Locks
// changesetMu internally for the brief write — concurrent apply/calc
// paths cannot torn-write or torn-read this pointer.
func (sd *SharedDomains) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	sd.changesetMu.Lock()
	sd.mem.(accHolder).SetChangesetAccumulator(acc)
	sd.changesetMu.Unlock()
}

// SetChangesetAccumulatorLocked is the unlocked variant for callers that
// already hold changesetMu via LockChangesetAccumulator (the calculator's
// per-block compute window).
func (sd *SharedDomains) SetChangesetAccumulatorLocked(acc *changeset.StateChangeSet) {
	sd.mem.(accHolder).SetChangesetAccumulator(acc)
}

// GetChangesetAccumulator returns the currently-installed live changeset
// accumulator (the one DomainPut writes diff entries into). Returns nil if
// none is installed. Locks changesetMu internally — must NOT be called
// while already holding the lock (use GetChangesetAccumulatorLocked).
func (sd *SharedDomains) GetChangesetAccumulator() *changeset.StateChangeSet {
	sd.changesetMu.Lock()
	defer sd.changesetMu.Unlock()
	if h, ok := sd.mem.(changesetSwitcher); ok {
		return h.GetChangesetAccumulator()
	}
	return nil
}

// GetChangesetAccumulatorLocked is the unlocked variant for callers that
// already hold changesetMu.
func (sd *SharedDomains) GetChangesetAccumulatorLocked() *changeset.StateChangeSet {
	if h, ok := sd.mem.(changesetSwitcher); ok {
		return h.GetChangesetAccumulator()
	}
	return nil
}

// GetChangesetByBlockNum returns the saved changeset for a given block
// number (and the block hash it was saved under), or (zero hash, nil) if
// no such changeset has been saved via SavePastChangesetAccumulator.
//
// WARNING: ambiguous when pastChangesAccumulator holds multiple changesets
// for the same block number (e.g. canonical + fork during a reorg-bounce).
// Prefer GetChangesetByHash when the caller has the block hash available.
func (sd *SharedDomains) GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet) {
	if h, ok := sd.mem.(changesetSwitcher); ok {
		return h.GetChangesetByBlockNum(blockNumber)
	}
	return common.Hash{}, nil
}

// GetChangesetByHash returns the saved changeset for an exact (blockNumber,
// blockHash) key, or nil if not found. Use this when the caller knows both —
// pastChangesAccumulator can hold multiple changesets per block number after
// a fork-bounce reorg, and number-only lookups are non-deterministic.
func (sd *SharedDomains) GetChangesetByHash(blockNumber uint64, blockHash common.Hash) *changeset.StateChangeSet {
	if h, ok := sd.mem.(changesetSwitcher); ok {
		return h.GetChangesetByHash(blockNumber, blockHash)
	}
	return nil
}

func (sd *SharedDomains) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet) {
	sd.mem.(accHolder).SavePastChangesetAccumulator(blockHash, blockNumber, acc)
}

func (sd *SharedDomains) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	d, ok, err := sd.mem.GetDiffset(tx, blockHash, blockNumber)
	if ok || err != nil {
		return d, ok, err
	}
	// Resolve through the parent chain: a fork-validation SD is freshly
	// constructed with an empty mem batch, so the diffsets of the canonical
	// blocks it must unwind live in the canonical generation's
	// pastChangesAccumulator, reachable only via the parent link. Without
	// this an unwind silently runs with no unwind set.
	if sd.parent != nil {
		return sd.parent.GetDiffset(tx, blockHash, blockNumber)
	}
	return d, ok, err
}

func (sd *SharedDomains) Unwind(txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.mem.Unwind(txNumUnwindTo, changeset)
	// After unwind the canonical commitment-domain values are rolled
	// back; any cached entries for keys touched in the unwind window
	// now hold stale bytes vs the post-unwind canonical state. Drop
	// those specifically — the rest of the cache (including pinned
	// branches whose keys weren't in the changeset) stays warm.
	if sd.branchCache != nil && changeset != nil {
		for _, diff := range changeset[kv.CommitmentDomain] {
			sd.branchCache.Invalidate([]byte(diff.Key))
		}
	}
}

func (sd *SharedDomains) Trace() bool {
	return sd.trace
}

func (sd *SharedDomains) CommitmentCapture() bool {
	return sd.commitmentCapture
}

func (sd *SharedDomains) GetMemBatch() kv.TemporalMemBatch { return sd.mem }
func (sd *SharedDomains) SetInMemHistoryReads(v bool)      { sd.mem.SetInMemHistoryReads(v) }
func (sd *SharedDomains) InMemHistoryReads() bool          { return sd.mem.InMemHistoryReads() }

// SetParent sets a parent SD for read-through domain chaining. Domain reads
// that miss in the local mem batch will check the parent's mem batch before
// falling through to the underlying tx/aggregator.
func (sd *SharedDomains) SetParent(parent *SharedDomains) { sd.parent = parent }

// BlockOverlay returns the in-memory overlay for block-level metadata (headers, bodies,
// canonical hashes, TD, stage progress, forkchoice markers). Callers can use this
// as a kv.RwTx to route rawdb writes through the overlay instead of a real RwTx.
// Returns nil if no overlay has been initialized via InitBlockOverlay.
func (sd *SharedDomains) BlockOverlay() *membatchwithdb.MemoryMutation { return sd.blockOverlay.Load() }

func (sd *SharedDomains) CloseBlockOverlay() {
	if overlay := sd.blockOverlay.Swap(nil); overlay != nil {
		overlay.Close()
	}
}

// BlockOverlayTemporalTx returns a read-only temporal view of the block overlay.
// This allows consumers (RPC, shutter) to read uncommitted block data with
// temporal (state history) support. Returns nil if no overlay is active.
func (sd *SharedDomains) BlockOverlayTemporalTx(roTx kv.TemporalTx) kv.TemporalTx {
	overlay := sd.blockOverlay.Load()
	if overlay == nil {
		return nil
	}
	return overlay.NewTemporalReadView(roTx)
}

// InitBlockOverlay creates (or replaces) the block-level metadata overlay backed by
// the given base transaction. Writes to the overlay are visible to subsequent reads
// and are flushed atomically alongside domain state via Flush().
func (sd *SharedDomains) InitBlockOverlay(tx kv.TemporalTx, tmpDir string) error {
	if old := sd.blockOverlay.Load(); old != nil {
		old.Close()
	}
	overlay, err := membatchwithdb.NewMemoryBatch(tx, tmpDir, sd.logger)
	if err != nil {
		return fmt.Errorf("init block overlay: %w", err)
	}
	sd.blockOverlay.Store(overlay)
	return nil
}
func (sd *SharedDomains) GetCommitmentCtx() *commitmentdb.SharedDomainsCommitmentContext {
	return sd.sdCtx
}
func (sd *SharedDomains) Logger() log.Logger { return sd.logger }

// SetStateCache sets the state cache for faster lookups.
func (sd *SharedDomains) SetStateCache(stateCache *cache.StateCache) {
	if !dbg.UseStateCache || stateCache == nil {
		sd.logger.Info("[state-cache] SetStateCache skipped",
			"useStateCache", dbg.UseStateCache, "stateCacheNil", stateCache == nil)
		return
	}
	sd.logger.Info("[state-cache] SetStateCache enabled on SD")
	sd.stateCache = stateCache
}

// GetStateCache returns the StateCache, or nil if not set.
func (sd *SharedDomains) GetStateCache() *cache.StateCache {
	return sd.stateCache
}

// ProbeReadLayers samples each independent state layer (this SD's mem,
// the parent SD's mem, and direct MDBX via tx.GetLatest) and returns
// the bytes from each. Read-only, intended for divergence-detection
// diagnostics — pinpoints which layer holds bytes that disagree with
// the BranchCache. Bytes are copied so the caller can hold them past
// tx lifetime.
func (sd *SharedDomains) ProbeReadLayers(domain kv.Domain, tx kv.TemporalTx, key []byte) (mem, parentMem, mdbx []byte, memOk, parentOk bool) {
	if v, _, ok := sd.mem.GetLatest(domain, key); ok {
		memOk = true
		mem = append([]byte(nil), v...)
	}
	if sd.parent != nil {
		if v, _, ok := sd.parent.mem.GetLatest(domain, key); ok {
			parentOk = true
			parentMem = append([]byte(nil), v...)
		}
	}
	if tx != nil {
		if v, _, err := tx.GetLatest(domain, key); err == nil {
			mdbx = append([]byte(nil), v...)
		}
	}
	return
}

func (sd *SharedDomains) ClearRam(resetCommitment bool) {
	// When the commitment calculator goroutine owns the Updates buffer,
	// skip ClearRam on the commitment context to avoid concurrent btree access.
	if resetCommitment && sd.sdCtx != nil && !sd.disableInlineTouchKey {
		sd.sdCtx.ClearRam()
	}
	sd.mem.ClearRam()
}

func (sd *SharedDomains) Size() uint64 {
	return sd.mem.SizeEstimate()
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	return sd.mem.IndexAdd(table, key, txNum)
}

func (sd *SharedDomains) StepSize() uint64 { return sd.stepSize }

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if stepSize is reached
func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.txNum = txNum
	sd.currentStep = kv.Step(txNum / sd.stepSize)
}

func (sd *SharedDomains) TxNum() uint64 { return sd.txNum }

// SetDisableInlineTouchKey disables the TouchKey call inside DomainPut/DomainDel.
// When the commitment calculator goroutine owns the Updates buffer, the inline
// TouchKey must be disabled to avoid concurrent writes.
func (sd *SharedDomains) SetDisableInlineTouchKey(disable bool) {
	sd.disableInlineTouchKey = disable
}

// InlineTouchKeyDisabled returns true when inline TouchKey is disabled.
func (sd *SharedDomains) InlineTouchKeyDisabled() bool {
	return sd.disableInlineTouchKey
}

func (sd *SharedDomains) SetTrace(b, capture bool) []string {
	sd.trace = b
	sd.commitmentCapture = capture
	return sd.sdCtx.GetCapture(true)
}

func (sd *SharedDomains) HasPrefix(domain kv.Domain, prefix []byte, roTx kv.Tx) ([]byte, []byte, bool, error) {
	return sd.mem.HasPrefix(domain, prefix, roTx)
}

func (sd *SharedDomains) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte) (cont bool, err error)) error {
	return sd.mem.IteratePrefix(domain, prefix, roTx, it)
}

func (sd *SharedDomains) Close() {
	if sd.sdCtx == nil { //idempotency
		return
	}

	sd.SetTxNum(0)
	sd.ResetPendingUpdates()

	//sd.walLock.Lock()
	//defer sd.walLock.Unlock()

	sd.mem.Close()

	sd.CloseBlockOverlay()

	sd.sdCtx.Close()
	sd.sdCtx = nil
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())

	if sd.sdCtx.HasPendingUpdate() {
		if ttx, ok := tx.(kv.TemporalTx); ok {
			if err := sd.FlushPendingUpdates(ctx, ttx); err != nil {
				return err
			}
		}
	}
	if overlay := sd.blockOverlay.Load(); overlay != nil {
		if err := overlay.Flush(ctx, tx); err != nil {
			return err
		}
	}
	// Update the cache with the bytes about to land in MDBX, atomically
	// with the flush. FlushWithCallback holds the latestState write lock
	// for the full window so a concurrent DomainPut cannot interleave
	// between the cache update and the MDBX write — readers see either
	// the local sd.mem value (during the lock) or the cached/MDBX value
	// (after release). No window where cache is stale vs MDBX.
	if sd.branchCache != nil {
		if err := sd.mem.FlushWithCallback(ctx, tx, kv.CommitmentDomain, func(k []byte, v []byte, step kv.Step) {
			if len(v) == 0 {
				sd.branchCache.Invalidate(k)
				return
			}
			sd.branchCache.Put(k, v, uint64(step), "sd.Flush")
		}); err != nil {
			return err
		}
		// Adaptive controller hook: now that the cache reflects this
		// batch's writes, decide promotions / extensions / demotions
		// for the contracts whose miss pressure crossed thresholds.
		// The reader uses the in-flight Flush tx so pre-cache reads
		// see the just-committed bytes (the tx's own writes are
		// visible to itself).
		if sd.adaptivePinController != nil {
			if ttx, ok := tx.(kv.TemporalTx); ok {
				reader := func(prefix []byte) ([]byte, uint64, bool, error) {
					v, step, err := ttx.GetLatest(kv.CommitmentDomain, prefix)
					if err != nil {
						return nil, 0, false, err
					}
					return v, uint64(step), len(v) > 0, nil
				}
				// Install per-call parallel-mode plumbing so the
				// controller's promote/extend uses the wave-BFS file-only
				// resolver. The factory captures ttx; cleared after the
				// call so a future OnBlockComplete can't reach a stale tx.
				factory := func() (commitment.BatchBranchResolver, func(), error) {
					resolve := func(keys [][]byte) ([][]byte, error) {
						d := ttx.Debug()
						vals := make([][]byte, len(keys))
						for i, k := range keys {
							v, found, _, _, err := d.GetLatestFromFiles(kv.CommitmentDomain, k, 0)
							if err != nil {
								return nil, err
							}
							if found {
								vc := make([]byte, len(v))
								copy(vc, v)
								vals[i] = vc
							}
						}
						return vals, nil
					}
					return resolve, nil, nil
				}
				provider := func(contractHash []byte) map[string][]byte {
					m := map[string][]byte{}
					c, cerr := ttx.CursorDupSort(kv.TblCommitmentVals)
					if cerr != nil {
						return m
					}
					defer c.Close()
					evenFrom, evenTo, oddFrom, oddTo := commitment.ContractTrunkKeyRanges(commitment.ContractNibbles(contractHash))
					scan := func(from, to []byte) {
						for k, v, err := c.Seek(from); k != nil && err == nil; k, v, err = c.NextNoDup() {
							if bytes.Compare(k, to) >= 0 {
								return
							}
							if len(v) < 8 {
								continue
							}
							m[string(common.Copy(k))] = common.Copy(v[8:])
						}
					}
					scan(evenFrom, evenTo)
					scan(oddFrom, oddTo)
					return m
				}
				sd.adaptivePinController.SetParallelMode(factory, provider)
				sd.adaptivePinController.OnBlockComplete(ctx, sd.txNum, reader)
				sd.adaptivePinController.SetParallelMode(nil, nil)
			}
		}
		// Refresh pinned-tier gauges once per Flush — once-per-batch
		// cadence avoids per-lookup cost in the hot read path.
		sd.branchCache.PublishMetrics()
		return nil
	}
	return sd.mem.Flush(ctx, tx)
}

// ClearBranchCache empties the aggregator-scope commitment BranchCache.
// Use after operations that mutate commitment state outside the normal
// sd.Flush callback path — notably SetHead unwind, which truncates
// commitment domain history but does not re-write all the keys whose
// cached values are now stale. Without this call, the next FCU sees
// the pre-unwind KeyCommitmentState and trips ErrBehindCommitment.
func (sd *SharedDomains) ClearBranchCache() {
	if sd.branchCache != nil {
		sd.branchCache.Clear()
	}
}

// TemporalDomain satisfaction
func (sd *SharedDomains) GetLatest(domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	if tx == nil {
		return nil, 0, errors.New("sd.GetLatest: unexpected nil tx")
	}
	var start time.Time
	if dbg.KVReadLevelledMetrics {
		start = time.Now()
	}
	maxStep := kv.Step(math.MaxUint64)

	// Check mem batch first - it has the current transaction's uncommitted state.
	// No need to populate stateCache here — mem is checked first on every read,
	// so the value is already accessible without caching it again.
	if v, step, ok := sd.mem.GetLatest(domain, k); ok {
		if dbg.KVReadLevelledMetrics {
			sd.metrics.UpdateCacheReads(domain, start)
		}
		changeset.IncReadTier(domain, changeset.TierMem)
		return v, step, nil
	} else {
		if step > 0 {
			maxStep = step
		}
	}

	// Check parent's mem batch (read-through chaining for child SDs)
	if sd.parent != nil {
		if v, step, ok := sd.parent.mem.GetLatest(domain, k); ok {
			if dbg.KVReadLevelledMetrics {
				sd.metrics.UpdateCacheReads(domain, start)
			}
			changeset.IncReadTier(domain, changeset.TierMem)
			return v, step, nil
		} else {
			if step > 0 && step < maxStep {
				maxStep = step
			}
		}
	}

	type MeteredGetter interface {
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *changeset.DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
	}

	// stateCache holds in-flight values from previous transactions in the same batch
	// that haven't been flushed to DB yet. Early return keeps correctness AND performance.
	if sd.stateCache != nil {
		v, ok := sd.stateCache.Get(domain, k)
		if dbg.KVReadLevelledMetrics {
			if ok {
				sd.metrics.UpdateStateCacheHit(domain)
			} else {
				sd.metrics.UpdateStateCacheMiss(domain)
			}
		}
		if ok {
			if dbg.AssertStateCache {
				// Fetch authoritative value from the backing tx and panic on any divergence.
				// sd.mem and sd.parent.mem were already checked above and missed, so the
				// backing tx is the single source of truth for this key at this point.
				var vDB []byte
				if aggTx, okAgg := tx.AggTx().(MeteredGetter); okAgg {
					vDB, _, _, _ = aggTx.MeteredGetLatest(domain, k, tx, maxStep, &sd.metrics, start)
				} else {
					vDB, _, _ = tx.GetLatest(domain, k)
				}
				if !bytes.Equal(v, vDB) {
					panic(fmt.Sprintf("stateCache divergence: domain=%v key=%x cached=%x db=%x txNum=%d",
						domain, k, v, vDB, sd.txNum))
				}
			}
			changeset.IncReadTier(domain, changeset.TierStateCache)
			return v, 0, nil
		}
	}

	// branchCache sits between sd.mem/parent.mem and the aggTx files for
	// CommitmentDomain only. It mirrors MDBX-flushed bytes; writers' fresh
	// bytes are held in sd.mem above, so a cache hit here is always
	// equivalent to reading from MDBX. Cleared by sd.Flush so a new MDBX
	// state never coexists with stale cache entries.
	if domain == kv.CommitmentDomain && sd.branchCache != nil {
		if cv, cstep, ok := sd.branchCache.Get(k); ok {
			changeset.IncReadTier(domain, changeset.TierStateCache)
			return cv, kv.Step(cstep), nil
		}
	}

	// CodeDomain L2b transparent bypass: many addresses share one codeHash
	// (proxies, factory-deployed clones, ERC-20 holder set, OpenZeppelin
	// templates). Today's addr-keyed cache misses on every fresh address
	// even when the bytecode is already in the L2b layer. Resolve the
	// codeHash from the account record (warm in AccountsDomain cache for
	// any account the EVM has already loaded) and probe L2b before paying
	// the code-file accessor stack cost.
	var codeEthHash []byte
	if domain == kv.CodeDomain && sd.stateCache != nil {
		if h := sd.codeHashForAddr(tx, k); len(h) > 0 {
			codeEthHash = h
			if cv, ok := sd.stateCache.GetCodeByHash(codeEthHash); ok {
				changeset.IncReadTier(domain, changeset.TierStateCache)
				return cv, 0, nil
			}
		}
	}

	if aggTx, ok := tx.AggTx().(MeteredGetter); ok {
		v, step, _, err = aggTx.MeteredGetLatest(domain, k, tx, maxStep, &sd.metrics, start)
	} else {
		v, step, err = tx.GetLatest(domain, k)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}

	// Populate state cache on successful storage read
	if sd.stateCache != nil {
		if domain == kv.CodeDomain && len(codeEthHash) > 0 && len(v) > 0 {
			sd.stateCache.PutCodeWithHash(k, v, codeEthHash)
		} else {
			sd.stateCache.Put(domain, k, v)
		}
	}
	if domain == kv.CommitmentDomain && sd.branchCache != nil && len(v) > 0 {
		sd.branchCache.Put(k, v, uint64(step), "sd.GetLatest")
	}

	return v, step, nil
}

// GetCodeSize returns the length of the contract code at addr, probing a
// size-only cache (geth-style codeSizeCache) before falling through to the
// full bytes path. For workloads dominated by EXTCODESIZE / EXTCODEHASH
// this avoids the file-accessor + decompression cost of the full bytes on
// the second-and-later access to any codeHash seen anywhere in the process.
//
// Correctness invariant: the fast path is purely additive. When it cannot
// answer, the function delegates to GetLatest(CodeDomain, addr) — the
// authoritative path that hits L1/parent/stateCache/L2b/file in order.
// Never short-circuits to (0, false, nil) based on account-record
// resolution alone; that broke EIP-7002 / EIP-7251 system-contract
// syscalls (the predeploy has CodeDomain entries but the AccountsDomain
// record may be empty at block boundary).
//
// Returns (size, true, nil) on success and (0, false, nil) only when
// CodeDomain itself confirms no code.
func (sd *SharedDomains) GetCodeSize(tx kv.TemporalTx, addr []byte) (int, bool, error) {
	if tx == nil {
		return 0, false, errors.New("sd.GetCodeSize: unexpected nil tx")
	}

	// Fast path: when we can resolve codeHash from the account cache AND
	// the size is in the size cache, return without loading bytes.
	if sd.stateCache != nil {
		if codeEthHash := sd.codeHashForAddr(tx, addr); len(codeEthHash) > 0 {
			if size, ok := sd.stateCache.GetCodeSizeByHash(codeEthHash); ok {
				return size, true, nil
			}
			if cv, ok := sd.stateCache.GetCodeByHash(codeEthHash); ok {
				sd.stateCache.PutCodeSizeByHash(codeEthHash, len(cv))
				return len(cv), true, nil
			}
		}
	}

	// Cold path: authoritative read via the normal SD.GetLatest chain.
	// Populates L1, L2b, and (via PutWithEthHash) the size layer for
	// future callers.
	v, _, err := sd.GetLatest(kv.CodeDomain, tx, addr)
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	return len(v), true, nil
}

// codeHashForAddr returns the Ethereum codeHash for an account, or nil if the
// account cannot be resolved or has no code. Reads the account through this
// SD's normal layered lookup chain so the AccountsDomain cache absorbs the
// cost (the typical case for any address the EVM has already loaded).
//
// Returns nil quietly on any error or missing account — the caller falls
// through to the addr-keyed file read so correctness is unaffected.
func (sd *SharedDomains) codeHashForAddr(tx kv.TemporalTx, addr []byte) []byte {
	if len(addr) == 0 {
		return nil
	}
	// Nethermind-style addr → codeHash LRU. Skip the full AccountsDomain
	// chain when the codeHash for this addr is already known. The zero
	// hash sentinel means "no code or missing account" (negative cache).
	if sd.stateCache != nil {
		if h, ok := sd.stateCache.GetAddrCodeHash(addr); ok {
			if h == ([32]byte{}) {
				return nil
			}
			return h[:]
		}
	}

	resolve := func() []byte {
		if v, _, ok := sd.mem.GetLatest(kv.AccountsDomain, addr); ok {
			return decodeAccountCodeHash(v)
		}
		if sd.parent != nil {
			if v, _, ok := sd.parent.mem.GetLatest(kv.AccountsDomain, addr); ok {
				return decodeAccountCodeHash(v)
			}
		}
		if sd.stateCache != nil {
			if v, ok := sd.stateCache.Get(kv.AccountsDomain, addr); ok {
				return decodeAccountCodeHash(v)
			}
		}
		v, _, err := tx.GetLatest(kv.AccountsDomain, addr)
		if err != nil || len(v) == 0 {
			return nil
		}
		return decodeAccountCodeHash(v)
	}

	h := resolve()
	if sd.stateCache != nil {
		var fixed [32]byte
		if len(h) == 32 {
			copy(fixed[:], h)
		}
		// Always populate, including the zero-hash sentinel for misses —
		// repeat lookups skip the whole resolve() chain.
		sd.stateCache.PutAddrCodeHash(addr, fixed)
	}
	return h
}

// decodeAccountCodeHash extracts the codeHash from an account's encoded
// (DecodeForStorage) bytes. Returns nil on decode error or when the account
// has no code (empty codeHash).
func decodeAccountCodeHash(enc []byte) []byte {
	if len(enc) == 0 {
		return nil
	}
	var acc accounts.Account
	// AccountsDomain values are SerialiseV3-encoded, so they must be decoded
	// with DeserialiseV3. DecodeForStorage is the legacy MDBX bitmask format
	// with an incompatible binary layout; applied to V3 bytes it silently
	// misparses and leaves CodeHash empty, so codeHashForAddr returned nil for
	// every contract and the CodeDomain ethHash bypass never fired.
	if err := accounts.DeserialiseV3(&acc, enc); err != nil {
		return nil
	}
	if acc.CodeHash.IsEmpty() {
		return nil
	}
	h := acc.CodeHash.Value()
	return h[:]
}

func (sd *SharedDomains) Metrics() *changeset.DomainMetrics {
	return &sd.metrics
}

func (sd *SharedDomains) LogMetrics() []any {
	var metrics []any

	sd.metrics.RLock()
	defer sd.metrics.RUnlock()

	if readCount := sd.metrics.CacheReadCount; readCount > 0 {
		metrics = append(metrics, "cache", common.PrettyCounter(readCount),
			"puts", common.PrettyCounter(sd.metrics.CachePutCount),
			"size", fmt.Sprintf("%s(%s/%s)",
				common.PrettyCounter(sd.metrics.CachePutSize), common.PrettyCounter(sd.metrics.CachePutKeySize), common.PrettyCounter(sd.metrics.CachePutValueSize)),
			"gets", common.PrettyCounter(sd.metrics.CacheGetCount), "size", common.PrettyCounter(sd.metrics.CacheGetSize),
			"cdur", common.Round(sd.metrics.CacheReadDuration/time.Duration(readCount), 0))
	}

	if hits, misses := sd.metrics.StateCacheHitCount, sd.metrics.StateCacheMissCount; hits+misses > 0 {
		metrics = append(metrics, "stateCache",
			fmt.Sprintf("hit=%s miss=%s rate=%.0f%%",
				common.PrettyCounter(hits),
				common.PrettyCounter(misses),
				100*float64(hits)/float64(hits+misses)))
	}

	if readCount := sd.metrics.DbReadCount; readCount > 0 {
		metrics = append(metrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(sd.metrics.DbReadDuration/time.Duration(readCount), 0))
	}

	if readCount := sd.metrics.FileReadCount; readCount > 0 {
		metrics = append(metrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(sd.metrics.FileReadDuration/time.Duration(readCount), 0))
	}

	return metrics
}

func (sd *SharedDomains) DomainLogMetrics() map[kv.Domain][]any {
	var logMetrics = map[kv.Domain][]any{}

	sd.metrics.RLock()
	defer sd.metrics.RUnlock()

	for domain, dm := range sd.metrics.Domains {
		var metrics []any

		if readCount := dm.CacheReadCount; readCount > 0 {
			metrics = append(metrics, "cache", common.PrettyCounter(readCount), "cdur", common.Round(dm.CacheReadDuration/time.Duration(readCount), 0))
		}

		if hits, misses := dm.StateCacheHitCount, dm.StateCacheMissCount; hits+misses > 0 {
			metrics = append(metrics, "stateCache",
				fmt.Sprintf("hit=%s miss=%s rate=%.0f%%",
					common.PrettyCounter(hits),
					common.PrettyCounter(misses),
					100*float64(hits)/float64(hits+misses)))
		}

		if readCount := dm.DbReadCount; readCount > 0 {
			metrics = append(metrics, "db", common.PrettyCounter(readCount), "dbdur", common.Round(dm.DbReadDuration/time.Duration(readCount), 0))
		}

		if readCount := dm.FileReadCount; readCount > 0 {
			metrics = append(metrics, "files", common.PrettyCounter(readCount), "fdur", common.Round(dm.DbReadDuration/time.Duration(readCount), 0))
		}

		if len(metrics) > 0 {
			logMetrics[domain] = metrics
		}
	}

	return logMetrics
}

func (sd *SharedDomains) GetAsOf(domain kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return sd.mem.GetAsOf(domain, key, ts)
}

// DomainPut
// Optimizations:
//   - user can provide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainPut(domain kv.Domain, roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
	return sd.domainPut(domain, roTx, k, v, txNum, prevVal, false)
}

// domainPut is the shared body for DomainPut (lockHeld=false) and
// domainPutNoLock (lockHeld=true). Factored so a new domain case or
// pre-check is written once. See changesetMu doc on the SharedDomains
// struct for the locking rationale.
func (sd *SharedDomains) domainPut(domain kv.Domain, roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte, lockHeld bool) error {
	if v == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}
	ks := string(k)
	if !sd.disableInlineTouchKey {
		sd.sdCtx.TouchKey(domain, ks, v)
	}
	if prevVal == nil {
		var err error
		prevVal, _, err = sd.GetLatest(domain, roTx, k)
		if err != nil {
			return err
		}
	}
	switch domain {
	case kv.CodeDomain, kv.AccountsDomain, kv.StorageDomain, kv.CommitmentDomain:
		if bytes.Equal(prevVal, v) {
			return nil
		}
	case kv.RCacheDomain:
		//noop
	default:
		if bytes.Equal(prevVal, v) {
			return nil
		}
	}

	// Update state cache when writing. For AccountsDomain, also invalidate
	// the addr → codeHash LRU above SD because the codeHash may have
	// changed (CREATE/CREATE2-replace; SELFDESTRUCT goes through DomainDel
	// which has its own invalidation). The next codeHashForAddr lookup
	// will repopulate from the freshly written account bytes.
	if sd.stateCache != nil {
		sd.stateCache.Put(domain, k, v)
		if domain == kv.AccountsDomain {
			sd.stateCache.DeleteAddrCodeHash(k)
		}
	}

	// Serialize against the calculator's accumulator-swap window — see
	// changesetMu doc on the SharedDomains struct. Skipped when the caller
	// already holds changesetMu (lockHeld=true, the FlushPendingUpdates
	// path), and currently also for CommitmentDomain — those writes
	// originate exclusively from the calculator's compute, which holds
	// changesetMu via LockChangesetAccumulator (re-acquiring would
	// self-deadlock). All other domains are written by the apply goroutine
	// and need to serialize against the swap.
	if !lockHeld && domain != kv.CommitmentDomain {
		sd.changesetMu.Lock()
		defer sd.changesetMu.Unlock()
	}
	return sd.mem.DomainPut(domain, ks, v, txNum, prevVal)
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, tx kv.TemporalTx, k []byte, txNum uint64, prevVal []byte) error {
	ks := string(k)
	if !sd.disableInlineTouchKey {
		sd.sdCtx.TouchKey(domain, ks, nil)
	}

	if prevVal == nil {
		var err error
		prevVal, _, err = sd.GetLatest(domain, tx, k)
		if err != nil {
			return err
		}
	}

	switch domain {
	case kv.AccountsDomain:
		if err := sd.DomainDelPrefix(kv.StorageDomain, tx, k, txNum); err != nil {
			return err
		}
		if err := sd.DomainDel(kv.CodeDomain, tx, k, txNum, nil); err != nil {
			return err
		}
		// Remove from state cache when account is deleted (including the
		// nethermind-style addr → codeHash mapping; SELFDESTRUCT and
		// CREATE-replace both reset the codeHash).
		if sd.stateCache != nil {
			sd.stateCache.Delete(kv.AccountsDomain, k)
			sd.stateCache.Delete(kv.CodeDomain, k)
			sd.stateCache.DeleteAddrCodeHash(k)
		}
		// AccountsDomain — apply-side. Serialize against swap window.
		sd.changesetMu.Lock()
		defer sd.changesetMu.Unlock()
		return sd.mem.DomainDel(kv.AccountsDomain, ks, txNum, prevVal)
	case kv.StorageDomain:
		// Remove from state cache when storage is deleted
		if sd.stateCache != nil {
			sd.stateCache.Delete(kv.StorageDomain, k)
		}
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		// Remove from state cache when code is deleted
		if sd.stateCache != nil {
			sd.stateCache.Delete(kv.CodeDomain, k)
		}
	default:
		//noop
	}
	// Serialize against the calculator's swap window for non-commitment
	// domains; CommitmentDomain skipped — see DomainPut comment.
	if domain != kv.CommitmentDomain {
		sd.changesetMu.Lock()
		defer sd.changesetMu.Unlock()
	}
	return sd.mem.DomainDel(domain, ks, txNum, prevVal)
}

func (sd *SharedDomains) DomainDelPrefix(domain kv.Domain, roTx kv.TemporalTx, prefix []byte, txNum uint64) error {
	if domain != kv.StorageDomain {
		return errors.New("DomainDelPrefix: not supported")
	}

	type tuple struct {
		k, v []byte
	}
	tombs := make([]tuple, 0, 8)

	if err := sd.IteratePrefix(kv.StorageDomain, prefix, roTx, func(k, v []byte) (bool, error) {
		tombs = append(tombs, tuple{k, v})
		return true, nil
	}); err != nil {
		return err
	}
	for _, tomb := range tombs {
		if err := sd.DomainDel(kv.StorageDomain, roTx, tomb.k, txNum, tomb.v); err != nil {
			return err
		}
	}

	if assert.Enable {
		forgotten := 0
		if err := sd.IteratePrefix(kv.StorageDomain, prefix, roTx, func(k, v []byte) (bool, error) {
			forgotten++
			return true, nil
		}); err != nil {
			return err
		}
		if forgotten > 0 {
			panic(fmt.Errorf("DomainDelPrefix: %d forgotten keys after '%x' prefix removal", forgotten, prefix))
		}
	}
	return nil
}

// DiscardWrites disables updates collection for further flushing into db.
// Instead, it keeps them temporarily available until .ClearRam/.Close will make them unavailable.
func (sd *SharedDomains) DiscardWrites(d kv.Domain) {
	// TODO: Deprecated - need convert this method to Constructor-Builder configuration
	if d >= kv.DomainLen {
		return
	}
	sd.mem.DiscardWrites(d)
}

func (sd *SharedDomains) GetCommitmentContext() *commitmentdb.SharedDomainsCommitmentContext {
	return sd.sdCtx
}

// SeekCommitment lookups latest available commitment and sets it as current
func (sd *SharedDomains) SeekCommitment(ctx context.Context, tx kv.TemporalTx) (txNum, blockNum uint64, err error) {
	txNum, blockNum, err = sd.sdCtx.SeekCommitment(ctx, tx)
	if err != nil {
		return 0, 0, err
	}
	sd.SetTxNum(txNum)
	return txNum, blockNum, nil
}

// ComputeCommitment evaluates commitment for gathered updates.
// If trieWarmup toggle was enabled via EnableTrieWarmup, pre-warms MDBX page cache by reading Branch data in parallel before processing.
func (sd *SharedDomains) ComputeCommitment(ctx context.Context, tx kv.TemporalTx, saveStateAfter bool, blockNum, txNum uint64, logPrefix string, onProgress func(*commitment.CommitProgress)) (rootHash []byte, err error) {
	return sd.computeCommitment(ctx, tx, saveStateAfter, blockNum, txNum, logPrefix, onProgress, false)
}

// ComputeCommitmentLocked is the variant for callers (the parallel
// commitment calculator) that already hold changesetMu via
// LockChangesetAccumulator. The pending-updates flush uses the *Locked
// internal path so it doesn't self-deadlock on the outer mutex.
//
// Routes the deferred branch writes from the previous block into the
// correct block's changeset (via the hash-aware lookup in
// FlushPendingUpdatesLocked) without releasing the calculator's outer
// lock — closing the SetChangesetAccumulator-vs-SetChangesetAccumulator
// races between calc-internal swap and the apply-side SetChangesetAccumulator.
func (sd *SharedDomains) ComputeCommitmentLocked(ctx context.Context, tx kv.TemporalTx, saveStateAfter bool, blockNum, txNum uint64, logPrefix string, onProgress func(*commitment.CommitProgress)) (rootHash []byte, err error) {
	return sd.computeCommitment(ctx, tx, saveStateAfter, blockNum, txNum, logPrefix, onProgress, true)
}

func (sd *SharedDomains) computeCommitment(ctx context.Context, tx kv.TemporalTx, saveStateAfter bool, blockNum, txNum uint64, logPrefix string, onProgress func(*commitment.CommitProgress), lockHeld bool) (rootHash []byte, err error) {
	// Flush any pending deferred commitment updates from the previous block
	// into the CORRECT block's changeset (via the hash-aware lookup in
	// FlushPendingUpdates). This ensures the branch writes are recorded in
	// the original block's diffset so they can be properly reverted on unwind.
	if lockHeld {
		err = sd.FlushPendingUpdatesLocked(ctx, tx)
	} else {
		err = sd.FlushPendingUpdates(ctx, tx)
	}
	if err != nil {
		return nil, err
	}
	return sd.sdCtx.ComputeCommitment(ctx, tx, saveStateAfter, blockNum, txNum, logPrefix, onProgress)
}

// EnableTrieWarmup enables parallel warmup of MDBX page cache during commitment.
// It requires a DB to be enabled via EnableParaTrieDB.
func (sd *SharedDomains) EnableTrieWarmup(trieWarmup bool) {
	sd.sdCtx.EnableTrieWarmup(trieWarmup)
}

func (sd *SharedDomains) EnableParaTrieDB(db kv.TemporalRoDB) {
	sd.sdCtx.EnableParaTrieDB(db)

	// Stage-init is the right firing point for both the env-driven
	// forced preload AND the adaptive controller's bind:
	//  (a) we have a kv.TemporalRoDB to spawn an own-tx preload goroutine
	//      with — avoids the cgo-pointer panic that the earlier shared-tx
	//      attempt produced.
	//  (b) we run from the stage-init path, not an engine request handler,
	//      so we don't block engine HTTP responses during the 3-4s preload
	//      window — the synchronous-from-NewSharedDomains shape caused the
	//      bench's first NewPayload to be dropped, breaking block validation.
	// TryClaimPreload guards fire-once-per-BranchCache-lifetime.
	if sd.branchCache == nil || !sd.branchCache.TryClaimPreload() {
		return
	}

	// Operator-override path: PIN_CONTRACT_TRUNKS forces specific
	// contracts to be pinned regardless of adaptive policy. Runs
	// first so its pin set is in place before the controller binds.
	if pinList := dbg.EnvString("PIN_CONTRACT_TRUNKS", ""); pinList != "" {
		triggerTrunkPreload(context.Background(), sd.branchCache, db, pinList, sd.logger)
	}

	// Bind the adaptive controller to the cache so it starts
	// observing miss pressure for un-forced contracts. The reader
	// for promote/extend preloads is constructed per-call from the
	// in-flight Flush tx (see SD.Flush).
	if sd.adaptivePinController != nil {
		sd.adaptivePinController.Bind()
		sd.logger.Info("[adaptive-pin] enabled",
			"max_promoted", commitment.DefaultAdaptivePinControllerConfig().MaxPromotedContracts,
			"per_contract_max_mb", commitment.DefaultAdaptivePinControllerConfig().PerContractMaxBudgetBytes/(1<<20))
	}
}

// SetDeferCommitmentUpdates enables or disables deferred commitment updates.
// When enabled, commitment branch updates are stored in the commitment context
// instead of being applied inline, and must be flushed later via FlushPendingUpdates.
func (sd *SharedDomains) SetDeferCommitmentUpdates(defer_ bool) {
	sd.sdCtx.SetDeferCommitmentUpdates(defer_)
}

// TouchChangedKeysFromHistory touches the changed keys in the commitment trie by reading the historical updates.
func (sd *SharedDomains) TouchChangedKeysFromHistory(tx kv.TemporalTx, fromTxNum, toTxNum uint64) (int, int, error) {
	var accountChanges, storageChanges int
	var err error
	accountChanges, err = sd.touchChangedKeys(tx, kv.AccountsDomain, fromTxNum, toTxNum)
	if err != nil {
		return accountChanges, storageChanges, err
	}
	storageChanges, err = sd.touchChangedKeys(tx, kv.StorageDomain, fromTxNum, toTxNum)
	if err != nil {
		return accountChanges, storageChanges, err
	}
	return accountChanges, storageChanges, err
}

// touchChangedKeys retrieves the stream of changed keys for the specified domain in [fromTxNum, toTxNum) range and
// touches them onto the commitment trie.
func (sd *SharedDomains) touchChangedKeys(tx kv.TemporalTx, d kv.Domain, fromTxNum uint64, toTxNum uint64) (int, error) {
	changes := 0
	it, err := tx.Debug().HistoryKeyTxNumRange(d, int(fromTxNum), int(toTxNum), order.Asc, -1)
	if err != nil {
		return changes, err
	}
	defer it.Close()
	var k []byte
	for it.HasNext() {
		k, _, err = it.Next()
		if err != nil {
			return changes, err
		}
		if !sd.disableInlineTouchKey {
			sd.GetCommitmentContext().TouchKey(d, string(k), nil)
		}
		changes++
	}
	return changes, nil
}

// triggerTrunkPreload spawns a goroutine that pre-pins the storage-trunk
// of each contract in pinList for the given BranchCache. pinList is a
// comma-separated list of 64-hex-char contract hashes (keccak256(addr)).
//
// The goroutine opens its OWN kv.TemporalRoTx via db.BeginTemporalRo,
// distinct from the staged-sync's main tx. Two earlier shapes failed:
//  1. Goroutine sharing the SD's MDBX tx → cgo "unpinned Go pointer"
//     panic from concurrent cursor use across goroutines.
//  2. Synchronous from NewSharedDomains → blocked the engine HTTP
//     handler for ~3-4s during the preload window, dropping the bench's
//     first NewPayload and breaking subsequent backward-download.
//
// Own-tx async sidesteps both. Caller must guarantee fire-once via
// BranchCache.TryClaimPreload (the only call site does this).
func triggerTrunkPreload(ctx context.Context, branchCache *commitment.BranchCache, db kv.TemporalRoDB, pinList string, logger log.Logger) {
	// RAM budget per contract. 64 MiB is the perf knee on the
	// SSTORE-bloat workload — covers the structural d=68 saturation
	// (~70K branches) plus most-shared d=69 branches, beyond which
	// marginal returns drop sharply (256 MiB sweep showed −2 ms TEST
	// took for +35 s preload).
	//
	// 64 MiB is the per-contract MAX for this PR. Larger budgets are
	// not safe to ship without (a) per-account touch-driven minimisation
	// that prunes the pinned set to the actually-useful subset (R11)
	// and (b) a global RAM cap aware of available memory (R12). Until
	// both land, an operator setting 256 MiB across many promoted
	// contracts could pin GBs without material perf benefit.
	//
	// Override is plausibly justified in narrow cases — a measured-hot
	// mainnet contract whose perf curve genuinely requires deeper
	// coverage, or operator-driven diagnostic profiling. NOT in scope
	// for this PR; an override path lands once R11 + R12 + a
	// per-contract whitelist mechanism exist. Until then, larger values
	// silently clamp with a warning so an operator can see they were
	// ignored.
	const maxPerContractBudgetMB = 64
	ramBudgetMB := dbg.EnvInt("PIN_TRUNK_RAM_BUDGET_MB", maxPerContractBudgetMB)
	if ramBudgetMB <= 0 {
		ramBudgetMB = maxPerContractBudgetMB
	}
	if ramBudgetMB > maxPerContractBudgetMB {
		logger.Warn("[trunk-preload] clamping budget to per-contract max",
			"requested_mb", ramBudgetMB, "clamped_mb", maxPerContractBudgetMB)
		ramBudgetMB = maxPerContractBudgetMB
	}
	ramBudgetBytes := ramBudgetMB << 20
	logger.Info("[trunk-preload] entering", "pin_list_raw", pinList)
	var hashes [][]byte
	for _, hexStr := range strings.Split(pinList, ",") {
		hexStr = strings.TrimSpace(hexStr)
		if hexStr == "" {
			continue
		}
		h, err := hex.DecodeString(hexStr)
		if err != nil || len(h) != 32 {
			logger.Warn("[trunk-preload] skipping invalid PIN_CONTRACT_TRUNKS entry",
				"entry", hexStr, "err", err, "len", len(h))
			continue
		}
		hashes = append(hashes, h)
	}
	logger.Info("[trunk-preload] hashes parsed", "count", len(hashes), "ram_budget_mb", ramBudgetMB)
	if len(hashes) == 0 {
		return
	}
	go func() {
		// PIN_TRUNK_PARALLEL (default on): the parallel file-only wave-BFS
		// (PreloadContractTrunkParallel). =false falls back to the per-prefix
		// BFS via tx.GetLatest. Worker txs: each goroutine MUST use its own
		// tx — MDBX cursors aren't concurrent-safe under one tx (cgo "unpinned
		// Go pointer" panic). Reads here go via the file layer only
		// (DebugGetLatestFromFiles): no MDBX cursor, no per-key cgo crossing,
		// and values are already dereferenced. Correct for the from-cold-
		// snapshot trigger path (MDBX holds nothing for the subtree).
		parallel := dbg.EnvBool("PIN_TRUNK_PARALLEL", true)
		nWorkers := runtime.GOMAXPROCS(0)
		if nWorkers < 1 {
			nWorkers = 1
		}
		if nWorkers > 16 {
			nWorkers = 16
		}
		if !parallel {
			nWorkers = 1
		}
		txs := make([]kv.TemporalTx, 0, nWorkers)
		for i := 0; i < nWorkers; i++ {
			t, err := db.BeginTemporalRo(ctx) //nolint:gocritic // collected into txs; all rolled back via the defer below
			if err != nil {
				for _, t := range txs {
					t.Rollback()
				}
				logger.Warn("[trunk-preload] BeginTemporalRo failed", "worker", i, "err", err)
				return
			}
			txs = append(txs, t)
		}
		defer func() {
			for _, t := range txs {
				t.Rollback()
			}
		}()

		reader := func(prefix []byte) ([]byte, uint64, bool, error) {
			v, step, err := txs[0].GetLatest(kv.CommitmentDomain, prefix)
			if err != nil {
				return nil, 0, false, err
			}
			return v, uint64(step), len(v) > 0, nil
		}

		resolve := func(keys [][]byte) ([][]byte, error) {
			n := len(keys)
			vals := make([][]byte, n)
			if n == 0 {
				return vals, nil
			}
			nw := nWorkers
			if nw > n {
				nw = n
			}
			chunk := (n + nw - 1) / nw
			var wg sync.WaitGroup
			var mu sync.Mutex
			var firstErr error
			for w := 0; w < nw; w++ {
				lo, hi := w*chunk, (w+1)*chunk
				if hi > n {
					hi = n
				}
				if lo >= hi {
					break
				}
				wg.Add(1)
				go func(t kv.TemporalTx, lo, hi int) {
					defer wg.Done()
					d := t.Debug()
					for i := lo; i < hi; i++ {
						v, found, _, _, err := d.GetLatestFromFiles(kv.CommitmentDomain, keys[i], 0)
						if err != nil {
							mu.Lock()
							if firstErr == nil {
								firstErr = err
							}
							mu.Unlock()
							return
						}
						if found {
							vc := make([]byte, len(v))
							copy(vc, v)
							vals[i] = vc
						}
					}
				}(txs[w], lo, hi)
			}
			wg.Wait()
			return vals, firstErr
		}

		// dbPrefetch: phase 1 — pull the contract's MDBX-resident commitment
		// branches (the freshest values: recently rewritten, not yet flushed to
		// files) into an in-memory map via one range cursor per parity range, so
		// the parallel wave-BFS resolves them from memory and the file layer is
		// authoritative only for the keys MDBX doesn't have. The DupSort
		// valsTable dups are invertedStep||value (latest first), so Seek +
		// NextNoDup walks the latest value per key. Bounded by ramBudgetBytes;
		// on a from-cold-snapshot start this finds nothing.
		dbPrefetch := func(h []byte) map[string][]byte {
			m := map[string][]byte{}
			c, cerr := txs[0].CursorDupSort(kv.TblCommitmentVals)
			if cerr != nil {
				logger.Warn("[trunk-preload] phase-1 CursorDupSort failed", "err", cerr)
				return m
			}
			defer c.Close()
			evenFrom, evenTo, oddFrom, oddTo := commitment.ContractTrunkKeyRanges(commitment.ContractNibbles(h))
			bytesBudget := ramBudgetBytes
			scanRange := func(from, to []byte) error {
				for k, v, err := c.Seek(from); k != nil; k, v, err = c.NextNoDup() {
					if err != nil {
						return err
					}
					if bytes.Compare(k, to) >= 0 {
						break
					}
					if len(v) < 8 {
						continue
					}
					val := common.Copy(v[8:])
					m[string(common.Copy(k))] = val
					if bytesBudget -= len(val) + 8; bytesBudget <= 0 {
						return nil
					}
				}
				return nil
			}
			if err := scanRange(evenFrom, evenTo); err != nil {
				logger.Warn("[trunk-preload] phase-1 even-range scan", "err", err)
			} else if bytesBudget > 0 {
				if err := scanRange(oddFrom, oddTo); err != nil {
					logger.Warn("[trunk-preload] phase-1 odd-range scan", "err", err)
				}
			}
			return m
		}

		for i, h := range hashes {
			started := time.Now()
			logger.Info("[trunk-preload] contract starting", "i", i, "hash", hex.EncodeToString(h), "parallel", parallel, "workers", nWorkers)
			var n int
			var err error
			if parallel {
				dbBranches := dbPrefetch(h)
				if len(dbBranches) > 0 {
					logger.Info("[trunk-preload] phase-1 db prefetch", "hash", hex.EncodeToString(h), "db_branches", len(dbBranches))
				}
				n, err = commitment.PreloadContractTrunkParallel(h, ramBudgetBytes, dbBranches, resolve, branchCache, logger)
			} else {
				n, err = commitment.PreloadContractTrunk(h, ramBudgetBytes, reader, branchCache, logger)
			}
			took := time.Since(started)
			if err != nil {
				logger.Warn("[trunk-preload] failed",
					"hash", hex.EncodeToString(h), "pinned_so_far", n, "took", took, "err", err)
				continue
			}
			logger.Info("[trunk-preload] contract done",
				"hash", hex.EncodeToString(h), "pinned", n, "took", took)
		}
		logger.Info("[trunk-preload] all done", "contracts", len(hashes))
	}()
}
