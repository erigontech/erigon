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
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/kvmetrics"
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

// CommitmentFlushCallback is invoked once per flushed commitment-domain tuple
// (key, value, step, txNum) by TemporalMemBatch.FlushWithCommitmentCallback.
type CommitmentFlushCallback func(k []byte, v []byte, step kv.Step, txNum uint64)

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

type cacheViews struct {
	state  cache.ReadView
	branch commitment.BranchReadView
}

// cacheViewsFor binds both process-global caches to the database and files
// generation pinned by tx. The common path reuses construction-time metadata;
// reads through another transaction derive its generation again.
func (sd *SharedDomains) cacheViewsFor(tx kv.TemporalTx) cacheViews {
	if tx == nil {
		return cacheViews{}
	}
	var stateGeneration, branchGeneration cache.Generation
	var stateEligible, branchEligible bool
	if tx.ViewID() == sd.baseViewID {
		if !sd.baseStateVersionKnown {
			return cacheViews{}
		}
		stateGeneration = sd.baseStateCacheGeneration
		branchGeneration = sd.baseBranchCacheGeneration
		stateEligible = sd.baseStateCacheEligible
		branchEligible = sd.baseBranchCacheEligible
	} else {
		stateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			return cacheViews{}
		}
		debug := tx.Debug()
		stateGeneration, branchGeneration = cacheGenerationsFor(debug, stateVersion)
		stateEligible = cacheViewEligible(debug, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain)
		branchEligible = cacheViewEligible(debug, kv.CommitmentDomain)
	}
	var views cacheViews
	if sd.stateCache != nil && stateEligible {
		views.state = sd.stateCache.View(stateGeneration)
	}
	if sd.branchCache != nil && branchEligible {
		views.branch = sd.branchCache.View(branchGeneration)
	}
	return views
}

func cacheGenerationsFor(debug kv.TemporalDebugTx, stateVersion uint64) (cache.Generation, cache.Generation) {
	stateGeneration := cache.StateGeneration(
		stateVersion,
		debug.TxNumsInFiles(kv.AccountsDomain),
		debug.TxNumsInFiles(kv.StorageDomain),
		debug.TxNumsInFiles(kv.CodeDomain),
	)
	branchGeneration := cache.BranchGeneration(stateVersion, debug.TxNumsInFiles(kv.CommitmentDomain))
	return stateGeneration, branchGeneration
}

// cacheViewEligible rejects a dependency-clamped domain view. Its reads mix
// database state with an older values frontier, so it has no exact cache
// identity.
func cacheViewEligible(debug kv.TemporalDebugTx, domains ...kv.Domain) bool {
	for _, domain := range domains {
		if _, ok := debug.DomainVisibleEnd(domain); !ok {
			return false
		}
	}
	return true
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

	// These fields describe the database snapshot used to construct this
	// SharedDomains. The common read path reuses them instead of reading cache
	// eligibility metadata for every GetLatest call.
	baseViewID                uint64
	baseStateCacheGeneration  cache.Generation
	baseBranchCacheGeneration cache.Generation
	baseStateVersionKnown     bool
	baseStateCacheEligible    bool
	baseBranchCacheEligible   bool

	txNum       uint64
	currentStep kv.Step
	// disableInlineTouchKey when true, DomainPut skips the TouchKey call.
	// Used when the commitment calculator goroutine owns the Updates buffer
	// and feeds touches via TouchPlainKeyDirect from the fan-out channel.
	disableInlineTouchKey bool
	mem                   kv.TemporalMemBatch
	metrics               kvmetrics.DomainMetrics

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

	// stateCache provides generation-bound reads and fills. cachePublisher is set
	// only when this SharedDomains owns publication of durable canonical state;
	// a speculative SharedDomains may read the cache but cannot move its
	// generation or change its authoritative entries.
	stateCache     *cache.StateCache
	cachePublisher cache.Publisher
	// Unwind and Merge preserve this flag after detaching the reader so a later
	// canonical Commit clears entries from the discarded state.
	clearStateCache bool

	// codeStore is the optional two-tier (in-mem + MDBX) codehash-keyed code
	// cache, reached via temporalGetter so an addr-keyed reader can serve a
	// code-by-hash read with the application's authoritative codehash.
	codeStore *cache.CodeStore

	// changesetMu serializes the parallel commitment calculator's swap of the
	// global current-changeset-accumulator pointer against DomainPut/DomainDel:
	// without it a block N+1 write can land in block N's changeset during the
	// swap+compute+restore window, so a later unwind reads stale prev-values.
	changesetMu sync.Mutex

	// branchCache is the aggregator-scope commitment cache. Local and parent
	// memory overlays take precedence; its generation view then prevents one
	// SharedDomains from observing another transaction's cached branches.
	branchCache     *commitment.BranchCache
	branchPublisher commitment.BranchPublisher
	// Like clearStateCache, this survives reader detachment and Merge.
	clearBranchCache bool

	// collector is the process-level KV-read metrics collector (aggregator
	// scope). Finished per-worker metrics are sent here (ownership transfer)
	// tagged by source. nil for test setups whose AggTx doesn't implement
	// kvmetrics.MetricsCollectorProvider.
	collector *kvmetrics.Collector

	// reqMetrics is an optional request-scoped accumulator for callers that read
	// through the plain AsGetter (nil per-read metrics) on a single goroutine —
	// e.g. an RPC handler that owns this SharedDomains for one request. Enabled
	// via StartRequestMetrics(source) and flushed to the collector at Close.
	// Single-owner (the request goroutine); never set on exec SDs, whose workers
	// pass their own per-worker instance via AsGetterMetered.
	reqMetrics *kvmetrics.DomainMetrics
	reqSource  kvmetrics.Source

	// adaptivePinController decides which contracts get pinned based on observed
	// miss pressure. nil when branchCache is nil or the adaptive layer is disabled.
	// Commit plans from the in-flight tx and publishes the staged pin changes
	// only after that transaction is durable.
	adaptivePinController *commitment.AdaptivePinController
}

// PickTrieVariant returns the commitment trie variant selected by the
// process-wide statecfg experimental-commitment flags. Callers that
// build a commitment.TrieConfig inline (e.g. short-lived RPC/builder/integrity
// SharedDomains) should use this so the flags are honored consistently across
// entry points instead of leaving Variant unset and relying on an implicit
// fallback inside the trie constructor.
func PickTrieVariant() commitment.TrieVariant {
	switch {
	// Selecting more than one experimental-commitment flag is a misconfiguration;
	// they are alternative paths. Streaming overlaps folding with execution, so it
	// wins over parallel.
	case statecfg.ExperimentalStreamingCommitment:
		return commitment.VariantStreamingHexPatricia
	case statecfg.ExperimentalParallelCommitment:
		return commitment.VariantParallelHexPatricia
	}
	return commitment.VariantHexPatriciaTrie
}

func NewSharedDomains(ctx context.Context, tx kv.TemporalTx, logger log.Logger, opts ...SharedDomainOption) (*SharedDomains, error) {
	o := sharedDomainOptions{
		trieCfg:              commitment.DefaultTrieConfig(),
		useSharedBranchCache: true,
	}
	o.trieCfg.Variant = PickTrieVariant()
	for _, opt := range opts {
		opt(&o)
	}
	trieCfg := o.trieCfg

	stateVersion, stateVersionErr := rawdb.GetStateVersion(tx)
	debug := tx.Debug()
	stateGeneration, branchGeneration := cacheGenerationsFor(debug, stateVersion)
	sd := &SharedDomains{
		logger:                    logger,
		metrics:                   kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
		stepSize:                  debug.StepSize(),
		baseViewID:                tx.ViewID(),
		baseStateCacheGeneration:  stateGeneration,
		baseBranchCacheGeneration: branchGeneration,
		baseStateVersionKnown:     stateVersionErr == nil,
		baseStateCacheEligible:    cacheViewEligible(debug, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain),
		baseBranchCacheEligible:   cacheViewEligible(debug, kv.CommitmentDomain),
	}

	sd.mem = tx.Debug().NewMemBatch(&sd.metrics)
	// Fetch the aggregator-scope branch cache (lives on the commitment
	// Domain, shared across all SharedDomains derived from this
	// aggregator). The duck-typed BranchCacheProvider lookup avoids
	// importing db/state directly — db/state already imports execctx, so
	// the reverse import would create a cycle.
	var branchCache *commitment.BranchCache
	if p, ok := tx.AggTx().(commitment.BranchCacheProvider); ok && o.useSharedBranchCache {
		branchCache = p.BranchCache()
	}
	sd.branchCache = branchCache
	if branchCache != nil {
		forbidVisibilityLowering(tx.AggTx())
		sd.branchPublisher = branchCache.Publisher()
	}
	if p, ok := tx.AggTx().(kvmetrics.MetricsCollectorProvider); ok {
		sd.collector = p.MetricsCollector()
	}
	sd.sdCtx = commitmentdb.NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, tx.Debug().Dirs().Tmp, trieCfg)

	// The pin controller is aggregator-scoped (co-located with branchCache) so pin
	// residency ages by block-access recency across all SharedDomains, not per-SD.
	if p, ok := tx.AggTx().(commitment.AdaptivePinControllerProvider); ok && o.useSharedBranchCache {
		sd.adaptivePinController = p.AdaptivePinController()
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
	if other.clearStateCache {
		sd.stateCache = nil
		sd.clearStateCache = true
	}
	if other.clearBranchCache {
		sd.branchCache = nil
		sd.clearBranchCache = true
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
		// Apply deferred branch writes under the pending update's block
		// changeset, then save it back. All accesses under changesetMu —
		// see concurrency contract on the wrappers above.
		defer sd.SwapChangesetAccumulatorLocked(cs)()

		if _, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch); err != nil {
			return err
		}

		switcher.SavePastChangesetAccumulator(blockHash, upd.BlockNum, cs)
		return nil
	}

	// No past changeset found — write into whatever is current.
	_, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch)
	return err
}

// domainPutNoLock is the lock-held variant of DomainPut for callers
// (FlushPendingUpdates) that already hold changesetMu externally; it stays
// correct even if the CommitmentDomain lock exemption in domainPut is removed.
func (sd *SharedDomains) domainPutNoLock(domain kv.Domain, roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
	return sd.domainPut(domain, roTx, k, v, txNum, prevVal, true)
}

type temporalGetter struct {
	sd *SharedDomains
	tx kv.TemporalTx
	// views bind both process-global caches to tx once per getter, keeping the
	// per-read path allocation-free.
	views cacheViews
	// m is an optional per-worker metrics instance to record reads into. nil
	// (the AsGetter default) collects nothing — there is no process-wide
	// accumulator, since AsGetter is used by many concurrent goroutines (RPC,
	// engine) where a shared one would be raced/unbounded. Exec workers pass
	// their own instance via AsGetterMetered and merge it at task end.
	m *kvmetrics.DomainMetrics
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return gt.sd.getLatestMetered(name, gt.tx, k, gt.m, gt.views)
}

// GetLatestContext is the context-aware read: it records into the per-worker,
// lock-free accumulator carried by ctx (a nil ctx-value collects no metrics).
// Concurrent workers (trie-warmup goroutines) pass their own accumulator via
// ctx, so they neither share metrics state with the main goroutine nor take any
// lock. Optional method — callers type-assert for it (mirrors the existing
// AggregatorRoTx.MeteredGetLatest pattern).
func (gt *temporalGetter) GetLatestContext(ctx context.Context, name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return gt.sd.getLatestMetered(name, gt.tx, k, kvmetrics.MetricsFromContext(ctx), gt.views)
}

// GetCodeSize returns the length of the code at addr without loading the
// bytes. Returns (size, true, nil) on size-cache hit, (size, true, nil)
// after a full-bytes load+populate, or (0, false, nil) when the account
// has no code. Errors propagate normally.
//
// Callers (ReaderV3.ReadAccountCodeSize, etc.) type-assert on this method
// so the existing kv.TemporalGetter interface is unchanged.
func (gt *temporalGetter) GetCodeSize(addr []byte, _ uint64) (int, bool, error) {
	return gt.sd.getCodeSize(gt.tx, gt.views, addr)
}

// GetCode returns contract code via the content-addressed fast path (see
// SD.GetCode): many addresses sharing one bytecode resolve to a single cached
// copy with no per-address CodeDomain read. Read-only — callers
// (ReaderV3.ReadAccountCode) type-assert this method; setters must not use it
// (they resolve prevVal through GetLatest, which is addr-keyed).
func (gt *temporalGetter) GetCode(addr []byte, _ uint64) ([]byte, bool, error) {
	return gt.sd.getCode(gt.tx, gt.views, addr)
}

func (gt *temporalGetter) HasPrefix(name kv.Domain, prefix []byte) (firstKey []byte, firstVal []byte, ok bool, err error) {
	return gt.sd.HasPrefix(name, prefix, gt.tx)
}

func (gt *temporalGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return gt.tx.StepsInFiles(entitySet...)
}

func (sd *SharedDomains) AsGetter(tx kv.TemporalTx) kv.TemporalGetter {
	return &temporalGetter{sd: sd, tx: tx, views: sd.cacheViewsFor(tx)}
}

// AsGetterNoMetrics is an explicit-intent alias of AsGetter (collects no
// metrics), for concurrent callers (RPC/engine) where that is deliberate.
func (sd *SharedDomains) AsGetterNoMetrics(tx kv.TemporalTx) kv.TemporalGetter {
	return &temporalGetter{sd: sd, tx: tx, views: sd.cacheViewsFor(tx)}
}

// AsGetterMetered returns a getter that records reads into the caller's own
// per-worker metrics instance m. m must be single-owner (one goroutine); the
// caller hands it off via MergeMetrics at task end (a lock per task, not per
// read) and allocates a fresh instance. Used by parallel-exec workers.
func (sd *SharedDomains) AsGetterMetered(tx kv.TemporalTx, m *kvmetrics.DomainMetrics) kv.TemporalGetter {
	return &temporalGetter{sd: sd, tx: tx, m: m, views: sd.cacheViewsFor(tx)}
}

// MergeMetrics hands a boundary producer's accumulator to BOTH sinks: the
// per-batch sd.metrics (under one lock, for the per-batch log line) and the
// process-level collector (grouped by source, for Prometheus). For low-frequency
// boundary producers (commitment fold, warmup teardown) off the per-tx hot path:
// the collector send blocks if the buffer is momentarily full (rare, brief, and
// lossless). Ownership of wm transfers to the collector — the caller must not
// touch wm again. The exec hot path does NOT use this (see LogMergeMetrics +
// Collector().TrySend, which never blocks and retains on a full buffer).
func (sd *SharedDomains) MergeMetrics(source kvmetrics.Source, wm *kvmetrics.DomainMetrics) {
	sd.metrics.Merge(wm)
	sd.collector.Send(source, wm)
}

// LogMergeMetrics folds wm into the per-batch sd.metrics aggregate only (the log
// line), without touching the collector. The exec hot path calls this each task
// for the log, and feeds the collector separately via a retained accumulator so
// a full collector buffer can never block or drop. wm is read, not retained.
func (sd *SharedDomains) LogMergeMetrics(wm *kvmetrics.DomainMetrics) {
	sd.metrics.Merge(wm)
}

// Collector returns the process-level KV-read metrics collector (may be nil).
func (sd *SharedDomains) Collector() *kvmetrics.Collector {
	return sd.collector
}

// StartRequestMetrics enables request-scoped metering for plain AsGetter reads on
// this SharedDomains, tagged with source. For single-goroutine owners (an RPC
// handler). The accumulator is flushed to the collector at Close. No-op when read
// metrics are off or there is no collector. Do NOT use on a SharedDomains shared
// across goroutines — the accumulator is single-owner.
func (sd *SharedDomains) StartRequestMetrics(source kvmetrics.Source) {
	if !dbg.KVReadLevelledMetrics || sd.collector == nil {
		return
	}
	sd.reqMetrics = kvmetrics.NewDomainMetrics()
	sd.reqSource = source
}

// flushRequestMetrics hands any request-scoped accumulator to the collector.
// Called at Close. Idempotent.
func (sd *SharedDomains) flushRequestMetrics() {
	if sd.reqMetrics == nil {
		return
	}
	sd.collector.Send(sd.reqSource, sd.reqMetrics)
	sd.reqMetrics = nil
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
// (SwapChangesetAccumulatorLocked / DetachChangesetAccumulatorLocked) —
// the public Set/Get acquire the same Mutex and would self-deadlock.
func (sd *SharedDomains) LockChangesetAccumulator()   { sd.changesetMu.Lock() }
func (sd *SharedDomains) UnlockChangesetAccumulator() { sd.changesetMu.Unlock() }

// SetChangesetAccumulator installs the given accumulator as the global
// "current" target for DomainPut/DomainDel diff recording. Locks
// changesetMu internally for the brief write — concurrent apply/calc
// paths cannot torn-write or torn-read this pointer.
func (sd *SharedDomains) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	sd.changesetMu.Lock()
	sd.setChangesetAccumulatorLocked(acc)
	sd.changesetMu.Unlock()
}

// setChangesetAccumulatorLocked is the unlocked variant of
// SetChangesetAccumulator for use under changesetMu.
func (sd *SharedDomains) setChangesetAccumulatorLocked(acc *changeset.StateChangeSet) {
	sd.mem.(accHolder).SetChangesetAccumulator(acc)
}

// GetChangesetAccumulator returns the currently-installed live changeset
// accumulator (the one DomainPut writes diff entries into). Returns nil if
// none is installed. Locks changesetMu internally — must NOT be called
// while already holding the lock (locked-window callers use
// SwapChangesetAccumulatorLocked / DetachChangesetAccumulatorLocked).
func (sd *SharedDomains) GetChangesetAccumulator() *changeset.StateChangeSet {
	sd.changesetMu.Lock()
	defer sd.changesetMu.Unlock()
	return sd.getChangesetAccumulatorLocked()
}

// getChangesetAccumulatorLocked is the unlocked variant of
// GetChangesetAccumulator for use under changesetMu.
func (sd *SharedDomains) getChangesetAccumulatorLocked() *changeset.StateChangeSet {
	if h, ok := sd.mem.(changesetSwitcher); ok {
		return h.GetChangesetAccumulator()
	}
	return nil
}

// SwapChangesetAccumulatorLocked installs the given changeset accumulator
// and returns a func that restores the previous one. Callers must hold
// changesetMu.
func (sd *SharedDomains) SwapChangesetAccumulatorLocked(acc *changeset.StateChangeSet) (restore func()) {
	prev := sd.getChangesetAccumulatorLocked()
	sd.setChangesetAccumulatorLocked(acc)
	return func() { sd.setChangesetAccumulatorLocked(prev) }
}

// DetachChangesetAccumulatorLocked installs a nil changeset accumulator and
// returns a func that restores the previous one. Callers must hold
// changesetMu.
func (sd *SharedDomains) DetachChangesetAccumulatorLocked() (restore func()) {
	return sd.SwapChangesetAccumulatorLocked(nil)
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

// Unwind drops [txNumUnwindTo, ∞)
func (sd *SharedDomains) Unwind(txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.mem.Unwind(txNumUnwindTo, changeset)
	// The global caches still describe the durable database until Commit.
	// Detaching keeps this rewound overlay from reading or filling that version.
	// If the overlay is committed, both caches are cleared because the unwind
	// diff is not a complete inventory of entries from the discarded fork.
	sd.stateCache = nil
	sd.branchCache = nil
	sd.clearStateCache = true
	sd.clearBranchCache = true
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
	overlay.DomainReader = sd
	sd.blockOverlay.Store(overlay)
	return nil
}

func (sd *SharedDomains) GetCommitmentCtx() *commitmentdb.SharedDomainsCommitmentContext {
	return sd.sdCtx
}

func (sd *SharedDomains) Logger() log.Logger { return sd.logger }

// SetStateCacheReader attaches the process-global cache for generation-checked
// reads and read-through fills. It does not grant authority to publish, clear,
// or otherwise move the cache's durable generation.
//
// This restricted capability is safe for speculative execution: its writes
// may be discarded, and its local unwind only detaches the reader. It cannot
// change the canonical cache observed by other transactions.
func (sd *SharedDomains) SetStateCacheReader(stateCache *cache.StateCache) {
	if !dbg.UseStateCache || stateCache == nil {
		return
	}
	if !sd.clearStateCache {
		sd.stateCache = stateCache
	}
}

// SetCanonicalStateCache attaches the same reader and also grants publication
// authority. Use it only for a SharedDomains whose Commit makes state durable:
// Commit may revoke existing views, apply the committed cache updates, and
// publish the resulting database and files generation. A canonical unwind may
// additionally clear all entries before publishing its rewound state.
//
// Initialize binds the process-global cache to this SharedDomains' base
// database and files snapshot. Keeping this authority separate from
// SetStateCacheReader prevents speculative rollback or unwind from changing
// globally visible cache state.
func (sd *SharedDomains) SetCanonicalStateCache(stateCache *cache.StateCache) {
	if !dbg.UseStateCache || stateCache == nil || !sd.baseStateVersionKnown {
		return
	}
	if !sd.clearStateCache {
		sd.stateCache = stateCache
	}
	sd.cachePublisher = stateCache.Publisher()
	sd.cachePublisher.Initialize(sd.baseStateCacheGeneration)
}

// BindStateCacheToAggregator binds StateCache to the aggregator's file
// publications and prevents domain-file visibility from moving backwards.
// PlainStateVersion tracks durable database state, but it does not change when
// the aggregator changes which files are visible.
//
// The binding is required even when reader fills are disabled because cache
// hits also rely on the same backing view. A database that cannot enforce the
// invariant is rejected instead of silently permitting unsafe cache reads.
func BindStateCacheToAggregator(db any, sc *cache.StateCache) {
	if sc == nil {
		return
	}
	h, ok := db.(interface{ Agg() any })
	if !ok {
		panic(fmt.Sprintf("assert: StateCache wired over %T, which cannot produce its aggregator — file-publication cache binding would be silently dropped", db))
	}
	agg := h.Agg()
	b, ok := agg.(interface{ BindStateCache(*cache.StateCache) })
	if !ok {
		panic(fmt.Sprintf("assert: aggregator %T lacks BindStateCache — file-publication cache invalidation would be silently dropped", agg))
	}
	b.BindStateCache(sc)
}

func forbidVisibilityLowering(agg any) {
	f, ok := agg.(interface{ ForbidVisibilityLowering() })
	if !ok {
		panic(fmt.Sprintf("assert: aggregator %T lacks ForbidVisibilityLowering — the visibility-lowering guard would be silently dropped", agg))
	}
	f.ForbidVisibilityLowering()
}

// SetCodeStore sets the persistent codehash-keyed code cache.
func (sd *SharedDomains) SetCodeStore(codeStore *cache.CodeStore) {
	sd.codeStore = codeStore
}

// PrintCacheStats logs the state cache hit/miss counters and resets them.
// No-op when the cache is disabled. The cache is an SD-internal detail, so
// callers observe it through SD rather than reaching for the cache directly.
func (sd *SharedDomains) PrintCacheStats() {
	if sd.stateCache != nil {
		sd.stateCache.PrintStatsAndReset()
	}
}

func (sd *SharedDomains) Size() uint64 {
	return sd.mem.SizeEstimate()
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) (err error) {
	return sd.mem.IndexAdd(table, key, txNum)
}

func (sd *SharedDomains) StepSize() uint64 { return sd.stepSize }

// IsUnfrozenStepEdge reports whether txNum is the last tx of a step whose
// commitment is not yet frozen into files — where a step-boundary checkpoint
// must be written.
func (sd *SharedDomains) IsUnfrozenStepEdge(roTx kv.TemporalTx, txNum uint64) bool {
	ss := sd.stepSize
	if ss == 0 || dbg.DiscardCommitment() {
		return false
	}
	if (txNum+1)%ss != 0 {
		return false
	}
	return txNum/ss >= uint64(roTx.StepsInFiles(kv.CommitmentDomain))
}

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

	sd.flushRequestMetrics()
	sd.SetTxNum(0)
	sd.ResetPendingUpdates()

	//sd.walLock.Lock()
	//defer sd.walLock.Unlock()

	sd.mem.Close()

	sd.CloseBlockOverlay()

	sd.sdCtx.Close()
	sd.sdCtx = nil
}

// Flush writes the in-memory batch without committing or publishing cache
// updates. A canonical SharedDomains must use Commit so the database and cache
// become visible in that order.
func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	return sd.flushMem(ctx, tx)
}

func (sd *SharedDomains) flushMem(ctx context.Context, tx kv.RwTx, opts ...kv.FlushOption) error {
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
	return sd.mem.Flush(ctx, tx, opts...)
}

// Commit flushes and commits tx before publishing either process-global cache.
// Cache views are revoked only around the database commit, so they continue to
// serve the old durable version while the in-memory batch is being flushed.
// tx must be dedicated to this operation because Commit consumes it.
func (sd *SharedDomains) Commit(ctx context.Context, tx kv.RwTx, validate ...func(tx kv.RwTx) error) error {
	defer mxFlushTook.ObserveDuration(time.Now())

	runValidate := func() error {
		for _, v := range validate {
			if v == nil {
				continue
			}
			if err := v(tx); err != nil {
				return err
			}
		}
		return nil
	}

	stateCacheEnabled := sd.cachePublisher.Enabled()
	branchCacheEnabled := sd.branchPublisher.Enabled()
	if !stateCacheEnabled && !branchCacheEnabled && sd.codeStore == nil {
		if err := sd.flushMem(ctx, tx); err != nil {
			return err
		}
		if err := runValidate(); err != nil {
			return err
		}
		return tx.Commit()
	}

	var stateUpdates []cache.Update
	stashState := func(domain kv.Domain) kv.FlushOption {
		return kv.WithFlushCallback(domain, func(key, value []byte, step kv.Step, txNum uint64) {
			stateUpdates = append(stateUpdates, cache.Update{
				Domain: domain,
				Key:    bytes.Clone(key),
				Value:  bytes.Clone(value),
				Step:   step,
				TxNum:  txNum,
			})
		})
	}
	var branchUpdates []commitment.BranchUpdate
	stashBranch := kv.WithFlushCallback(kv.CommitmentDomain, func(key, value []byte, step kv.Step, txNum uint64) {
		branchUpdates = append(branchUpdates, commitment.BranchUpdate{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(value),
			Step:  uint64(step),
			TxNum: txNum,
		})
	})

	var opts []kv.FlushOption
	if branchCacheEnabled {
		opts = append(opts, stashBranch)
	}
	if stateCacheEnabled {
		opts = append(opts, stashState(kv.AccountsDomain), stashState(kv.StorageDomain))
	}
	// CodeDomain flush stashes state-cache updates and collects code for the
	// persistent store. The code-store MDBX write is deferred to after flushMem —
	// an in-callback tx.Put interleaves with the in-progress domain flush and
	// corrupts it (reorg/unwind wrong root).
	var codeStoreWrites [][2][]byte
	if stateCacheEnabled || sd.codeStore != nil {
		opts = append(opts, kv.WithFlushCallback(kv.CodeDomain, func(key, value []byte, step kv.Step, txNum uint64) {
			if sd.codeStore != nil && len(value) > 0 {
				codeStoreWrites = append(codeStoreWrites, [2][]byte{crypto.Keccak256(value), bytes.Clone(value)})
			}
			if stateCacheEnabled {
				stateUpdates = append(stateUpdates, cache.Update{
					Domain: kv.CodeDomain,
					Key:    bytes.Clone(key),
					Value:  bytes.Clone(value),
					Step:   step,
					TxNum:  txNum,
				})
			}
		}))
	}
	if err := sd.flushMem(ctx, tx, opts...); err != nil {
		return err
	}
	for _, cw := range codeStoreWrites {
		if err := sd.codeStore.PutByHash(tx, cw[0], cw[1]); err != nil {
			return err
		}
	}
	if err := runValidate(); err != nil {
		return err
	}

	var stateGeneration, branchGeneration cache.Generation
	if stateCacheEnabled || branchCacheEnabled {
		stateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			return fmt.Errorf("read plain state version: %w", err)
		}
		stateGeneration = sd.baseStateCacheGeneration.WithStateVersion(stateVersion)
		branchGeneration = sd.baseBranchCacheGeneration.WithStateVersion(stateVersion)
	}

	var statePublication *cache.Publication
	var branchPublication *commitment.BranchPublication
	var adaptivePlan *commitment.AdaptivePinPlan
	defer func() {
		statePublication.Abort()
		branchPublication.Abort()
		adaptivePlan.Abort()
	}()

	// Canonical commits and file-view changes both acquire BranchCache before
	// StateCache. Keeping one order prevents their publications from deadlocking.
	if branchCacheEnabled {
		if !sd.clearBranchCache {
			adaptivePlan = sd.planAdaptivePins(tx)
		}
		branchPublication = sd.branchPublisher.Begin()
	}
	if stateCacheEnabled {
		statePublication = sd.cachePublisher.Begin()
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	statePublication.Publish(stateGeneration, stateUpdates, sd.clearStateCache)
	statePublication = nil
	branchPublication.Publish(branchGeneration, branchUpdates, sd.clearBranchCache, adaptivePlan)
	branchPublication = nil
	adaptivePlan.Commit()
	adaptivePlan = nil
	if sd.clearBranchCache && sd.adaptivePinController != nil {
		sd.adaptivePinController.Reset()
	}
	sd.clearStateCache = false
	sd.clearBranchCache = false
	return nil
}

// planAdaptivePins reads the uncommitted transaction because it contains the
// branches just flushed by Commit. The plan does not mutate BranchCache until
// it is included in the post-commit publication.
func (sd *SharedDomains) planAdaptivePins(tx kv.RwTx) *commitment.AdaptivePinPlan {
	if sd.adaptivePinController == nil {
		return nil
	}
	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil
	}
	reader := func(prefix []byte) ([]byte, uint64, bool, error) {
		value, step, err := ttx.GetLatest(kv.CommitmentDomain, prefix)
		if err != nil {
			return nil, 0, false, err
		}
		return value, uint64(step), len(value) > 0, nil
	}
	factory := func() (commitment.BatchBranchResolver, func(), error) {
		return pinBranchResolver(ttx), nil, nil
	}
	provider := func(contractHash []byte) map[string][]byte {
		branches := map[string][]byte{}
		cursor, err := ttx.CursorDupSort(kv.TblCommitmentVals)
		if err != nil {
			return branches
		}
		defer cursor.Close()

		evenFrom, evenTo, oddFrom, oddTo := commitment.ContractTrunkKeyRanges(commitment.ContractNibbles(contractHash))
		budget := sd.adaptivePinController.PerContractBudgetBytes()
		scanned := 0
		scan := func(from, to []byte) {
			for key, value, err := cursor.Seek(from); key != nil; key, value, err = cursor.NextNoDup() {
				if err != nil {
					return
				}
				if to != nil && bytes.Compare(key, to) >= 0 {
					return
				}
				if len(value) < 8 {
					continue
				}
				branches[string(key)] = bytes.Clone(value[8:])
				if scanned += len(key) + len(value); scanned >= budget {
					return
				}
			}
		}
		scan(evenFrom, evenTo)
		scan(oddFrom, oddTo)
		return branches
	}
	return sd.adaptivePinController.PlanBlock(
		sd.txNum,
		sd.baseBranchCacheGeneration,
		reader,
		factory,
		provider,
	)
}

// TemporalDomain satisfaction. Collects no read metrics — see
// temporalGetter.GetLatest for why there is no process-wide accumulator.
func (sd *SharedDomains) GetLatest(domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	return sd.getLatestMetered(domain, tx, k, nil, sd.cacheViewsFor(tx))
}

// GetLatestContext is the context-aware read for callers that read on behalf of
// a concurrent worker: metrics go to the per-worker, lock-free accumulator
// carried by ctx (nil ctx-value => no metrics). Lets a worker's reader meter
// without any shared accumulator or lock. Mirrors temporalGetter.GetLatestContext
// for readers that hold the SD directly (e.g. the committer's asOfStateReader).
func (sd *SharedDomains) GetLatestContext(ctx context.Context, domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	return sd.getLatestMetered(domain, tx, k, kvmetrics.MetricsFromContext(ctx), sd.cacheViewsFor(tx))
}

// servableUnderBound gates a cached entry against an in-flight unwind's
// per-key maxStep: a hit above the bound would diverge from the bounded read
// taken without the cache.
func servableUnderBound(cStep, maxStep kv.Step) bool {
	return cStep <= maxStep
}

// getLatestMetered is the read implementation. wm is the caller's lock-free
// per-task/per-worker metrics accumulator (nil disables metrics for the call).
// No global metrics lock is taken on this hot path — accumulators are combined
// into the shared DomainMetrics later via Merge.
func (sd *SharedDomains) getLatestMetered(domain kv.Domain, tx kv.TemporalTx, k []byte, wm *kvmetrics.DomainMetrics, views cacheViews) (v []byte, step kv.Step, err error) {
	if tx == nil {
		return nil, 0, errors.New("sd.GetLatest: unexpected nil tx")
	}
	var start time.Time
	if dbg.KVReadLevelledMetrics {
		start = time.Now()
		// Plain AsGetter reads (wm == nil) on a request-scoped SD fold into the
		// request accumulator. Short-circuits for exec workers (wm != nil), which
		// never touch reqMetrics — so no cross-goroutine access.
		if wm == nil {
			wm = sd.reqMetrics
		}
	}
	maxStep := kv.NoStepBound

	// Check mem batch first - it has the current transaction's uncommitted state.
	// No need to populate stateCache here — mem is checked first on every read,
	// so the value is already accessible without caching it again.
	if v, step, ok := sd.mem.GetLatest(domain, k); ok {
		if dbg.KVReadLevelledMetrics {
			wm.UpdateCacheReads(domain, start)
		}
		return v, step, nil
	} else if step < maxStep {
		maxStep = step
	}

	// Check parent's mem batch (read-through chaining for child SDs)
	if sd.parent != nil {
		if v, step, ok := sd.parent.mem.GetLatest(domain, k); ok {
			if dbg.KVReadLevelledMetrics {
				wm.UpdateCacheReads(domain, start)
			}
			return v, step, nil
		} else if step < maxStep {
			maxStep = step
		}
	}

	type MeteredGetter interface {
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *kvmetrics.DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
	}

	// stateCache holds committed values shared across domain readers.
	if sd.stateCache != nil {
		v, cStep, ok := views.state.GetWithStep(domain, k)
		if ok && !servableUnderBound(cStep, maxStep) {
			ok = false
		}
		if dbg.KVReadLevelledMetrics {
			if ok {
				wm.UpdateStateCacheHit(domain)
			} else {
				wm.UpdateStateCacheMiss(domain)
			}
		}
		if ok {
			// The divergence assert is skipped while the mem overlay bounds this
			// key (in-flight unwind): MDBX still holds the not-yet-deleted dying
			// rows inside the bound, so the "authoritative" read can return
			// dead-fork bytes and blame the cache for a legitimate hit.
			if dbg.AssertStateCache && maxStep == kv.NoStepBound {
				// Fetch authoritative value from the backing tx and panic on any divergence.
				// sd.mem and sd.parent.mem were already checked above and missed, so the
				// backing tx is the single source of truth for this key at this point.
				var vDB []byte
				var dbErr error
				if aggTx, okAgg := tx.AggTx().(MeteredGetter); okAgg {
					vDB, _, _, dbErr = aggTx.MeteredGetLatest(domain, k, tx, maxStep, wm, start)
				} else {
					vDB, _, dbErr = tx.GetLatest(domain, k)
				}
				// A transient read error leaves vDB nil; comparing against it would
				// panic "divergence" on an I/O fault even when the cache was correct.
				// Surface the real error instead.
				if dbErr != nil {
					return nil, 0, fmt.Errorf("AssertStateCache: authoritative read failed: %w", dbErr)
				}
				if !bytes.Equal(v, vDB) {
					panic(fmt.Sprintf("stateCache divergence: domain=%v key=%x cached=%x db=%x txNum=%d",
						domain, k, v, vDB, sd.txNum))
				}
			}
			return v, cStep, nil
		}
	}

	// branchCache sits between sd.mem/parent.mem and the aggTx files for
	// CommitmentDomain only. Snapshot-isolated readers must disable it because
	// concurrent commits can advance the cache beyond their transaction view.
	if domain == kv.CommitmentDomain && sd.branchCache != nil {
		if cv, cStepU64, ok := views.branch.Get(k); ok {
			// Get returns the on-disk step index directly — do NOT divide by
			// StepSize (that double-division collapsed cStep to ~0, defeating the
			// gate).
			cStep := kv.Step(cStepU64)
			if servableUnderBound(cStep, maxStep) {
				return cv, cStep, nil
			}
		}
	}

	if aggTx, ok := tx.AggTx().(MeteredGetter); ok {
		v, step, _, err = aggTx.MeteredGetLatest(domain, k, tx, maxStep, wm, start)
	} else {
		v, step, err = tx.GetLatest(domain, k)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}

	// View freshness is rechecked while the fill is serialized against cache
	// publication.
	if sd.stateCache != nil && sd.stateCache.Caches(domain) {
		views.state.Fill(domain, k, v, step)
	}
	if domain == kv.CommitmentDomain && sd.branchCache != nil {
		views.branch.Fill(k, v, uint64(step))
	}

	return v, step, nil
}

// GetCodeSize returns the length of the contract code at addr, probing a
// size-only cache before falling through to the
// full bytes path. For workloads dominated by EXTCODESIZE / EXTCODEHASH
// this avoids the file-accessor + decompression cost of the full bytes on
// the second-and-later access to any codeHash seen anywhere in the process.
//
// READ-ONLY contract (same as GetCode): the codeHash fast path resolves from
// the account record, so it must not feed a DomainPut prevVal — setters use
// the addr-keyed GetLatest. Only pure getters (EXTCODESIZE / EXTCODEHASH) use
// this shortcut.
//
// Correctness invariant: the fast path is purely additive. When it cannot
// answer, the function delegates to GetLatest(CodeDomain, addr) — the
// authoritative path that hits L1/parent/stateCache/codeHashToCode/file in order.
// Never short-circuits to (0, false, nil) based on account-record
// resolution alone; that broke EIP-7002 / EIP-7251 system-contract
// syscalls (the predeploy has CodeDomain entries but the AccountsDomain
// record may be empty at block boundary).
//
// Returns (size, true, nil) on success and (0, false, nil) only when
// CodeDomain itself confirms no code.
func (sd *SharedDomains) GetCodeSize(tx kv.TemporalTx, addr []byte, _ uint64) (int, bool, error) {
	return sd.getCodeSize(tx, sd.cacheViewsFor(tx), addr)
}

func (sd *SharedDomains) getCodeSize(tx kv.TemporalTx, views cacheViews, addr []byte) (int, bool, error) {
	if tx == nil {
		return 0, false, errors.New("sd.GetCodeSize: unexpected nil tx")
	}

	// Fast path: when we can resolve codeHash from the account cache AND
	// the size is in the size cache, return without loading bytes.
	if sd.stateCache != nil {
		if codeHash := sd.codeHashForAddr(tx, views.state, addr); len(codeHash) > 0 {
			if size, ok := views.state.GetCodeSizeByHash(codeHash); ok {
				return size, true, nil
			}
			if cv, ok := views.state.GetCodeByHash(codeHash); ok {
				views.state.FillCodeSize(codeHash, len(cv))
				return len(cv), true, nil
			}
		}
	}

	// Cold path: authoritative read via the normal SD.GetLatest chain.
	// Populates L1, codeHashToCode, and (via PutWithCodeHash) the size layer for
	// future callers.
	v, _, err := sd.getLatestMetered(kv.CodeDomain, tx, addr, nil, views)
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	return len(v), true, nil
}

// GetCode returns the contract code at addr. The fast path resolves the
// account's codeHash and returns the content-addressed bytes from the code
// cache without touching the per-address CodeDomain — so many addresses
// sharing one bytecode (proxies, clones) resolve to a single cached copy with
// no disk read. The cold path is the authoritative addr-keyed GetLatest, which
// also populates the caches.
//
// READ-ONLY contract: this is for pure getters (EVM EXTCODECOPY / CALL,
// ReaderV3.ReadAccountCode). It MUST NOT resolve a DomainPut prevVal. The fast
// path answers from the account record, which during a deploy is updated with
// the new codeHash before the code write lands; a write's prevVal read through
// it would see the about-to-be-written bytes and the DomainPut diff would elide
// the write. Setters therefore resolve prevVal through GetLatest, which is
// addr-keyed (domain-faithful); only getters use this codeHash shortcut.
func (sd *SharedDomains) GetCode(tx kv.TemporalTx, addr []byte, _ uint64) ([]byte, bool, error) {
	return sd.getCode(tx, sd.cacheViewsFor(tx), addr)
}

func (sd *SharedDomains) getCode(tx kv.TemporalTx, views cacheViews, addr []byte) ([]byte, bool, error) {
	if tx == nil {
		return nil, false, errors.New("sd.GetCode: unexpected nil tx")
	}

	// Fast path: addr → account codeHash → content-addressed bytes, no
	// per-address CodeDomain read. The codeHash is resolved mem-first, so it
	// reflects in-block code changes — keying the code store off it (rather than
	// a stateObject's stale snapshot) is reorg-safe.
	var codeHash []byte
	if sd.stateCache != nil || sd.codeStore != nil {
		if codeHash = sd.codeHashForAddr(tx, views.state, addr); len(codeHash) > 0 {
			if sd.stateCache != nil {
				if cv, ok := views.state.GetCodeByHash(codeHash); ok {
					return cv, true, nil
				}
			}
			if sd.codeStore != nil {
				if cv, ok := sd.codeStore.GetByHash(tx, codeHash); ok {
					return cv, true, nil
				}
			}
		}
	}

	// Cold path: authoritative addr-keyed read (also populates the caches).
	v, _, err := sd.getLatestMetered(kv.CodeDomain, tx, addr, nil, views)
	if err != nil {
		return nil, false, err
	}
	if len(v) == 0 {
		return nil, false, nil
	}
	return v, true, nil
}

// codeHashForAddr returns the Ethereum codeHash for an account, or nil if the
// account cannot be resolved or has no code. Reads the account through this
// SD's normal layered lookup chain so the AccountsDomain cache absorbs the
// cost (the typical case for any address the EVM has already loaded).
//
// Returns nil quietly on any error or missing account — the caller falls
// through to the addr-keyed file read so correctness is unaffected.
func (sd *SharedDomains) codeHashForAddr(tx kv.TemporalTx, view cache.ReadView, addr []byte) []byte {
	if len(addr) == 0 {
		return nil
	}
	// In-batch state is authoritative: sd.mem / parent.mem hold this batch's
	// uncommitted account writes, while the addr→codeHash LRU is invalidated only
	// on flush. Route mem-first; the LRU is a committed-state layer that may only
	// answer once mem has missed.
	if v, _, ok := sd.mem.GetLatest(kv.AccountsDomain, addr); ok {
		return accounts.DeserialiseV3CodeHash(v)
	}
	if sd.parent != nil {
		if v, _, ok := sd.parent.mem.GetLatest(kv.AccountsDomain, addr); ok {
			return accounts.DeserialiseV3CodeHash(v)
		}
	}

	// Below mem: the addr → codeHash LRU caches committed state
	// (flush-invalidated). The zero-hash sentinel means "no code / missing
	// account" (negative cache).
	if sd.stateCache != nil {
		if h, ok := view.GetAddrCodeHash(addr); ok {
			if h == ([32]byte{}) {
				return nil
			}
			return h[:]
		}
	}

	// Resolve from the committed layers (stateCache → MDBX/files). mem is
	// intentionally not consulted here because it was checked above.
	resolve := func() ([]byte, bool) {
		if sd.stateCache != nil {
			if v, ok := view.Get(kv.AccountsDomain, addr); ok {
				return accounts.DeserialiseV3CodeHash(v), true
			}
		}
		v, _, err := tx.GetLatest(kv.AccountsDomain, addr)
		if err != nil {
			return nil, false
		}
		if len(v) == 0 {
			return nil, true
		}
		return accounts.DeserialiseV3CodeHash(v), true
	}

	h, resolved := resolve()
	if resolved && sd.stateCache != nil {
		var fixed [32]byte
		if len(h) == 32 {
			copy(fixed[:], h)
		}
		view.SeedAddrCodeHash(addr, fixed)
	}
	return h
}

func (sd *SharedDomains) Metrics() *kvmetrics.DomainMetrics {
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

func (sd *SharedDomains) HistorySeek(domain kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return sd.mem.HistorySeek(domain, key, ts)
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

	// The state cache is NOT updated here. This write goes into sd.mem and
	// is served from there (checked first on every read, fork-isolated via
	// the parent chain); the shared cache is refreshed only on flush
	// (SharedDomains.Flush → FlushWithCallback), so it mirrors committed,
	// fork-agnostic state. A per-write update would leak non-flushed,
	// fork-specific bytes into a sibling fork's reads.

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

	// Deleting an account cascades to its storage and code — run before the
	// absent-key skip so leftover storage/code is still wiped even if the
	// account itself is already gone.
	if domain == kv.AccountsDomain {
		if err := sd.DomainDelPrefix(kv.StorageDomain, tx, k, txNum); err != nil {
			return err
		}
		if err := sd.DomainDel(kv.CodeDomain, tx, k, txNum, nil); err != nil {
			return err
		}
	}

	// Deleting an already-absent key is a no-op: recording it would append a
	// redundant empty->empty history row (mirrors domainPut's bytes.Equal
	// dedup). prevVal is nil when the key was never written, but []byte{} for a
	// flushed tombstone (getLatestFromDb strips the step prefix) — so test len,
	// not nil.
	if len(prevVal) == 0 {
		return nil
	}

	// State cache is refreshed on flush only — see DomainPut. Serialize against
	// the calculator's swap window for non-commitment domains; CommitmentDomain
	// skipped — see DomainPut comment.
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

	if dbg.AssertEnabled {
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

// DiscardWrites disables updates collection for further flushing into db;
// the values stay readable in memory.
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
	if sd.adaptivePinController != nil {
		sd.adaptivePinController.Bind()
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
