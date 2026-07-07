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
	"math"
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

	// stateCache is an optional cache for state data (accounts, storage, code)
	stateCache *cache.StateCache

	// codeStore is the optional two-tier (in-mem + MDBX) codehash-keyed code
	// cache, reached via temporalGetter so an addr-keyed reader can serve a
	// code-by-hash read with the application's authoritative codehash.
	codeStore *cache.CodeStore

	// changesetMu serializes the parallel commitment calculator's swap of the
	// global current-changeset-accumulator pointer against DomainPut/DomainDel:
	// without it a block N+1 write can land in block N's changeset during the
	// swap+compute+restore window, so a later unwind reads stale prev-values.
	changesetMu sync.Mutex

	// branchCache is the aggregator-scope commitment-branch cache. It sits
	// behind sd.mem and sd.parent.mem in the read chain (consulted only after
	// both miss, before the aggTx files/MDBX read), so writers' in-flight
	// bytes always mask the cache and cross-SD pollution is impossible.
	// May be nil for test setups whose AggTx doesn't implement
	// commitment.BranchCacheProvider.
	branchCache *commitment.BranchCache

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
	// Its miss callback is wired via Bind in EnableParaTrieDB; OnBlockComplete fires
	// from Commit using the in-flight (pre-Commit) tx.
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
	o := sharedDomainOptions{trieCfg: commitment.DefaultTrieConfig()}
	o.trieCfg.Variant = PickTrieVariant()
	for _, opt := range opts {
		opt(&o)
	}
	trieCfg := o.trieCfg

	sd := &SharedDomains{
		logger:   logger,
		metrics:  kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
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
	if p, ok := tx.AggTx().(kvmetrics.MetricsCollectorProvider); ok {
		sd.collector = p.MetricsCollector()
	}
	sd.sdCtx = commitmentdb.NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, tx.Debug().Dirs().Tmp, trieCfg)

	// The pin controller is aggregator-scoped (co-located with branchCache) so pin
	// residency ages by block-access recency across all SharedDomains, not per-SD.
	if p, ok := tx.AggTx().(commitment.AdaptivePinControllerProvider); ok {
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
// (FlushPendingUpdates) that already hold changesetMu externally; it stays
// correct even if the CommitmentDomain lock exemption in domainPut is removed.
func (sd *SharedDomains) domainPutNoLock(domain kv.Domain, roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
	return sd.domainPut(domain, roTx, k, v, txNum, prevVal, true)
}

type temporalGetter struct {
	sd *SharedDomains
	tx kv.TemporalTx
	// m is an optional per-worker metrics instance to record reads into. nil
	// (the AsGetter default) collects nothing — there is no process-wide
	// accumulator, since AsGetter is used by many concurrent goroutines (RPC,
	// engine) where a shared one would be raced/unbounded. Exec workers pass
	// their own instance via AsGetterMetered and merge it at task end.
	m *kvmetrics.DomainMetrics
}

func (gt *temporalGetter) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return gt.sd.getLatestMetered(name, gt.tx, k, gt.m)
}

// GetLatestContext is the context-aware read: it records into the per-worker,
// lock-free accumulator carried by ctx (a nil ctx-value collects no metrics).
// Concurrent workers (trie-warmup goroutines) pass their own accumulator via
// ctx, so they neither share metrics state with the main goroutine nor take any
// lock. Optional method — callers type-assert for it (mirrors the existing
// AggregatorRoTx.MeteredGetLatest pattern).
func (gt *temporalGetter) GetLatestContext(ctx context.Context, name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return gt.sd.getLatestMetered(name, gt.tx, k, kvmetrics.MetricsFromContext(ctx))
}

// GetCodeSize returns the length of the code at addr without loading the
// bytes. Returns (size, true, nil) on size-cache hit, (size, true, nil)
// after a full-bytes load+populate, or (0, false, nil) when the account
// has no code. Errors propagate normally.
//
// Callers (ReaderV3.ReadAccountCodeSize, etc.) type-assert on this method
// so the existing kv.TemporalGetter interface is unchanged. txNum is the
// caller's read txNum, used to stamp any cache entry it populates.
func (gt *temporalGetter) GetCodeSize(addr []byte, txNum uint64) (int, bool, error) {
	return gt.sd.GetCodeSize(gt.tx, addr, txNum)
}

// GetCode returns contract code via the content-addressed fast path (see
// SD.GetCode): many addresses sharing one bytecode resolve to a single cached
// copy with no per-address CodeDomain read. Read-only — callers
// (ReaderV3.ReadAccountCode) type-assert this method; setters must not use it
// (they resolve prevVal through GetLatest, which is addr-keyed). txNum is the
// caller's read txNum, used to stamp any cache entry it populates.
func (gt *temporalGetter) GetCode(addr []byte, txNum uint64) ([]byte, bool, error) {
	return gt.sd.GetCode(gt.tx, addr, txNum)
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
	return &temporalGetter{sd: sd, tx: tx}
}

// AsGetterNoMetrics is an explicit-intent alias of AsGetter (collects no
// metrics), for concurrent callers (RPC/engine) where that is deliberate.
func (sd *SharedDomains) AsGetterNoMetrics(tx kv.TemporalTx) kv.TemporalGetter {
	return &temporalGetter{sd: sd, tx: tx}
}

// AsGetterMetered returns a getter that records reads into the caller's own
// per-worker metrics instance m. m must be single-owner (one goroutine); the
// caller hands it off via MergeMetrics at task end (a lock per task, not per
// read) and allocates a fresh instance. Used by parallel-exec workers.
func (sd *SharedDomains) AsGetterMetered(tx kv.TemporalTx, m *kvmetrics.DomainMetrics) kv.TemporalGetter {
	return &temporalGetter{sd: sd, tx: tx, m: m}
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
	// Tx/epoch-aware unwind of the commitment BranchCache: every cached branch
	// whose bytes belong to the rolled-back window (txN at/above the unwind
	// point, superseded epoch) is now stale vs the post-unwind canonical state.
	// Unwind(txNum) bumps the epoch and lowers the floor (O(1), no scan); those
	// entries are dropped lazily on their next Get — covering entries seeded by
	// the read-pop and the trunk preload that the changeset-gated Invalidate
	// below misses (which is what left stale committed branches a fork-validation
	// then read as a wrong trie root). The explicit Invalidate of the unwound
	// changeset keys is a redundant fast path for keys known dead right now.
	if sd.branchCache != nil {
		sd.branchCache.Unwind(txNumUnwindTo)
		if changeset != nil {
			for _, diff := range changeset[kv.CommitmentDomain] {
				sd.branchCache.Invalidate([]byte(diff.Key))
			}
		}
	}
	// Invalidate the state cache for everything above the unwind point. txNum/epoch
	// based and diffset-free (see StateCache.Unwind), so it runs unconditionally —
	// independent of whether changesets were generated for the unwound range, which
	// they are not below the reorg window. Matches the domain overlay's maxtx prune.
	if sd.stateCache != nil {
		sd.stateCache.Unwind(txNumUnwindTo)
	}
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

// SetStateCache hands this SD the process-global state cache to manage.
//
// Coherence is structural, enforced by the architecture rather than by
// remembering to call this: app components reach state only through the SD, and
// the SD owns cache population (on flush) and invalidation (sd.Unwind →
// stateCache.Unwind). It is not *additionally* type-enforced only because the
// cache crosses the app/storage boundary — the storage layer can't depend on an
// app-level cache type. The single desync vector is a component that
// deliberately bypasses the SD (raw domain reads + direct cache writes, e.g.
// read-ahead warmup), which then owns its cache coherence explicitly.
func (sd *SharedDomains) SetStateCache(stateCache *cache.StateCache) {
	if !dbg.UseStateCache || stateCache == nil {
		return
	}
	sd.stateCache = stateCache
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

// The state caches (the account/storage StateCache and the commitment
// BranchCache) are an internal implementation detail of SharedDomains. No
// external entity accesses or mutates them directly — callers drive state
// through Flush / Commit / GetLatest / DomainPut, and the cache lifecycle
// (population, invalidation, commit-gating) is owned entirely here.

// Flush writes the in-memory batch into tx without committing. It deliberately
// does NOT touch the caches: plain Flush leaves the commit to the caller (who
// may still roll back), so it must not warm a cache with state that could be
// rolled back. Cache entries are populated elsewhere — by Commit after a
// successful commit, and by reads (GetLatest) — each stamped with a
// conservative upper-bound txNum. It is that txNum stamp, not population
// timing, that keeps the cache correct: an unwind lowers the floor so every
// entry reflecting a now-dead fork is evicted, and mem-first masking means a
// later in-memory write shadows a stale cached read. Callers that flush a tx
// they commit themselves get a cache-safe (cold-but-correct) result; use
// Commit to also keep the cache warm.
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

type cacheUpdate struct {
	domain kv.Domain
	key    []byte
	val    []byte
	step   kv.Step
	txN    uint64
}

// Commit flushes the in-memory batch into tx, commits tx, and only then applies
// the flushed domain bytes to the in-memory caches — CommitmentDomain to the
// BranchCache, Accounts/Storage/Code to the StateCache. The flush is implicit in
// committing the shared-domain state. Tying cache population to commit success
// makes it impossible by construction for an aggregator-lifetime cache to hold a
// value a failed commit rolled back — so no caller clears a cache or reaches into
// the SD's internal caches after committing. Entries are stamped with the value's
// per-key write txNum (delivered by the callback) as the unwind floor, so
// invalidation is tx-precise: an unwind to a txNum inside the latest step drops
// exactly the entries above it, not the whole step. All caches honor the
// same (txNum, epoch) model. tx MUST be a flush-specific transaction: it is
// committed here.
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

	if sd.branchCache == nil && sd.stateCache == nil && sd.codeStore == nil {
		if err := sd.flushMem(ctx, tx); err != nil {
			return err
		}
		if err := runValidate(); err != nil {
			return err
		}
		return tx.Commit()
	}

	// Stash every cache-bound domain tuple during the flush; apply them only
	// after the commit succeeds. On a failed commit the stash is discarded, so
	// no cache is ever advanced past durable MDBX state.
	var pending []cacheUpdate
	stash := func(domain kv.Domain) kv.FlushOption {
		return kv.WithFlushCallback(domain, func(k []byte, v []byte, step kv.Step, txNum uint64) {
			pending = append(pending, cacheUpdate{
				domain: domain,
				key:    append([]byte(nil), k...),
				val:    append([]byte(nil), v...),
				step:   step,
				txN:    txNum,
			})
		})
	}
	var opts []kv.FlushOption
	if sd.branchCache != nil {
		opts = append(opts, stash(kv.CommitmentDomain))
	}
	if sd.stateCache != nil {
		opts = append(opts, stash(kv.AccountsDomain), stash(kv.StorageDomain))
	}
	// CodeDomain flush stashes state-cache updates and collects code for the
	// persistent store. The code-store MDBX write is deferred to after flushMem —
	// an in-callback tx.Put interleaves with the in-progress domain flush and
	// corrupts it (reorg/unwind wrong root).
	var codeStoreWrites [][2][]byte
	if sd.stateCache != nil || sd.codeStore != nil {
		opts = append(opts, kv.WithFlushCallback(kv.CodeDomain, func(k []byte, v []byte, step kv.Step, txNum uint64) {
			if sd.codeStore != nil && len(v) > 0 {
				codeStoreWrites = append(codeStoreWrites, [2][]byte{crypto.Keccak256(v), append([]byte(nil), v...)})
			}
			if sd.stateCache != nil {
				pending = append(pending, cacheUpdate{
					domain: kv.CodeDomain,
					key:    append([]byte(nil), k...),
					val:    append([]byte(nil), v...),
					step:   step,
					txN:    txNum,
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
	// Adaptive pin promotions/demotions run on the in-flight (pre-Commit) tx so
	// the preload sees the just-flushed bytes.
	if sd.adaptivePinController != nil {
		if ttx, ok := tx.(kv.TemporalTx); ok {
			reader := func(prefix []byte) ([]byte, uint64, bool, error) {
				v, step, err := ttx.GetLatest(kv.CommitmentDomain, prefix)
				if err != nil {
					return nil, 0, false, err
				}
				return v, uint64(step), len(v) > 0, nil
			}
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
							vals[i] = common.Copy(v)
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
				// Bound the scan by the per-contract pin ceiling — the preload can't
				// pin more than that, so gathering further is pure waste on the
				// Commit path. A nil `to` (all-0xff prefix) means scan to the range's
				// natural end, not stop immediately.
				budget := sd.adaptivePinController.PerContractBudgetBytes()
				scanned := 0
				scan := func(from, to []byte) {
					for k, v, err := c.Seek(from); k != nil; k, v, err = c.NextNoDup() {
						if err != nil {
							return // best-effort residency hint: keep what was gathered
						}
						if to != nil && bytes.Compare(k, to) >= 0 {
							return
						}
						if len(v) < 8 {
							continue
						}
						m[string(common.Copy(k))] = common.Copy(v[8:])
						if scanned += len(k) + len(v); scanned >= budget {
							return
						}
					}
				}
				scan(evenFrom, evenTo)
				scan(oddFrom, oddTo)
				return m
			}
			sd.adaptivePinController.OnBlockComplete(ctx, sd.txNum, reader, factory, provider)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for i := range pending {
		u := &pending[i]
		switch u.domain {
		case kv.CommitmentDomain:
			if len(u.val) == 0 {
				sd.branchCache.Invalidate(u.key)
			} else {
				sd.branchCache.Put(u.key, u.val, uint64(u.step), u.txN)
			}
		case kv.AccountsDomain:
			if len(u.val) == 0 {
				sd.stateCache.Delete(kv.AccountsDomain, u.key)
				sd.stateCache.Delete(kv.CodeDomain, u.key)
				sd.stateCache.DeleteAddrCodeHash(u.key)
			} else {
				sd.stateCache.Put(kv.AccountsDomain, u.key, u.val, u.txN)
				sd.stateCache.DeleteAddrCodeHash(u.key)
			}
		case kv.StorageDomain:
			if len(u.val) == 0 {
				sd.stateCache.Delete(kv.StorageDomain, u.key)
			} else {
				sd.stateCache.Put(kv.StorageDomain, u.key, u.val, u.txN)
			}
		case kv.CodeDomain:
			if len(u.val) == 0 {
				sd.stateCache.Delete(kv.CodeDomain, u.key)
			} else {
				// Validated committed code: populate the addr layer AND the
				// content-addressed codeHash->code map, keyed by keccak(v) so each
				// entry is self-consistent by construction. The read-fill path
				// (PutCodeWithHash on a cold GetLatest, below) populates the same
				// way — both key on keccak(v), never a separately-read account
				// codeHash, so the shared map only ever holds self-consistent entries.
				sd.stateCache.PutCodeWithHash(u.key, u.val, crypto.Keccak256(u.val), u.txN)
			}
		}
	}
	return nil
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

// DetachBranchCache makes this SharedDomains ignore the aggregator-scope
// BranchCache: commitment branch reads go straight to sd.mem/overlay/MDBX and
// no read populates the shared cache. Used for fork-validation SDs, which read
// transient fork state — sharing the canonical BranchCache let them read stale
// committed branches (wrong trie root → INVALID payload) and pollute the cache
// with fork-transient branches. Only sd.branchCache (the read/populate path,
// domain_shared GetLatest) is consulted, so nil-ing it fully detaches; the
// canonical SDs keep their warm cache.
func (sd *SharedDomains) DetachBranchCache() {
	sd.branchCache = nil
}

// TemporalDomain satisfaction. Collects no read metrics — see
// temporalGetter.GetLatest for why there is no process-wide accumulator.
func (sd *SharedDomains) GetLatest(domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	return sd.getLatestMetered(domain, tx, k, nil)
}

// GetLatestContext is the context-aware read for callers that read on behalf of
// a concurrent worker: metrics go to the per-worker, lock-free accumulator
// carried by ctx (nil ctx-value => no metrics). Lets a worker's reader meter
// without any shared accumulator or lock. Mirrors temporalGetter.GetLatestContext
// for readers that hold the SD directly (e.g. the committer's asOfStateReader).
func (sd *SharedDomains) GetLatestContext(ctx context.Context, domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	return sd.getLatestMetered(domain, tx, k, kvmetrics.MetricsFromContext(ctx))
}

// getLatestMetered is the read implementation. wm is the caller's lock-free
// per-task/per-worker metrics accumulator (nil disables metrics for the call).
// No global metrics lock is taken on this hot path — accumulators are combined
// into the shared DomainMetrics later via Merge.
func (sd *SharedDomains) getLatestMetered(domain kv.Domain, tx kv.TemporalTx, k []byte, wm *kvmetrics.DomainMetrics) (v []byte, step kv.Step, err error) {
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
	maxStep := kv.Step(math.MaxUint64)

	// Check mem batch first - it has the current transaction's uncommitted state.
	// No need to populate stateCache here — mem is checked first on every read,
	// so the value is already accessible without caching it again.
	if v, step, ok := sd.mem.GetLatest(domain, k); ok {
		if dbg.KVReadLevelledMetrics {
			wm.UpdateCacheReads(domain, start)
		}
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
				wm.UpdateCacheReads(domain, start)
			}
			return v, step, nil
		} else {
			if step > 0 && step < maxStep {
				maxStep = step
			}
		}
	}

	type MeteredGetter interface {
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *kvmetrics.DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
	}
	// MeteredGetterWithTxN exposes the txN of the read so the
	// BranchCache entry can be tagged; falls back to MeteredGetter
	// when only the legacy interface is implemented (test stubs).
	type MeteredGetterWithTxN interface {
		MeteredGetLatestWithTxN(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *kvmetrics.DomainMetrics, start time.Time) (v []byte, step kv.Step, txN uint64, ok bool, err error)
	}

	// stateCache holds in-flight values from previous transactions in the same batch
	// that haven't been flushed to DB yet. Early return keeps correctness AND performance.
	if sd.stateCache != nil {
		v, cTxNum, ok := sd.stateCache.GetWithTxNum(domain, k)
		cStep := kv.Step(cTxNum / sd.StepSize())
		// Respect maxStep, mirroring the BranchCache gate below. sd.mem /
		// sd.parent.mem lowered maxStep above when an in-flight unwind re-bound
		// this key to an earlier step (the per-key unwindChangeset signal). A
		// cached entry from a higher step would diverge from the (maxStep-bounded)
		// DB read the cache-disabled path takes, so treat it as a miss and fall
		// through; the Put below refreshes it. For direct domains the (txNum,epoch)
		// floor in Get usually already drops such entries — this keeps the two
		// read paths identical regardless.
		if ok && cStep > maxStep {
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
			if dbg.AssertStateCache {
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
	// CommitmentDomain only. It mirrors MDBX-flushed bytes; writers' fresh
	// bytes are held in sd.mem above, so a cache hit here is always
	// equivalent to reading from MDBX. Refreshed per-key by sd.Flush and
	// evicted by txN watermark on unwind, so a cache hit never coexists
	// with a newer MDBX state.
	if domain == kv.CommitmentDomain && sd.branchCache != nil {
		if cv, cStepU64, ok := sd.branchCache.Get(k); ok {
			// Respect maxStep. sd.mem / sd.parent.mem lowered maxStep above when an
			// unwound key reports its restored step via unwindChangeset (the per-key
			// in-mem unwind signal). The cache's global epoch/floor is coarser than
			// that per-key signal, so a cached entry below the floor can still belong
			// to a step the unwind re-bound away. Serving it then diverges from the
			// (maxStep-bounded) DB read the cache-disabled path takes. Fall through to
			// the bounded read in that case; the Put below refreshes the entry.
			// Get returns the on-disk step index directly — do NOT divide by
			// StepSize (that double-division collapsed cStep to ~0, defeating the
			// gate).
			cStep := kv.Step(cStepU64)
			if cStep <= maxStep {
				return cv, cStep, nil
			}
		}
	}

	var readTxN uint64
	var txNKnown bool
	switch aggTx := tx.AggTx().(type) {
	case MeteredGetterWithTxN:
		v, step, readTxN, _, err = aggTx.MeteredGetLatestWithTxN(domain, k, tx, maxStep, wm, start)
		txNKnown = true
	case MeteredGetter:
		v, step, _, err = aggTx.MeteredGetLatest(domain, k, tx, maxStep, wm, start)
	default:
		v, step, err = tx.GetLatest(domain, k)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}

	// Populate state cache on successful read. Stamp with an upper bound on the
	// value's write txNum (the read only gives us the file/step it came from):
	// the last txNum of that step. An unwind below this bound can't leave the
	// value stale, so frozen-step reads stay warm across unwinds while
	// recent-step reads are dropped.
	if sd.stateCache != nil {
		readTxNum := (uint64(step)+1)*sd.StepSize() - 1
		if domain == kv.CodeDomain {
			if len(v) > 0 {
				// This SD getter is the single place that populates the code cache
				// on a read. Key the content-addressed entry by the code's OWN hash,
				// keccak(v) — NEVER a separately-read account codeHash, which under
				// parallel exec can be a skewed/cross-account value and would poison
				// the shared codeHash->code map for every account sharing that hash.
				// keccak(v) makes every cached entry self-consistent, so a skewed
				// account read can never produce a bad entry.
				sd.stateCache.PutCodeWithHash(k, v, crypto.Keccak256(v), readTxNum)
			}
		} else {
			sd.stateCache.Put(domain, k, v, readTxNum)
		}
	}
	// Only cache a branch when the read's txN is known: a txN=0 entry would
	// be treated as immortal by UnwindTo, so skip the Put rather than insert
	// an entry that can never be unwind-evicted.
	if domain == kv.CommitmentDomain && sd.branchCache != nil && len(v) > 0 && txNKnown {
		sd.branchCache.Put(k, v, uint64(step), readTxN)
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
func (sd *SharedDomains) GetCodeSize(tx kv.TemporalTx, addr []byte, txNum uint64) (int, bool, error) {
	if tx == nil {
		return 0, false, errors.New("sd.GetCodeSize: unexpected nil tx")
	}

	// Fast path: when we can resolve codeHash from the account cache AND
	// the size is in the size cache, return without loading bytes.
	if sd.stateCache != nil {
		if codeHash := sd.codeHashForAddr(tx, addr, txNum); len(codeHash) > 0 {
			if size, ok := sd.stateCache.GetCodeSizeByHash(codeHash); ok {
				return size, true, nil
			}
			if cv, ok := sd.stateCache.GetCodeByHash(codeHash); ok {
				// txNum is a conservative upper bound: >= the live code's write
				// txNum, so the size drops on any unwind that drops the code.
				sd.stateCache.PutCodeSizeByHash(codeHash, len(cv), txNum)
				return len(cv), true, nil
			}
		}
	}

	// Cold path: authoritative read via the normal SD.GetLatest chain.
	// Populates L1, codeHashToCode, and (via PutWithCodeHash) the size layer for
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
func (sd *SharedDomains) GetCode(tx kv.TemporalTx, addr []byte, txNum uint64) ([]byte, bool, error) {
	if tx == nil {
		return nil, false, errors.New("sd.GetCode: unexpected nil tx")
	}

	// Fast path: addr → account codeHash → content-addressed bytes, no
	// per-address CodeDomain read. The codeHash is resolved mem-first, so it
	// reflects in-block code changes — keying the code store off it (rather than
	// a stateObject's stale snapshot) is reorg-safe.
	var codeHash []byte
	if sd.stateCache != nil || sd.codeStore != nil {
		if codeHash = sd.codeHashForAddr(tx, addr, txNum); len(codeHash) > 0 {
			if sd.stateCache != nil {
				if cv, ok := sd.stateCache.GetCodeByHash(codeHash); ok {
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
	v, _, err := sd.GetLatest(kv.CodeDomain, tx, addr)
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
//
// txNum stamps the addr→codeHash cache entry (a conservative upper bound for
// unwind invalidation). It is passed in by the caller — never read from the
// shared sd.txNum, which a parallel exec worker on this read path must not
// touch (the exec loop advances it concurrently).
func (sd *SharedDomains) codeHashForAddr(tx kv.TemporalTx, addr []byte, txNum uint64) []byte {
	if len(addr) == 0 {
		return nil
	}
	// In-batch state is authoritative: sd.mem / parent.mem hold this batch's
	// uncommitted account writes, while the addr→codeHash LRU is invalidated only
	// on flush. Route mem-first; the LRU is a committed-state layer that may only
	// answer once mem has missed.
	if v, _, ok := sd.mem.GetLatest(kv.AccountsDomain, addr); ok {
		return decodeAccountCodeHash(v)
	}
	if sd.parent != nil {
		if v, _, ok := sd.parent.mem.GetLatest(kv.AccountsDomain, addr); ok {
			return decodeAccountCodeHash(v)
		}
	}

	// Below mem: the addr → codeHash LRU caches committed state
	// (flush-invalidated). The zero-hash sentinel means "no code / missing
	// account" (negative cache).
	if sd.stateCache != nil {
		if h, ok := sd.stateCache.GetAddrCodeHash(addr); ok {
			if h == ([32]byte{}) {
				return nil
			}
			return h[:]
		}
	}

	// Resolve from the committed layers (stateCache → MDBX/files) and populate
	// the LRU. mem is intentionally not consulted here — it was checked above.
	resolve := func() []byte {
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
		// repeat lookups skip the whole resolve() chain. txNum is a
		// conservative upper bound (>= the resolved account's write txNum), so
		// the mapping drops on any unwind that reverts that account.
		sd.stateCache.PutAddrCodeHash(addr, fixed, txNum)
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
	// misparses and leaves CodeHash empty.
	if err := accounts.DeserialiseV3(&acc, enc); err != nil {
		return nil
	}
	if acc.CodeHash.IsEmpty() {
		return nil
	}
	h := acc.CodeHash.Value()
	return h[:]
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

	switch domain {
	case kv.AccountsDomain:
		if err := sd.DomainDelPrefix(kv.StorageDomain, tx, k, txNum); err != nil {
			return err
		}
		if err := sd.DomainDel(kv.CodeDomain, tx, k, txNum, nil); err != nil {
			return err
		}
		// State cache is refreshed on flush only — see DomainPut. The flush
		// callback handles the empty-value (delete) case for accounts, code
		// and the addr→codeHash mapping.
		// AccountsDomain — apply-side. Serialize against swap window.
		sd.changesetMu.Lock()
		defer sd.changesetMu.Unlock()
		return sd.mem.DomainDel(kv.AccountsDomain, ks, txNum, prevVal)
	case kv.StorageDomain:
		// State cache refreshed on flush only — see DomainPut.
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		// State cache refreshed on flush only — see DomainPut.
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
