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
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
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

// domainVisibleEndMemo caches DomainVisibleEnd per domain for one view at a time.
// Its sequence counter keeps lock-free reads coherent across view changes.
type domainVisibleEndMemo struct {
	ends   [kv.DomainLen]atomic.Uint64
	mu     sync.Mutex
	seq    atomic.Uint64
	viewID atomic.Uint64
	state  atomic.Uint32
}

// state packs two bits per domain into one word so a single atomic load
// returns a consistent (loaded, ok) pair: loadedBit says ends[domain] is
// memoized, okBit is the memoized ok answer of DomainVisibleEnd. The array
// size asserts at compile time that both halves fit in uint32.
var _ [32 - 2*int(kv.DomainLen)]struct{}

func visibleEndBits(domain kv.Domain) (loadedBit, okBit uint32) {
	loadedBit = uint32(1) << uint32(domain)
	return loadedBit, loadedBit << uint32(kv.DomainLen)
}

func (m *domainVisibleEndMemo) get(tx kv.TemporalTx, domain kv.Domain) (uint64, bool) {
	viewID := tx.ViewID()
	loadedBit, okBit := visibleEndBits(domain)
	seq := m.seq.Load()
	if seq&1 == 0 && m.viewID.Load() == viewID {
		if state := m.state.Load(); state&loadedBit != 0 {
			end := m.ends[domain].Load()
			if m.seq.Load() == seq {
				return end, state&okBit != 0
			}
		}
	}
	return m.load(tx, domain, viewID, loadedBit, okBit)
}

func (m *domainVisibleEndMemo) load(tx kv.TemporalTx, domain kv.Domain, viewID uint64, loadedBit, okBit uint32) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cachedViewID := m.viewID.Load()
	state := m.state.Load()
	if cachedViewID == viewID && state&loadedBit != 0 {
		return m.ends[domain].Load(), state&okBit != 0
	}

	m.seq.Add(1)
	defer m.seq.Add(1)

	if cachedViewID != viewID {
		state = 0
		m.viewID.Store(viewID)
	}
	end, ok := tx.Debug().DomainVisibleEnd(domain)
	m.ends[domain].Store(end)
	state |= loadedBit
	if ok {
		state |= okBit
	}
	m.state.Store(state)
	return end, ok
}

// reset takes mu so an in-flight load can't re-store pre-reset bits.
func (m *domainVisibleEndMemo) reset() {
	m.mu.Lock()
	m.seq.Add(1)
	m.state.Store(0)
	m.seq.Add(1)
	m.mu.Unlock()
}

func (sd *SharedDomains) domainVisibleEnd(tx kv.TemporalTx, domain kv.Domain) (uint64, bool) {
	if _, ok := tx.(kv.TemporalRwTx); ok {
		return sd.visibleEnds.get(tx, domain)
	}
	return tx.Debug().DomainVisibleEnd(domain)
}

// sdFrontier adapts one (SharedDomains, tx) pair to cache.Frontier: writable
// txs go through the SD's flush-coherent memo, read-only txs use their own
// tx-local memo.
type sdFrontier struct {
	sd *SharedDomains
	tx kv.TemporalTx
}

func (f sdFrontier) DomainVisibleEnd(domain kv.Domain) (uint64, bool) {
	return f.sd.domainVisibleEnd(f.tx, domain)
}

// cacheGenerationTx unwraps table overlays because their sequence metadata
// belongs to the overlay, while cache fills read temporal domains from the
// backing transaction.
func cacheGenerationTx(tx kv.TemporalTx) kv.TemporalTx {
	for tx != nil {
		wrapper, ok := tx.(interface{ UnderlyingTx() kv.TemporalTx })
		if !ok {
			return tx
		}
		tx = wrapper.UnderlyingTx()
	}
	return nil
}

// cacheFrontierFor binds fill authority to the transaction's durable state
// version. StateCache admits it only while that version is current.
func (sd *SharedDomains) cacheFrontierFor(tx kv.TemporalTx) cache.Frontier {
	generationTx := cacheGenerationTx(tx)
	if generationTx == nil {
		return nil
	}
	stateVersion := sd.baseStateVersion
	_, txWritable := generationTx.(kv.TemporalRwTx)
	// A write transaction's ViewID is the snapshot ID it will create. After
	// commit, a new read transaction can have that ID but a newer state version.
	useBaseStateVersion := generationTx.ViewID() == sd.baseViewID && txWritable == sd.baseTxWritable
	if !useBaseStateVersion {
		var err error
		stateVersion, err = rawdb.GetStateVersion(generationTx)
		if err != nil {
			return nil
		}
	}
	return cache.FrontierWithStateVersion(sdFrontier{sd: sd, tx: tx}, stateVersion)
}

// cacheViewFor binds the shared state cache to tx's read view. Boxing the
// frontier allocates, so per-read paths hold the view in their getter instead
// of rebuilding it per call.
func (sd *SharedDomains) cacheViewFor(tx kv.TemporalTx) cache.ReadView {
	if sd.stateCache == nil {
		return cache.ReadView{}
	}
	return sd.stateCache.View(sd.cacheFrontierFor(tx))
}

// cacheReader is a frontier-less view: admission-gated fills are disabled,
// content-addressed fills still work. Safe on a nil cache.
func (sd *SharedDomains) cacheReader() cache.ReadView { return sd.stateCache.View(nil) }

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

	baseViewID       uint64
	baseTxWritable   bool
	baseStateVersion uint64

	txNum       uint64
	currentStep kv.Step
	// disableInlineTouchKey skips the inline TouchKey in DomainPut/DomainDel when
	// the commitment calculator owns the Updates buffer and feeds touches itself.
	disableInlineTouchKey bool
	mem                   kv.TemporalMemBatch
	metrics               kvmetrics.DomainMetrics
	nonExecMetrics        kvmetrics.DomainMetrics

	commitmentNanos atomic.Int64

	// blockOverlay is an in-memory overlay for block-level metadata writes,
	// flushed atomically alongside domain state via Flush(), letting execution
	// run without holding an RwTx. Atomic because concurrent readers may call
	// BlockOverlay() while Close() nils the pointer.
	blockOverlay atomic.Pointer[membatchwithdb.MemoryMutation]

	// parent is an optional parent SD for read-through chaining: domain reads
	// that miss the local mem batch fall through to the parent's mem batch
	// before the underlying tx.
	parent *SharedDomains

	// stateCache is an optional cache for state data (accounts, storage, code);
	// cacheApplier is its authoritative writer handle (commit/unwind only).
	stateCache   *cache.StateCache
	cacheApplier cache.Applier
	cacheUnwind  cacheUnwindState

	// Backing frontiers stay fixed while writes and staged unwinds remain in
	// mem; both reach the transaction during flush, which resets the memo.
	visibleEnds domainVisibleEndMemo

	// codeStore is the optional two-tier (in-mem + MDBX) codehash-keyed code
	// cache, reached via StateGetter so an addr-keyed reader can serve a
	// code-by-hash read with the application's authoritative codehash.
	codeStore *cache.CodeStore

	// changesetMu serializes the exec loop's install of a block's changeset
	// accumulator against the calculator's swap of the commitment writer's diff.
	// Writers other than commitment are never redirected, so DomainPut and
	// DomainDel do not take it — see SwapCommitmentDiffLocked.
	changesetMu sync.Mutex

	// branchCache is the aggregator-scope commitment-branch cache. It sits
	// behind sd.mem and sd.parent.mem in the read chain, so writers' in-flight
	// bytes always mask it. May be nil.
	branchCache *commitment.BranchCache

	// collector is the process-level KV-read metrics collector. May be nil.
	collector *kvmetrics.Collector

	// reqMetrics is an optional request-scoped accumulator for a single-goroutine
	// owner (an RPC handler) reading through the plain AsStateGetter; flushed to
	// the collector at Close. Never set on exec SDs, whose workers pass their own.
	reqMetrics *kvmetrics.DomainMetrics
	reqSource  kvmetrics.Source

	// adaptivePinController decides which contracts get pinned based on observed
	// miss pressure. nil when branchCache is nil or the adaptive layer is disabled.
	adaptivePinController *commitment.AdaptivePinController
}

// cacheUnwindState records the lowest boundary that the next durable cache
// publication must invalidate. It is separate from mem-batch changesets
// because an unwind without changesets must still revoke cache entries;
// merging states keeps the lowest boundary to cover every discarded range.
type cacheUnwindState struct {
	toTxNum uint64
	pending bool
}

// PickTrieVariant returns the commitment trie variant selected by the
// process-wide statecfg experimental-commitment flags. Callers building a
// commitment.TrieConfig inline should use this so the flags are honored
// consistently rather than relying on the trie constructor's fallback.
func PickTrieVariant() commitment.TrieVariant {
	if statecfg.ExperimentalParallelCommitment {
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

	generationTx := cacheGenerationTx(tx)
	if generationTx == nil {
		return nil, errors.New("state version transaction is nil")
	}
	stateVersion, err := rawdb.GetStateVersion(generationTx)
	if err != nil {
		return nil, fmt.Errorf("read base state version: %w", err)
	}
	_, baseTxWritable := generationTx.(kv.TemporalRwTx)
	sd := &SharedDomains{
		logger:           logger,
		metrics:          kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
		nonExecMetrics:   kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}},
		stepSize:         tx.Debug().StepSize(),
		baseViewID:       generationTx.ViewID(),
		baseTxWritable:   baseTxWritable,
		baseStateVersion: stateVersion,
	}

	if o.mem != nil {
		sd.mem = o.mem
	} else {
		sd.mem = tx.Debug().NewMemBatch(&sd.metrics)
	}
	// Duck-typed BranchCacheProvider lookup: db/state already imports execctx,
	// so importing it here to get the aggregator-scope branch cache would cycle.
	var branchCache *commitment.BranchCache
	if p, ok := tx.AggTx().(commitment.BranchCacheProvider); ok && o.useSharedBranchCache {
		branchCache = p.BranchCache()
	}
	sd.branchCache = branchCache
	if p, ok := tx.AggTx().(kvmetrics.MetricsCollectorProvider); ok {
		sd.collector = p.MetricsCollector()
	}
	sd.sdCtx = commitmentdb.NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, tx.Debug().Dirs().Tmp, trieCfg)

	// Aggregator-scoped (co-located with branchCache) so pin residency ages by
	// block-access recency across all SharedDomains, not per-SD.
	if p, ok := tx.AggTx().(commitment.AdaptivePinControllerProvider); ok && o.useSharedBranchCache {
		sd.adaptivePinController = p.AdaptivePinController()
	}

	if o.paraTrieDB != nil {
		sd.EnableParaTrieDB(o.paraTrieDB)
	}

	_, blockNum, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return sd, err
	}

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

// AsPutDelWithDiff is AsPutDel, but commitment-domain writes route through
// diff explicitly rather than the shared SetChangesetAccumulator target — see
// commitmentDiffPutDel.
func (sd *SharedDomains) AsPutDelWithDiff(tx kv.TemporalTx, diff *kv.DomainDiff) kv.TemporalPutDel {
	return &commitmentDiffPutDel{temporalPutDel{sd, tx}, diff}
}

type commitmentDiffPutDel struct {
	temporalPutDel
	diff *kv.DomainDiff
}

func (p *commitmentDiffPutDel) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte) error {
	if domain == kv.CommitmentDomain {
		return p.sd.DomainPutCommitmentDiff(p.tx, k, v, txNum, prevVal, p.diff)
	}
	return p.temporalPutDel.DomainPut(domain, k, v, txNum, prevVal)
}

func (p *commitmentDiffPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte) error {
	if domain == kv.CommitmentDomain {
		panic("commitmentDiffPutDel.DomainDel called for kv.CommitmentDomain: branch removal must go through DomainPut with an empty, non-nil value so it routes through the explicit diff")
	}
	return p.temporalPutDel.DomainDel(domain, k, txNum, prevVal)
}

func (p *commitmentDiffPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	if domain == kv.CommitmentDomain {
		panic("commitmentDiffPutDel.DomainDelPrefix called for kv.CommitmentDomain: not supported by the explicit-diff routing path")
	}
	return p.temporalPutDel.DomainDelPrefix(domain, prefix, txNum)
}

// commitmentBranchDiffWriter is implemented by TemporalMemBatch to route a
// commitment-domain branch write into an explicit diff.
type commitmentBranchDiffWriter interface {
	PutCommitmentBranchDiff(k string, v []byte, txNum uint64, preval []byte, diff *kv.DomainDiff) error
}

// resolvePrevVal touches the commitment key and resolves prevVal from the
// latest domain value when the caller did not supply one.
func (sd *SharedDomains) resolvePrevVal(domain kv.Domain, roTx kv.TemporalTx, k []byte, ks string, v, prevVal []byte) ([]byte, error) {
	if !sd.disableInlineTouchKey {
		sd.sdCtx.TouchKey(domain, ks, v)
	}
	if prevVal == nil {
		var err error
		prevVal, _, err = sd.GetLatest(domain, roTx, k)
		if err != nil {
			return nil, err
		}
	}
	return prevVal, nil
}

// DomainPutCommitmentDiff is DomainPut(kv.CommitmentDomain, ...) with an
// explicit diff target instead of whatever SetChangesetAccumulator most
// recently installed on the commitment writer. The commitment domain has
// exactly one writer (the parallel commitment calculator), so this needs no
// lock to stay race-free — see TemporalMemBatch.PutCommitmentBranchDiff.
func (sd *SharedDomains) DomainPutCommitmentDiff(roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte, diff *kv.DomainDiff) error {
	if v == nil {
		return errors.New("DomainPutCommitmentDiff: trying to put nil value, not allowed")
	}
	ks := string(k)
	prevVal, err := sd.resolvePrevVal(kv.CommitmentDomain, roTx, k, ks, v, prevVal)
	if err != nil {
		return err
	}
	if bytes.Equal(prevVal, v) {
		return nil
	}
	return sd.mem.(commitmentBranchDiffWriter).PutCommitmentBranchDiff(ks, v, txNum, prevVal, diff)
}

// changesetSwitcher is implemented by TemporalMemBatch to get/set changesets for deferred writes.
type changesetSwitcher interface {
	// GetChangesetByBlockNum returns the changeset for a given block number and
	// the block hash it is keyed under.
	GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet)
	// GetChangesetByHash returns the changeset saved under (blockNumber, blockHash).
	// Prefer over GetChangesetByBlockNum when both are known: multiple changesets
	// can exist per block number after a fork-bounce reorg.
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
	if other.cacheUnwind.pending {
		// A shared cache was invalidated when the child staged the unwind;
		// otherwise invalidate the parent's cache before it serves merged state.
		if sd.stateCache != other.stateCache {
			sd.cacheApplier.Unwind(other.cacheUnwind.toTxNum)
		}
		sd.stageCacheUnwind(other.cacheUnwind.toTxNum)
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

// FlushPendingUpdates applies the pending deferred commitment update under the
// corresponding block's changeset. Acquires changesetMu itself; the inner swap
// mutates the global accumulator pointer that DomainPut/DomainDel write through.
func (sd *SharedDomains) FlushPendingUpdates(ctx context.Context, tx kv.TemporalTx) error {
	return sd.flushPendingUpdates(ctx, tx, false)
}

// FlushPendingUpdatesLocked is FlushPendingUpdates for callers that already
// hold changesetMu via LockChangesetAccumulator.
func (sd *SharedDomains) FlushPendingUpdatesLocked(ctx context.Context, tx kv.TemporalTx) error {
	return sd.flushPendingUpdates(ctx, tx, true)
}

// FlushPendingUpdatesWithoutChangeset applies the pending deferred commitment
// update as raw branch records, bypassing the changeset accumulator. Used by an
// isolated compute whose branch deltas must not pend into another block's
// changeset; lock-free, so it can run deferred after the accumulator is
// re-attached.
func (sd *SharedDomains) FlushPendingUpdatesWithoutChangeset(tx kv.TemporalTx) error {
	upd := sd.sdCtx.TakePendingUpdate()
	if upd == nil {
		return nil
	}
	defer upd.Clear()
	putBranch := func(prefix, data, prevData []byte) error {
		return sd.DomainPutCommitmentDiff(tx, prefix, data, upd.TxNum, prevData, nil)
	}
	_, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch, upd.Metrics)
	return err
}

func (sd *SharedDomains) flushPendingUpdates(ctx context.Context, tx kv.TemporalTx, lockHeld bool) error {
	upd := sd.sdCtx.TakePendingUpdate()
	if upd == nil {
		return nil
	}
	defer upd.Clear()

	putBranch := func(prefix, data, prevData []byte) error {
		return sd.DomainPut(kv.CommitmentDomain, tx, prefix, data, upd.TxNum, prevData)
	}

	if !lockHeld {
		sd.changesetMu.Lock()
		defer sd.changesetMu.Unlock()
	}

	switcher, ok := sd.mem.(changesetSwitcher)
	if !ok {
		_, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch, upd.Metrics)
		return err
	}

	// Hash-aware lookup disambiguates multiple changesets for the same block
	// number (canonical + fork during a reorg-bounce); falls back to number-only
	// when the update carries no hash.
	var blockHash common.Hash
	var cs *changeset.StateChangeSet
	if upd.BlockHash != (common.Hash{}) {
		blockHash = upd.BlockHash
		cs = switcher.GetChangesetByHash(upd.BlockNum, blockHash)
	} else {
		blockHash, cs = switcher.GetChangesetByBlockNum(upd.BlockNum)
	}
	if cs != nil {
		defer sd.SwapCommitmentDiffLocked(cs)()

		if _, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch, upd.Metrics); err != nil {
			return err
		}

		switcher.SavePastChangesetAccumulator(blockHash, upd.BlockNum, cs)
		return nil
	}

	// No past changeset found — write into whatever is current.
	_, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch, upd.Metrics)
	return err
}

// AsStateGetter returns an execution-aware getter with optimized code reads.
func (sd *SharedDomains) AsStateGetter(tx kv.TemporalTx, opts execctxapi.StateGetterOptions) execctxapi.StateGetter {
	metrics := opts.Metrics()
	if !dbg.KVReadLevelledMetrics {
		metrics = nil
	}
	return &stateGetter{sd: sd, tx: tx, m: metrics, view: sd.cacheViewFor(tx)}
}

// DomainReader is the read-only domain view held by the parallel executor's exec
// flow, so exec-side writes don't compile. Extends membatchwithdb.DomainReader
// with the getter/iterate methods the finalize readers need.
type DomainReader interface {
	membatchwithdb.DomainReader
	AsGetter(tx kv.TemporalTx) execctxapi.StateGetter
	IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte) (cont bool, err error)) error
}

// AsGetter returns a metrics-free execution getter.
func (sd *SharedDomains) AsGetter(tx kv.TemporalTx) execctxapi.StateGetter {
	return sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
}

// AsGetterMetered returns a getter that records reads into the caller's single-owner metrics instance m.
func (sd *SharedDomains) AsGetterMetered(tx kv.TemporalTx, m *kvmetrics.DomainMetrics) execctxapi.StateGetter {
	return sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}.WithMetrics(m))
}

// MergeMetrics hands a boundary producer's accumulator to the per-batch sd.metrics,
// the process-level collector, and (unless the source is exec) sd.nonExecMetrics.
// For low-frequency boundary producers only: the collector send may block briefly
// on a full buffer. Ownership of wm transfers to the collector.
func (sd *SharedDomains) MergeMetrics(source kvmetrics.Source, wm *kvmetrics.DomainMetrics) {
	sd.metrics.Merge(wm)
	if dbg.KVReadLevelledMetrics && source != kvmetrics.SourceExec {
		sd.nonExecMetrics.Merge(wm)
	}
	sd.collector.Send(source, wm)
}

// LogMergeMetrics folds wm into the per-batch sd.metrics aggregate only, without
// touching the collector, so it can never block on a full collector buffer. wm
// is read, not retained.
func (sd *SharedDomains) LogMergeMetrics(wm *kvmetrics.DomainMetrics) {
	sd.metrics.Merge(wm)
}

// Collector returns the process-level KV-read metrics collector (may be nil).
func (sd *SharedDomains) Collector() *kvmetrics.Collector {
	return sd.collector
}

// StartRequestMetrics enables request-scoped metering for plain AsStateGetter
// reads, flushed to the collector at Close. No-op when read metrics are off or
// there is no collector. The accumulator is single-owner: do NOT use on a
// SharedDomains shared across goroutines.
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
// swap+use+restore sequence on the global accumulator pointer. Holders must
// pair Lock with Unlock and keep the critical section short. Inside the locked
// window use the *Locked variants; the public Set/Get re-acquire the same Mutex
// and would self-deadlock.
func (sd *SharedDomains) LockChangesetAccumulator()   { sd.changesetMu.Lock() }
func (sd *SharedDomains) UnlockChangesetAccumulator() { sd.changesetMu.Unlock() }

// SetChangesetAccumulator installs acc as the current target for
// DomainPut/DomainDel diff recording, locking changesetMu internally.
func (sd *SharedDomains) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	sd.changesetMu.Lock()
	sd.setChangesetAccumulatorLocked(acc)
	sd.changesetMu.Unlock()
}

func (sd *SharedDomains) setChangesetAccumulatorLocked(acc *changeset.StateChangeSet) {
	sd.mem.(accHolder).SetChangesetAccumulator(acc)
}

// GetChangesetAccumulator returns the currently-installed live changeset
// accumulator, or nil. Locks changesetMu internally — must NOT be called while
// already holding the lock.
func (sd *SharedDomains) GetChangesetAccumulator() *changeset.StateChangeSet {
	sd.changesetMu.Lock()
	defer sd.changesetMu.Unlock()
	return sd.getChangesetAccumulatorLocked()
}

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

// commitmentDiffSwapper must be implemented by every mem batch behind
// SharedDomains: the only alternative is redirecting all domain writers,
// which is unsafe now that DomainPut takes no lock.
type commitmentDiffSwapper interface {
	SetCommitmentDiff(acc *changeset.StateChangeSet)
	SetCommitmentDiffRaw(d *kv.DomainDiff)
	CommitmentDiff() *kv.DomainDiff
}

// SwapCommitmentDiffLocked points the commitment writer's diff at acc and
// returns a func restoring the previous one. Every other domain writer is left
// alone, so apply-side writes need no lock. Callers must hold changesetMu.
// Used only by flushPendingUpdates's hash-aware routing — a call with a known
// target diff should use DomainPutCommitmentDiff instead, which needs no lock.
func (sd *SharedDomains) SwapCommitmentDiffLocked(acc *changeset.StateChangeSet) (restore func()) {
	h := sd.mem.(commitmentDiffSwapper)
	prev := h.CommitmentDiff()
	h.SetCommitmentDiff(acc)
	return func() { h.SetCommitmentDiffRaw(prev) }
}

// GetChangesetByBlockNum returns the saved changeset for a block number (and
// the hash it was saved under), or (zero hash, nil). Ambiguous when multiple
// changesets exist for the block number after a reorg-bounce; prefer
// GetChangesetByHash when the hash is available.
func (sd *SharedDomains) GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet) {
	if h, ok := sd.mem.(changesetSwitcher); ok {
		return h.GetChangesetByBlockNum(blockNumber)
	}
	return common.Hash{}, nil
}

// GetChangesetByHash returns the saved changeset for an exact (blockNumber,
// blockHash) key, or nil. Prefer over GetChangesetByBlockNum when both are
// known: multiple changesets can exist per block number after a reorg-bounce.
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
	// A fork-validation SD has an empty mem batch, so the diffsets of the
	// canonical blocks it must unwind live only in the parent's accumulator.
	// Without this an unwind silently runs with no unwind set.
	if sd.parent != nil {
		return sd.parent.GetDiffset(tx, blockHash, blockNumber)
	}
	return d, ok, err
}

// Unwind drops [txNumUnwindTo, ∞)
func (sd *SharedDomains) Unwind(txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) {
	sd.mem.Unwind(txNumUnwindTo, changeset)
	// Epoch-aware BranchCache unwind: bumping the epoch and lowering the floor
	// (O(1)) drops every branch in the rolled-back window lazily on next Get,
	// covering read-pop/preload entries the changeset-gated Invalidate misses.
	// The explicit Invalidate is a fast path for keys known dead right now.
	if sd.branchCache != nil {
		sd.branchCache.Unwind(txNumUnwindTo)
		if changeset != nil {
			for _, diff := range changeset[kv.CommitmentDomain] {
				sd.branchCache.Invalidate([]byte(diff.Key))
			}
		}
	}
	// Diffset-free and unconditional: changesets are not generated below the
	// reorg window. Commit repeats this at the durable state-version boundary.
	sd.cacheApplier.Unwind(txNumUnwindTo)
	sd.stageCacheUnwind(txNumUnwindTo)
}

// stageCacheUnwind retains the lowest boundary so every staged discarded
// range is covered by the next durable cache publication.
func (sd *SharedDomains) stageCacheUnwind(txNumUnwindTo uint64) {
	if !sd.cacheUnwind.pending || txNumUnwindTo < sd.cacheUnwind.toTxNum {
		sd.cacheUnwind.toTxNum = txNumUnwindTo
	}
	sd.cacheUnwind.pending = true
}

func (sd *SharedDomains) GetMemBatch() kv.TemporalMemBatch { return sd.mem }
func (sd *SharedDomains) SetInMemHistoryReads(v bool)      { sd.mem.SetInMemHistoryReads(v) }
func (sd *SharedDomains) InMemHistoryReads() bool          { return sd.mem.InMemHistoryReads() }

// SetParent sets a parent SD for read-through domain chaining. Domain reads
// that miss in the local mem batch will check the parent's mem batch before
// falling through to the underlying tx/aggregator.
func (sd *SharedDomains) SetParent(parent *SharedDomains) { sd.parent = parent }

// BlockOverlay returns the in-memory block-level metadata overlay, usable as a
// kv.RwTx to route rawdb writes through it instead of a real RwTx. Returns nil
// if none was initialized via InitBlockOverlay.
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

// SetStateCache hands this SD the process-global state cache to manage:
// Commit applies committed updates after a successful DB commit, Unwind
// invalidates them, and the SD's reads populate it through admission-gated
// fills. No-op when USE_STATE_CACHE is off or the cache is nil.
func (sd *SharedDomains) SetStateCache(stateCache *cache.StateCache) {
	if !dbg.UseStateCache || stateCache == nil {
		return
	}
	sd.BindStateCache(stateCache)
}

// BindStateCache attaches a cache unconditionally, bypassing the USE_STATE_CACHE
// check in SetStateCache. Tests use it so they always exercise the cache without
// mutating the process-global flag, which would race t.Parallel tests.
func (sd *SharedDomains) BindStateCache(stateCache *cache.StateCache) {
	sd.stateCache = stateCache
	sd.cacheApplier = stateCache.Applier()
	sd.cacheApplier.Initialize(sd.baseStateVersion)
}

// GuardAggregatorForCache forbids visibility lowering on db's aggregator when
// sc is a fill-enabled StateCache, because fill admission relies on view
// frontiers never decreasing. Duck-typed but load-bearing: a db that cannot
// produce its aggregator panics rather than silently drop the guard. A nil or
// apply-only cache needs no guard.
func GuardAggregatorForCache(db any, sc *cache.StateCache) {
	if sc == nil || !sc.FillsEnabled() {
		return
	}
	h, ok := db.(interface{ Agg() any })
	if !ok {
		panic(fmt.Sprintf("assert: fill-enabled StateCache wired over %T, which cannot produce its aggregator — the visibility-lowering guard would be silently dropped", db))
	}
	agg := h.Agg()
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
// No-op when the cache is disabled.
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
	// Exec-only mode never advances commitment, so there is no unfrozen step edge to
	// trigger commitment work on.
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

// Flush writes the in-memory batch into tx without committing. It deliberately
// does NOT touch the caches: the caller may still roll back, so Flush must not
// warm a cache with state that could vanish. An SD with an attached state cache
// must therefore route every flush through Commit — Flush neither applies nor
// invalidates, and Commit only collects updates from its own flush, so an
// earlier plain Flush's keys would never be applied. Cache-less callers may
// Flush and commit themselves.
func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	return sd.flushMem(ctx, tx)
}

func (sd *SharedDomains) flushMem(ctx context.Context, tx kv.RwTx, opts ...kv.FlushOption) error {
	defer sd.visibleEnds.reset()
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

type branchCacheUpdate struct {
	key  []byte
	val  []byte
	step kv.Step
	txN  uint64
}

// ProjectedStateVersion returns the durable state version produced by the next
// successful Commit.
func (sd *SharedDomains) ProjectedStateVersion() (uint64, error) {
	if sd.baseStateVersion == math.MaxUint64 {
		return 0, errors.New("state version overflow")
	}
	return sd.baseStateVersion + 1, nil
}

func (sd *SharedDomains) stateVersionsForCommit(tx kv.Tx) (source, target uint64, err error) {
	target, err = sd.ProjectedStateVersion()
	if err != nil {
		return 0, 0, err
	}
	current, err := rawdb.GetStateVersion(tx)
	if err != nil {
		return 0, 0, fmt.Errorf("read state version before flush: %w", err)
	}
	if current != sd.baseStateVersion {
		return 0, 0, fmt.Errorf("state version changed since SharedDomains was created: base=%d current=%d", sd.baseStateVersion, current)
	}
	return sd.baseStateVersion, target, nil
}

func requireStateVersion(tx kv.Tx, expected uint64) error {
	actual, err := rawdb.GetStateVersion(tx)
	if err != nil {
		return fmt.Errorf("read state version before commit: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("unexpected state version after flush: expected=%d actual=%d", expected, actual)
	}
	return nil
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
// committed here. Commit is terminal for this SharedDomains value; continue
// with a new one on a fresh transaction. The domain flush advances
// PlainStateVersion exactly once; Commit verifies both its starting version and
// the version it will publish.
// Validation callbacks run after the domain flush and before the MDBX commit.
// A callback error leaves the transaction uncommitted for the caller to roll back.
func (sd *SharedDomains) Commit(ctx context.Context, tx kv.RwTx, validate ...func(tx kv.RwTx) error) error {
	defer mxFlushTook.ObserveDuration(time.Now())
	sourceStateVersion, committedStateVersion, err := sd.stateVersionsForCommit(tx)
	if err != nil {
		return err
	}

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
		if err := requireStateVersion(tx, committedStateVersion); err != nil {
			return err
		}
		return tx.Commit()
	}

	// Stash every cache-bound domain tuple during the flush and publish it only
	// after the commit succeeds, so the cache never advances ahead of durable
	// MDBX state. Borrows the batch's buffers rather than copying the flush.
	var pendingBranches []branchCacheUpdate
	var pendingState []cache.StateUpdate
	stash := func(domain kv.Domain) kv.FlushOption {
		return kv.WithFlushCallback(domain, func(k []byte, v []byte, step kv.Step, txNum uint64) {
			if domain == kv.CommitmentDomain {
				pendingBranches = append(pendingBranches, branchCacheUpdate{
					key:  k,
					val:  v,
					step: step,
					txN:  txNum,
				})
				return
			}
			pendingState = append(pendingState, cache.StateUpdate{
				Domain: domain,
				Key:    k,
				Value:  v,
				TxNum:  txNum,
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
	// The code-store MDBX write is deferred to after flushMem — an in-callback
	// tx.Put interleaves with the in-progress domain flush and corrupts it.
	var codeStoreWrites [][2][]byte
	if sd.stateCache != nil || sd.codeStore != nil {
		opts = append(opts, kv.WithFlushCallback(kv.CodeDomain, func(k []byte, v []byte, step kv.Step, txNum uint64) {
			var codeHash []byte
			if sd.codeStore != nil && len(v) > 0 {
				codeHash = crypto.Keccak256(v)
				codeStoreWrites = append(codeStoreWrites, [2][]byte{codeHash, v})
			}
			if sd.stateCache != nil {
				pendingState = append(pendingState, cache.StateUpdate{
					Domain:   kv.CodeDomain,
					Key:      k,
					Value:    v,
					CodeHash: codeHash,
					TxNum:    txNum,
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
	// Runs on the in-flight (pre-Commit) tx so the preload sees the just-flushed bytes.
	if sd.adaptivePinController != nil {
		if ttx, ok := tx.(kv.TemporalTx); ok {
			provider := func(contractHash []byte) map[string][]byte {
				m := map[string][]byte{}
				c, cerr := ttx.CursorDupSort(kv.TblCommitmentVals)
				if cerr != nil {
					return m
				}
				defer c.Close()
				evenFrom, evenTo, oddFrom, oddTo := commitment.ContractTrunkKeyRanges(commitment.ContractNibbles(contractHash))
				// Bound the scan by the per-contract pin ceiling — gathering more
				// than the preload can pin is waste. A nil `to` means scan to the
				// range's natural end.
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
						m[string(k)] = bytes.Clone(v[8:])
						if scanned += len(k) + len(v); scanned >= budget {
							return
						}
					}
				}
				scan(evenFrom, evenTo)
				scan(oddFrom, oddTo)
				return m
			}
			sd.adaptivePinController.OnBlockComplete(ctx, sd.txNum, pinBranchResolver(ttx), provider)
		}
	}
	if err := requireStateVersion(tx, committedStateVersion); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for i := range pendingBranches {
		u := &pendingBranches[i]
		if len(u.val) == 0 {
			sd.branchCache.Invalidate(u.key)
		} else {
			sd.branchCache.Put(u.key, u.val, uint64(u.step), u.txN)
		}
	}
	if sd.stateCache != nil {
		if sd.cacheUnwind.pending {
			sd.cacheApplier.PublishUnwind(sourceStateVersion, committedStateVersion, sd.cacheUnwind.toTxNum, pendingState)
		} else {
			sd.cacheApplier.Publish(sourceStateVersion, committedStateVersion, pendingState)
		}
		sd.cacheUnwind = cacheUnwindState{}
	}
	return nil
}

// TemporalDomain satisfaction. Direct reads use request metrics when configured.
func (sd *SharedDomains) GetLatest(domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	return sd.getLatest(domain, tx, k, nil, time.Time{}, kv.NoStepBound, sd.cacheReader(), getLatestOptions{})
}

// GetLatestFromMemory returns the latest in-memory (sd.mem) value for key,
// without consulting the domain files.
func (sd *SharedDomains) GetLatestFromMemory(domain kv.Domain, key []byte) (v []byte, maxStep kv.Step, ok bool) {
	v, _, maxStep, ok = sd.latestFromMem(domain, key)
	return v, maxStep, ok
}

// GetLatestContext is GetLatest with per-worker metrics carried on ctx: the
// commitment worker's lock-free accumulator (a nil ctx-value collects nothing,
// so concurrent workers neither share metrics state nor take a lock).
func (sd *SharedDomains) GetLatestContext(ctx context.Context, domain kv.Domain, tx kv.TemporalTx, k []byte) (v []byte, step kv.Step, err error) {
	var wm kv.GetLatestMetrics
	if m := kvmetrics.MetricsFromContext(ctx); m != nil {
		wm = m
	}
	return sd.getLatest(domain, tx, k, wm, time.Time{}, kv.NoStepBound, sd.cacheReader(), getLatestOptions{})
}

// servableUnderBound gates a value against an in-flight unwind's per-key
// maxStep. Callers convert their unit first: StateCache stamps txNums, while
// mem batches and BranchCache already use step indices.
func servableUnderBound(cStep, maxStep kv.Step) bool {
	return cStep <= maxStep
}

// latestFromMem carries a child's staged-unwind bound into its parent lookup.
// A parent value above that bound belongs to the discarded fork and is skipped.
func (sd *SharedDomains) latestFromMem(domain kv.Domain, key []byte) (v []byte, step, maxStep kv.Step, ok bool) {
	maxStep = kv.NoStepBound
	v, step, ok = sd.mem.GetLatest(domain, key)
	if ok {
		return v, step, maxStep, true
	}
	maxStep = min(maxStep, step)

	if sd.parent == nil {
		return nil, 0, maxStep, false
	}
	v, step, ok = sd.parent.mem.GetLatest(domain, key)
	if ok {
		if servableUnderBound(step, maxStep) {
			return v, step, maxStep, true
		}
		return nil, 0, maxStep, false
	}
	return nil, 0, min(maxStep, step), false
}

type getLatestOptions struct {
	codeHash []byte
	buf      []byte
}

func (opts getLatestOptions) withCodeHash(codeHash []byte) getLatestOptions {
	opts.codeHash = codeHash
	return opts
}

// getLatest is the read implementation. wm is the caller's lock-free
// per-worker metrics accumulator (nil disables metrics); no global metrics lock
// is taken on this hot path.
func (sd *SharedDomains) getLatest(domain kv.Domain, tx kv.TemporalTx, k []byte, wm kv.GetLatestMetrics, start time.Time, stepBound kv.Step, view cache.ReadView, opts getLatestOptions) (v []byte, step kv.Step, err error) {
	if tx == nil {
		return nil, 0, errors.New("sd.GetLatest: unexpected nil tx")
	}
	if dbg.KVReadLevelledMetrics {
		if start.IsZero() {
			start = time.Now()
		}
		// Plain AsStateGetter reads (wm == nil) on a request-scoped SD fold into
		// the request accumulator. Exec workers (wm != nil) never touch reqMetrics.
		if wm == nil && sd.reqMetrics != nil {
			wm = sd.reqMetrics
		}
	} else {
		wm = nil
	}
	// Mem batches hold the current transaction's uncommitted state, so a hit
	// needs no shared-cache fill. Parent hits also obey any bound from the child.
	v, step, stagedMaxStep, ok := sd.latestFromMem(domain, k)
	maxStep := min(stagedMaxStep, stepBound)
	if ok && servableUnderBound(step, maxStep) {
		if wm != nil {
			wm.UpdateCacheReads(domain, start)
		}
		return v, step, nil
	}
	// stateCache holds committed values shared across domain readers.
	if sd.stateCache != nil {
		v, cTxNum, ok := view.GetWithTxNum(domain, k)
		// The cache stamps txNums — divide to get the step the entry reflects.
		// A negative uses the last txNum included by its read-view frontier, not
		// the step of a deletion.
		cStep := kv.Step(cTxNum / sd.StepSize())
		if ok && !servableUnderBound(cStep, maxStep) {
			ok = false
		}
		if wm != nil {
			if ok {
				wm.UpdateStateCacheHit(domain, start)
			} else {
				wm.UpdateStateCacheMiss(domain)
			}
		}
		if ok {
			// Skip the divergence assert while a mem overlay bounds this key (in-flight
			// unwind): MDBX still holds not-yet-deleted dying rows inside the bound, so
			// the authoritative read could return dead-fork bytes and blame the cache.
			if dbg.AssertStateCache && maxStep == kv.NoStepBound {
				var vDB []byte
				var err error
				getOpts := kv.GetLatestOptions{}
				if wm != nil {
					getOpts = getOpts.WithMetrics(wm, start)
				}
				vDB, _, err = tx.GetLatest(domain, k, getOpts)
				if err != nil {
					return nil, 0, fmt.Errorf("AssertStateCache: authoritative read failed: %w", err)
				}
				if !bytes.Equal(v, vDB) {
					panic(fmt.Sprintf("stateCache divergence: domain=%v key=%x cached=%x db=%x txNum=%d",
						domain, k, v, vDB, sd.txNum))
				}
			}
			return v, cStep, nil
		}
	}

	// branchCache serves CommitmentDomain only. Snapshot-isolated readers must
	// disable it: concurrent commits can advance it beyond their tx view.
	useBranchCache := domain == kv.CommitmentDomain && sd.branchCache != nil
	if useBranchCache {
		if cv, cStepU64, ok := sd.branchCache.Get(k); ok {
			// Get returns the on-disk step index directly — do NOT divide by StepSize.
			cStep := kv.Step(cStepU64)
			if servableUnderBound(cStep, maxStep) {
				return cv, cStep, nil
			}
		}
	}

	getOpts := kv.GetLatestOptions{}
	if wm != nil {
		getOpts = getOpts.WithMetrics(wm, start)
	}
	if maxStep != kv.NoStepBound {
		getOpts = getOpts.WithMaxStep(maxStep)
	}
	if useBranchCache {
		getOpts = getOpts.WithBranchCache()
	}
	willFill := maxStep == kv.NoStepBound && sd.stateCache != nil && sd.stateCache.Caches(domain)
	fillsCode := willFill && len(opts.codeHash) == len(common.Hash{})
	if fillsCode {
		getOpts = getOpts.WithBuf(opts.buf)
	}

	v, step, err = tx.GetLatest(domain, k, getOpts)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}

	if willFill {
		readTxNum := step.LastTxNum(sd.StepSize())
		fillView := view
		if fillView.NeedsFrontier() {
			// Frontier-less views resolve the frontier on the miss path, where the
			// binding cost is amortized by the backing read. Stale views skip it.
			fillView = fillView.WithFrontier(sd.cacheFrontierFor(tx))
		}
		if fillsCode {
			v = fillView.FillCode(k, v, opts.codeHash, readTxNum)
		} else {
			fillView.Fill(domain, k, v, readTxNum)
		}
	}
	return v, step, nil
}

// GetCodeSize returns the length of the contract code at addr, probing a
// size-only cache before the full bytes path so repeated EXTCODESIZE /
// EXTCODEHASH avoid the file-accessor + decompression cost.
//
// READ-ONLY contract (as GetCode): the codeHash fast path resolves from the
// account record, so it must not feed a DomainPut prevVal. The fast path is
// purely additive — when it cannot answer, this delegates to the authoritative
// addr-keyed GetLatest(CodeDomain, addr) and never short-circuits to
// (0, false, nil) on account resolution alone (which would break system-contract
// predeploys whose AccountsDomain record is empty at block boundary).
func (sd *SharedDomains) GetCodeSize(tx kv.TemporalTx, addr []byte, txNum uint64) (int, bool, error) {
	return sd.getCodeSize(tx, sd.cacheReader(), addr, txNum)
}

func (sd *SharedDomains) getCodeSize(tx kv.TemporalTx, view cache.ReadView, addr []byte, txNum uint64) (int, bool, error) {
	if tx == nil {
		return 0, false, errors.New("sd.GetCodeSize: unexpected nil tx")
	}

	// Fast path: resolve codeHash from the account cache and answer from the
	// size cache without loading bytes.
	var codeHash []byte
	if sd.stateCache != nil {
		if codeHash = sd.codeHashForAddr(tx, view, addr, txNum); len(codeHash) > 0 {
			if size, ok := view.GetCodeSizeByHash(codeHash); ok {
				return size, true, nil
			}
			if cv, ok := view.GetCodeByHash(codeHash); ok {
				// txNum is a conservative upper bound so the size drops on any
				// unwind that drops the code.
				view.FillCodeSize(codeHash, len(cv), txNum)
				return len(cv), true, nil
			}
		}
	}

	size, found, answered, err := sd.getLatestValSize(kv.CodeDomain, tx, addr, view)
	if err != nil {
		return 0, false, err
	}
	if answered {
		if !found || size == 0 {
			return 0, false, nil
		}
		if len(codeHash) == len(common.Hash{}) {
			view.FillCodeSize(codeHash, size, txNum)
		}
		return size, true, nil
	}

	v, _, err := sd.getLatest(kv.CodeDomain, tx, addr, nil, time.Time{}, kv.NoStepBound, view, getLatestOptions{}.withCodeHash(codeHash))
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	return len(v), true, nil
}

func (sd *SharedDomains) getLatestValSize(domain kv.Domain, tx kv.TemporalTx, k []byte, view cache.ReadView) (size int, found bool, answered bool, err error) {
	v, _, maxStep, ok := sd.latestFromMem(domain, k)
	if ok {
		return len(v), true, true, nil
	}
	if sd.stateCache != nil {
		if v, txNum, ok := view.GetWithTxNum(domain, k); ok && servableUnderBound(kv.Step(txNum/sd.StepSize()), maxStep) {
			return len(v), true, true, nil
		}
	}
	if maxStep != kv.NoStepBound {
		return 0, false, false, nil
	}
	size, found, err = tx.GetLatestValSize(domain, k)
	return size, found, true, err
}

// GetCode returns the contract code at addr. The fast path resolves the
// account's codeHash and returns the content-addressed bytes from the code
// cache, so many addresses sharing one bytecode (proxies, clones) resolve to a
// single cached copy with no disk read. The cold path is the authoritative
// addr-keyed GetLatest, which also populates the caches.
//
// READ-ONLY contract: for pure getters only. It MUST NOT resolve a DomainPut
// prevVal — during a deploy the account record carries the new codeHash before
// the code write lands, so a prevVal read through the fast path would see the
// about-to-be-written bytes and elide the write. Setters resolve prevVal
// through the addr-keyed GetLatest instead.
func (sd *SharedDomains) GetCode(tx kv.TemporalTx, addr []byte, txNum uint64) ([]byte, bool, error) {
	return sd.getCode(tx, sd.cacheReader(), addr, txNum, nil)
}

func (sd *SharedDomains) getCode(tx kv.TemporalTx, view cache.ReadView, addr []byte, txNum uint64, buf []byte) ([]byte, bool, error) {
	if tx == nil {
		return nil, false, errors.New("sd.GetCode: unexpected nil tx")
	}

	// Fast path: addr → account codeHash → content-addressed bytes. codeHash is
	// resolved mem-first so it reflects in-block code changes and stays reorg-safe.
	var codeHash []byte
	if sd.stateCache != nil || sd.codeStore != nil {
		if codeHash = sd.codeHashForAddr(tx, view, addr, txNum); len(codeHash) > 0 {
			if sd.stateCache != nil {
				if cv, ok := view.GetCodeByHash(codeHash); ok {
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
	v, _, err := sd.getLatest(kv.CodeDomain, tx, addr, nil, time.Time{}, kv.NoStepBound, view, getLatestOptions{codeHash: codeHash, buf: buf})
	if err != nil {
		return nil, false, err
	}
	if len(v) == 0 {
		return nil, false, nil
	}
	return v, true, nil
}

// CodeHashForAddr resolves the code hash for addr as of txNum through the shared
// domains' cache view. Returns nil quietly on any error or missing account — the
// caller falls through to the addr-keyed file read. txNum is passed in rather
// than read from sd.txNum, which the parallel exec loop advances concurrently.
func (sd *SharedDomains) CodeHashForAddr(tx kv.TemporalTx, addr []byte, txNum uint64) []byte {
	return sd.codeHashForAddr(tx, sd.cacheReader(), addr, txNum)
}

func (sd *SharedDomains) codeHashForAddr(tx kv.TemporalTx, view cache.ReadView, addr []byte, txNum uint64) []byte {
	if len(addr) == 0 {
		return nil
	}
	// Route mem-first: sd.mem / parent.mem hold this batch's uncommitted account
	// writes and are authoritative; the addr→codeHash LRU is a committed-state
	// layer invalidated only on flush, so it may answer only after mem misses.
	v, _, maxStep, ok := sd.latestFromMem(kv.AccountsDomain, addr)
	if ok {
		return accounts.DeserialiseV3CodeHash(v)
	}
	if maxStep != kv.NoStepBound {
		// A staged unwind bounds the committed lookup. Reuse the normal account
		// path so every source observes the same bound.
		v, _, err := sd.getLatest(kv.AccountsDomain, tx, addr, nil, time.Time{}, kv.NoStepBound, view, getLatestOptions{})
		if err != nil {
			return nil
		}
		return accounts.DeserialiseV3CodeHash(v)
	}

	// Below mem: the addr→codeHash LRU caches committed state. The zero-hash
	// sentinel is the negative-cache marker for "no code / missing account".
	if sd.stateCache != nil {
		if h, ok := view.GetAddrCodeHash(addr); ok {
			if h == ([32]byte{}) {
				return nil
			}
			return h[:]
		}
	}

	// Resolve from the committed layers (stateCache → MDBX/files); mem was
	// checked above. fromReadView reports whether the record came from the tx's
	// read view.
	resolve := func() ([]byte, bool) {
		if sd.stateCache != nil {
			if v, ok := view.Get(kv.AccountsDomain, addr); ok {
				return accounts.DeserialiseV3CodeHash(v), false
			}
		}
		v, _, err := tx.GetLatest(kv.AccountsDomain, addr, kv.GetLatestOptions{})
		if err != nil {
			return nil, false
		}
		if len(v) == 0 {
			return nil, true
		}
		return accounts.DeserialiseV3CodeHash(v), true
	}

	h, fromReadView := resolve()
	if fromReadView && sd.stateCache != nil {
		var fixed [32]byte
		if len(h) == 32 {
			copy(fixed[:], h)
		}
		// Only a view-sourced record may seed the mapping: the admission gate
		// vouches for the tx's frontier, whereas a cache-sourced record can lag a
		// just-committed flush and slip pre-apply state past the gate.
		seedView := view
		if seedView.NeedsFrontier() {
			seedView = seedView.WithFrontier(sd.cacheFrontierFor(tx))
		}
		seedView.SeedAddrCodeHash(addr, fixed, txNum)
	}
	return h
}

func (sd *SharedDomains) Metrics() *kvmetrics.DomainMetrics {
	return &sd.metrics
}

func (sd *SharedDomains) NonExecMetrics() *kvmetrics.DomainMetrics {
	return &sd.nonExecMetrics
}

func (sd *SharedDomains) AddCommitmentTime(d time.Duration) {
	sd.commitmentNanos.Add(int64(d))
}

func (sd *SharedDomains) TakeCommitmentTime() time.Duration {
	return time.Duration(sd.commitmentNanos.Swap(0))
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
	return sd.domainPut(domain, roTx, k, v, txNum, prevVal)
}

func (sd *SharedDomains) domainPut(domain kv.Domain, roTx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
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

	// The write stays isolated in sd.mem and reaches the shared state cache only
	// after a successful Commit; publishing earlier could expose fork-specific state.
	// No changesetMu: the calculator redirects only the commitment writer's diff
	// (SwapCommitmentDiffLocked), never this write's domain, so apply-side writes
	// need no lock.
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
	// redundant empty->empty history row. prevVal is nil when the key was never
	// written but []byte{} for a flushed tombstone — so test len, not nil.
	if len(prevVal) == 0 {
		return nil
	}

	// No changesetMu — as in domainPut, the calculator redirects only the
	// commitment writer's diff, never this delete's domain.
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

func (sd *SharedDomains) computeCommitment(ctx context.Context, tx kv.TemporalTx, saveStateAfter bool, blockNum, txNum uint64, logPrefix string, onProgress func(*commitment.CommitProgress), lockHeld bool) (rootHash []byte, err error) {
	// Flush the previous block's pending deferred updates into its own changeset
	// (hash-aware lookup) so its branch writes can be reverted on unwind.
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
