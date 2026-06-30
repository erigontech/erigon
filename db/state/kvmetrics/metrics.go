// Package kvmetrics holds the KV-read IO metrics for the state domains
// (per-domain cache/db/file read counts, stateCache hit/miss, cache puts) and
// the process-level Collector that aggregates them across every read path
// (exec, commitment, warmup, RPC, engine). It is a leaf package: it imports only
// the kv types and the metrics façade, never db/state or its consumers, so it
// can be imported freely from every layer that does state reads.
package kvmetrics

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// DomainIOMetrics holds flat IO counters for one domain (or, embedded in
// DomainMetrics, the all-domains total).
type DomainIOMetrics struct {
	CacheReadCount    int64
	CacheReadDuration time.Duration
	CacheGetCount     int64
	CachePutCount     int64
	CacheGetSize      int
	CacheGetKeySize   int
	CacheGetValueSize int
	CachePutSize      int
	CachePutKeySize   int
	CachePutValueSize int
	DbReadCount       int64
	DbReadDuration    time.Duration
	FileReadCount     int64
	FileReadDuration  time.Duration
	// UniqueFileReadCount tracks distinct prefixes that ever fell through
	// to a file read (process-cumulative). The ratio FileReadCount /
	// UniqueFileReadCount is the read amplification factor: how many
	// times each unique prefix was re-read from the file layer (cache
	// misses on the same prefix). Updated by UpdateFileReadsUnique;
	// gated by dbg.KVReadLevelledMetrics same as FileReadCount.
	UniqueFileReadCount int64
	// UniqueLenBuckets is the byte-length distribution of distinct prefixes seen
	// by UpdateFileReadsUnique, bucketed by power-of-two length. Since the
	// compact-encoded prefix length is ~1+⌈depth/2⌉ bytes, larger buckets are
	// deeper trie nodes — used to localise where per-block file reads concentrate
	// (top trunk vs storage-subtree vs leaf-parents). See lenBucket for the edges.
	UniqueLenBuckets [10]int64

	// StateCache hit/miss tracks the SharedDomains.stateCache layer
	// specifically (the per-execution Account/Storage/Code cache), distinct
	// from CacheReadCount which counts sd.mem and sd.parent.mem hits.
	// Hit means stateCache.Get returned ok (we skipped MDBX+files).
	// Miss means stateCache.Get returned !ok and we fell through to aggTx.
	StateCacheHitCount  int64
	StateCacheMissCount int64
}

// DomainMetrics is used in two roles. As the shared aggregate, it is read for a
// snapshot under RLock and written only by Merge under Lock — so a separate
// goroutine always sees a consistent snapshot. As a per-worker instance (one
// per goroutine, via NewDomainMetrics), its Update* run lock-free because the
// instance is single-owner; the worker's counts are folded into the aggregate
// via Merge when the worker's task finishes. The embedded mutex therefore
// guards aggregation/snapshot only — never the per-read Update* path.
//
// The process-level Collector folds per-worker instances under its own single
// goroutine, so on that path the mutex is not used at all (see collector.go).
type DomainMetrics struct {
	sync.RWMutex
	DomainIOMetrics
	Domains map[kv.Domain]*DomainIOMetrics

	// seenFileReads dedups prefixes for UpdateFileReadsUnique. Capped at
	// maxSeenFileReads entries: past the cap, uniqueness counting stops so the
	// set can't grow without bound under a long-lived dbg.KVReadLevelledMetrics run.
	seenFileReads    sync.Map
	seenFileReadsLen atomic.Int64
}

// maxSeenFileReads bounds the read-amplification dedup set; beyond it
// UniqueFileReadCount saturates rather than tracking every prefix forever.
const maxSeenFileReads = 1 << 20

// NewDomainMetrics returns a fresh instance (used for the aggregate and for
// per-worker instances).
func NewDomainMetrics() *DomainMetrics {
	return &DomainMetrics{Domains: map[kv.Domain]*DomainIOMetrics{}}
}

// Reset zeroes a per-worker instance after its counts have been merged into the
// aggregate, so it can be reused for the next task. Lock-free: called by the
// single owning worker. The Domains map is cleared but kept allocated.
func (dm *DomainMetrics) Reset() {
	if dm == nil {
		return
	}
	dm.DomainIOMetrics = DomainIOMetrics{}
	for k := range dm.Domains {
		delete(dm.Domains, k)
	}
	dm.seenFileReads.Range(func(k, _ any) bool {
		dm.seenFileReads.Delete(k)
		return true
	})
	dm.seenFileReadsLen.Store(0)
}

// domainEntry returns the per-domain counters, creating them on first use.
// Lock-free: only called on a single-owner per-worker instance (the per-domain
// roll-up into the aggregate happens in Merge, under the aggregate's lock).
func (dm *DomainMetrics) domainEntry(domain kv.Domain) *DomainIOMetrics {
	d, ok := dm.Domains[domain]
	if !ok {
		d = &DomainIOMetrics{}
		dm.Domains[domain] = d
	}
	return d
}

// Update* record one read into this (single-owner) instance, lock-free. Do NOT
// call them on the shared aggregate from multiple goroutines — use a per-worker
// instance and Merge it in.
func (dm *DomainMetrics) UpdateCacheReads(domain kv.Domain, start time.Time) {
	if dm == nil {
		return
	}
	d := time.Since(start)
	dm.CacheReadCount++
	dm.CacheReadDuration += d
	de := dm.domainEntry(domain)
	de.CacheReadCount++
	de.CacheReadDuration += d
}

func (dm *DomainMetrics) UpdateDbReads(domain kv.Domain, start time.Time) {
	if dm == nil {
		return
	}
	d := time.Since(start)
	dm.DbReadCount++
	dm.DbReadDuration += d
	de := dm.domainEntry(domain)
	de.DbReadCount++
	de.DbReadDuration += d
}

func (dm *DomainMetrics) UpdateStateCacheHit(domain kv.Domain) {
	if dm == nil {
		return
	}
	dm.StateCacheHitCount++
	dm.domainEntry(domain).StateCacheHitCount++
}

func (dm *DomainMetrics) UpdateStateCacheMiss(domain kv.Domain) {
	if dm == nil {
		return
	}
	dm.StateCacheMissCount++
	dm.domainEntry(domain).StateCacheMissCount++
}

func (dm *DomainMetrics) UpdateFileReads(domain kv.Domain, start time.Time) {
	if dm == nil {
		return
	}
	d := time.Since(start)
	dm.FileReadCount++
	dm.FileReadDuration += d
	de := dm.domainEntry(domain)
	de.FileReadCount++
	de.FileReadDuration += d
}

// lenBucket maps prefix byte-length to a UniqueLenBuckets index.
func lenBucket(n int) int {
	switch {
	case n <= 1:
		return 0
	case n <= 4:
		return 1
	case n <= 8:
		return 2
	case n <= 16:
		return 3
	case n <= 32:
		return 4
	case n == 33:
		return 5
	case n <= 36:
		return 6
	case n <= 44:
		return 7
	case n <= 64:
		return 8
	default:
		return 9
	}
}

// UpdateFileReadsUnique is like UpdateFileReads but also tracks prefix
// uniqueness for the read-amplification ratio (FileReadCount /
// UniqueFileReadCount) and the prefix-length distribution. Lock-free on the
// counters (single-owner per-worker instance); seenFileReads is a sync.Map so
// the dedup set stays correct even when shared across workers.
func (dm *DomainMetrics) UpdateFileReadsUnique(domain kv.Domain, key []byte, start time.Time) {
	if dm == nil {
		return
	}
	// Composite key so two domains can hold the same prefix shape without colliding.
	domainKey := domain.String() + ":" + string(key)
	alreadySeen := true
	if dm.seenFileReadsLen.Load() < maxSeenFileReads {
		if _, loaded := dm.seenFileReads.LoadOrStore(domainKey, struct{}{}); !loaded {
			dm.seenFileReadsLen.Add(1)
			alreadySeen = false
		}
	}
	bucket := lenBucket(len(key))

	d := time.Since(start)
	dm.FileReadCount++
	dm.FileReadDuration += d
	de := dm.domainEntry(domain)
	de.FileReadCount++
	de.FileReadDuration += d
	if !alreadySeen {
		dm.UniqueFileReadCount++
		dm.UniqueLenBuckets[bucket]++
		de.UniqueFileReadCount++
		de.UniqueLenBuckets[bucket]++
	}
}

// Merge folds a finished per-worker instance into this aggregate under the
// aggregate's lock — the join. This and the RLock'd snapshot are the only
// protected operations. (The Collector path folds without this lock; see
// collector.go.)
func (dm *DomainMetrics) Merge(src *DomainMetrics) {
	if src == nil {
		return
	}
	dm.Lock()
	defer dm.Unlock()
	dm.mergeLocked(src)
}

// mergeLocked folds src into dm without taking dm's lock. Used by Merge (which
// holds the lock) and by the Collector goroutine (single-owner, no lock needed).
func (dm *DomainMetrics) mergeLocked(src *DomainMetrics) {
	addIOMetrics(&dm.DomainIOMetrics, &src.DomainIOMetrics)
	for d, sd := range src.Domains {
		addIOMetrics(dm.domainEntry(d), sd)
	}
}

func addIOMetrics(dst, src *DomainIOMetrics) {
	dst.CacheReadCount += src.CacheReadCount
	dst.CacheReadDuration += src.CacheReadDuration
	dst.CacheGetCount += src.CacheGetCount
	dst.CachePutCount += src.CachePutCount
	dst.CacheGetSize += src.CacheGetSize
	dst.CacheGetKeySize += src.CacheGetKeySize
	dst.CacheGetValueSize += src.CacheGetValueSize
	dst.CachePutSize += src.CachePutSize
	dst.CachePutKeySize += src.CachePutKeySize
	dst.CachePutValueSize += src.CachePutValueSize
	dst.DbReadCount += src.DbReadCount
	dst.DbReadDuration += src.DbReadDuration
	dst.FileReadCount += src.FileReadCount
	dst.FileReadDuration += src.FileReadDuration
	dst.UniqueFileReadCount += src.UniqueFileReadCount
	for i := range dst.UniqueLenBuckets {
		dst.UniqueLenBuckets[i] += src.UniqueLenBuckets[i]
	}
	dst.StateCacheHitCount += src.StateCacheHitCount
	dst.StateCacheMissCount += src.StateCacheMissCount
}

// ctxKey is this package's typed-int context-key namespace (the node/app model).
type ctxKey int

const ckMetrics ctxKey = iota

// ContextWithMetrics attaches a per-worker metrics instance to ctx; the read
// path extracts it via MetricsFromContext so a concurrent worker accumulates
// into its own instance and never touches shared state.
func ContextWithMetrics(ctx context.Context, m *DomainMetrics) context.Context {
	return context.WithValue(ctx, ckMetrics, m)
}

// MetricsFromContext returns the per-worker instance, or nil (reads then collect
// no metrics — a valid no-op).
func MetricsFromContext(ctx context.Context) *DomainMetrics {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(ckMetrics).(*DomainMetrics)
	return m
}
