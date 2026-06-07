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
	// UniqueLenBuckets is the byte-length distribution of distinct
	// prefixes seen by UpdateFileReadsUnique. The compact-encoded
	// prefix length is 1 + ⌈depth/2⌉ bytes (HP encoding), so byte
	// length maps to trie depth as follows:
	//   0 : 1 byte    (depth 0-1, root)
	//   1 : 2-4 bytes (depth 2-7, top trunk)
	//   2 : 5-8 bytes (depth 8-15)
	//   3 : 9-16 bytes (depth 16-31)
	//   4 : 17-32 bytes (depth 32-63, near account leaf)
	//   5 : 33 bytes (depth 64, storage subtree root)
	//   6 : 34-36 bytes (depth 65-70, storage subtree top)
	//   7 : 37-44 bytes (depth 71-86, mid storage)
	//   8 : 45-64 bytes (depth 87-127, leaf-parents and deep)
	//   9 : 65+ bytes (out of range)
	// Used to localise where the per-block file reads concentrate —
	// e.g., are the 25K commitment reads on bloat workload
	// storage-subtree-trunk (where per-contract pinning helps) or
	// leaf-parents (where it doesn't)?
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
}

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

// UpdateFileReadsUnique records a file read. The distinct-prefix (read
// amplification) tracking it used to do was removed: it had no consumer and
// cost a string alloc + map insert on every file read. Thin alias so call sites
// are unchanged.
func (dm *DomainMetrics) UpdateFileReadsUnique(domain kv.Domain, key []byte, start time.Time) {
	dm.UpdateFileReads(domain, start)
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
