package commitment

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// BranchPrefetcher pre-fetches branch nodes into the persistent BranchCache
// in the background, triggered at TouchKey time (during execution) so that
// branch data is already cached when Process/HashSort runs later.
//
// This gives the trie a warm persistent cache without waiting for HashSort.
type BranchPrefetcher struct {
	cache      *BranchCache
	ctxFactory TrieContextFactory
	maxDepth   int
	numWorkers int

	work   chan []byte // hashed keys to prefetch branches for
	g      *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc

	prefetched atomic.Uint64
	started    atomic.Bool
}

// NewBranchPrefetcher creates a prefetcher that populates the given BranchCache.
// The prefetcher's goroutine lifecycle is tied to ctx: if ctx is cancelled the
// workers exit even without an explicit Stop() call. Call Stop() for orderly drain.
// Call Start() to begin processing, then Submit() hashed keys as they arrive.
func NewBranchPrefetcher(ctx context.Context, cache *BranchCache, ctxFactory TrieContextFactory, numWorkers, maxDepth int) *BranchPrefetcher {
	ctx, cancel := context.WithCancel(ctx)
	return &BranchPrefetcher{
		cache:      cache,
		ctxFactory: ctxFactory,
		maxDepth:   maxDepth,
		numWorkers: numWorkers,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start launches background prefetch workers.
func (p *BranchPrefetcher) Start() {
	if p.started.Swap(true) {
		return
	}
	p.work = make(chan []byte, p.numWorkers*128)
	p.g, p.ctx = errgroup.WithContext(p.ctx)

	for i := 0; i < p.numWorkers; i++ {
		p.g.Go(func() error {
			trieCtx, cleanup := p.ctxFactory()
			if cleanup != nil {
				defer cleanup()
			}

			for hashedKey := range p.work {
				select {
				case <-p.ctx.Done():
					return p.ctx.Err()
				default:
				}
				p.prefetchBranches(trieCtx, hashedKey)
			}
			return nil
		})
	}
}

// prefetchBranches walks nibble prefixes of hashedKey and loads branch nodes
// into the persistent cache. Stops at first missing branch (leaf zone).
func (p *BranchPrefetcher) prefetchBranches(trieCtx PatriciaContext, hashedKey []byte) {
	for depth := 1; depth <= len(hashedKey) && depth <= p.maxDepth; depth++ {
		prefix := HexNibblesToCompactBytes(hashedKey[:depth])

		// Already in cache — skip DB read
		if p.cache.Contains(prefix) {
			continue
		}

		branchData, _, err := trieCtx.Branch(prefix)
		if err != nil || len(branchData) < 4 {
			break // no branch at this depth, stop walking
		}

		p.cache.Put(prefix, branchData)
		p.prefetched.Add(1)
	}
}

// Submit enqueues a hashed key for branch prefetching. Non-blocking; drops if full.
// Safe to call concurrently with Stop — uses defer/recover to handle the channel
// being closed between the ctx.Done() check and the send.
func (p *BranchPrefetcher) Submit(hashedKey []byte) {
	if !p.started.Load() {
		return
	}
	// Make a copy since caller may reuse the buffer
	key := make([]byte, len(hashedKey))
	copy(key, hashedKey)

	// Use ctx.Done() to detect shutdown. The defer/recover guards against the
	// narrow window where Stop() closes the channel between our select check
	// and the actual send.
	defer func() { recover() }() //nolint:errcheck
	select {
	case <-p.ctx.Done():
		return
	case p.work <- key:
	default: // drop if channel full — prefetching is best-effort
	}
}

// SubmitPlainKey hashes a plain key (address or address+slot) and submits it for prefetch.
// Safe to call concurrently with Stop (see Submit for details).
func (p *BranchPrefetcher) SubmitPlainKey(plainKey []byte) {
	if !p.started.Load() {
		return
	}
	hashedKey := KeyToHexNibbleHash(plainKey)

	defer func() { recover() }() //nolint:errcheck
	select {
	case <-p.ctx.Done():
		return
	case p.work <- hashedKey:
	default:
	}
}

// Prefetched returns the number of branch nodes prefetched into cache.
func (p *BranchPrefetcher) Prefetched() uint64 {
	return p.prefetched.Load()
}

// Stop cancels the context, closes the work channel, and waits for workers to finish.
// Safe to call multiple times — only the first call has effect.
func (p *BranchPrefetcher) Stop() {
	if !p.started.CompareAndSwap(true, false) {
		return
	}
	// Cancel first so Submit/SubmitPlainKey observe ctx.Done() before we close
	// the channel, eliminating the TOCTOU race.
	p.cancel()
	if p.work != nil {
		close(p.work)
	}
	if p.g != nil {
		p.g.Wait()
	}
}
