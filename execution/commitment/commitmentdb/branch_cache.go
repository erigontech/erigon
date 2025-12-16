package commitmentdb

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// BranchCache holds pre-warmed branch data for parallel reads.
// Thread-safe for concurrent reads and writes.
type BranchCache struct {
	mu    sync.RWMutex
	cache map[string]branchCacheEntry
}

type branchCacheEntry struct {
	data []byte
	step kv.Step
	err  error
}

func NewBranchCache() *BranchCache {
	return &BranchCache{
		cache: make(map[string]branchCacheEntry),
	}
}

func (c *BranchCache) Get(prefix []byte) (data []byte, step kv.Step, err error, found bool) {
	c.mu.RLock()
	entry, ok := c.cache[string(prefix)]
	c.mu.RUnlock()
	if !ok {
		return nil, 0, nil, false
	}
	return entry.data, entry.step, entry.err, true
}

func (c *BranchCache) Put(prefix []byte, data []byte, step kv.Step, err error) {
	c.mu.Lock()
	// Make a copy of data since the underlying buffer may be reused
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	c.cache[string(prefix)] = branchCacheEntry{data: dataCopy, step: step, err: err}
	c.mu.Unlock()
}

func (c *BranchCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

func (c *BranchCache) Clear() {
	c.mu.Lock()
	c.cache = make(map[string]branchCacheEntry)
	c.mu.Unlock()
}

// CachedTrieContext wraps a TrieContext and uses cache for Branch reads.
// It implements commitment.PatriciaContext.
type CachedTrieContext struct {
	ctx   commitment.PatriciaContext
	cache *BranchCache
}

func NewCachedTrieContext(ctx commitment.PatriciaContext, cache *BranchCache) *CachedTrieContext {
	return &CachedTrieContext{ctx: ctx, cache: cache}
}

func (c *CachedTrieContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if c.cache != nil {
		if data, step, err, found := c.cache.Get(prefix); found {
			return data, step, err
		}
	}
	return c.ctx.Branch(prefix)
}

func (c *CachedTrieContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error {
	return c.ctx.PutBranch(prefix, data, prevData, prevStep)
}

func (c *CachedTrieContext) Account(plainKey []byte) (*commitment.Update, error) {
	return c.ctx.Account(plainKey)
}

func (c *CachedTrieContext) Storage(plainKey []byte) (*commitment.Update, error) {
	return c.ctx.Storage(plainKey)
}

// TrieContextFactory creates new TrieContext instances for parallel warmup.
// Each TrieContext uses its own MDBX transaction for thread safety.
type TrieContextFactory func() (commitment.PatriciaContext, func())

// WarmupBranches pre-fetches branch data in parallel using multiple TrieContexts.
// The ctxFactory creates a new TrieContext (with its own MDBX transaction) for each worker.
// The cleanup function returned by ctxFactory is called when the worker finishes.
func WarmupBranches(ctx context.Context, prefixes [][]byte, cache *BranchCache, numWorkers int, ctxFactory TrieContextFactory) error {
	if len(prefixes) == 0 || numWorkers <= 0 {
		return nil
	}

	// Create work channel
	work := make(chan []byte, len(prefixes))
	for _, p := range prefixes {
		work <- p
	}
	close(work)

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			trieCtx, cleanup := ctxFactory()
			if cleanup != nil {
				defer cleanup()
			}

			for prefix := range work {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				data, step, err := trieCtx.Branch(prefix)
				cache.Put(prefix, data, step, err)
			}
			return nil
		})
	}

	return g.Wait()
}

// CollectBranchPrefixes extracts all unique prefixes from hashed keys up to maxDepth.
// These prefixes are converted to compact bytes format used by Branch().
// Uses ForEachHashedKey which is non-destructive (only works for ModeUpdate mode).
func CollectBranchPrefixes(ctx context.Context, updates *commitment.Updates, maxDepth int) ([][]byte, error) {
	seen := make(map[string]struct{})
	var prefixes [][]byte

	err := updates.ForEachHashedKey(ctx, func(hashedKey []byte) error {
		// Add all prefixes of this hashed key up to maxDepth
		for depth := 0; depth <= maxDepth && depth <= len(hashedKey); depth++ {
			prefix := hashedKey[:depth]
			compactPrefix := commitment.HexNibblesToCompactBytes(prefix)
			key := string(compactPrefix)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				// Make a copy since compactPrefix might be reused
				prefixCopy := make([]byte, len(compactPrefix))
				copy(prefixCopy, compactPrefix)
				prefixes = append(prefixes, prefixCopy)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return prefixes, nil
}

// CollectBranchPrefixesFromKeys extracts all unique prefixes from a slice of hashed keys.
// This is used by HashSortWithPrefetch which provides all keys upfront.
func CollectBranchPrefixesFromKeys(hashedKeys [][]byte, maxDepth int) [][]byte {
	seen := make(map[string]struct{})
	var prefixes [][]byte

	for _, hashedKey := range hashedKeys {
		// Add all prefixes of this hashed key up to maxDepth
		for depth := 0; depth <= maxDepth && depth <= len(hashedKey); depth++ {
			prefix := hashedKey[:depth]
			compactPrefix := commitment.HexNibblesToCompactBytes(prefix)
			key := string(compactPrefix)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				// Make a copy since compactPrefix might be reused
				prefixCopy := make([]byte, len(compactPrefix))
				copy(prefixCopy, compactPrefix)
				prefixes = append(prefixes, prefixCopy)
			}
		}
	}

	return prefixes
}
