package commitmentdb

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/execution/commitment"
)

// TrieContextFactory creates new TrieContext instances for parallel warmup.
// Each TrieContext uses its own MDBX transaction for thread safety.
// Returns the context and a cleanup function to close the transaction.
type TrieContextFactory func() (commitment.PatriciaContext, func())

// WarmupBranches pre-fetches branch data in parallel to warm up MDBX page cache.
// Each worker gets its own TrieContext (with its own MDBX transaction).
// The reads warm up the cache - results are not stored since Process will re-read them.
func WarmupBranches(ctx context.Context, prefixes [][]byte, numWorkers int, ctxFactory TrieContextFactory) error {
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

				// Just read to warm cache - we don't need the result
				_, _, _ = trieCtx.Branch(prefix)
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
