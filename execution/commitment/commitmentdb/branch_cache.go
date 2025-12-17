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

// CollectBranchPrefixesFromKeys extracts prefixes where branch nodes exist in the trie.
// Instead of warming all theoretical prefixes, this only warms divergence points -
// where consecutive sorted keys differ, which is where actual branch nodes exist.
// This is used by HashSortWithPrefetch which provides all keys upfront (already sorted).
func CollectBranchPrefixesFromKeys(hashedKeys [][]byte, maxDepth int) [][]byte {
	if len(hashedKeys) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	var prefixes [][]byte

	// Always add root (empty prefix)
	prefixes = append(prefixes, commitment.HexNibblesToCompactBytes(nil))
	seen[""] = struct{}{}

	// For the first key, add the path to its first nibble (where it branches from root)
	if len(hashedKeys[0]) > 0 && maxDepth >= 1 {
		prefix := hashedKeys[0][:1]
		compactPrefix := commitment.HexNibblesToCompactBytes(prefix)
		key := string(compactPrefix)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			prefixCopy := make([]byte, len(compactPrefix))
			copy(prefixCopy, compactPrefix)
			prefixes = append(prefixes, prefixCopy)
		}
	}

	// Find divergence points between consecutive keys
	// These are where actual branch nodes exist in the trie
	for i := 1; i < len(hashedKeys); i++ {
		prev := hashedKeys[i-1]
		curr := hashedKeys[i]

		// Find the common prefix length (where they diverge)
		cpl := 0
		minLen := len(prev)
		if len(curr) < minLen {
			minLen = len(curr)
		}
		for cpl < minLen && prev[cpl] == curr[cpl] {
			cpl++
		}

		// The divergence point is where a branch node must exist
		divergeDepth := cpl + 1
		if divergeDepth > maxDepth {
			divergeDepth = maxDepth
		}

		// Add the divergence prefix (the common part + 1)
		if divergeDepth > 0 && divergeDepth <= len(curr) {
			prefix := curr[:divergeDepth]
			compactPrefix := commitment.HexNibblesToCompactBytes(prefix)
			key := string(compactPrefix)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				prefixCopy := make([]byte, len(compactPrefix))
				copy(prefixCopy, compactPrefix)
				prefixes = append(prefixes, prefixCopy)
			}
		}

		// Also add the previous key's branch at divergence point
		if divergeDepth > 0 && divergeDepth <= len(prev) {
			prefix := prev[:divergeDepth]
			compactPrefix := commitment.HexNibblesToCompactBytes(prefix)
			key := string(compactPrefix)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				prefixCopy := make([]byte, len(compactPrefix))
				copy(prefixCopy, compactPrefix)
				prefixes = append(prefixes, prefixCopy)
			}
		}
	}

	return prefixes
}
