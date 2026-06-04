package commitment

import "github.com/erigontech/erigon/common/dbg"

// Default values for TrieConfig fields. Exported so callers (squeeze, merge,
// commitment_context) can fall back to the same defaults when a per-instance
// TrieConfig is not available.
const (
	DefaultMaxDeferredUpdates     = 50_000
	DefaultRebuildShardMaxSteps   = 64
	DefaultKeyReferencingMinSteps = 2
	DefaultWarmupNumWorkers       = 16
)

// TrieConfig holds configuration for commitment tries. It is passed through the
// constructor chain and is mostly treated as set-once, but a few operational
// toggles (warmup cache, CSV metrics) may be flipped at runtime by the trie
// implementations via dedicated setters.
type TrieConfig struct {
	Variant                TrieVariant // selects trie implementation (default: VariantHexPatriciaTrie)
	DeferBranchUpdates     bool        // collect branch updates and apply them at the end of Process (default: true)
	LeaveDeferredForCaller bool        // leave deferred updates for caller to handle via TakeDeferredUpdates (default: false)
	EnableWarmupCache      bool        // enable warmup cache during Process (default: true)
	EnableTrieWarmup       bool        // enable parallel MDBX page-cache warmup during commitment (default: true)
	CsvMetricsFilePrefix   string      // CSV metrics output prefix; empty = check env var
	MemoizationOff         bool        // disable memoized hashes in computeCellHash (default: false)

	// WarmupNumWorkers is the number of parallel workers used by the MDBX page-cache
	// warmup during commitment. 0 = use DefaultWarmupNumWorkers (16).
	WarmupNumWorkers int
}

// DefaultTrieConfig returns production defaults for TrieConfig.
func DefaultTrieConfig() TrieConfig {
	return TrieConfig{
		Variant:            VariantHexPatriciaTrie,
		DeferBranchUpdates: true,
		EnableTrieWarmup:   true,
		EnableWarmupCache:  true,
	}
}

// Subtrie returns a copy of c configured for a concurrent sub-trie: deferred
// branch updates are disabled since sub-tries fold directly into the parent.
func (c TrieConfig) Subtrie() TrieConfig {
	s := c
	s.DeferBranchUpdates = false
	return s
}

// WarmupNumWorkersOrDefault resolves the warmup worker count: the configured
// value if set, otherwise the env-tunable dbg.TipTrieWarmupers (default NumCPU*8),
// falling back to DefaultWarmupNumWorkers when that is non-positive.
func (c TrieConfig) WarmupNumWorkersOrDefault() int {
	if c.WarmupNumWorkers != 0 {
		return c.WarmupNumWorkers
	}
	if dbg.TipTrieWarmupers > 0 {
		return dbg.TipTrieWarmupers
	}
	return DefaultWarmupNumWorkers
}
