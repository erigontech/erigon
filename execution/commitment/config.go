package commitment

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
	MaxDeferredUpdates     int         // flush threshold; 0 = use DefaultMaxDeferredUpdates
	EnableWarmupCache      bool        // enable warmup cache during Process (default: true)
	EnableTrieWarmup       bool        // enable parallel MDBX page-cache warmup during commitment (default: true)
	CsvMetricsFilePrefix   string      // CSV metrics output prefix; empty = check env var
	MemoizationOff         bool        // disable memoized hashes in computeCellHash (default: false)
	SubtrieConfig          *TrieConfig // config for ConcurrentPH sub-tries; nil = derive from parent

	// RebuildShardMaxSteps caps the initial shard size (in step count) used when rebuilding
	// commitment files in db/state/squeeze.go. The actual size is min(largest pow-2 ≤ stepsInShard, this).
	// 0 = use DefaultRebuildShardMaxSteps (64).
	RebuildShardMaxSteps uint64

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

// maxDeferredUpdatesOrDefault returns MaxDeferredUpdates if set, otherwise DefaultMaxDeferredUpdates.
func (c TrieConfig) maxDeferredUpdatesOrDefault() int {
	if c.MaxDeferredUpdates == 0 {
		return DefaultMaxDeferredUpdates
	}
	return c.MaxDeferredUpdates
}

// RebuildShardMaxStepsOrDefault returns RebuildShardMaxSteps if set, otherwise DefaultRebuildShardMaxSteps.
func (c TrieConfig) RebuildShardMaxStepsOrDefault() uint64 {
	if c.RebuildShardMaxSteps == 0 {
		return DefaultRebuildShardMaxSteps
	}
	return c.RebuildShardMaxSteps
}

// WarmupNumWorkersOrDefault returns WarmupNumWorkers if set, otherwise DefaultWarmupNumWorkers.
func (c TrieConfig) WarmupNumWorkersOrDefault() int {
	if c.WarmupNumWorkers == 0 {
		return DefaultWarmupNumWorkers
	}
	return c.WarmupNumWorkers
}
