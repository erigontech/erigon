package commitment

const defaultMaxDeferredUpdates = 50_000

// TrieConfig holds static, set-once configuration for commitment tries.
// It is passed through the constructor chain and should not be modified after construction.
type TrieConfig struct {
	Variant                TrieVariant // selects trie implementation (default: VariantHexPatriciaTrie)
	DeferBranchUpdates     bool        // collect branch updates and apply them at the end of Process (default: true)
	LeaveDeferredForCaller bool        // leave deferred updates for caller to handle via TakeDeferredUpdates (default: false)
	MaxDeferredUpdates     int         // flush threshold; 0 = use default (50,000)
	EnableWarmupCache      bool        // enable warmup cache during Process (default: false)
	EnableTrieWarmup       bool        // enable parallel MDBX page-cache warmup during commitment (default: true)
	CsvMetricsFilePrefix   string      // CSV metrics output prefix; empty = check env var
	MemoizationOff         bool        // disable memoized hashes in computeCellHash (default: false)
	SubtrieConfig          *TrieConfig // config for ConcurrentPH sub-tries; nil = derive from parent
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

// maxDeferredUpdatesOrDefault returns MaxDeferredUpdates if set, otherwise the default (50,000).
func (c TrieConfig) maxDeferredUpdatesOrDefault() int {
	if c.MaxDeferredUpdates == 0 {
		return defaultMaxDeferredUpdates
	}
	return c.MaxDeferredUpdates
}
