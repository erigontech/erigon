package commitment

// Default values for TrieConfig fields. Exported so callers (squeeze, merge,
// commitment_context) can fall back to the same defaults when a per-instance
// TrieConfig is not available.
const (
	DefaultMaxDeferredUpdates     = 50_000
	DefaultRebuildShardMaxSteps   = 16
	DefaultKeyReferencingMinSteps = 2
	DefaultWarmupNumWorkers       = 16
)

// TrieConfig holds static, set-once configuration for commitment tries.
// It is passed through the constructor chain and should not be modified after construction.
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
	// 0 = use DefaultRebuildShardMaxSteps (16).
	RebuildShardMaxSteps uint64

	// KeyReferencingMinSteps is the minimum step span at which the merge encoder replaces
	// full account/storage keys in commitment branch values with file-offset references.
	// 0 = use DefaultKeyReferencingMinSteps (2).
	KeyReferencingMinSteps uint64

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

// KeyReferencingMinStepsOrDefault returns KeyReferencingMinSteps if set, otherwise DefaultKeyReferencingMinSteps.
func (c TrieConfig) KeyReferencingMinStepsOrDefault() uint64 {
	if c.KeyReferencingMinSteps == 0 {
		return DefaultKeyReferencingMinSteps
	}
	return c.KeyReferencingMinSteps
}

// WarmupNumWorkersOrDefault returns WarmupNumWorkers if set, otherwise DefaultWarmupNumWorkers.
func (c TrieConfig) WarmupNumWorkersOrDefault() int {
	if c.WarmupNumWorkers == 0 {
		return DefaultWarmupNumWorkers
	}
	return c.WarmupNumWorkers
}

// TODO: more candidates for TrieConfig fields (follow-up):
//   - hashSortBatchSize (commitment.go) — internal sort batch size, currently 10_000.
//   - WarmupMaxDepth (warmuper.go) — currently 128, mathematically tied to max key length;
//     not really a tunable but worth exposing as a Default* if external code ever needs it.
