package commitment

import "github.com/erigontech/erigon/common/dbg"

const (
	DefaultMaxDeferredUpdates     = 50_000
	DefaultRebuildShardMaxSteps   = 64
	DefaultKeyReferencingMinSteps = 2
)

type TrieConfig struct {
	Variant                TrieVariant
	DeferBranchUpdates     bool
	LeaveDeferredForCaller bool
	EnableTrieWarmup       bool
	CsvMetricsFilePrefix   string // empty falls back to the env var

	MemoizationOff bool
	// 0 = use dbg.TipTrieWarmupers (env TIP_TRIE_WARMUPERS)
	WarmupNumWorkers int
}

func DefaultTrieConfig() TrieConfig {
	return TrieConfig{
		Variant:            VariantHexPatriciaTrie,
		DeferBranchUpdates: true,
		EnableTrieWarmup:   true,
	}
}

func (c TrieConfig) Subtrie() TrieConfig {
	s := c
	s.DeferBranchUpdates = false
	return s
}

func (c TrieConfig) WarmupNumWorkersOrDefault() int {
	if c.WarmupNumWorkers != 0 {
		return c.WarmupNumWorkers
	}
	return dbg.TipTrieWarmupers
}
