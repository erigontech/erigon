package commitment

import (
	"testing"

	"github.com/erigontech/erigon/common/length"
)

func TestDefaultTrieConfig(t *testing.T) {
	cfg := DefaultTrieConfig()

	if cfg.Variant != VariantHexPatriciaTrie {
		t.Errorf("Variant should default to VariantHexPatriciaTrie, got %v", cfg.Variant)
	}
	if !cfg.DeferBranchUpdates {
		t.Error("DeferBranchUpdates should default to true")
	}
	if cfg.LeaveDeferredForCaller {
		t.Error("LeaveDeferredForCaller should default to false")
	}
	if cfg.MaxDeferredUpdates != 0 {
		t.Errorf("MaxDeferredUpdates should default to 0 (use default), got %d", cfg.MaxDeferredUpdates)
	}
	if !cfg.EnableWarmupCache {
		t.Error("EnableWarmupCache should default to true")
	}
	if !cfg.EnableTrieWarmup {
		t.Error("EnableTrieWarmup should default to true")
	}
	if cfg.CsvMetricsFilePrefix != "" {
		t.Errorf("CsvMetricsFilePrefix should default to empty, got %q", cfg.CsvMetricsFilePrefix)
	}
	if cfg.MemoizationOff {
		t.Error("MemoizationOff should default to false")
	}
	if cfg.SubtrieConfig != nil {
		t.Error("SubtrieConfig should default to nil")
	}
	if cfg.RebuildShardMaxSteps != 0 {
		t.Errorf("RebuildShardMaxSteps should default to 0 (use default), got %d", cfg.RebuildShardMaxSteps)
	}
	if cfg.WarmupNumWorkers != 0 {
		t.Errorf("WarmupNumWorkers should default to 0 (use default), got %d", cfg.WarmupNumWorkers)
	}
}

func TestTrieConfig_OrDefaultHelpers(t *testing.T) {
	cfg := TrieConfig{}
	if got := cfg.RebuildShardMaxStepsOrDefault(); got != DefaultRebuildShardMaxSteps {
		t.Errorf("RebuildShardMaxStepsOrDefault: expected %d, got %d", DefaultRebuildShardMaxSteps, got)
	}
	if got := cfg.WarmupNumWorkersOrDefault(); got != DefaultWarmupNumWorkers {
		t.Errorf("WarmupNumWorkersOrDefault: expected %d, got %d", DefaultWarmupNumWorkers, got)
	}

	cfg = TrieConfig{RebuildShardMaxSteps: 7, WarmupNumWorkers: 3}
	if got := cfg.RebuildShardMaxStepsOrDefault(); got != 7 {
		t.Errorf("RebuildShardMaxStepsOrDefault: expected 7, got %d", got)
	}
	if got := cfg.WarmupNumWorkersOrDefault(); got != 3 {
		t.Errorf("WarmupNumWorkersOrDefault: expected 3, got %d", got)
	}
}

func TestTrieConfig_maxDeferredUpdatesOrDefault(t *testing.T) {
	t.Run("zero returns default", func(t *testing.T) {
		cfg := TrieConfig{}
		if got := cfg.maxDeferredUpdatesOrDefault(); got != DefaultMaxDeferredUpdates {
			t.Errorf("expected %d, got %d", DefaultMaxDeferredUpdates, got)
		}
	})
	t.Run("custom value preserved", func(t *testing.T) {
		cfg := TrieConfig{MaxDeferredUpdates: 1000}
		if got := cfg.maxDeferredUpdatesOrDefault(); got != 1000 {
			t.Errorf("expected 1000, got %d", got)
		}
	})
}

func TestTrieConfig_PropagationToHPH(t *testing.T) {
	cfg := TrieConfig{
		DeferBranchUpdates:     false,
		LeaveDeferredForCaller: true,
		EnableWarmupCache:      true,
		MemoizationOff:         true,
	}

	hph := NewHexPatriciaHashed(length.Addr, nil, cfg)
	defer hph.Release()

	if hph.cfg != cfg {
		t.Error("stored cfg should match what was passed")
	}
	if hph.branchEncoder.deferUpdates {
		t.Error("branchEncoder.deferUpdates should be false")
	}
	if !hph.leaveDeferredForCaller {
		t.Error("leaveDeferredForCaller should be true")
	}
	if !hph.enableWarmupCache {
		t.Error("enableWarmupCache should be true")
	}
	if !hph.memoizationOff {
		t.Error("memoizationOff should be true")
	}
}

func TestTrieConfig_SpawnSubTrieInheritsConfig(t *testing.T) {
	cfg := TrieConfig{
		DeferBranchUpdates:     true,
		LeaveDeferredForCaller: true,
		MemoizationOff:         true,
	}

	parent := NewHexPatriciaHashed(length.Addr, nil, cfg)
	defer parent.Release()

	sub := parent.SpawnSubTrie(nil, 0)
	defer sub.Release()

	// Sub-trie should inherit config but with DeferBranchUpdates forced to false
	if sub.cfg.DeferBranchUpdates {
		t.Error("sub-trie DeferBranchUpdates should be false")
	}
	if !sub.cfg.LeaveDeferredForCaller {
		t.Error("sub-trie should inherit LeaveDeferredForCaller=true")
	}
	if !sub.cfg.MemoizationOff {
		t.Error("sub-trie should inherit MemoizationOff=true")
	}
	if sub.branchEncoder.deferUpdates {
		t.Error("sub-trie branchEncoder should not defer updates")
	}
}

func TestTrieConfig_ConcurrentPatriciaHashedPropagation(t *testing.T) {
	cfg := TrieConfig{
		DeferBranchUpdates: true,
		MemoizationOff:     true,
	}

	root := NewHexPatriciaHashed(length.Addr, nil, cfg)
	cph := NewConcurrentPatriciaHashed(root, nil)
	defer cph.Release()

	// Root config should match
	if !cph.root.cfg.MemoizationOff {
		t.Error("root should inherit MemoizationOff=true")
	}

	// Mounts inherit config via SpawnSubTrie, with DeferBranchUpdates=false
	for i, mount := range cph.mounts {
		if mount.cfg.DeferBranchUpdates {
			t.Errorf("mount[%d] DeferBranchUpdates should be false", i)
		}
		if !mount.cfg.MemoizationOff {
			t.Errorf("mount[%d] should inherit MemoizationOff=true", i)
		}
	}

	// Runtime EnableWarmupCache should propagate to root and all mounts
	cph.EnableWarmupCache(true)
	if !cph.root.cfg.EnableWarmupCache {
		t.Error("EnableWarmupCache should update root config")
	}
	for i, mount := range cph.mounts {
		if !mount.cfg.EnableWarmupCache {
			t.Errorf("mount[%d] cfg.EnableWarmupCache should be true after EnableWarmupCache(true)", i)
		}
	}
}

func TestTrieConfig_MaxDeferredUpdatesApplied(t *testing.T) {
	cfg := TrieConfig{
		DeferBranchUpdates: true,
		MaxDeferredUpdates: 999,
	}

	hph := NewHexPatriciaHashed(length.Addr, nil, cfg)
	defer hph.Release()

	if hph.branchEncoder.maxDeferredUpdates != 999 {
		t.Errorf("expected branchEncoder.maxDeferredUpdates=999, got %d", hph.branchEncoder.maxDeferredUpdates)
	}

	// Default config should give DefaultMaxDeferredUpdates
	hph2 := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	defer hph2.Release()

	if hph2.branchEncoder.maxDeferredUpdates != DefaultMaxDeferredUpdates {
		t.Errorf("expected branchEncoder.maxDeferredUpdates=%d, got %d", DefaultMaxDeferredUpdates, hph2.branchEncoder.maxDeferredUpdates)
	}
}
