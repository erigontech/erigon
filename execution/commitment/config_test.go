package commitment

import (
	"testing"

	"github.com/erigontech/erigon/common/dbg"
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
	if cfg.EnableTrieWarmup {
		t.Error("EnableTrieWarmup should default to false")
	}
	if cfg.CsvMetricsFilePrefix != "" {
		t.Errorf("CsvMetricsFilePrefix should default to empty, got %q", cfg.CsvMetricsFilePrefix)
	}
	if cfg.MemoizationOff {
		t.Error("MemoizationOff should default to false")
	}
	if cfg.WarmupNumWorkers != 0 {
		t.Errorf("WarmupNumWorkers should default to 0 (use default), got %d", cfg.WarmupNumWorkers)
	}
}

func TestTrieConfig_OrDefaultHelpers(t *testing.T) {
	cfg := TrieConfig{}
	if got := cfg.WarmupNumWorkersOrDefault(); got != dbg.TipTrieWarmupers {
		t.Errorf("WarmupNumWorkersOrDefault: expected %d, got %d", dbg.TipTrieWarmupers, got)
	}

	cfg = TrieConfig{WarmupNumWorkers: 3}
	if got := cfg.WarmupNumWorkersOrDefault(); got != 3 {
		t.Errorf("WarmupNumWorkersOrDefault: expected 3, got %d", got)
	}
}

func TestTrieConfig_WarmupNumWorkers_EnvDisable(t *testing.T) {
	old := dbg.TipTrieWarmupers
	dbg.TipTrieWarmupers = 0
	defer func() { dbg.TipTrieWarmupers = old }()

	if got := (TrieConfig{}).WarmupNumWorkersOrDefault(); got != 0 {
		t.Errorf("explicit TIP_TRIE_WARMUPERS=0 must propagate as 0 (warmup disabled), got %d", got)
	}
}

func TestTrieConfig_PropagationToHPH(t *testing.T) {
	cfg := TrieConfig{
		DeferBranchUpdates:     false,
		LeaveDeferredForCaller: true,
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
	if !hph.branchEncoder.callerOwnsDeferred {
		t.Error("branchEncoder.callerOwnsDeferred should be true")
	}
	if !hph.memoizationOff {
		t.Error("memoizationOff should be true")
	}
}
