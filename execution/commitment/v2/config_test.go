package v2

import (
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestTrieConfig_PropagationToHPH(t *testing.T) {
	cfg := commitment.TrieConfig{
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
