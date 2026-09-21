// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestNewAdaptivePinController_ZeroConfigResolvesToDefaults(t *testing.T) {
	c := NewAdaptivePinController(NewBranchCache(64), AdaptivePinControllerConfig{}, log.Root())
	if want := DefaultAdaptivePinControllerConfig(); c.cfg != want {
		t.Fatalf("zero-value config resolved to %+v, want %+v", c.cfg, want)
	}
}

func TestNewAdaptivePinController_ExplicitConfigWins(t *testing.T) {
	cfg := AdaptivePinControllerConfig{
		PromoteThresholdMisses:    7,
		MaxPromotedContracts:      3,
		DemoteCooldownBlocks:      11,
		InitialViewBudgetBytes:    1 << 20,
		ExtensionBudgetBytes:      2 << 20,
		PerContractMaxBudgetBytes: 3 << 20,
	}
	c := NewAdaptivePinController(NewBranchCache(64), cfg, log.Root())
	if c.cfg != cfg {
		t.Fatalf("explicit config was overwritten: got %+v, want %+v", c.cfg, cfg)
	}
}

func TestAdaptivePin_PromoteRecordsPreloadMetrics(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	bytesBefore := mxPreloadBytesTotal.GetValue()

	c := NewAdaptivePinController(NewBranchCache(64), AdaptivePinControllerConfig{}, log.Root())
	var h [32]byte
	copy(h[:], hash)

	c.mu.Lock()
	state, err := c.promoteLocked(context.Background(), h, 1, resolve, nil)
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if state.parallel.UsedBytes() == 0 {
		t.Fatal("promote pinned nothing, so the metric assertions below would be vacuous")
	}

	if got := mxPreloadBytesTotal.GetValue() - bytesBefore; got <= 0 {
		t.Errorf("commitment_trunk_preload_bytes_total advanced by %v after promoting a contract, want > 0", got)
	}
}

func TestAdaptivePin_ExtendRecordsPreloadMetrics(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	cfg := AdaptivePinControllerConfig{InitialViewBudgetBytes: minEntryBytes + 1}
	c := NewAdaptivePinController(NewBranchCache(64), cfg, log.Root())
	var h [32]byte
	copy(h[:], hash)

	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.promoteLocked(context.Background(), h, 1, resolve, nil)
	if err != nil {
		t.Fatal(err)
	}
	if state.parallel.QueueRemaining() == 0 {
		t.Fatal("initial view drained the queue, so there is no extension to measure")
	}

	bytesBefore := mxPreloadBytesTotal.GetValue()

	if _, err := c.runExtensionLocked(context.Background(), state, 2, 1<<20, resolve, nil); err != nil {
		t.Fatal(err)
	}

	if got := mxPreloadBytesTotal.GetValue() - bytesBefore; got <= 0 {
		t.Errorf("commitment_trunk_preload_bytes_total advanced by %v after an extension, want > 0", got)
	}
}

func TestAdaptivePin_RebuildsAfterPinnedSetDrains(t *testing.T) {
	hash, tree, allPaths := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	cache := NewBranchCache(64)
	cfg := AdaptivePinControllerConfig{PromoteThresholdMisses: 1, InitialViewBudgetBytes: 1 << 20}
	c := NewAdaptivePinController(cache, cfg, log.Root())
	missPrefix := nibbles.HexToCompact(ContractNibbles(hash))

	c.onCacheMiss(missPrefix)
	c.OnBlockComplete(context.Background(), 10, resolve, nil)

	full := cache.PinnedCount()
	if full != len(allPaths) {
		t.Fatalf("promote pinned %d entries for a %d-node tree; the drain below needs a full pin set", full, len(allPaths))
	}

	for i, p := range allPaths {
		if i*2 > full {
			break
		}
		cache.Invalidate(nibbles.HexToCompact(p))
	}
	if left := cache.PinnedCount(); left*2 >= full {
		t.Fatalf("drain left %d of %d entries, not the majority loss this test is about", left, full)
	}

	c.onCacheMiss(missPrefix)
	c.OnBlockComplete(context.Background(), 11, resolve, nil)

	if got := cache.PinnedCount(); got != full {
		t.Errorf("pinned entries after a drained trunk saw fresh misses: %d, want %d", got, full)
	}
}

func TestAdaptivePin_RebuildIgnoresThePromotionThreshold(t *testing.T) {
	hash, tree, allPaths := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	cache := NewBranchCache(64)
	cfg := AdaptivePinControllerConfig{PromoteThresholdMisses: 100, InitialViewBudgetBytes: 1 << 20}
	c := NewAdaptivePinController(cache, cfg, log.Root())
	missPrefix := nibbles.HexToCompact(ContractNibbles(hash))

	feedMisses := func(n int) {
		for range n {
			c.onCacheMiss(missPrefix)
		}
	}

	feedMisses(100)
	c.OnBlockComplete(context.Background(), 10, resolve, nil)
	full := cache.PinnedCount()
	if full != len(allPaths) {
		t.Fatalf("promote pinned %d entries for a %d-node tree; the drain below needs a full pin set", full, len(allPaths))
	}

	for i, p := range allPaths {
		if i*2 > full {
			break
		}
		cache.Invalidate(nibbles.HexToCompact(p))
	}

	feedMisses(99)
	c.OnBlockComplete(context.Background(), 11, resolve, nil)

	if got := cache.PinnedCount(); got != full {
		t.Errorf("pinned entries after a drain seen with %d misses (threshold %d): %d, want %d",
			99, cfg.PromoteThresholdMisses, got, full)
	}
}

func TestAdaptivePin_RebuildKeepsTheContractSlot(t *testing.T) {
	hash, tree, allPaths := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	cache := NewBranchCache(64)
	cfg := AdaptivePinControllerConfig{
		PromoteThresholdMisses: 1,
		MaxPromotedContracts:   1,
		InitialViewBudgetBytes: 1 << 20,
	}
	c := NewAdaptivePinController(cache, cfg, log.Root())

	missPrefix := nibbles.HexToCompact(ContractNibbles(hash))
	rival := make([]byte, 32)
	for i := range rival {
		rival[i] = 0x77
	}
	rivalPrefix := nibbles.HexToCompact(ContractNibbles(rival))

	c.onCacheMiss(missPrefix)
	c.OnBlockComplete(context.Background(), 10, resolve, nil)
	full := cache.PinnedCount()
	if full != len(allPaths) {
		t.Fatalf("promote pinned %d entries for a %d-node tree; the drain below needs a full pin set", full, len(allPaths))
	}

	for i, p := range allPaths {
		if i*2 > full {
			break
		}
		cache.Invalidate(nibbles.HexToCompact(p))
	}

	c.onCacheMiss(missPrefix)
	for range 50 {
		c.onCacheMiss(rivalPrefix)
	}
	c.OnBlockComplete(context.Background(), 11, resolve, nil)

	if got := cache.PinnedCount(); got != full {
		t.Errorf("pinned entries after a drain contested by a busier contract: %d, want %d", got, full)
	}
}

func TestAdaptivePin_RebuildKeepsPinsEarnedByExtensions(t *testing.T) {
	hash, tree, allPaths := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	cache := NewBranchCache(64)
	cfg := AdaptivePinControllerConfig{
		PromoteThresholdMisses:    1,
		InitialViewBudgetBytes:    700,
		ExtensionBudgetBytes:      1 << 20,
		PerContractMaxBudgetBytes: 1 << 20,
	}
	c := NewAdaptivePinController(cache, cfg, log.Root())
	missPrefix := nibbles.HexToCompact(ContractNibbles(hash))

	c.onCacheMiss(missPrefix)
	c.OnBlockComplete(context.Background(), 10, resolve, nil)
	if initial := cache.PinnedCount(); initial == 0 || initial >= len(allPaths) {
		t.Fatalf("initial view pinned %d of %d paths; this test needs a partial view that extensions grow", initial, len(allPaths))
	}

	c.onCacheMiss(missPrefix)
	c.OnBlockComplete(context.Background(), 11, resolve, nil)
	full := cache.PinnedCount()
	if full != len(allPaths) {
		t.Fatalf("extension grew the pin set to %d of %d paths; this test needs it full", full, len(allPaths))
	}

	for i, p := range allPaths {
		if i*2 > full {
			break
		}
		cache.Invalidate(nibbles.HexToCompact(p))
	}
	survivors := cache.PinnedCount()

	c.onCacheMiss(missPrefix)
	c.OnBlockComplete(context.Background(), 12, resolve, nil)

	if got := cache.PinnedCount(); got < survivors {
		t.Errorf("rebuild left %d live pins, fewer than the %d that survived the drain", got, survivors)
	}
	if got := cache.PinnedCount(); got != full {
		t.Errorf("pinned entries after rebuilding a contract grown by extensions: %d, want %d", got, full)
	}
}

func TestRecordPreload_RecordsElapsedAndBytes(t *testing.T) {
	// Fabricated: a real preload is microseconds, below timer resolution.
	const elapsed = 50 * time.Millisecond

	for _, tc := range []struct {
		name        string
		bytesPinned int
		wantBytes   float64
	}{
		{"pinned bytes are counted", 4096, 4096},
		{"a step that pinned nothing still counts its time", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bytesBefore := mxPreloadBytesTotal.GetValue()
			secondsBefore := mxPreloadDurationSecondsTotal.GetValue()

			recordPreload(time.Now().Add(-elapsed), tc.bytesPinned)

			if got := mxPreloadBytesTotal.GetValue() - bytesBefore; got != tc.wantBytes {
				t.Errorf("commitment_trunk_preload_bytes_total advanced by %v, want %v", got, tc.wantBytes)
			}
			// Differencing a growing accumulator loses up to half an ULP of its
			// magnitude, so a constant slack stops covering it as the total rises.
			secondsAfter := mxPreloadDurationSecondsTotal.GetValue()
			slack := math.Nextafter(secondsAfter, math.Inf(1)) - secondsAfter
			if got := secondsAfter - secondsBefore; got+slack < elapsed.Seconds() {
				t.Errorf("commitment_trunk_preload_duration_seconds_total advanced by %v, want >= %v", got, elapsed.Seconds())
			}
		})
	}
}
