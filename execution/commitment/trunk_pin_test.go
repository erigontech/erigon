// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeReader returns a deterministic branch for any prefix that has a
// 16-bit bitmap encoded after the 2-byte touchMap. The bitmap is
// derived from the path so children are reproducible across runs.
func fakeReader(saturated bool) CommitmentReader {
	return func(prefix []byte) ([]byte, uint64, bool, error) {
		// Synthesize a branch: 2 B touchMap (zero) || 2 B bitmap || nothing.
		// bitmap: full (0xFFFF) when saturated; sparse (single child idx 0)
		// otherwise — produces a narrow trunk that exhausts the BFS quickly.
		var bitmap uint16
		if saturated {
			bitmap = 0xFFFF
		} else {
			bitmap = 0x0001
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint16(buf[2:4], bitmap)
		return buf, 1, true, nil
	}
}

// TestPinEntry_AndPinnedCount confirms basic pin-tier mechanics.
func TestPinEntry_AndPinnedCount(t *testing.T) {
	c := NewBranchCache(8)
	require.Equal(t, 0, c.PinnedCount())
	c.PinEntry([]byte{0x12, 0x34, 0x56}, []byte{0xab, 0xcd}, 1, "test")
	require.Equal(t, 1, c.PinnedCount())

	// Pinned entry is hit, not the tail.
	data, _, ok := c.Get([]byte{0x12, 0x34, 0x56})
	require.True(t, ok)
	require.Equal(t, []byte{0xab, 0xcd}, data)
	hits, _, _ := c.PinnedStats()
	require.Equal(t, uint64(1), hits)
}

// TestInvalidate_RemovesFromPinned ensures Invalidate (now pin-aware)
// deletes a pinned entry, not just LRU/root.
func TestInvalidate_RemovesFromPinned(t *testing.T) {
	c := NewBranchCache(8)
	c.PinEntry([]byte{0x12, 0x34}, []byte{0x99}, 1, "test")
	require.Equal(t, 1, c.PinnedCount())

	c.Invalidate([]byte{0x12, 0x34})
	require.Equal(t, 0, c.PinnedCount())
	_, _, ok := c.Get([]byte{0x12, 0x34})
	require.False(t, ok)
}

// TestGetWithOrigin_ChecksPinnedTier ensures GetWithOrigin reads
// pinned entries (was a correctness gap pre-fix).
func TestGetWithOrigin_ChecksPinnedTier(t *testing.T) {
	c := NewBranchCache(8)
	c.PinEntry([]byte{0x12, 0x34}, []byte{0xab}, 5, "preload")
	data, origin, _, _, ok := c.GetWithOrigin([]byte{0x12, 0x34})
	require.True(t, ok)
	require.Equal(t, []byte{0xab}, data)
	require.Equal(t, "preload", origin)
}

// TestClear_ResetsPinnedTier ensures Clear empties pinned + resets
// pinned-stats counters (was a correctness gap pre-fix).
func TestClear_ResetsPinnedTier(t *testing.T) {
	c := NewBranchCache(8)
	c.PinEntry([]byte{0x12, 0x34}, []byte{0xab}, 1, "test")
	_, _, _ = c.Get([]byte{0x12, 0x34}) // bump pinned hit
	c.Clear()
	require.Equal(t, 0, c.PinnedCount())
	hits, misses, _ := c.PinnedStats()
	require.Equal(t, uint64(0), hits)
	require.Equal(t, uint64(0), misses)
}

// TestMissCallback_FiresOnTripleMiss ensures the callback fires only
// when ALL three tiers (root, pinned, LRU) miss — not on cache hits.
func TestMissCallback_FiresOnTripleMiss(t *testing.T) {
	c := NewBranchCache(8)
	var fired atomic.Uint64
	c.SetMissCallback(func(prefix []byte) {
		fired.Add(1)
	})

	// Hit (pinned): callback should NOT fire.
	c.PinEntry([]byte{0x12, 0x34}, []byte{0xff}, 1, "test")
	_, _, _ = c.Get([]byte{0x12, 0x34})
	require.Equal(t, uint64(0), fired.Load(), "pinned hit must not fire callback")

	// Miss: callback fires.
	_, _, _ = c.Get([]byte{0x99, 0x88, 0x77})
	require.Equal(t, uint64(1), fired.Load(), "triple-miss must fire callback")

	// Unbind: subsequent miss does not fire.
	c.SetMissCallback(nil)
	_, _, _ = c.Get([]byte{0xaa, 0xbb})
	require.Equal(t, uint64(1), fired.Load(), "unbound callback must not fire")
}

// TestContractHashFromPrefix_DecodesStorageTrunk ensures the helper
// extracts the contract hash from a 33+ B prefix and rejects shorter
// account-trie prefixes.
func TestContractHashFromPrefix_DecodesStorageTrunk(t *testing.T) {
	// 33-byte prefix: 1 HP flag + 32 contract bytes.
	prefix := make([]byte, 33)
	prefix[0] = 0x00 // HP flag
	for i := 1; i <= 32; i++ {
		prefix[i] = byte(i)
	}
	hash, ok := ContractHashFromPrefix(prefix)
	require.True(t, ok)
	for i := 0; i < 32; i++ {
		require.Equal(t, byte(i+1), hash[i])
	}

	// Short prefix: account-trie, must be rejected.
	_, ok = ContractHashFromPrefix([]byte{0x00, 0x12, 0x34})
	require.False(t, ok)
}

// TestContractTrunkPreload_PhasedRun confirms initial+extension semantics:
// first Run consumes its budget, second Run continues from where it left.
func TestContractTrunkPreload_PhasedRun(t *testing.T) {
	c := NewBranchCache(64)
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i)
	}
	p, err := NewContractTrunkPreload(hash)
	require.NoError(t, err)

	reader := fakeReader(false /* sparse */)

	// Initial Run with tiny budget — pins the root and stops.
	n1, queueEmpty, err := p.Run(1<<10, reader, c, nil)
	require.NoError(t, err)
	require.Greater(t, n1, 0, "initial Run must pin at least the root")
	require.False(t, queueEmpty, "sparse trie shouldn't exhaust on tiny budget")
	totalAfterInitial := p.PinnedTotal()
	require.Equal(t, n1, totalAfterInitial)

	// Extension Run continues from where it stopped.
	n2, _, err := p.Run(1<<10, reader, c, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n2, 0)
	require.Equal(t, totalAfterInitial+n2, p.PinnedTotal())

	// Pinned prefixes are tracked for demotion.
	require.Equal(t, p.PinnedTotal(), len(p.PinnedPrefixes()))
}

// TestAdaptivePinController_PromoteOnThreshold confirms a contract
// crossing PromoteThresholdMisses gets promoted on the next
// OnBlockComplete.
func TestAdaptivePinController_PromoteOnThreshold(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 3
	cfg.InitialViewBudgetBytes = 1 << 14 // 16 KiB; small but enough for sparse trie
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	// Forge a 33-byte prefix that decodes to a known contract hash.
	contractHash := [32]byte{}
	for i := range contractHash {
		contractHash[i] = byte(i + 1)
	}
	prefix := append([]byte{0x00}, contractHash[:]...)

	// Trigger 3 cache-misses for the same contract.
	for i := 0; i < 3; i++ {
		_, _, _ = c.Get(prefix)
	}

	// OnBlockComplete should promote the contract.
	ctrl.OnBlockComplete(context.Background(), 1, fakeReader(false), nil)
	require.Len(t, ctrl.PromotedContracts(), 1)
	require.Equal(t, contractHash, ctrl.PromotedContracts()[0])
	require.Greater(t, c.PinnedCount(), 0)
}

// TestAdaptivePinController_DemoteOnCold confirms a promoted contract
// gets demoted after DemoteCooldownBlocks consecutive blocks with no
// misses.
func TestAdaptivePinController_DemoteOnCold(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.DemoteCooldownBlocks = 2
	cfg.InitialViewBudgetBytes = 1 << 14
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	contractHash := [32]byte{}
	for i := range contractHash {
		contractHash[i] = byte(i + 7)
	}
	prefix := append([]byte{0x00}, contractHash[:]...)

	// Hot block: triggers promotion.
	_, _, _ = c.Get(prefix)
	ctrl.OnBlockComplete(context.Background(), 1, fakeReader(false), nil)
	require.Len(t, ctrl.PromotedContracts(), 1)
	pinnedAfterPromote := c.PinnedCount()
	require.Greater(t, pinnedAfterPromote, 0)

	// Cold blocks: no misses for the contract. After cooldown the
	// controller demotes and invalidates the pin set.
	ctrl.OnBlockComplete(context.Background(), 2, fakeReader(false), nil)
	require.Len(t, ctrl.PromotedContracts(), 1, "still pinned after 1 cold block")

	ctrl.OnBlockComplete(context.Background(), 3, fakeReader(false), nil)
	require.Len(t, ctrl.PromotedContracts(), 0, "demoted after 2 cold blocks")
	require.Equal(t, 0, c.PinnedCount(), "pin set invalidated on demotion")
}
