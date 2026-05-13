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
	ctrl.OnBlockComplete(context.Background(), 1, fakeReader(false))
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
	ctrl.OnBlockComplete(context.Background(), 1, fakeReader(false))
	require.Len(t, ctrl.PromotedContracts(), 1)
	pinnedAfterPromote := c.PinnedCount()
	require.Greater(t, pinnedAfterPromote, 0)

	// Cold blocks: no misses for the contract. After cooldown the
	// controller demotes and invalidates the pin set.
	ctrl.OnBlockComplete(context.Background(), 2, fakeReader(false))
	require.Len(t, ctrl.PromotedContracts(), 1, "still pinned after 1 cold block")

	ctrl.OnBlockComplete(context.Background(), 3, fakeReader(false))
	require.Len(t, ctrl.PromotedContracts(), 0, "demoted after 2 cold blocks")
	require.Equal(t, 0, c.PinnedCount(), "pin set invalidated on demotion")
}

// --- Parallel-mode adaptive controller tests ---

// makeParallelResolver returns a BatchBranchResolver paired with a usage
// counter so tests can assert it was actually invoked.
func makeParallelResolver(saturated bool, calls *int32) BatchBranchResolver {
	return func(keys [][]byte) ([][]byte, error) {
		atomic.AddInt32(calls, int32(len(keys)))
		vals := make([][]byte, len(keys))
		var bitmap uint16
		if saturated {
			bitmap = 0xFFFF
		} else {
			bitmap = 0x0001
		}
		for i := range keys {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint16(buf[2:4], bitmap)
			vals[i] = buf
		}
		return vals, nil
	}
}

// TestAdaptivePinController_ParallelMode_PromoteUsesParallelResolver
// confirms that with parallel mode installed, a freshly-promoted contract
// uses the BatchBranchResolver (not the serial CommitmentReader). The
// serial reader is wired to panic so we know if it's called.
func TestAdaptivePinController_ParallelMode_PromoteUsesParallelResolver(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 3
	cfg.InitialViewBudgetBytes = 1 << 14
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	var resolverCalls int32
	resolver := makeParallelResolver(false, &resolverCalls)

	dbBranchesCalls := 0
	provider := func(contractHash []byte) map[string][]byte {
		dbBranchesCalls++
		return nil // file-only — let the resolver supply everything
	}

	factoryCalls := 0
	factory := func() (BatchBranchResolver, func(), error) {
		factoryCalls++
		return resolver, nil, nil
	}
	ctrl.SetParallelMode(factory, provider)

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

	// Wire a reader that fails the test if called.
	panicReader := func(prefix []byte) ([]byte, uint64, bool, error) {
		t.Fatalf("parallel mode must not fall back to serial reader; called with prefix=%x", prefix)
		return nil, 0, false, nil
	}

	ctrl.OnBlockComplete(context.Background(), 1, panicReader)
	require.Len(t, ctrl.PromotedContracts(), 1)
	require.Equal(t, contractHash, ctrl.PromotedContracts()[0])
	require.Greater(t, c.PinnedCount(), 0)
	require.Equal(t, 1, factoryCalls, "factory should be called exactly once per OnBlockComplete")
	require.Greater(t, atomic.LoadInt32(&resolverCalls), int32(0), "resolver should be called for the new contract")
	require.GreaterOrEqual(t, dbBranchesCalls, 1, "dbBranchesProvider should be consulted for the new contract")
}

// TestAdaptivePinController_ParallelMode_ExtendUsesResumableState confirms
// that the per-block extension reuses the same parallel state across calls
// (state.parallel persists; PinnedTotal grows monotonically). To trigger
// an extension miss for an already-promoted contract we Get a *deeper*
// prefix than what the initial promotion pinned, otherwise the lookup
// would hit the pinned tier and never invoke the miss callback.
func TestAdaptivePinController_ParallelMode_ExtendUsesResumableState(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.InitialViewBudgetBytes = 1 << 12 // 4 KiB — small initial view
	cfg.ExtensionBudgetBytes = 1 << 12   // 4 KiB per extension step
	cfg.PerContractMaxBudgetBytes = 1 << 20
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	var resolverCalls int32
	resolver := makeParallelResolver(true /* full bitmap — many children */, &resolverCalls)
	factory := func() (BatchBranchResolver, func(), error) {
		return resolver, nil, nil
	}
	ctrl.SetParallelMode(factory, nil)

	contractHash := [32]byte{}
	for i := range contractHash {
		contractHash[i] = byte(i + 13)
	}
	rootPrefix := append([]byte{0x00}, contractHash[:]...)
	// deepPrefix has the contract hash in bytes 1..33 (so the miss
	// callback attributes it to our contract) plus extra trailing bytes
	// so it's not the same key as anything the preload could pin.
	makeDeepPrefix := func(seed byte) []byte {
		p := make([]byte, 50)
		p[0] = 0x10 // HP flag for odd-depth path
		copy(p[1:33], contractHash[:])
		for i := 33; i < 50; i++ {
			p[i] = byte(i*7) ^ seed
		}
		return p
	}

	// Block 1: trigger miss on the root path, promote.
	_, _, _ = c.Get(rootPrefix)
	ctrl.OnBlockComplete(context.Background(), 1, nil)
	require.Len(t, ctrl.PromotedContracts(), 1)
	pinnedAfterPromote := c.PinnedCount()
	require.Greater(t, pinnedAfterPromote, 0)

	// Block 2: miss on a DEEPER prefix in the same contract's subtree —
	// not yet pinned, so the miss callback fires. Extension fires.
	_, _, _ = c.Get(makeDeepPrefix(0x11))
	ctrl.OnBlockComplete(context.Background(), 2, nil)
	pinnedAfterExt1 := c.PinnedCount()
	require.GreaterOrEqual(t, pinnedAfterExt1, pinnedAfterPromote, "extension should not shrink the pin set")
	require.Greater(t, pinnedAfterExt1, pinnedAfterPromote, "extension should add at least one entry under a saturated trie")

	// Block 3: another extension via a different deep prefix.
	_, _, _ = c.Get(makeDeepPrefix(0x22))
	ctrl.OnBlockComplete(context.Background(), 3, nil)
	pinnedAfterExt2 := c.PinnedCount()
	require.GreaterOrEqual(t, pinnedAfterExt2, pinnedAfterExt1)
}

// TestAdaptivePinController_ParallelMode_FactoryErrorFallsBackToSerial
// confirms that when the parallel factory returns an error, the controller
// falls back to the serial CommitmentReader path without crashing.
func TestAdaptivePinController_ParallelMode_FactoryErrorFallsBackToSerial(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.InitialViewBudgetBytes = 1 << 14
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	factory := func() (BatchBranchResolver, func(), error) {
		return nil, nil, errFakeFactory
	}
	ctrl.SetParallelMode(factory, nil)

	contractHash := [32]byte{}
	for i := range contractHash {
		contractHash[i] = byte(i + 21)
	}
	prefix := append([]byte{0x00}, contractHash[:]...)
	_, _, _ = c.Get(prefix)

	ctrl.OnBlockComplete(context.Background(), 1, fakeReader(false))
	require.Len(t, ctrl.PromotedContracts(), 1, "should still promote via serial fallback")
}

// TestAdaptivePinController_ParallelMode_DemoteInvalidates confirms that
// demotion invalidates the parallel-state pin set the same as serial.
func TestAdaptivePinController_ParallelMode_DemoteInvalidates(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.DemoteCooldownBlocks = 2
	cfg.InitialViewBudgetBytes = 1 << 14
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	var resolverCalls int32
	resolver := makeParallelResolver(false, &resolverCalls)
	factory := func() (BatchBranchResolver, func(), error) { return resolver, nil, nil }
	ctrl.SetParallelMode(factory, nil)

	contractHash := [32]byte{}
	for i := range contractHash {
		contractHash[i] = byte(i + 29)
	}
	prefix := append([]byte{0x00}, contractHash[:]...)

	// Promote.
	_, _, _ = c.Get(prefix)
	ctrl.OnBlockComplete(context.Background(), 1, nil)
	require.Len(t, ctrl.PromotedContracts(), 1)
	require.Greater(t, c.PinnedCount(), 0)

	// Cold blocks — demote on 2nd.
	ctrl.OnBlockComplete(context.Background(), 2, nil)
	ctrl.OnBlockComplete(context.Background(), 3, nil)
	require.Len(t, ctrl.PromotedContracts(), 0, "demoted after 2 cold blocks")
	require.Equal(t, 0, c.PinnedCount(), "parallel-state pin set invalidated on demotion")
}

// TestAdaptivePinController_ParallelMode_ReleaseCallbackInvoked confirms
// that a non-nil release callback returned by the factory is called once
// per OnBlockComplete (so tx-scoped resources can be cleaned up).
func TestAdaptivePinController_ParallelMode_ReleaseCallbackInvoked(t *testing.T) {
	c := NewBranchCache(64)
	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.InitialViewBudgetBytes = 1 << 14
	ctrl := NewAdaptivePinController(c, cfg, nil)
	ctrl.Bind()

	var resolverCalls int32
	resolver := makeParallelResolver(false, &resolverCalls)
	var releaseCount int32
	factory := func() (BatchBranchResolver, func(), error) {
		return resolver, func() { atomic.AddInt32(&releaseCount, 1) }, nil
	}
	ctrl.SetParallelMode(factory, nil)

	contractHash := [32]byte{}
	for i := range contractHash {
		contractHash[i] = byte(i + 33)
	}
	prefix := append([]byte{0x00}, contractHash[:]...)
	_, _, _ = c.Get(prefix)

	ctrl.OnBlockComplete(context.Background(), 1, nil)
	require.Equal(t, int32(1), atomic.LoadInt32(&releaseCount), "release callback called once per OnBlockComplete")

	// No misses next block — release still called (factory is invoked per block
	// regardless of whether the controller pins anything).
	ctrl.OnBlockComplete(context.Background(), 2, nil)
	require.Equal(t, int32(2), atomic.LoadInt32(&releaseCount))
}

var errFakeFactory = stringError("fake factory failure")

type stringError string

func (s stringError) Error() string { return string(s) }
