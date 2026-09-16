// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package builder

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestBuildContextStoreCopiesAndKeepsActiveSlots(t *testing.T) {
	store := NewBuildContextStore()
	slot41, slot42, slot43 := uint64(41), uint64(42), uint64(43)
	root := common.Hash{0x11}
	withdrawal := &types.Withdrawal{Index: 1}
	params := &Parameters{SlotNumber: &slot41, ParentBeaconBlockRoot: &root, Withdrawals: []*types.Withdrawal{withdrawal}, ValidatedProposerContext: true}
	store.publish(params)

	params.ParentBeaconBlockRoot[0] = 0x22
	withdrawal.Index = 2
	resolved, _, ok := store.Resolve(slot41)
	require.True(t, ok)
	require.Equal(t, common.Hash{0x11}, *resolved.ParentBeaconBlockRoot)
	require.Equal(t, uint64(1), uint64(resolved.Withdrawals[0].Index))

	store.publish(&Parameters{SlotNumber: &slot42, ValidatedProposerContext: true})
	store.publish(&Parameters{SlotNumber: &slot43, ValidatedProposerContext: true})
	_, _, ok = store.Resolve(slot41)
	require.True(t, ok)
	_, _, ok = store.Resolve(slot42)
	require.True(t, ok)
	_, _, ok = store.Resolve(slot43)
	require.True(t, ok)
}

func TestBuildContextStoreWrapKeepsContextAfterBuild(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	var generation uint64
	wrapped := store.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		resolved, token, ok := store.Resolve(slot)
		require.True(t, ok)
		require.Equal(t, params.ParentHash, resolved.ParentHash)
		require.NotZero(t, params.privateBundleGeneration)
		require.Equal(t, token, params.privateBundleGeneration)
		generation = token
		return nil, nil
	})

	params := &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x44}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}
	_, err := wrapped(t.Context(), params, nil)
	require.NoError(t, err)
	require.Zero(t, params.privateBundleGeneration)
	_, token, ok := store.Resolve(slot)
	require.True(t, ok)
	require.Equal(t, generation, token)
	require.True(t, store.IsActive(slot))
	require.True(t, store.IsContextActive(slot, generation))
}

func TestBuildContextStoreWrapReusesContextAcrossBuildRefresh(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	var generations []uint64
	wrapped := store.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		generations = append(generations, params.privateBundleGeneration)
		return nil, nil
	})
	params := &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x44}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}

	_, err := wrapped(t.Context(), params, nil)
	require.NoError(t, err)
	_, err = wrapped(t.Context(), params.Copy(), nil)
	require.NoError(t, err)

	require.Len(t, generations, 2)
	require.NotZero(t, generations[0])
	require.Equal(t, generations[0], generations[1])
}

func TestBuildContextStoreWrapSupersedesPreviousSlot(t *testing.T) {
	store := NewBuildContextStore()
	wrapped := store.Wrap(func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, nil
	})
	slot42, slot43 := uint64(42), uint64(43)

	_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot42, ParentHash: common.Hash{0x42}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)
	_, oldGeneration, ok := store.Resolve(slot42)
	require.True(t, ok)
	_, err = wrapped(t.Context(), &Parameters{SlotNumber: &slot43, ParentHash: common.Hash{0x43}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)

	_, _, ok = store.Resolve(slot42)
	require.False(t, ok)
	require.False(t, store.IsContextActive(slot42, oldGeneration))
	_, _, ok = store.Resolve(slot43)
	require.True(t, ok)
}

func TestBuildContextStoreWrapKeepsChangedParentsAtSameSlot(t *testing.T) {
	store := NewBuildContextStore()
	wrapped := store.Wrap(func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, nil
	})
	slot := uint64(42)

	_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x11}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)
	_, oldGeneration, ok := store.Resolve(slot)
	require.True(t, ok)
	_, err = wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x22}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)

	resolved, newGeneration, ok := store.Resolve(slot)
	require.True(t, ok)
	require.Equal(t, common.Hash{0x22}, resolved.ParentHash)
	require.NotEqual(t, oldGeneration, newGeneration)
	require.True(t, store.IsContextActive(slot, oldGeneration))
	require.False(t, store.IsContextCurrent(slot, oldGeneration))
	_, _, ok = store.ResolveForParent(slot, common.Hash{0x11})
	require.False(t, ok)
	resolved, generation, ok := store.ResolveForParent(slot, common.Hash{0x22})
	require.True(t, ok)
	require.Equal(t, common.Hash{0x22}, resolved.ParentHash)
	require.Equal(t, newGeneration, generation)
	require.True(t, store.IsContextCurrent(slot, newGeneration))
}

func TestBuildContextStoreInvalidationDoesNotFallBackToStaleBeaconContext(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	parent := common.Hash{0x11}
	root1 := common.Hash{0x21}
	root2 := common.Hash{0x22}
	first := &Parameters{
		SlotNumber:               &slot,
		ParentHash:               parent,
		ParentBeaconBlockRoot:    &root1,
		Timestamp:                math.MaxInt64,
		ValidatedProposerContext: true,
	}
	prepared := store.Prepare(first)
	_, firstGeneration, ok := store.ResolveForParent(slot, parent)
	require.True(t, ok)
	require.True(t, store.IsContextCurrent(slot, firstGeneration))

	store.Invalidate(first)
	_, _, ok = store.ResolveForParent(slot, parent)
	require.False(t, ok)
	require.True(t, store.IsContextActive(slot, firstGeneration))
	require.False(t, store.IsContextCurrent(slot, firstGeneration))
	require.ErrorContains(t, store.WithCurrent(slot, firstGeneration, func() error { return nil }), "no longer current")
	require.NotZero(t, prepared.privateBundleGeneration)

	second := first.Copy()
	second.ParentBeaconBlockRoot = &root2
	store.Prepare(second)
	resolved, secondGeneration, ok := store.ResolveForParent(slot, parent)
	require.True(t, ok)
	require.Equal(t, root2, *resolved.ParentBeaconBlockRoot)
	require.NotEqual(t, firstGeneration, secondGeneration)
	require.True(t, store.IsContextCurrent(slot, secondGeneration))
	require.False(t, store.IsContextCurrent(slot, firstGeneration))
}

func TestBuildContextStoreWrapDoesNotReactivateOlderSlot(t *testing.T) {
	store := NewBuildContextStore()
	var generation uint64
	wrapped := store.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		generation = params.privateBundleGeneration
		return nil, nil
	})
	slot42, slot43 := uint64(42), uint64(43)

	_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot43, ParentHash: common.Hash{0x43}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)
	require.NotZero(t, generation)
	_, err = wrapped(t.Context(), &Parameters{SlotNumber: &slot42, ParentHash: common.Hash{0x42}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)

	require.Zero(t, generation)
	_, _, ok := store.Resolve(slot42)
	require.False(t, ok)
	_, _, ok = store.Resolve(slot43)
	require.True(t, ok)
}

func TestBuildContextStoreWrapDoesNotPublishTransientContext(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	var generation uint64
	wrapped := store.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		generation = params.privateBundleGeneration
		return nil, nil
	})
	production := &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x44}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}

	_, err := wrapped(t.Context(), production, nil)
	require.NoError(t, err)
	productionGeneration := generation
	require.NotZero(t, productionGeneration)
	transient := production.Copy()
	transient.TransientPayload = true
	transientOnlyStore := NewBuildContextStore()
	_, err = transientOnlyStore.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		require.Zero(t, params.privateBundleGeneration)
		return nil, nil
	})(t.Context(), transient.Copy(), nil)
	require.NoError(t, err)
	_, _, ok := transientOnlyStore.Resolve(slot)
	require.False(t, ok)
	_, err = wrapped(t.Context(), transient, nil)
	require.NoError(t, err)

	require.Zero(t, generation)
	_, retainedGeneration, ok := store.Resolve(slot)
	require.True(t, ok)
	require.Equal(t, productionGeneration, retainedGeneration)
}

func TestBuildContextStoreWrapExpiresWithoutReplacement(t *testing.T) {
	store := NewBuildContextStore()
	now := uint64(99)
	store.now = func() uint64 { return now }
	slot := uint64(42)
	var retainedGeneration uint64
	wrapped := store.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		retainedGeneration = params.privateBundleGeneration
		return nil, nil
	})

	_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x44}, Timestamp: 100, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)
	_, generation, ok := store.Resolve(slot)
	require.True(t, ok)
	require.NotZero(t, generation)
	require.True(t, store.IsActive(slot))
	require.True(t, store.IsContextActive(slot, retainedGeneration))
	require.NoError(t, store.WithActive(slot, retainedGeneration, func() error { return nil }))

	now = 100

	_, generation, ok = store.Resolve(slot)
	require.False(t, ok)
	require.Zero(t, generation)
	require.False(t, store.IsActive(slot))
	require.False(t, store.IsContextActive(slot, retainedGeneration))
	require.ErrorContains(t, store.WithActive(slot, retainedGeneration, func() error { return nil }), "no longer active")
}

func TestBuildContextStoreAdmissionWindowExtendsSubsecondTargetExpiry(t *testing.T) {
	store := NewBuildContextStore(WithBuildContextAdmissionWindow(350 * time.Millisecond))
	now := uint64(100)
	store.now = func() uint64 { return now }
	slot := uint64(42)
	parent := common.Hash{0x44}
	prepared := store.Prepare(&Parameters{
		SlotNumber: &slot, ParentHash: parent, Timestamp: now, ValidatedProposerContext: true,
	})
	require.NotZero(t, prepared.privateBundleGeneration)
	_, _, ok := store.ResolveForParent(slot, parent)
	require.True(t, ok)

	now++
	_, _, ok = store.ResolveForParent(slot, parent)
	require.True(t, ok)

	now++
	_, _, ok = store.ResolveForParent(slot, parent)
	require.False(t, ok)
}

func TestBuildContextStoreInvalidatedRetryGetsFreshGeneration(t *testing.T) {
	var reboundSlot, reboundFrom, reboundTo uint64
	var reboundParent common.Hash
	store := NewBuildContextStore(WithBuildContextGenerationRebinder(func(slot uint64, parent common.Hash, from, to uint64) {
		reboundSlot, reboundParent, reboundFrom, reboundTo = slot, parent, from, to
	}))
	slot := uint64(42)
	params := &Parameters{
		SlotNumber: &slot, ParentHash: common.Hash{0x44}, Timestamp: math.MaxInt64, ValidatedProposerContext: true,
	}
	first := store.Prepare(params)
	require.NotZero(t, first.privateBundleGeneration)
	store.Invalidate(first)

	second := store.Prepare(params)
	require.NotZero(t, second.privateBundleGeneration)
	require.NotEqual(t, first.privateBundleGeneration, second.privateBundleGeneration)
	require.Equal(t, slot, reboundSlot)
	require.Equal(t, params.ParentHash, reboundParent)
	require.Equal(t, first.privateBundleGeneration, reboundFrom)
	require.Equal(t, second.privateBundleGeneration, reboundTo)
}

func TestBuildContextStoreInvalidatesSupersededExactGeneration(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	parent := common.Hash{0x44}
	root1 := common.Hash{0x11}
	root2 := common.Hash{0x22}
	firstParams := &Parameters{
		SlotNumber: &slot, ParentHash: parent, ParentBeaconBlockRoot: &root1,
		Timestamp: math.MaxInt64, ValidatedProposerContext: true,
	}
	first := store.Prepare(firstParams)
	secondParams := firstParams.Copy()
	secondParams.ParentBeaconBlockRoot = &root2
	second := store.Prepare(secondParams)
	require.NotEqual(t, first.privateBundleGeneration, second.privateBundleGeneration)

	store.Invalidate(firstParams)
	current, generation, ok := store.ResolveForParent(slot, parent)
	require.True(t, ok)
	require.Equal(t, root2, *current.ParentBeaconBlockRoot)
	require.Equal(t, second.privateBundleGeneration, generation)

	retry := store.Prepare(firstParams)
	require.NotEqual(t, first.privateBundleGeneration, retry.privateBundleGeneration)
	require.NotEqual(t, second.privateBundleGeneration, retry.privateBundleGeneration)
}

func TestBuildContextStoreRetainsInvalidatedGenerationWithAdmittedBundle(t *testing.T) {
	pinned := make(map[uint64]bool)
	var reboundFrom, reboundTo uint64
	store := NewBuildContextStore(
		WithBuildContextPinnedGenerations(func(uint64) map[uint64]struct{} {
			result := make(map[uint64]struct{})
			for generation, isPinned := range pinned {
				if isPinned {
					result[generation] = struct{}{}
				}
			}
			return result
		}),
		WithBuildContextGenerationRebinder(func(_ uint64, _ common.Hash, from, to uint64) {
			reboundFrom, reboundTo = from, to
			pinned[from] = false
			pinned[to] = true
		}),
	)
	slot := uint64(42)
	parent := common.Hash{0x44}
	root := common.Hash{0x01}
	params := &Parameters{
		SlotNumber: &slot, ParentHash: parent, ParentBeaconBlockRoot: &root,
		Timestamp: math.MaxInt64, ValidatedProposerContext: true,
	}
	first := store.Prepare(params)
	pinned[first.privateBundleGeneration] = true
	store.Invalidate(first)

	for i := range maxRetainedBuildContextsPerSlot - 1 {
		other := params.Copy()
		otherRoot := common.Hash{byte(i + 2)}
		other.ParentBeaconBlockRoot = &otherRoot
		prepared := store.Prepare(other)
		pinned[prepared.privateBundleGeneration] = true
	}
	retry := store.Prepare(params)

	require.Equal(t, first.privateBundleGeneration, reboundFrom)
	require.Equal(t, retry.privateBundleGeneration, reboundTo)
	require.NotEqual(t, first.privateBundleGeneration, retry.privateBundleGeneration)
	require.True(t, store.IsContextCurrent(slot, retry.privateBundleGeneration))
	_, currentGeneration, ok := store.ResolveForParent(slot, parent)
	require.True(t, ok)
	require.Equal(t, retry.privateBundleGeneration, currentGeneration)
}

func TestBuildContextStoreKeepsFreshGenerationWhenAllRetainedContextsArePinned(t *testing.T) {
	pinned := make(map[uint64]struct{})
	store := NewBuildContextStore(WithBuildContextPinnedGenerations(func(uint64) map[uint64]struct{} {
		return pinned
	}))
	slot := uint64(42)
	for i := range maxRetainedBuildContextsPerSlot {
		root := common.Hash{byte(i + 1)}
		prepared := store.Prepare(&Parameters{
			SlotNumber: &slot, ParentHash: common.Hash{0x44}, ParentBeaconBlockRoot: &root,
			Timestamp: math.MaxInt64, ValidatedProposerContext: true,
		})
		pinned[prepared.privateBundleGeneration] = struct{}{}
	}

	freshRoot := common.Hash{0xff}
	fresh := store.Prepare(&Parameters{
		SlotNumber: &slot, ParentHash: common.Hash{0x44}, ParentBeaconBlockRoot: &freshRoot,
		Timestamp: math.MaxInt64, ValidatedProposerContext: true,
	})

	require.True(t, store.IsContextCurrent(slot, fresh.privateBundleGeneration))
	_, generation, ok := store.ResolveForParent(slot, common.Hash{0x44})
	require.True(t, ok)
	require.Equal(t, fresh.privateBundleGeneration, generation)
}

func TestBuildContextStoreWrapBoundsSameSlotForks(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	wrapped := store.Wrap(func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, nil
	})
	var firstGeneration uint64
	for i := 0; i <= maxRetainedBuildContextsPerSlot; i++ {
		parent := common.Hash{byte(i + 1)}
		_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: parent, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
		require.NoError(t, err)
		_, generation, ok := store.ResolveForParent(slot, parent)
		require.True(t, ok)
		if i == 0 {
			firstGeneration = generation
		}
	}

	require.False(t, store.IsContextActive(slot, firstGeneration))
	_, _, ok := store.ResolveForParent(slot, common.Hash{0x01})
	require.False(t, ok)
}

func TestBuildContextStoreWrapKeepsRevisitedContextAtCapacity(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	wrapped := store.Wrap(func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, nil
	})
	var firstGeneration uint64
	for i := range maxRetainedBuildContextsPerSlot {
		parent := common.Hash{byte(i + 1)}
		_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: parent, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
		require.NoError(t, err)
		_, generation, ok := store.ResolveForParent(slot, parent)
		require.True(t, ok)
		if i == 0 {
			firstGeneration = generation
		}
	}

	_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x01}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)
	_, err = wrapped(t.Context(), &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x11}, Timestamp: math.MaxInt64, ValidatedProposerContext: true}, nil)
	require.NoError(t, err)

	require.True(t, store.IsContextActive(slot, firstGeneration))
	_, _, ok := store.ResolveForParent(slot, common.Hash{0x02})
	require.False(t, ok)
}

func TestBuildContextStoreIgnoresIncompleteParameters(t *testing.T) {
	store := NewBuildContextStore()
	store.publish(nil)
	store.publish(&Parameters{})
	_, _, ok := store.Resolve(0)
	require.False(t, ok)
}

func TestBuildContextStoreOlderBuildCannotReleaseReplacement(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	oldSlot, oldToken := store.publish(&Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x11}, ValidatedProposerContext: true})
	newSlot, newToken := store.publish(&Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x22}, ValidatedProposerContext: true})

	store.release(oldSlot, oldToken)
	resolved, _, ok := store.Resolve(slot)
	require.True(t, ok)
	require.Equal(t, common.Hash{0x22}, resolved.ParentHash)

	store.release(newSlot, newToken)
	_, _, ok = store.Resolve(slot)
	require.False(t, ok)
}

func TestBuildContextStoreNewerBuildReleaseRestoresOlderActiveBuild(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	oldSlot, oldToken := store.publish(&Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x11}, ValidatedProposerContext: true})
	newSlot, newToken := store.publish(&Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x22}, ValidatedProposerContext: true})

	store.release(newSlot, newToken)
	resolved, _, ok := store.Resolve(slot)
	require.True(t, ok)
	require.Equal(t, common.Hash{0x11}, resolved.ParentHash)

	store.release(oldSlot, oldToken)
	_, _, ok = store.Resolve(slot)
	require.False(t, ok)
}

func TestBuildContextStoreTracksSameSlotGenerationsIndependently(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	oldSlot, oldToken := store.publish(&Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x11}, ValidatedProposerContext: true})
	newSlot, newToken := store.publish(&Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x22}, ValidatedProposerContext: true})

	require.True(t, store.IsContextActive(slot, oldToken))
	require.True(t, store.IsContextActive(slot, newToken))

	store.release(newSlot, newToken)
	require.True(t, store.IsActive(slot))
	require.True(t, store.IsContextActive(slot, oldToken))
	require.False(t, store.IsContextActive(slot, newToken))

	store.release(oldSlot, oldToken)
	require.False(t, store.IsActive(slot))
	require.False(t, store.IsContextActive(slot, oldToken))
}

func TestBuildContextStoreKeepsAllActiveSlots(t *testing.T) {
	store := NewBuildContextStore()
	for slot := uint64(41); slot <= 43; slot++ {
		current := slot
		store.publish(&Parameters{SlotNumber: &current, ValidatedProposerContext: true})
	}
	for slot := uint64(41); slot <= 43; slot++ {
		_, _, ok := store.Resolve(slot)
		require.True(t, ok)
	}
}

func TestBuildContextStoreIgnoresUnvalidatedProposerContext(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	wrapped := store.Wrap(func(_ context.Context, _ *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		_, _, ok := store.Resolve(slot)
		require.False(t, ok)
		return nil, nil
	})

	_, err := wrapped(t.Context(), &Parameters{SlotNumber: &slot}, nil)
	require.NoError(t, err)
}

func TestBuildContextStoreActivityCanBeCheckedInsideLease(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	_, token := store.publish(&Parameters{SlotNumber: &slot, ValidatedProposerContext: true})

	require.NoError(t, store.WithActive(slot, token, func() error {
		require.True(t, store.IsActive(slot))
		require.True(t, store.IsContextActive(slot, token))
		return nil
	}))
	store.release(slot, token)
	require.False(t, store.IsContextActive(slot, token))
}
