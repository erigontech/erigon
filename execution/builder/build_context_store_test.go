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
	resolved, generation, ok := store.ResolveForParent(slot, common.Hash{0x11})
	require.True(t, ok)
	require.Equal(t, common.Hash{0x11}, resolved.ParentHash)
	require.Equal(t, oldGeneration, generation)
	resolved, generation, ok = store.ResolveForParent(slot, common.Hash{0x22})
	require.True(t, ok)
	require.Equal(t, common.Hash{0x22}, resolved.ParentHash)
	require.Equal(t, newGeneration, generation)
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
