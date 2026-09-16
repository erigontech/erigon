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

func TestBuildContextStoreWrapPublishesBeforeBuild(t *testing.T) {
	store := NewBuildContextStore()
	slot := uint64(42)
	wrapped := store.Wrap(func(_ context.Context, params *Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		resolved, _, ok := store.Resolve(slot)
		require.True(t, ok)
		require.Equal(t, params.ParentHash, resolved.ParentHash)
		require.NotZero(t, params.privateBundleGeneration)
		return nil, nil
	})

	params := &Parameters{SlotNumber: &slot, ParentHash: common.Hash{0x44}, ValidatedProposerContext: true}
	_, err := wrapped(t.Context(), params, nil)
	require.NoError(t, err)
	require.Zero(t, params.privateBundleGeneration)
	_, _, ok := store.Resolve(slot)
	require.False(t, ok)
	require.False(t, store.IsActive(slot))
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
