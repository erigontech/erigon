// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package builder

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddedBuilderStatusIgnoresOlderSlotUpdates(t *testing.T) {
	status := NewEmbeddedBuilderStatus(true)
	status.RecordAttempt(42)
	status.RecordAttempt(41)
	status.RecordBid(42, 120)
	status.RecordBid(41, 999)
	status.RecordOutcome(41, BuilderOutcomeFailed)

	snapshot := status.Snapshot()
	require.Equal(t, uint64(42), snapshot.LastAttemptSlot)
	require.Equal(t, uint64(42), snapshot.LastBidSlot)
	require.Equal(t, uint64(120), snapshot.LastBidValueGwei)
	require.Equal(t, uint64(42), snapshot.LastOutcomeSlot)
	require.Equal(t, BuilderOutcomePublished, snapshot.LastOutcome)
}

func TestEmbeddedBuilderStatusReportsConfigurationSeparatelyFromPhase(t *testing.T) {
	disabled := NewEmbeddedBuilderStatus(false).Snapshot()
	require.False(t, disabled.Enabled)
	require.Equal(t, BuilderPhaseDisabled, disabled.Phase)
	require.Equal(t, BuilderDisabledNotConfigured, disabled.Reason)

	status := NewEmbeddedBuilderStatus(true)
	status.MarkDisabled(BuilderDisabledPendingPayloadStore)
	snapshot := status.Snapshot()
	require.True(t, snapshot.Enabled)
	require.Equal(t, BuilderPhaseDisabled, snapshot.Phase)
	require.Equal(t, BuilderDisabledPendingPayloadStore, snapshot.Reason)
}

func TestEmbeddedBuilderStatusRecordsAvailableCollateral(t *testing.T) {
	status := NewEmbeddedBuilderStatus(true)
	status.RecordAvailableCollateral(123)
	status.RecordAvailableCollateral(999)

	require.Equal(t, uint64(999), status.Snapshot().AvailableCollateralGwei)
	status.RecordAvailableCollateral(0)
	require.Zero(t, status.Snapshot().AvailableCollateralGwei)
}

func TestEmbeddedBuilderStatusPreservesPublishedOutcomeAtSameSlot(t *testing.T) {
	status := NewEmbeddedBuilderStatus(true)
	status.RecordOutcome(42, BuilderOutcomeExecutionBusy)
	status.RecordBid(42, 120)
	status.RecordOutcome(42, BuilderOutcomeFailed)

	snapshot := status.Snapshot()
	require.Equal(t, uint64(42), snapshot.LastOutcomeSlot)
	require.Equal(t, BuilderOutcomePublished, snapshot.LastOutcome)
}

func TestEmbeddedBuilderStatusKeepsNewerFailureWhenOlderBidArrives(t *testing.T) {
	status := NewEmbeddedBuilderStatus(true)
	status.RecordOutcome(43, BuilderOutcomeFailed)
	status.RecordBid(42, 120)

	snapshot := status.Snapshot()
	require.Equal(t, uint64(42), snapshot.LastBidSlot)
	require.Equal(t, uint64(43), snapshot.LastOutcomeSlot)
	require.Equal(t, BuilderOutcomeFailed, snapshot.LastOutcome)
}

func TestEmbeddedBuilderStatusSerializesConcurrentSlotUpdates(t *testing.T) {
	status := NewEmbeddedBuilderStatus(true)
	var wg sync.WaitGroup
	for slot := uint64(1); slot <= 64; slot++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			status.RecordOutcome(slot, BuilderOutcomeFailed)
		}()
		go func() {
			defer wg.Done()
			status.RecordBid(slot, slot)
		}()
	}
	wg.Wait()

	snapshot := status.Snapshot()
	require.Equal(t, uint64(64), snapshot.LastBidSlot)
	require.Equal(t, uint64(64), snapshot.LastBidValueGwei)
	require.Equal(t, uint64(64), snapshot.LastOutcomeSlot)
	require.Equal(t, BuilderOutcomePublished, snapshot.LastOutcome)
}
