// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddedBuilderStatusIgnoresOlderSlotUpdates(t *testing.T) {
	status := NewEmbeddedBuilderStatus(true)
	status.RecordAttempt(42)
	status.RecordAttempt(41)
	status.RecordBid(42, 120)
	status.RecordBid(41, 999)

	snapshot := status.Snapshot()
	require.Equal(t, uint64(42), snapshot.LastAttemptSlot)
	require.Equal(t, uint64(42), snapshot.LastBidSlot)
	require.Equal(t, uint64(120), snapshot.LastBidValueGwei)
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
