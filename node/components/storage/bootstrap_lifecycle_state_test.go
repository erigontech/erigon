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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestBootstrapAddFile_LifecycleDriven_LandsAtIndexed pins the contract
// that under LifecycleDrivenByStorage=true, bootstrap files enter at
// LifecycleIndexed so the lifecycle driver runs OnValidation
// (HeaderChain + TxRoot + CommitmentDomain + ReceiptRoot) on them.
//
// Without this, bootstrap files would skip validation entirely — exactly
// the gap that let publisher6 advertise a corrupt
// v2.0-commitment.0-8192.kv (inherited from preverified) and propagate
// it to consumers. The downstream symptom was a "nonce too low" exec
// failure 30 minutes into consumer startup, miles from the root cause.
//
// Mirrors the exact addBootstrapFile shape in provider.go's bootstrap
// loop — if that closure changes, this test must change to match.
func TestBootstrapAddFile_LifecycleDriven_LandsAtIndexed(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()

	// Shape mirrors provider.go addBootstrapFile when
	// bootstrapInLifecycle = true:
	//   - AddFile(Local=true, Advertisable=false) → LifecycleDownloaded
	//   - AdvanceTo(LifecycleIndexed) → LifecycleIndexed (driver picks up)
	entry := &snapshot.FileEntry{
		Name:         "v2.0-commitment.8976-8984.kv",
		Local:        true,
		Advertisable: false,
	}
	snapshot.PopulateFromName(entry)
	require.NoError(t, inv.AddFile(entry))
	require.True(t, inv.AdvanceTo(entry.Name, snapshot.LifecycleIndexed))

	got, ok := inv.LifecycleState(entry.Name)
	require.True(t, ok)
	require.Equal(t, snapshot.LifecycleIndexed, got,
		"bootstrap file under LifecycleDrivenByStorage=true must land at Indexed so OnValidation fires")
}

// TestBootstrapAddFile_LegacyPath_LandsAtAdvertisable pins the
// back-compat path: under LifecycleDrivenByStorage=false there's no
// driver to run validation, so bootstrap files go directly to
// Advertisable. Mirrors addBootstrapFile when bootstrapInLifecycle =
// false.
func TestBootstrapAddFile_LegacyPath_LandsAtAdvertisable(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()

	entry := &snapshot.FileEntry{
		Name:         "v2.0-commitment.8976-8984.kv",
		Local:        true,
		Advertisable: true,
	}
	snapshot.PopulateFromName(entry)
	require.NoError(t, inv.AddFile(entry))

	got, ok := inv.LifecycleState(entry.Name)
	require.True(t, ok)
	require.Equal(t, snapshot.LifecycleAdvertisable, got,
		"bootstrap file under LifecycleDrivenByStorage=false must land at Advertisable directly (no driver to advance it)")
}
