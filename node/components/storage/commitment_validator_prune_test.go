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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestPruneModeExpectsBodyAbsent_GuardClauses pins the early-exit
// branches: an uninitialised PruneMode, archive mode (Blocks disabled),
// or a nil/empty BlockReader all return false — meaning "treat body
// absence as transient, pause and retry" (the safe default).
//
// Without these guards, a tool / test that constructs a validator with
// the zero-value PruneMode would silently classify ALL missing bodies
// as "intentionally pruned" and skip Phase C without recording the
// safety check — invisible regression.
func TestPruneModeExpectsBodyAbsent_GuardClauses(t *testing.T) {
	t.Parallel()

	// Uninitialised PruneMode (zero value) → false.
	v := CommitmentDomainValidator{}
	require.False(t, v.pruneModeExpectsBodyAbsent(1_000_000),
		"uninitialised PruneMode must NOT classify any block as intentionally pruned")

	// Archive mode (Blocks.Enabled() = false) → false even with a
	// BlockReader present. Archive should never assume bodies absent.
	v = CommitmentDomainValidator{PruneMode: prune.ArchiveMode}
	require.False(t, v.pruneModeExpectsBodyAbsent(1_000_000),
		"archive mode must NOT classify any block as intentionally pruned")

	// Minimal mode but nil BlockReader → false (can't determine tip,
	// fail safe to "body should be available, pause and retry").
	v = CommitmentDomainValidator{PruneMode: prune.MinimalMode}
	require.False(t, v.pruneModeExpectsBodyAbsent(1_000_000),
		"nil BlockReader must NOT classify any block as intentionally pruned (would mis-skip Phase C otherwise)")
}

// TestExtractBootstrapCommitmentAnchors_NilSafe confirms the helper
// short-circuits on nil inventory and nil DB instead of panicking —
// tools / tests that don't fully initialise a Provider sometimes hit
// this path.
func TestExtractBootstrapCommitmentAnchors_NilSafe(t *testing.T) {
	t.Parallel()
	require.NotPanics(t, func() {
		extractBootstrapCommitmentAnchors(context.Background(), nil, nil, nil, nil)
	}, "nil inventory + nil DB must short-circuit, not panic")
}

// TestExtractBootstrapCommitmentAnchors_EmptyInventoryIsNoOp confirms
// the helper handles an empty inventory cleanly — no commitment files
// means there's nothing to extract from, NOT an error.
func TestExtractBootstrapCommitmentAnchors_EmptyInventoryIsNoOp(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	require.NotPanics(t, func() {
		extractBootstrapCommitmentAnchors(context.Background(), inv, nil, nil, nil)
	}, "empty inventory must short-circuit before attempting to open a tx")
}
