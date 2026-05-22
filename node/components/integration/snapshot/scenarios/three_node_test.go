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

package scenarios_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestThreeNode_MergeDivergence runs the M3 gate scenario:
//
//  1. Leecher C starts empty.
//  2. Peer A is seeded with HoodiMerged500 (single merged file per domain role).
//  3. Peer B is seeded with HoodiUnmerged500 (accounts unmerged into 5×100-step
//     slices per role; other domains identical to A).
//  4. A announces first, then B.
//  5. C's orchestrator must:
//     - Download A's files.
//     - Skip B's accounts files, because their ranges are already covered by
//     A's merged accounts entries (now in C's pending/local).
//     - Skip B's duplicate non-accounts entries for the same reason.
//  6. C's accounts coverage at TrustVerified equals A's — [0, 500) as one
//     merged block, not five.
func TestThreeNode_MergeDivergence(t *testing.T) {
	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))

	require.NoError(t, leecher.Start(context.Background()))

	merged := harness.NewFakePeer("peer-merged", coord)
	merged.Seed(harness.HoodiMerged500())

	unmerged := harness.NewFakePeer("peer-unmerged", coord)
	unmerged.Seed(harness.HoodiUnmerged500())

	// Ordering matters: merged peer announces first so its files are in
	// pending/local before the unmerged peer's fine-grained slices arrive.
	merged.AnnounceTo(leecher)
	unmerged.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	mergedFiles := harness.HoodiMerged500().FileCount()

	// Leecher should have downloaded each of A's files exactly once and
	// zero of B's redundant files.
	requestCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadRequested{}))
	require.Equal(t, mergedFiles, requestCount,
		"leecher should request only the merged peer's files; unmerged slices are already covered")

	completeCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadComplete{}))
	require.Equal(t, mergedFiles, completeCount, "every request should complete")

	failedCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{}))
	require.Equal(t, 0, failedCount, "no downloads should fail")

	// Accounts coverage is the full [0, 500) as a single range — proving C
	// took the merged path, not the unmerged one.
	accountsCov := leecher.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 500}}, accountsCov,
		"accounts coverage should match the merged peer's single range")

	// Leecher holds exactly the merged peer's accounts files — two entries
	// (.kv + .kvi), not ten.
	accountsFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, accountsFiles, 2, "expected 2 merged accounts files locally")
}

// TestThreeNode_MergeDivergence_UnmergedFirst is the symmetric case: the
// unmerged peer announces first, so C ends up with the five unmerged slices.
// Coverage is still complete; merge-preference is ordering-dependent without
// a look-ahead buffer, which this test deliberately locks in as current
// behaviour.
func TestThreeNode_MergeDivergence_UnmergedFirst(t *testing.T) {
	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))

	require.NoError(t, leecher.Start(context.Background()))

	merged := harness.NewFakePeer("peer-merged", coord)
	merged.Seed(harness.HoodiMerged500())

	unmerged := harness.NewFakePeer("peer-unmerged", coord)
	unmerged.Seed(harness.HoodiUnmerged500())

	unmerged.AnnounceTo(leecher)
	merged.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	// Coverage is still complete — divergence does not leave the leecher
	// short of data.
	accountsCov := leecher.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 500}}, accountsCov,
		"accounts coverage should still be [0, 500) regardless of announce order")

	// Five unmerged slices × two roles = ten accounts files locally.
	accountsFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, accountsFiles, 10, "unmerged-first ordering yields 10 unmerged accounts files")
}
