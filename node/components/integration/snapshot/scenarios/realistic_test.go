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

// Realistic-flow counterparts to the simulation-only scenarios. These tests
// run the same assertions against real downloader.Provider, real
// sentry.Provider, and MockStorage wired through the framework event bus —
// proving the event contract holds end-to-end against production component
// code, not just SimulatedTransport.

package scenarios_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func waitForRealistic(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

// TestRealistic_SingleNode_InventoryLoaded is the realistic counterpart to
// TestSingleNodeFlow_LoadAndQuery. It asserts InventoryLoaded fires exactly
// once with the seeded inventory when the full component stack is wired.
func TestRealistic_SingleNode_InventoryLoaded(t *testing.T) {
	node := harness.NewRealNode(t, nil, nil)
	t.Cleanup(func() { _ = node.Close() })

	harness.HoodiBaseline().Apply(node.Inventory)

	var loadedCount atomic.Int32
	require.NoError(t, node.Bus.Subscribe(func(e flow.InventoryLoaded) {
		loadedCount.Add(1)
	}))

	require.NoError(t, node.Start(context.Background()))
	node.Bus.WaitAsync()

	require.Equal(t, int32(1), loadedCount.Load(), "InventoryLoaded should fire exactly once")

	accounts := node.Inventory.Coverage(snapshot.DomainAccounts)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 256}}, accounts,
		"accounts coverage should match fixture range")

	reqType := reflect.TypeOf(flow.DownloadRequested{})
	require.Equal(t, 0, node.Bus.CountOfType(reqType),
		"no DownloadRequested should fire without peers")
}

// TestRealistic_TwoNode_GapFill is the realistic counterpart to
// TestTwoNode_GapFillMergedOnly. The peer is driven through the target's
// sentry.Provider, and downloads flow through the production Downloader
// component (backed by InprocClient — no network).
func TestRealistic_TwoNode_GapFill(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewRealNode(t, nil, coord)
	t.Cleanup(func() { _ = leecher.Close() })

	var (
		manifestCount atomic.Int32
		requestCount  atomic.Int32
		completeCount atomic.Int32
		promotedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadRequested) { requestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadComplete) { completeCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	require.NoError(t, leecher.Start(context.Background()))

	peer := harness.NewInprocPeer("peer-A", coord)
	peer.Seed(harness.HoodiBaseline())
	peer.AnnounceTo(leecher.Sentry)

	expectedFiles := int32(harness.HoodiBaseline().FileCount())
	waitForRealistic(t, func() bool { return promotedCount.Load() == expectedFiles },
		3*time.Second, "all files promoted to TrustVerified")

	require.Equal(t, int32(1), manifestCount.Load(), "PeerManifestReceived should fire once")
	require.Equal(t, expectedFiles, requestCount.Load(), "one DownloadRequested per peer file")
	require.Equal(t, expectedFiles, completeCount.Load(), "every request should complete")

	for _, d := range peer.Inventory.Domains() {
		peerCov := peer.Inventory.Coverage(d)
		leecherCov := leecher.Inventory.CoverageAtTrust(d, snapshot.TrustVerified)
		require.Equal(t, peerCov, leecherCov, "coverage mismatch for domain %s", d)
	}

	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})),
		"no DownloadFailed expected")
	require.Equal(t, int(expectedFiles), len(leecher.Storage.Recorded()),
		"MockStorage.RecordFile should have been invoked once per completed download")
}

// TestRealistic_ThreeNode_MergeDivergence is the realistic counterpart to
// TestThreeNode_MergeDivergence. Both peers announce through the leecher's
// sentry; downloads flow through the real Downloader component.
func TestRealistic_ThreeNode_MergeDivergence(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewRealNode(t, nil, coord)
	t.Cleanup(func() { _ = leecher.Close() })

	require.NoError(t, leecher.Start(context.Background()))

	merged := harness.NewInprocPeer("peer-merged", coord)
	merged.Seed(harness.HoodiMerged500())

	unmerged := harness.NewInprocPeer("peer-unmerged", coord)
	unmerged.Seed(harness.HoodiUnmerged500())

	merged.AnnounceTo(leecher.Sentry)
	unmerged.AnnounceTo(leecher.Sentry)

	mergedFiles := harness.HoodiMerged500().FileCount()
	waitForRealistic(t, func() bool {
		return leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadComplete{})) == mergedFiles
	}, 3*time.Second, "merged peer's downloads to complete")

	requestCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadRequested{}))
	require.Equal(t, mergedFiles, requestCount,
		"leecher should request only the merged peer's files")

	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})),
		"no downloads should fail")

	accountsCov := leecher.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 500}}, accountsCov,
		"accounts coverage should match the merged peer's single range")

	accountsFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, accountsFiles, 2, "expected 2 merged accounts files locally")
}

// TestRealistic_Breadth_EmptyAndPartialManifests verifies the realistic path
// tolerates peers with empty or partial manifests without spurious events.
func TestRealistic_Breadth_EmptyAndPartialManifests(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewRealNode(t, nil, coord)
	t.Cleanup(func() { _ = leecher.Close() })

	require.NoError(t, leecher.Start(context.Background()))

	// Empty peer — no inventory at all.
	empty := harness.NewInprocPeer("peer-empty", coord)
	empty.AnnounceTo(leecher.Sentry)

	// After processing an empty manifest, no gap-fill should have fired.
	leecher.Bus.WaitAsync()
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadRequested{})),
		"empty manifest should not trigger any downloads")
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})))
}

// TestRealistic_SoakLite_Churn is a downsized soak scenario: announce,
// verify, depart, repeat over a small number of iterations to exercise the
// sentry peer lifecycle against the flow orchestrator's pending-map cleanup.
func TestRealistic_SoakLite_Churn(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewRealNode(t, nil, coord)
	t.Cleanup(func() { _ = leecher.Close() })

	require.NoError(t, leecher.Start(context.Background()))

	const iterations = 5
	fixture := harness.HoodiBaseline()
	expectedPerIter := fixture.FileCount()

	var completeCount atomic.Int32
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadComplete) { completeCount.Add(1) }))

	for i := 0; i < iterations; i++ {
		peer := harness.NewInprocPeer("peer-soak", coord)
		peer.Seed(fixture)
		peer.AnnounceTo(leecher.Sentry)

		// First iteration triggers downloads; subsequent iterations re-announce
		// the same inventory, but the files are already local so no new
		// downloads are requested.
		if i == 0 {
			waitForRealistic(t, func() bool {
				return completeCount.Load() == int32(expectedPerIter)
			}, 3*time.Second, "initial downloads to complete")
		} else {
			leecher.Bus.WaitAsync()
		}

		peer.Depart(leecher.Sentry)
		leecher.Bus.WaitAsync()
	}

	// Pending map must drain fully across the churn — no stuck downloads.
	require.Equal(t, 0, leecher.Orch.PendingCount(), "pending map must drain")
	require.Equal(t, int32(expectedPerIter), completeCount.Load(),
		"re-announcing the same inventory must not re-download")
}
