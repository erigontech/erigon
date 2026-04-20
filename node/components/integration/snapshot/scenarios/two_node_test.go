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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestTwoNode_GapFillMergedOnly runs the M2 gate scenario:
//
//  1. Leecher node B starts empty.
//  2. FakePeer A is seeded with HoodiBaseline and announces its manifest
//     on B's bus.
//  3. B's orchestrator computes gaps vs A's coverage and emits
//     DownloadRequested for every missing file.
//  4. B's SimulatedTransport responds to each request by looking up the
//     info-hash in the shared Coordinator and publishing DownloadComplete.
//  5. B's orchestrator adds each completed download to B's inventory at
//     TrustVerified and publishes TrustPromoted.
//
// Assertions:
//   - PeerManifestReceived fires exactly once on B.
//   - DownloadRequested count matches A's total file count.
//   - DownloadComplete count matches DownloadRequested count (no failures).
//   - TrustPromoted count matches DownloadComplete count.
//   - B's coverage per domain at TrustVerified equals A's.
func TestTwoNode_GapFillMergedOnly(t *testing.T) {
	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))

	var (
		manifestCount atomic.Int32
		requestCount  atomic.Int32
		completeCount atomic.Int32
		promotedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) {
		manifestCount.Add(1)
	}))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadRequested) {
		requestCount.Add(1)
	}))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadComplete) {
		completeCount.Add(1)
	}))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) {
		promotedCount.Add(1)
	}))

	require.NoError(t, leecher.Start(context.Background()))

	// Seed the peer and announce.
	peer := harness.NewFakePeer("peer-A", coord)
	peer.Seed(harness.HoodiBaseline())
	peer.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	expectedFiles := harness.HoodiBaseline().FileCount()

	require.Equal(t, int32(1), manifestCount.Load(), "PeerManifestReceived should fire once")
	require.Equal(t, int32(expectedFiles), requestCount.Load(), "one DownloadRequested per peer file")
	require.Equal(t, int32(expectedFiles), completeCount.Load(), "every request should complete")
	require.Equal(t, int32(expectedFiles), promotedCount.Load(), "every completion should promote trust")

	// Leecher's TrustVerified coverage matches peer's full coverage, per domain.
	for _, d := range peer.Inventory.Domains() {
		peerCov := peer.Inventory.Coverage(d)
		leecherCov := leecher.Inventory.CoverageAtTrust(d, snapshot.TrustVerified)
		require.Equal(t, peerCov, leecherCov, "coverage mismatch for domain %s", d)
	}

	// No DownloadFailed events.
	failedType := reflect.TypeOf(flow.DownloadFailed{})
	require.Equal(t, 0, leecher.Bus.CountOfType(failedType))
}

// TestTwoNode_NoGapNoDownload verifies that if the leecher already has the
// peer's coverage, no DownloadRequested events fire — gap analysis must
// short-circuit correctly.
func TestTwoNode_NoGapNoDownload(t *testing.T) {
	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))

	// Leecher already has the baseline content locally.
	harness.HoodiBaseline().Apply(leecher.Inventory)

	require.NoError(t, leecher.Start(context.Background()))

	// Peer announces the same coverage.
	peer := harness.NewFakePeer("peer-A", coord)
	peer.Seed(harness.HoodiBaseline())
	peer.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	requestType := reflect.TypeOf(flow.DownloadRequested{})
	require.Equal(t, 0, leecher.Bus.CountOfType(requestType),
		"no DownloadRequested should fire when the leecher already has the coverage")
}
