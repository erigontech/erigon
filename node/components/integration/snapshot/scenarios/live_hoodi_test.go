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
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// envLiveHoodiSnapshotsDir points at a real erigon snapshots directory
// (typically <datadir>/snapshots). When unset the live-Hoodi tests skip, so
// CI does not depend on the availability of pre-synced data.
//
// To run locally:
//
//	LIVE_HOODI_SNAPSHOTS_DIR=$HOME/hoodi-datadir/snapshots \
//	    go test ./node/components/integration/snapshot/scenarios/... \
//	        -run TestLiveHoodi
//
// For the test to produce unmerged-tip fixtures, run erigon with the
// NO_MERGE=1 environment variable — the aggregator's merge loop
// short-circuits, leaving unmerged step files on disk for the snapshot
// distribution flow to rationalise across peers.
const envLiveHoodiSnapshotsDir = "LIVE_HOODI_SNAPSHOTS_DIR"

// TestLiveHoodi_TipFixtures exercises the M4 gate: real Hoodi snapshot files
// on disk drive a FakePeer, and a simulated leecher must reach full coverage
// over them. The scenario is intentionally a superset of the two-node happy
// path — the live peer replaces the procedural HoodiBaseline fixture, which
// means the event-flow contract has to survive against the shape of a real
// running node rather than a hand-built abstraction.
//
// Opt-in via LIVE_HOODI_SNAPSHOTS_DIR. CI skips by default.
func TestLiveHoodi_TipFixtures(t *testing.T) {
	snapshotsDir := os.Getenv(envLiveHoodiSnapshotsDir)
	if snapshotsDir == "" {
		t.Skipf("set %s to a real erigon snapshots directory to run this test", envLiveHoodiSnapshotsDir)
	}

	fixture, err := harness.LoadLiveFixture("live_hoodi", snapshotsDir)
	require.NoError(t, err, "load live fixture")
	require.Greater(t, fixture.FileCount(), 0, "live fixture should not be empty")

	t.Logf("live fixture loaded: %d files across %d domains + %d blocks",
		fixture.FileCount(), len(fixture.Domains), len(fixture.Blocks))

	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	require.NoError(t, leecher.Start(context.Background()))

	peer := harness.NewFakePeer("live-hoodi", coord)
	peer.Seed(fixture)
	peer.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	// A real datadir contains overlapping-range files (merged + unmerged
	// variants of the same data), so the orchestrator legitimately skips
	// some peer entries via its role-coverage dedup. The invariants that
	// still must hold end-to-end:
	//
	//   - at least one DownloadRequested fired
	//   - every DownloadRequested resolves — no DownloadFailed
	//   - DownloadRequested count equals DownloadComplete count
	//   - per-domain coverage at TrustVerified equals the live peer's full
	//     coverage (the leecher ends up with the same data, possibly at
	//     different file granularity)
	requestCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadRequested{}))
	require.Greater(t, requestCount, 0, "leecher should request at least one file")
	completeCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadComplete{}))
	require.Equal(t, requestCount, completeCount,
		"every request should complete against the live coordinator")
	failedCount := leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{}))
	require.Equal(t, 0, failedCount, "no downloads should fail")

	for _, d := range peer.Inventory.Domains() {
		peerCov := peer.Inventory.Coverage(d)
		leecherCov := leecher.Inventory.CoverageAtTrust(d, snapshot.TrustVerified)
		require.Equal(t, peerCov, leecherCov, "coverage mismatch for domain %s", d)
	}
}
