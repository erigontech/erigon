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
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

// The soak-lite tests exercise the orchestrator and harness under repeated /
// adversarial loads on the SimulatedTransport. They do not need a network or
// real file bytes — their goal is to catch leaks and state-management bugs
// in the flow logic before that logic sees real transport surprises.
//
// All tests skip under `go test -short` (and therefore under `make test-short`)
// so the fast lane stays fast; `make test-all` includes them.

// TestSoakLite_GapFillLoop runs the two-node gap-fill scenario in a tight
// loop against fresh Nodes. Asserts that:
//
//   - Every iteration completes successfully (no accumulated failure).
//   - Goroutine count at the end is not materially higher than at the start.
//   - No Node leaks its worker pool or handler subscriptions.
func TestSoakLite_GapFillLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("soak-lite tests skipped under -short")
	}

	const iterations = 200

	runtime.GC()
	beforeGoroutines := runtime.NumGoroutine()

	for i := 0; i < iterations; i++ {
		runGapFillIteration(t, i)
	}

	runtime.GC()
	afterGoroutines := runtime.NumGoroutine()

	// Allow small drift — the workerpool and event bus may keep a handful
	// of goroutines alive across iterations. Reject obvious leaks (tens of
	// goroutines per iteration).
	require.LessOrEqual(t, afterGoroutines, beforeGoroutines+20,
		"goroutine count grew unboundedly across %d iterations: before=%d after=%d",
		iterations, beforeGoroutines, afterGoroutines)
}

func runGapFillIteration(t *testing.T, i int) {
	t.Helper()
	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	defer func() {
		if err := leecher.Close(); err != nil {
			t.Fatalf("iter %d: close: %v", i, err)
		}
	}()
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	require.NoError(t, leecher.Start(context.Background()))

	peer := harness.NewFakePeer(fmt.Sprintf("peer-%d", i), coord)
	peer.Seed(harness.HoodiBaseline())
	peer.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	require.Equal(t, 0, leecher.Orch.PendingCount(),
		"iter %d: pending should drain to zero after the scenario completes", i)
}

// TestSoakLite_PeerChurn sends repeated announce / PeerDeparted cycles at a
// single Node from many distinct peers and asserts that the orchestrator's
// peer-file cache does not grow without bound beyond what the fixture would
// require.
//
// Current orchestrator semantics key peerFiles by file name across all peers
// — so advertising the same HoodiBaseline from N peers ends with peerFiles
// equal to the fixture size, not N × fixture size. This test locks that in.
func TestSoakLite_PeerChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("soak-lite tests skipped under -short")
	}

	const peerCount = 100

	coord := harness.NewCoordinator()
	node := harness.NewNode(nil)
	t.Cleanup(func() { _ = node.Close() })
	require.NoError(t, node.AttachSimulatedTransport(coord))
	require.NoError(t, node.Start(context.Background()))

	for i := 0; i < peerCount; i++ {
		peer := harness.NewFakePeer(fmt.Sprintf("peer-%d", i), coord)
		peer.Seed(harness.HoodiBaseline())
		peer.AnnounceTo(node)
		node.Bus.Publish(flow.PeerDeparted{PeerID: peer.ID})
	}

	node.Bus.WaitAsync()

	baseline := harness.HoodiBaseline().FileCount()
	require.Equal(t, baseline, node.Orch.PeerFilesCount(),
		"peerFiles should cap at fixture size regardless of peer churn")
	require.Equal(t, 0, node.Orch.PendingCount(),
		"pending should be zero after churn completes")
}

// TestSoakLite_RandomFailures installs a predicate that fails ~40% of
// downloads and asserts that:
//
//   - Failed downloads are observed as DownloadFailed events.
//   - Pending cleanup runs on failure (the orchestrator clears the entry
//     so retries are not blocked by the role-coverage check).
//   - When the peer re-announces with failures disabled, the leecher
//     recovers full coverage.
func TestSoakLite_RandomFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("soak-lite tests skipped under -short")
	}

	coord := harness.NewCoordinator()

	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	sim, ok := leecher.Transport.(*harness.SimulatedTransport)
	require.True(t, ok, "simulated transport expected on leecher")

	rng := rand.New(rand.NewPCG(1, 2))
	sim.SetFailFn(func(name string) bool {
		// ~40% failure rate.
		return rng.IntN(10) < 4
	})

	require.NoError(t, leecher.Start(context.Background()))

	peer := harness.NewFakePeer("flaky", coord)
	peer.Seed(harness.HoodiBaseline())
	peer.AnnounceTo(leecher)
	leecher.Bus.WaitAsync()

	failedType := reflect.TypeOf(flow.DownloadFailed{})
	failed := leecher.Bus.CountOfType(failedType)
	require.Greater(t, failed, 0, "expected some failures under fault injection")

	require.Equal(t, 0, leecher.Orch.PendingCount(),
		"every failure should clear its pending entry")

	// Failures disabled; re-announce triggers retries for the files that
	// failed on the first pass.
	sim.SetFailFn(nil)
	peer.AnnounceTo(leecher)
	leecher.Bus.WaitAsync()

	baseline := harness.HoodiBaseline().FileCount()
	completeType := reflect.TypeOf(flow.DownloadComplete{})
	complete := leecher.Bus.CountOfType(completeType)
	require.GreaterOrEqual(t, complete, baseline,
		"after re-announce without failures, every file should eventually complete")
	require.Equal(t, 0, leecher.Orch.PendingCount(),
		"pending should be zero after recovery completes")
}
