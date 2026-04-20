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
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

// TestSoakTorrent_RepeatedTransfer runs the two-node torrent scenario in a
// loop against fresh Node + TorrentTransport pairs and asserts that the
// process goroutine count does not grow unboundedly. Each iteration
// creates and tears down two torrent clients, so any client-side
// shutdown-leak surfaces quickly.
//
// Skipped under -short. anacrolix/torrent keeps background goroutines
// alive per torrent (trackers, peer handshakes, IO dispatch); the drift
// tolerance reflects that.
func TestSoakTorrent_RepeatedTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("torrent-soak tests skipped under -short")
	}

	const iterations = 15

	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < iterations; i++ {
		runTorrentSoakIteration(t, i)
	}

	// anacrolix/torrent Client.Close returns before its background
	// goroutines fully exit — trackers, peer connections, dispatch loops
	// all drain asynchronously. Poll until goroutine count settles.
	runtime.GC()
	after := waitForGoroutinesToSettle(t, before+50, 10*time.Second)

	require.LessOrEqual(t, after, before+50,
		"goroutines grew unboundedly across %d iterations: before=%d after=%d",
		iterations, before, after)
}

// waitForGoroutinesToSettle polls runtime.NumGoroutine() until it hits
// target or the deadline elapses, GC'ing between samples so finalisers
// release per-torrent resources. Returns the last observed count.
func waitForGoroutinesToSettle(t *testing.T, target int, deadline time.Duration) int {
	t.Helper()
	end := time.Now().Add(deadline)
	var last int
	for time.Now().Before(end) {
		runtime.GC()
		last = runtime.NumGoroutine()
		if last <= target {
			return last
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last
}

func runTorrentSoakIteration(t *testing.T, i int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seederNode := harness.NewNode(nil)
	seederT, err := harness.NewTorrentTransport(seederNode.Bus, t.TempDir())
	require.NoError(t, err)
	seederNode.Transport = seederT
	require.NoError(t, seederNode.Start(ctx))
	defer func() {
		if closeErr := seederNode.Close(); closeErr != nil {
			t.Fatalf("iter %d: seeder close: %v", i, closeErr)
		}
	}()

	leecherNode := harness.NewNode(nil)
	leecherT, err := harness.NewTorrentTransport(leecherNode.Bus, t.TempDir())
	require.NoError(t, err)
	leecherNode.Transport = leecherT
	require.NoError(t, leecherNode.Start(ctx))
	defer func() {
		if closeErr := leecherNode.Close(); closeErr != nil {
			t.Fatalf("iter %d: leecher close: %v", i, closeErr)
		}
	}()

	// Seed a small file unique to this iteration so the info-hash differs
	// run-to-run — avoids any cross-iteration torrent reuse inside the
	// anacrolix client layer.
	name := fmt.Sprintf("iter-%d.kv", i)
	data := []byte(fmt.Sprintf("iteration %d payload", i))
	hash, err := seederT.Seed(name, data)
	require.NoError(t, err)

	leecherT.AddPeer("seeder", "127.0.0.1", seederT.LocalPort())
	leecherNode.Bus.Publish(flow.DownloadRequested{
		FileName:  name,
		InfoHash:  hash,
		FromPeers: []string{"seeder"},
	})

	completeType := reflect.TypeOf(flow.DownloadComplete{})
	require.Eventually(t, func() bool {
		return leecherNode.Bus.CountOfType(completeType) >= 1
	}, 30*time.Second, 25*time.Millisecond, "iter %d: download did not complete", i)
}
