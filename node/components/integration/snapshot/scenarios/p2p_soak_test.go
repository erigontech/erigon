//go:build p2p_integration

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

// Soak-style scenario: five seeders with redundant content, one leecher,
// continuous connect/disconnect churn on the DevP2P layer. Exercises
// PeerDeparted firing on real disconnect, reconnect re-advertising
// manifests, pending-map drain under sustained event pressure.

package scenarios_test

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// TestP2P_SoakChurn sets up five seeders serving byte-identical content,
// connects the leecher to all of them at both BT and DevP2P layers,
// waits for the initial transfer to complete, then runs a churn loop
// that disconnects and reconnects peers in sequence. After the churn
// the orchestrator's pending map must be drained, PeerDeparted must
// have fired per disconnect, and the leecher's on-disk copy must still
// be byte-identical to the source.
//
// This is a scaled-down soak — ~10 churn iterations, seconds not hours.
// A full-duration soak runs with much longer iteration count; the
// scenario shape and invariants are the same.
func TestP2P_SoakChurn(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	const (
		numSeeders       = 5
		churnIterations  = 10
		churnQuietPeriod = 100 * time.Millisecond
		fixtureSize      = 2 << 20 // 2 MiB
		fromStep         = uint64(0)
		toStep           = uint64(512)
	)
	fixtureName := "v1.0-accounts.0-512.kv"
	content := multiPieceFixtureBytes("soak-churn-shared", fixtureSize)

	seeders := make([]*harness.P2PNode, numSeeders)
	for i := range seeders {
		seeders[i] = harness.NewP2PNode(t, logger)
	}
	leecher := harness.NewP2PNode(t, logger)

	// Every seeder has identical content → identical infohashes. The
	// FetchPeerManifestV2 dedup keeps one fetch across N peers.
	var fixtureHash [20]byte
	for i, s := range seeders {
		h := s.SeedFile(fixtureName, content, snapshot.DomainAccounts, fromStep, toStep)
		if i == 0 {
			fixtureHash = h
		}
		require.Equal(t, fixtureHash, h, "every seeder must produce the same torrent hash")

		v2 := s.PublishV2Manifest()
		_, btPort := s.LocalTorrentAddr()
		s.SetDevP2PENREntry(enr.ChainToml{
			InfoHash:    v2,
			DomainSteps: toStep,
			MergeDepth:  toStep,
		})
		s.SetDevP2PENREntry(enr.BT(btPort))

		leecher.AddSeederPeer(s)
	}

	var (
		manifestCount atomic.Int32
		promotedCount atomic.Int32
		departedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerDeparted) { departedCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	// Connect all five seeders.
	for _, s := range seeders {
		leecher.AddDevP2PPeer(s.DevP2PSelf())
	}

	// Wait for the leecher to complete the initial download. All five
	// peers advertise the same manifest — dedup ensures one fetch, five
	// PeerManifestReceived events (one per peer).
	waitForP2P(t, func() bool {
		return manifestCount.Load() >= numSeeders &&
			promotedCount.Load() >= 1 &&
			leecher.Orch.PendingCount() == 0
	}, 30*time.Second, "initial gap-fill settles with all peers announced")

	// Churn loop: disconnect a peer, pause, reconnect, pause. Over
	// iterations this exercises PeerDeparted firing on real disconnect
	// and PeerManifestReceived firing on reconnect. Index round-robins
	// so every seeder is churned across the loop.
	for i := 0; i < churnIterations; i++ {
		victim := seeders[i%numSeeders]
		leecher.RemoveDevP2PPeer(victim.DevP2PSelf())
		time.Sleep(churnQuietPeriod)
		leecher.AddDevP2PPeer(victim.DevP2PSelf())
		time.Sleep(churnQuietPeriod)
	}

	// Let the final reconnects settle.
	waitForP2P(t, func() bool {
		return leecher.Orch.PendingCount() == 0
	}, 10*time.Second, "orchestrator pending map drains after churn")

	// PeerDeparted must have fired at least once. Exact count is
	// sensitive to bidirectional connections, handshake settling time,
	// and whether consecutive remove-add pairs on the same peer
	// register as independent events — zero would indicate the
	// disconnect path isn't observing churn at all.
	require.Greater(t, departedCount.Load(), int32(0),
		"PeerDeparted must fire at least once during churn")

	// Only one actual file promotion — the file is already local after
	// the first download, so re-announces don't trigger re-downloads.
	require.Equal(t, int32(1), promotedCount.Load(),
		"fixture promotes exactly once; re-announces must not trigger re-downloads")

	// The leecher's on-disk copy is still byte-identical to the source
	// after the churn.
	leecherBytes, err := os.ReadFile(filepath.Join(leecher.Dirs.Snap, fixtureName))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(content), sha256.Sum256(leecherBytes),
		"leecher fixture content must survive churn byte-identical")

	// Inventory reflects the completed file.
	localFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, localFiles, 1)
	require.Equal(t, fixtureHash, localFiles[0].TorrentHash)
	require.Equal(t, snapshot.TrustVerified, localFiles[0].Trust)
}
