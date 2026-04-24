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

// Real-P2P tests that move actual bytes through the full component loop.
// These tests do NOT run in CI — they bring up real anacrolix/torrent
// clients on loopback, use static peering (DHT + trackers off), and
// measure real transport timing.
//
// Invocation:
//
//	go test -tags=p2p_integration -timeout 10m \
//	  ./node/components/integration/snapshot/scenarios/...

package scenarios_test

import (
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// waitForP2P polls cond with a generous budget. Torrent startup on
// loopback can take a few seconds depending on the platform.
func waitForP2P(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

// TestP2P_TwoNode_ChainTomlV2Exchange is the minimum proof that the
// real-torrent lifecycle composes:
//
//  1. Seeder writes a fixture file to disk, seeds it, builds its V2
//     manifest, seeds chain.toml.v2.
//  2. Leecher is told about the seeder's BT address (static peer).
//  3. Leecher receives a sentry.PeerConnected carrying the seeder's ENR
//     (ChainToml{V2 infohash + DomainSteps + MergeDepth}).
//  4. manifest_exchange fetches chain.toml.v2 via real torrent → parses
//     → publishes flow.PeerManifestReceived.
//  5. Flow orchestrator computes gaps → publishes flow.DownloadRequested
//     for the fixture file.
//  6. Downloader bus handler calls Client.Download → real torrent
//     transfer → flow.DownloadComplete.
//  7. Orchestrator records file at TrustVerified + publishes TrustPromoted.
//
// Assertion: leecher's inventory contains the fixture file with the same
// torrent hash the seeder advertised, at TrustVerified.
func TestP2P_TwoNode_ChainTomlV2Exchange(t *testing.T) {
	// Use a real logger so we can see what the real components are doing;
	// the p2p_integration tag already implies verbose diagnosis is fine.
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	seeder := harness.NewP2PNode(t, logger)
	leecher := harness.NewP2PNode(t, logger)

	// Seeder: write a real fixture file and start seeding it.
	fixtureName := "v1.0-accounts.0-2048.kv"
	fixtureContent := []byte(
		"# fake accounts domain file — real bytes for real piece hashes\n" +
			"# size: roughly 180 bytes, single piece under default chunk size\n" +
			"account-0=0x01,account-1=0x02,account-2=0x03,account-3=0x04,account-4=0x05")
	fixtureHash := seeder.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, 0, 2048)
	require.NotEqual(t, [20]byte{}, fixtureHash, "seeder must produce a non-zero infohash for the fixture")

	// Seeder: publish its V2 manifest (chain.toml.v2 + seed). This is
	// what the leecher will fetch via manifest_exchange.
	v2Hash := seeder.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	// Leecher: add the seeder as a static BT peer so new torrents it
	// creates can reach the seeder without DHT / trackers.
	leecher.AddSeederPeer(seeder)

	// Subscribe leecher-side observers for the assertion budget.
	var (
		manifestCount  atomic.Int32
		promotedCount  atomic.Int32
		requestedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadRequested) { requestedCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	// Construct the seeder's synthetic ENR carrying the V2 chain-toml
	// entry and a BT port pointer. This is what a V2-ENR discovery
	// adapter would build from a discovered peer.
	_, btPort := seeder.LocalTorrentAddr()
	seederIP := net.IPv4(127, 0, 0, 1)
	seederKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	var rec enr.Record
	rec.Set(enr.ChainToml{
		AuthoritativeBlocks: 0,
		KnownBlocks:         0,
		InfoHash:            v2Hash,
		DomainSteps:         2048,
		MergeDepth:          2048,
	})
	rec.Set(enr.BT(btPort))
	rec.Set(enr.IP(seederIP))
	require.NoError(t, enode.SignV4(&rec, seederKey))
	seederNode, err := enode.New(enode.ValidSchemes, &rec)
	require.NoError(t, err)

	// Drive the production consumer path: fire PeerConnected on the
	// leecher's bus. manifest_exchange subscribes, sees the ENR, fetches
	// chain.toml.v2 via torrent, publishes PeerManifestReceived, which
	// the flow orchestrator turns into DownloadRequested, which the
	// downloader bus-handler turns into a real Client.Download call.
	leecher.Sentry.PublishPeerConnected(seederNode)

	// Wait for the full loop: fixture promoted to TrustVerified on leecher.
	waitForP2P(t, func() bool { return promotedCount.Load() >= 1 },
		30*time.Second, "fixture to be promoted to TrustVerified on leecher")

	// The one-file scenario: exactly one manifest received, one request
	// issued, one file promoted.
	require.Equal(t, int32(1), manifestCount.Load(), "one PeerManifestReceived")
	require.Equal(t, int32(1), requestedCount.Load(), "one DownloadRequested for the single fixture")
	require.Equal(t, int32(1), promotedCount.Load(), "one TrustPromoted after completion")

	// Leecher's inventory should now carry the fixture at TrustVerified
	// with the same torrent hash the seeder published.
	localFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, localFiles, 1, "leecher should have exactly one accounts file locally")
	require.Equal(t, fixtureName, localFiles[0].Name)
	require.Equal(t, fixtureHash, localFiles[0].TorrentHash,
		"leecher's local file must share the seeder's torrent hash")
	require.Equal(t, snapshot.TrustVerified, localFiles[0].Trust)
}
