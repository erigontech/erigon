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
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
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

// multiPieceFixtureBytes builds a deterministic multi-piece content blob
// so the torrent layer exercises real piece scheduling, not single-piece
// edge cases. Default anacrolix chunk size is 16 KiB, piece size for
// bit-sized manifests defaults higher — pick a blob that spans several
// pieces at any reasonable piece size.
func multiPieceFixtureBytes(peerID string, sizeBytes int) []byte {
	seed := sha256.Sum256([]byte(peerID + ":fixture"))
	out := make([]byte, sizeBytes)
	// Fill with a repeating hash chain so content is deterministic and
	// non-compressible (avoids anacrolix shortcuts on uniform data).
	block := seed[:]
	for i := 0; i < sizeBytes; i += len(block) {
		end := i + len(block)
		if end > sizeBytes {
			end = sizeBytes
		}
		copy(out[i:end], block[:end-i])
		next := sha256.Sum256(block)
		block = next[:]
	}
	return out
}

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

	// Seeder: write a real fixture file and start seeding it. 2 MiB so
	// the file spans multiple torrent pieces — exercising piece
	// scheduling, hash verification across pieces, and non-trivial
	// transfer duration even on loopback.
	fixtureName := "v1.0-accounts.0-2048.kv"
	const fixtureSize = 2 << 20 // 2 MiB
	fixtureContent := multiPieceFixtureBytes("seeder-A", fixtureSize)
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

	// Bytes actually moved: the leecher's on-disk copy must be exactly
	// the seeder's fixture content. This is the only assertion that
	// proves real data transferred over the wire — the torrent hash and
	// TrustPromoted can both fire from metadata alone without a byte
	// changing hands.
	leecherPath := filepath.Join(leecher.Dirs.Snap, fixtureName)
	leecherBytes, err := os.ReadFile(leecherPath)
	require.NoError(t, err, "leecher must have the fixture on disk")
	require.Equal(t, len(fixtureContent), len(leecherBytes),
		"leecher's fixture file size must match seeder's")
	require.Equal(t, sha256.Sum256(fixtureContent), sha256.Sum256(leecherBytes),
		"leecher's fixture content must be byte-identical to seeder's")
}
