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
// clients on loopback, real sentry.Provider + eth/68 DevP2P listeners,
// and static peering (DHT + trackers off), and measure real transport
// timing.
//
// Invocation:
//
//	go test -tags=p2p_integration -timeout 10m \
//	  ./node/components/integration/snapshot/scenarios/...

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

// waitForP2P polls cond with a generous budget. Torrent startup and
// DevP2P handshake on loopback can take a few seconds depending on the
// platform.
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

// TestP2P_TwoNode_ChainTomlV2Exchange runs the full real-stack lifecycle:
//
//  1. Seeder writes a 2 MiB fixture, seeds it + chain.toml.v2 via real
//     anacrolix/torrent, advertises the V2 infohash + BT port on its
//     signed ENR via the real sentry LocalNode.
//  2. Leecher statically peers with the seeder at both the BitTorrent
//     and DevP2P layers. DevP2P discovery is stubbed (NoDiscovery=true)
//     but the handshake, eth/68 status exchange, and peer-event
//     auto-wire are all real.
//  3. Real RLPx handshake completes → sentry's peer-event observer
//     publishes sentry.PeerConnected → manifest_exchange fetches
//     chain.toml.v2 via torrent → parses → publishes
//     flow.PeerManifestReceived → gap-fill → real piece-hashed transfer
//     → TrustPromoted.
//  4. Byte-equality of the leecher's on-disk fixture against the
//     seeder's source confirms real data moved, not just metadata.
func TestP2P_TwoNode_ChainTomlV2Exchange(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	seeder := harness.NewP2PNode(t, logger)
	leecher := harness.NewP2PNode(t, logger)

	// Seeder: write a real fixture file and start seeding it. 2 MiB
	// spans multiple torrent pieces — exercising piece scheduling,
	// hash verification across pieces, and non-trivial transfer
	// duration even on loopback.
	fixtureName := "v1.0-accounts.0-2048.kv"
	const fixtureSize = 2 << 20
	fixtureContent := multiPieceFixtureBytes("seeder-A", fixtureSize)
	fixtureHash := seeder.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, 0, 2048)
	require.NotEqual(t, [20]byte{}, fixtureHash)

	v2Hash := seeder.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	// Seeder publishes its V2 chain-toml + BT port on its live signed
	// ENR via real sentry LocalNode. Any peer connecting afterwards
	// sees these in the handshake.
	_, btPort := seeder.LocalTorrentAddr()
	seeder.SetDevP2PENREntry(enr.ChainToml{
		AuthoritativeBlocks: 0,
		KnownBlocks:         0,
		InfoHash:            v2Hash,
		DomainSteps:         2048,
		MergeDepth:          2048,
	})
	seeder.SetDevP2PENREntry(enr.BT(btPort))

	// Leecher needs BT static peers for the fixture download (BT and
	// DevP2P peering are separate networks).
	leecher.AddSeederPeer(seeder)

	// Observers for the assertion budget.
	var (
		manifestCount  atomic.Int32
		promotedCount  atomic.Int32
		requestedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.DownloadRequested) { requestedCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	// Trigger the whole production lifecycle with one static-peer dial.
	// Leecher dials seeder → RLPx → eth/68 status exchange → sentry's
	// peer-event observer fires → PublishPeerConnected → manifest
	// fetch → gap-fill → file transfer → TrustPromoted.
	leecher.AddDevP2PPeer(seeder.DevP2PSelf())

	waitForP2P(t, func() bool { return promotedCount.Load() >= 1 },
		30*time.Second, "fixture promoted to TrustVerified on leecher")

	require.Equal(t, int32(1), manifestCount.Load(), "one PeerManifestReceived")
	require.Equal(t, int32(1), requestedCount.Load(), "one DownloadRequested for the single fixture")
	require.Equal(t, int32(1), promotedCount.Load(), "one TrustPromoted after completion")

	localFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, localFiles, 1)
	require.Equal(t, fixtureName, localFiles[0].Name)
	require.Equal(t, fixtureHash, localFiles[0].TorrentHash)
	require.Equal(t, snapshot.TrustVerified, localFiles[0].Trust)

	// Byte-equality: the only assertion that proves real data moved.
	leecherPath := filepath.Join(leecher.Dirs.Snap, fixtureName)
	leecherBytes, err := os.ReadFile(leecherPath)
	require.NoError(t, err)
	require.Equal(t, len(fixtureContent), len(leecherBytes))
	require.Equal(t, sha256.Sum256(fixtureContent), sha256.Sum256(leecherBytes))
}
