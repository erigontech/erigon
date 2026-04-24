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

// Real-DevP2P variant: each node runs a real p2p.Server, peers discover
// each other via static-peer dial, sentry.PublishPeerConnected fires
// automatically from the DevP2P peer-add event — no manual wiring from
// the test. The torrent layer carries real bytes as in p2p_twonode_test.

package scenarios_test

import (
	"context"
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

// TestP2P_TwoNode_RealDevP2PDiscovery brings up real p2p.Server
// listeners on both nodes, lets them discover each other via static
// peer dial, and verifies the full snapshot-flow lifecycle fires from
// the real peer-add event — no manual PublishPeerConnected from the
// test.
func TestP2P_TwoNode_RealDevP2PDiscovery(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	seeder := harness.NewP2PNode(t, logger)
	leecher := harness.NewP2PNode(t, logger)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Real DevP2P listeners on each side. The listener forwards the
	// p2p.Server's peer-add event to sentry.PublishPeerConnected. The
	// listener also holds the peer's full *enode.Node so our
	// ChainToml ENR entries survive the handshake.
	seederListener := harness.StartDevP2PListener(t, ctx, seeder.Sentry, logger)
	t.Cleanup(seederListener.Close)
	leecherListener := harness.StartDevP2PListener(t, ctx, leecher.Sentry, logger)
	t.Cleanup(leecherListener.Close)

	// Seeder: write a 2 MiB fixture, seed it, publish V2 manifest with
	// the real infohash.
	fixtureName := "v1.0-accounts.0-2048.kv"
	fixtureContent := multiPieceFixtureBytes("seeder-DevP2P", 2<<20)
	fixtureHash := seeder.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, 0, 2048)
	v2Hash := seeder.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	// Seeder exposes its BT port + V2 infohash via ENR. These go on the
	// live LocalNode so the signed record the handshake delivers
	// actually carries them.
	_, btPort := seeder.LocalTorrentAddr()
	seederListener.SetENREntry(enr.ChainToml{
		AuthoritativeBlocks: 0,
		KnownBlocks:         0,
		InfoHash:            v2Hash,
		DomainSteps:         2048,
		MergeDepth:          2048,
	})
	seederListener.SetENREntry(enr.BT(btPort))

	// Leecher still needs the seeder's BT address so its torrent client
	// can fetch fixture files. The DevP2P listener doesn't drive the
	// torrent layer — static-peer plumbing for BT is separate.
	leecher.AddSeederPeer(seeder)

	// Observers to assert the downstream pipeline fires.
	var (
		manifestCount atomic.Int32
		promotedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	// The leecher connects to the seeder via real DevP2P dial. On
	// successful handshake, the leecher's listener receives
	// PeerEventTypeAdd → sentry.PublishPeerConnected → manifest_exchange
	// onPeerConnected → fetches chain.toml.v2 via torrent → flow
	// orchestrator publishes DownloadRequested → downloader hauls the
	// fixture across → TrustPromoted.
	leecherListener.AddStaticPeer(seederListener.Self())

	// DevP2P handshake + subsequent torrent fetch; loopback is fast but
	// still not instant. Allow 30s.
	waitForP2P(t, func() bool { return promotedCount.Load() >= 1 },
		30*time.Second, "TrustPromoted via real DevP2P discovery")

	require.Equal(t, int32(1), manifestCount.Load(), "one PeerManifestReceived from real handshake")
	require.Equal(t, int32(1), promotedCount.Load(), "one TrustPromoted after real transfer")

	// Byte-equality: prove real data transferred, not just metadata.
	leecherPath := filepath.Join(leecher.Dirs.Snap, fixtureName)
	leecherBytes, err := os.ReadFile(leecherPath)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(fixtureContent), sha256.Sum256(leecherBytes))

	// Leecher's inventory matches seeder's hash.
	localFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, localFiles, 1)
	require.Equal(t, fixtureHash, localFiles[0].TorrentHash)
	require.Equal(t, snapshot.TrustVerified, localFiles[0].Trust)
}
