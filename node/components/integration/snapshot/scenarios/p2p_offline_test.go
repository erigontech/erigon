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

// Tests that verify the snapshot-flow lifecycle survives a peer dropping
// mid-scenario. The leecher has two seeders with identical content;
// after both handshakes complete and both manifests are received, one
// seeder goes offline. The fixture must still promote via the survivor.

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

// TestP2P_OnePeerOffline_SurvivorCompletes runs the "one of N seeders
// drops mid-scenario" case:
//
//  1. Two seeders (A, B) seed byte-identical content for the same
//     fixture file, at canonical range [0, 512).
//  2. Leecher static-peers with both at the DevP2P and BitTorrent
//     layers, waits until both PeerManifestReceived events have fired
//     (proof that both handshakes completed and both manifests were
//     fetched via torrent).
//  3. Seeder A is closed — its DevP2P listener stops, its torrent
//     client closes, real PeerDisconnected fires on the leecher's
//     sentry, which the flow orchestrator turns into PeerDeparted.
//  4. The leecher's torrent client falls back to seeder B for the
//     fixture file download. TrustPromoted must fire.
//  5. Byte-equality on the leecher's on-disk copy confirms the data
//     actually transferred, not just the hash.
func TestP2P_OnePeerOffline_SurvivorCompletes(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	seederA := harness.NewP2PNode(t, logger)
	seederB := harness.NewP2PNode(t, logger)
	leecher := harness.NewP2PNode(t, logger)

	// Identical content on both seeders so they share a torrent infohash
	// for the fixture; the leecher's torrent client can draw pieces from
	// either, and falling off one must not drop the transfer.
	const (
		fromStep = uint64(0)
		toStep   = uint64(512)
		size     = 2 << 20 // 2 MiB
	)
	fixtureName := "v1.0-accounts.0-512.kv"
	fixtureContent := multiPieceFixtureBytes("shared-content", size)

	fixtureHashA := seederA.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, fromStep, toStep)
	fixtureHashB := seederB.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, fromStep, toStep)
	require.Equal(t, fixtureHashA, fixtureHashB,
		"identical content must produce identical torrent hashes")

	// Two nodes with identical inventory produce a byte-identical V2
	// manifest; the generation id in the filename is the manifest's own
	// content hash, so the filenames — and the torrent infohashes that
	// cover them — match.
	v2HashA := seederA.PublishV2Manifest()
	v2HashB := seederB.PublishV2Manifest()
	require.Equal(t, v2HashA, v2HashB,
		"identical inventory must produce identical V2 manifest hashes")

	// Both seeders advertise the same V2 chain-toml entry + their own
	// BT port. Leecher will reach A's and B's BT addresses separately
	// even though the manifest is the same.
	_, btPortA := seederA.LocalTorrentAddr()
	seederA.SetDevP2PENREntry(enr.ChainToml{
		InfoHash:    v2HashA,
		DomainSteps: toStep,
		MergeDepth:  toStep,
	})
	seederA.SetDevP2PENREntry(enr.BT(btPortA))

	_, btPortB := seederB.LocalTorrentAddr()
	seederB.SetDevP2PENREntry(enr.ChainToml{
		InfoHash:    v2HashB,
		DomainSteps: toStep,
		MergeDepth:  toStep,
	})
	seederB.SetDevP2PENREntry(enr.BT(btPortB))

	// Leecher statically peers with both at BT and DevP2P layers.
	leecher.AddSeederPeer(seederA)
	leecher.AddSeederPeer(seederB)

	var (
		manifestCount atomic.Int32
		departedCount atomic.Int32
		promotedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerDeparted) { departedCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	leecher.AddDevP2PPeer(seederA.DevP2PSelf())
	leecher.AddDevP2PPeer(seederB.DevP2PSelf())

	// Wait until both manifests have been fetched. Two peers advertise
	// the same V2 infohash (the manifest is content-addressed); the
	// FetchPeerManifestV2 dedupe by hash means the second call reuses
	// the first's bytes and both PeerManifestReceived events still
	// fire, each tagged with its own peerID.
	waitForP2P(t, func() bool { return manifestCount.Load() >= 2 },
		30*time.Second, "both PeerManifestReceived events to fire")

	// Seeder A drops. Its sentry disconnects cleanly; the leecher's
	// real peer-event observer sees the disconnect and fires
	// PeerDeparted on the flow bus. Whatever BT connection the
	// leecher had open to A's torrent client also drops, and anacrolix
	// falls back to seeder B for remaining pieces.
	seederA.Close()

	// Wait for the fixture to land via the survivor.
	waitForP2P(t, func() bool { return promotedCount.Load() >= 1 },
		30*time.Second, "fixture TrustPromoted after seeder A offline")

	require.GreaterOrEqual(t, departedCount.Load(), int32(1),
		"leecher must observe PeerDeparted when A drops")

	// Byte-equality: the leecher's on-disk copy equals the seeders'
	// source content. This is the only assertion that proves real
	// bytes survived the peer churn.
	leecherBytes, err := os.ReadFile(filepath.Join(leecher.Dirs.Snap, fixtureName))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(fixtureContent), sha256.Sum256(leecherBytes),
		"leecher's fixture content must be byte-identical to seeder's")

	// Leecher's inventory reflects the completed file.
	localFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, localFiles, 1)
	require.Equal(t, fixtureName, localFiles[0].Name)
	require.Equal(t, fixtureHashA, localFiles[0].TorrentHash)
	require.Equal(t, snapshot.TrustVerified, localFiles[0].Trust)
}
