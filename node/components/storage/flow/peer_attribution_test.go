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

package flow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestPeerAttribution_MultipleAdvertisersTracked confirms peerFiles
// records every peer that advertises a file, exposed via PeersOffering.
// The orchestrator's first-claim-wins shape rule still holds — the
// entry doesn't get overwritten on subsequent advertisements — but the
// peer set grows as more peers announce.
func TestPeerAttribution_MultipleAdvertisersTracked(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name: "v1.0-accounts.0-2048.kv",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {entry}},
	})
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-B",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {entry}},
	})
	bus.WaitAsync()

	require.Equal(t, []string{"peer-A", "peer-B"}, o.PeersOffering(entry.Name))
}

// TestPeerAttribution_PartialDepartureKeepsClaim confirms that when one
// of two advertisers leaves, the claim survives with the remaining
// peer still attributed.
func TestPeerAttribution_PartialDepartureKeepsClaim(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name: "v1.0-accounts.0-2048.kv",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {entry}},
	})
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-B",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {entry}},
	})
	bus.WaitAsync()

	bus.Publish(PeerDeparted{PeerID: "peer-A"})
	bus.WaitAsync()

	require.Equal(t, []string{"peer-B"}, o.PeersOffering(entry.Name),
		"only peer-B remains as advertiser after peer-A departs")
	require.Equal(t, 1, o.PeerFilesCount())
}

// TestPeerAttribution_AllAdvertisersGoneEvictsClaim confirms that once
// every advertiser has departed AND no download is in flight, the claim
// is removed entirely.
func TestPeerAttribution_AllAdvertisersGoneEvictsClaim(t *testing.T) {
	bus := newBusForTest()
	// Pre-seed the inventory so haveLocally short-circuits gap-fill —
	// otherwise the manifest publishes a DownloadRequested and pending
	// keeps the claim alive even after departure.
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name:  "v1.0-accounts.0-2048.kv",
		Local: true, Trust: snapshot.TrustVerified,
	})
	storage := &recordingStorage{inv: inv}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name: "v1.0-accounts.0-2048.kv",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {entry}},
	})
	bus.WaitAsync()
	require.Equal(t, 1, o.PeerFilesCount())

	bus.Publish(PeerDeparted{PeerID: "peer-A"})
	bus.WaitAsync()

	require.Nil(t, o.PeersOffering(entry.Name))
	require.Equal(t, 0, o.PeerFilesCount(),
		"last advertiser gone with no in-flight download — claim must be evicted")
}

// TestPeerAttribution_DepartureDuringInflightRetainedThenCleanedOnComplete
// covers the timing where the only advertiser leaves while the file is
// still being downloaded. PeerDeparted must keep the claim (so
// onDownloadComplete can resolve the file shape), and DownloadComplete
// must then evict the orphaned claim once pending clears.
func TestPeerAttribution_DepartureDuringInflightRetainedThenCleanedOnComplete(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x01},
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {entry}},
	})
	bus.WaitAsync()
	// Peer A is the sole advertiser; gap-fill made the file pending.
	require.Equal(t, 1, o.PendingCount())

	bus.Publish(PeerDeparted{PeerID: "peer-A"})
	bus.WaitAsync()
	// Claim retained: peer set is empty but a download is still in
	// flight, so onDownloadComplete needs the entry to translate bytes
	// into the local FileEntry.
	require.Equal(t, 1, o.PeerFilesCount(),
		"in-flight download keeps claim alive past the last departure")

	bus.Publish(DownloadComplete{
		FileName: entry.Name,
		InfoHash: entry.TorrentHash,
		Size:     1024,
	})
	waitUntil(t, func() bool { return o.PendingCount() == 0 }, 2*time.Second,
		"DownloadComplete clears pending")
	bus.WaitAsync()

	require.Equal(t, 0, o.PeerFilesCount(),
		"orphaned claim evicted once pending drains")
	require.Nil(t, o.PeersOffering(entry.Name))
}
