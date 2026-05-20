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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// staticTrustFilter is a TrustFilter for tests — a simple set of
// trusted peer IDs.
type staticTrustFilter struct {
	mu      sync.Mutex
	trusted map[string]struct{}
}

func newStaticTrustFilter(ids ...string) *staticTrustFilter {
	f := &staticTrustFilter{trusted: map[string]struct{}{}}
	for _, id := range ids {
		f.trusted[id] = struct{}{}
	}
	return f
}

func (f *staticTrustFilter) Trusted(peerID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.trusted[peerID]
	return ok
}

func (f *staticTrustFilter) revoke(peerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.trusted, peerID)
}

func TestTrustFilter_NilTrustPreservesGapFill(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu       sync.Mutex
		observed []string
	)
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		observed = append(observed, e.FileName)
		mu.Unlock()
	}))

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {
				{Domain: testDomain, FromStep: 0, ToStep: 2048, Name: "v1.0-accounts.0-2048.kv"},
			},
		},
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) == 1
	}, 2*time.Second, "DownloadRequested with nil trust")
}

func TestTrustFilter_UntrustedPeerNoDownload(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.SetTrust(newStaticTrustFilter())) // empty allowlist
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu       sync.Mutex
		observed int
	)
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		observed++
		mu.Unlock()
	}))

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {
				{Domain: testDomain, FromStep: 0, ToStep: 2048, Name: "v1.0-accounts.0-2048.kv"},
			},
		},
	})
	bus.WaitAsync()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, observed,
		"untrusted peer must not produce DownloadRequested")
	require.Equal(t, 0, o.PendingCount(),
		"untrusted peer must not enter pending — leaves the gap open for a trusted advertiser")

	// Attribution still recorded so PeersOffering reflects who claimed
	// what (informational, separated from trust).
	require.Equal(t, []string{"peer-A"}, o.PeersOffering("v1.0-accounts.0-2048.kv"))
}

func TestTrustFilter_MixedPeersOnlyTrustedDownload(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.SetTrust(newStaticTrustFilter("peer-good")))
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu       sync.Mutex
		observed []DownloadRequested
	)
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		observed = append(observed, e)
		mu.Unlock()
	}))

	entryA := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name: "v1.0-accounts.0-2048.kv",
	}
	entryB := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 2048, ToStep: 4096,
		Name: "v1.0-accounts.2048-4096.kv",
	}

	// Untrusted advertises both first.
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-bad",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entryA, entryB},
		},
	})
	bus.WaitAsync()

	mu.Lock()
	require.Empty(t, observed, "untrusted manifest must not produce DownloadRequested")
	mu.Unlock()

	// Trusted advertises both — this is the manifest the orchestrator
	// acts on.
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-good",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entryA, entryB},
		},
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) == 2
	}, 2*time.Second, "trusted advertiser drives both downloads")

	mu.Lock()
	defer mu.Unlock()
	for _, e := range observed {
		require.Equal(t, []string{"peer-good"}, e.FromPeers,
			"FromPeers must be the trusted peer only")
	}

	// PeersOffering reflects BOTH advertisers — trust filtering is at
	// the action layer, not the attribution layer.
	require.ElementsMatch(t,
		[]string{"peer-bad", "peer-good"},
		o.PeersOffering(entryA.Name))
}

func TestTrustFilter_QueuedBlocksDroppedWhenTrustRevokedBeforeDrain(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	filter := newStaticTrustFilter("peer-A")
	require.NoError(t, o.SetTrust(filter))
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu               sync.Mutex
		stateReqs        []DownloadRequested
		blockReqs        []DownloadRequested
		initialStateOnce int
	)
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		defer mu.Unlock()
		if e.Domain != "" {
			stateReqs = append(stateReqs, e)
		} else {
			blockReqs = append(blockReqs, e)
		}
	}))
	require.NoError(t, bus.Subscribe(func(InitialStateReady) {
		mu.Lock()
		defer mu.Unlock()
		initialStateOnce++
	}))

	stateEntry := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x01},
	}
	// caplin .seg (not a block file) so this exercises phase-2
	// queueing. Post-2026-05-18 phase-1 includes ALL block file types
	// (headers + bodies + transactions) because FillDBFromSnapshots's
	// alignMin=true FrozenBlocks gate would collapse to 0 if any block
	// type is missing. Caplin is the only phase-2 category left.
	caplinEntry := &snapshot.FileEntry{
		Kind:        snapshot.KindCaplin,
		Name:        "caplin/v1.1-000000-000010-beaconblocks.seg",
		TorrentHash: [20]byte{0x02},
	}

	// Manifest carries state + caplin. Trust passes at entry; state
	// is requested immediately, caplin queues behind InitialStateReady.
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {stateEntry},
		},
		Caplin: []*snapshot.FileEntry{caplinEntry},
	})
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(stateReqs) == 1
	}, 2*time.Second, "state DownloadRequested fires")

	// Revoke trust BEFORE the state download completes. The drain
	// time defensive check must catch this.
	filter.revoke("peer-A")

	// Complete the state download. This decrements statePending,
	// triggers fireInitialStateReady → drains queue. Drain time
	// re-checks trust → peer-A no longer trusted → drop block req.
	bus.Publish(DownloadComplete{
		FileName: stateEntry.Name,
		InfoHash: stateEntry.TorrentHash,
	})
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return initialStateOnce == 1
	}, 2*time.Second, "InitialStateReady fires after state drains")
	bus.WaitAsync()

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, blockReqs,
		"block requests for a now-untrusted peer must be dropped at drain time")
	require.Equal(t, 0, o.PendingCount(),
		"dropped queue entries must clear pending so a trusted advertiser can re-request")
}

func TestSetTrust_CannotChangeAfterStart(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	err := o.SetTrust(newStaticTrustFilter())
	require.Error(t, err)
	require.Contains(t, err.Error(), "already started")
}
