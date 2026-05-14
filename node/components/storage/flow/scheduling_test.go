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

// TestOrchestratorDownloadRequestedOrderedLaterFirst verifies the
// orchestrator emits DownloadRequested events in descending range order
// so later files are fetched before older ones. This matters for
// sync-to-tip: a consumer that can pull the most recent snapshot first
// reaches a usable state sooner than one that walks history forward.
func TestOrchestratorDownloadRequestedOrderedLaterFirst(t *testing.T) {
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

	// Peer advertises four domain files across two step-ranges, two roles
	// each. Later range = higher step bound. Intentionally shuffled in
	// the input so the sort actually has to do something.
	peerEntries := []*snapshot.FileEntry{
		{Domain: testDomain, FromStep: 0, ToStep: 2048, Name: "v1.0-accounts.0-2048.kv"},
		{Domain: testDomain, FromStep: 2048, ToStep: 4096, Name: "v1.0-accounts.2048-4096.kvi"},
		{Domain: testDomain, FromStep: 0, ToStep: 2048, Name: "v1.0-accounts.0-2048.kvi"},
		{Domain: testDomain, FromStep: 2048, ToStep: 4096, Name: "v1.0-accounts.2048-4096.kv"},
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-X",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: peerEntries},
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) == 4
	}, 2*time.Second, "four DownloadRequested publications")

	mu.Lock()
	defer mu.Unlock()

	// Strict invariant: first two must be the 2048-4096 range (later),
	// last two the 0-2048 range (earlier). Within a range the tie-break
	// is descending by name, which groups .kvi before .kv — that's a
	// deterministic-order artefact and pinned here so regressions show up.
	require.Equal(t, []string{
		"v1.0-accounts.2048-4096.kvi",
		"v1.0-accounts.2048-4096.kv",
		"v1.0-accounts.0-2048.kvi",
		"v1.0-accounts.0-2048.kv",
	}, observed)
}

// TestOrchestratorDownloadRequestedBlockFilesOrderedByName verifies that
// block files (FromStep / ToStep both zero in peer manifests) still sort
// later-first via their zero-padded numeric segments in the filename.
func TestOrchestratorDownloadRequestedBlockFilesOrderedByName(t *testing.T) {
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

	// Block files carry block numbers in FromStep / ToStep just like
	// domain files carry step ranges — see harness/fixtures.go mkBlock.
	blocks := []*snapshot.FileEntry{
		{Name: "v1.0-000000-000500-headers.seg", FromStep: 0, ToStep: 500},
		{Name: "v1.0-001000-001500-headers.seg", FromStep: 1000, ToStep: 1500},
		{Name: "v1.0-000500-001000-headers.seg", FromStep: 500, ToStep: 1000},
	}
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-Y",
		Blocks: blocks,
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) == 3
	}, 2*time.Second, "three DownloadRequested for blocks")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		"v1.0-001000-001500-headers.seg",
		"v1.0-000500-001000-headers.seg",
		"v1.0-000000-000500-headers.seg",
	}, observed, "block files must be requested latest-first")
}

// TestOrchestratorHeaderFilesDispatchInPhase1 pins the contract that
// block-header files (*-headers.seg) are part of phase 1 alongside
// state-domain files. The reason: every cryptographic state validator
// cross-references header.stateRoot for the snapshot tip block, so
// "InitialStateReady" without headers landed would mean "state files
// arrived and pass internal-consistency checks" — not "state is
// validated against consensus". Including headers in phase 1 keeps the
// gate's contract honest, and as a side-effect collapses Caplin's
// historical-blocks wait (it targets the EL's frozen-headers tip as
// lowestBlockToReach).
//
// Concrete shape verified here:
//   - State entry + header entry dispatch immediately in parallel.
//   - Bodies entry does NOT dispatch until InitialStateReady.
//   - InitialStateReady does NOT fire until both state and header have
//     completed.
//   - Once both complete, InitialStateReady fires AND the bodies entry
//     drains from the phase-2 queue.
func TestOrchestratorHeaderFilesDispatchInPhase1(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu              sync.Mutex
		downloads       []string
		stateReadyFired bool
	)
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		downloads = append(downloads, e.FileName)
		mu.Unlock()
	}))
	require.NoError(t, bus.Subscribe(func(InitialStateReady) {
		mu.Lock()
		stateReadyFired = true
		mu.Unlock()
	}))

	stateEntry := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 2048,
		Name: "v1.0-accounts.0-2048.kv",
	}
	headerEntry := &snapshot.FileEntry{
		FromStep: 0, ToStep: 2000,
		Name: "v1.0-000000-002000-headers.seg",
	}
	bodyEntry := &snapshot.FileEntry{
		FromStep: 0, ToStep: 2000,
		Name: "v1.0-000000-002000-bodies.seg",
	}

	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-Z",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateEntry}},
		Blocks:  []*snapshot.FileEntry{headerEntry, bodyEntry},
	})

	// State and header dispatch in parallel; bodies are held for phase 2.
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(downloads) == 2
	}, 2*time.Second, "state + header dispatched in phase 1")

	mu.Lock()
	require.ElementsMatch(t,
		[]string{"v1.0-accounts.0-2048.kv", "v1.0-000000-002000-headers.seg"},
		downloads,
		"phase 1 must include state + header, exclude bodies",
	)
	require.False(t, stateReadyFired,
		"InitialStateReady must wait while header is still pending")
	mu.Unlock()

	// Complete the state file; header still pending → InitialStateReady
	// must NOT fire yet. This is the contract change vs. pre-piece-A:
	// state alone is no longer sufficient.
	bus.Publish(DownloadComplete{
		FileName: "v1.0-accounts.0-2048.kv",
		InfoHash: [20]byte{0xaa},
		Size:     1024,
	})

	// Brief settle window; assert the negative.
	for i := 0; i < 5; i++ {
		mu.Lock()
		fired := stateReadyFired
		mu.Unlock()
		require.False(t, fired,
			"InitialStateReady fired prematurely — header not yet landed")
		time.Sleep(20 * time.Millisecond)
	}

	// Complete the header; now phase 1 is done.
	bus.Publish(DownloadComplete{
		FileName: "v1.0-000000-002000-headers.seg",
		InfoHash: [20]byte{0xbb},
		Size:     2048,
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stateReadyFired
	}, 2*time.Second, "InitialStateReady fires after state + header complete")

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(downloads) == 3
	}, 2*time.Second, "body drains from phase-2 queue after InitialStateReady")

	mu.Lock()
	require.Contains(t, downloads, "v1.0-000000-002000-bodies.seg",
		"body must dispatch from the phase-2 queue once state-ready fires")
	mu.Unlock()
}
