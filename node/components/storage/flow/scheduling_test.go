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

// TestOrchestratorBlockFilesDispatchInPhase1 pins the (C)-era contract
// that ALL block file types (headers + bodies + transactions) are part
// of phase 1 alongside state-domain files. The reason:
// rawdbreset.FillDBFromSnapshots in the postIndexed pipeline uses
// blockReader.FrozenBlocks() which alignMin-collapses to
// min(headers, bodies, transactions) — if any type is missing,
// FrozenBlocks() is 0 and the seed is a no-op. Observed 2026-05-18:
// kv.HeaderTD never populated, Caplin's BlockCollector.Flush failed for
// 9.5h with "parent's total difficulty not found".
//
// Caplin's "headers only" needs are served by a SEPARATE earlier
// signal, BlockHeadersReady, so it doesn't block on bodies/tx.
//
// Concrete shape verified here:
//   - State + header + body entries dispatch immediately in parallel
//     (all phase-1).
//   - Caplin entry does NOT dispatch until InitialStateReady (phase-2).
//   - InitialStateReady does NOT fire until state + header + body all
//     complete.
//   - Once they complete, caplin drains from the phase-2 queue.
func TestOrchestratorBlockFilesDispatchInPhase1(t *testing.T) {
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
	caplinEntry := &snapshot.FileEntry{
		Kind: snapshot.KindCaplin,
		Name: "caplin/v1.1-000000-000010-beaconblocks.seg",
	}

	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-Z",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateEntry}},
		Blocks:  []*snapshot.FileEntry{headerEntry, bodyEntry},
		Caplin:  []*snapshot.FileEntry{caplinEntry},
	})

	// State + header + body all dispatch in parallel (all phase 1).
	// Caplin is held for phase 2.
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(downloads) == 3
	}, 2*time.Second, "state + header + body dispatched in phase 1")

	mu.Lock()
	require.ElementsMatch(t,
		[]string{
			"v1.0-accounts.0-2048.kv",
			"v1.0-000000-002000-headers.seg",
			"v1.0-000000-002000-bodies.seg",
		},
		downloads,
		"phase 1 must include state + headers + bodies, exclude caplin",
	)
	require.False(t, stateReadyFired,
		"InitialStateReady must wait while any phase-1 file is still pending")
	mu.Unlock()

	// Complete state + header but NOT body — InitialStateReady must
	// not fire yet. Bodies are now part of phase 1's FrozenBlocks gate.
	bus.Publish(DownloadComplete{FileName: stateEntry.Name, InfoHash: [20]byte{0xaa}, Size: 1024})
	bus.Publish(DownloadComplete{FileName: headerEntry.Name, InfoHash: [20]byte{0xbb}, Size: 2048})

	for i := 0; i < 5; i++ {
		mu.Lock()
		fired := stateReadyFired
		mu.Unlock()
		require.False(t, fired,
			"InitialStateReady fired prematurely — body not yet landed")
		time.Sleep(20 * time.Millisecond)
	}

	// Complete the body — now all of phase 1 is done.
	bus.Publish(DownloadComplete{FileName: bodyEntry.Name, InfoHash: [20]byte{0xcc}, Size: 4096})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stateReadyFired
	}, 2*time.Second, "InitialStateReady fires after state + header + body complete")

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(downloads) == 4
	}, 2*time.Second, "caplin drains from phase-2 queue after InitialStateReady")

	mu.Lock()
	require.Contains(t, downloads, caplinEntry.Name,
		"caplin must dispatch from the phase-2 queue once state-ready fires")
	mu.Unlock()
}

// TestOrchestratorSaltMetaDispatchInPhase1 pins the bug-L contract that
// salt and meta entries are phase-1 prerequisites alongside state-domain
// files. The reason: ReloadSalt (called by the OtterSync post-download
// bookkeeping after InitialStateReady) needs salt on disk; if salt
// landed via phase-2 (queued behind InitialStateReady), exec start
// errors with "salt not found on ReloadSalt".
//
// Concrete shape verified here:
//   - State entry + meta entry + salt entry all dispatch immediately
//     in parallel.
//   - Caplin entry does NOT dispatch until InitialStateReady.
//   - InitialStateReady does NOT fire until ALL three of
//     state/meta/salt have completed.
//   - Caplin drains from the phase-2 queue after InitialStateReady.
func TestOrchestratorSaltMetaDispatchInPhase1(t *testing.T) {
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
	metaEntry := &snapshot.FileEntry{
		Kind: snapshot.KindMeta,
		Name: "erigondb.toml",
	}
	saltEntry := &snapshot.FileEntry{
		Kind: snapshot.KindSalt,
		Name: "salt-state.txt",
	}
	caplinEntry := &snapshot.FileEntry{
		Kind: snapshot.KindCaplin,
		Name: "caplin/v1.1-000000-000010-beaconblocks.seg",
	}

	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-salt",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateEntry}},
		Meta:    []*snapshot.FileEntry{metaEntry},
		Salt:    []*snapshot.FileEntry{saltEntry},
		Caplin:  []*snapshot.FileEntry{caplinEntry},
	})

	// State, meta, salt all dispatch immediately. Caplin is phase 2 and waits.
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(downloads) == 3
	}, 2*time.Second, "state + meta + salt dispatched in phase 1")

	mu.Lock()
	require.ElementsMatch(t,
		[]string{stateEntry.Name, metaEntry.Name, saltEntry.Name},
		downloads,
		"phase 1 must include state + meta + salt, exclude caplin",
	)
	require.False(t, stateReadyFired,
		"InitialStateReady must wait while meta/salt are still pending")
	mu.Unlock()

	// Complete state and meta — salt still pending; ready must not fire.
	bus.Publish(DownloadComplete{FileName: stateEntry.Name, InfoHash: [20]byte{0xaa}, Size: 1024})
	bus.Publish(DownloadComplete{FileName: metaEntry.Name, InfoHash: [20]byte{0xbb}, Size: 100})

	for i := 0; i < 5; i++ {
		mu.Lock()
		fired := stateReadyFired
		mu.Unlock()
		require.False(t, fired,
			"InitialStateReady fired prematurely — salt still pending. This is the bug-L regression guard.")
		time.Sleep(20 * time.Millisecond)
	}

	// Complete salt — phase 1 is done; ready fires and caplin drains.
	bus.Publish(DownloadComplete{FileName: saltEntry.Name, InfoHash: [20]byte{0xcc}, Size: 50})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stateReadyFired
	}, 2*time.Second, "InitialStateReady fires after state + meta + salt complete")

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(downloads) == 4
	}, 2*time.Second, "caplin drains from phase-2 queue after InitialStateReady")

	mu.Lock()
	require.Contains(t, downloads, caplinEntry.Name,
		"caplin must dispatch from the phase-2 queue once state-ready fires")
	mu.Unlock()
}
