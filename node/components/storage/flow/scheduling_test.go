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
