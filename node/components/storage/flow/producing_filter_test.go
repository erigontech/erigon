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

// A peer manifest declaring a file the local node is currently producing
// (retire dropped it, recovery-exec will rebuild it) must NOT be turned
// into a DownloadRequested. Otherwise the downloader renames a .part into
// the file path while a fresh reader is opening an mmap over it, torn
// pages surface as decompressor SIGSEGV. Repro: verify11 iter 5
// (v2.2-commitment.310-311.kv dropped by FinalizeUnwind actionRemove, peer
// re-declared, downloader re-fetched, SIGSEGV in Getter.nextPattern).
func TestOrchestrator_ProducingFilesAreNotRequested(t *testing.T) {
	bus := newBusForTest()
	inv := snapshot.NewInventory()
	storage := &recordingStorage{inv: inv}
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

	// Mark v2.2-commitment.310-311.kv as producing: local node had it,
	// dropped it (RemoveFile), plans to rebuild.
	const producing = "v2.2-commitment.310-311.kv"
	_ = inv.AddFile(&snapshot.FileEntry{
		Domain:   snapshot.DomainCommitment,
		FromStep: 310, ToStep: 311,
		Name:  producing,
		Kind:  snapshot.KindKV,
		Local: true,
	})
	inv.RemoveFile(producing)
	require.True(t, inv.IsProducing(producing))

	// Peer manifest re-declares the producing file plus an innocuous
	// unrelated file. Only the unrelated file should trigger a download.
	const other = "v2.2-commitment.288-304.kv"
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-X",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311, Name: producing, Kind: snapshot.KindKV},
				{Domain: snapshot.DomainCommitment, FromStep: 288, ToStep: 304, Name: other, Kind: snapshot.KindKV},
			},
		},
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) >= 1
	}, 2*time.Second, "at least one DownloadRequested for the non-producing file")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{other}, observed,
		"producing file must be filtered out; only the unrelated file gets a DownloadRequested")
}

// After a producing file is re-added to the Inventory (recovery-exec
// rebuilt it), a subsequent peer manifest re-declaration is still not
// requested — the local Inventory now has it (haveLocally guard) and
// the producing mark is cleared. But if a fresh peer entry arrives for a
// DIFFERENT file that was never producing, it must still be requested.
func TestOrchestrator_ProducingClearsAfterAddFile(t *testing.T) {
	bus := newBusForTest()
	inv := snapshot.NewInventory()
	storage := &recordingStorage{inv: inv}
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

	const wasProducing = "v2.2-commitment.310-311.kv"
	_ = inv.AddFile(&snapshot.FileEntry{
		Domain:   snapshot.DomainCommitment,
		FromStep: 310, ToStep: 311,
		Name:  wasProducing,
		Kind:  snapshot.KindKV,
		Local: true,
	})
	inv.RemoveFile(wasProducing)
	// Recovery-exec rebuilt it — file is back in the authoritative set.
	_ = inv.AddFile(&snapshot.FileEntry{
		Domain:   snapshot.DomainCommitment,
		FromStep: 310, ToStep: 311,
		Name:  wasProducing,
		Kind:  snapshot.KindKV,
		Local: true,
	})
	require.False(t, inv.IsProducing(wasProducing))

	const fresh = "v2.2-commitment.311-312.kv"
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-X",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				// Producing-then-rebuilt file: haveLocally=true, skipped by the
				// pre-existing coverage path (no producing filter needed here).
				{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311, Name: wasProducing, Kind: snapshot.KindKV},
				// A new peer file at a range we don't have — should be requested.
				{Domain: snapshot.DomainCommitment, FromStep: 311, ToStep: 312, Name: fresh, Kind: snapshot.KindKV},
			},
		},
	})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) >= 1
	}, 2*time.Second, "fresh file is downloaded")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{fresh}, observed,
		"clean state: fresh file downloaded, rebuilt-producing file suppressed via haveLocally")
}
