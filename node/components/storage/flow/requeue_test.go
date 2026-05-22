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

// TestRequeueForDownload pins the re-queue path used by the storage
// Provider's settle-watcher under the `redownload` revalidation policy:
// a file the orchestrator already knows (peer-claimed) can be re-issued
// for download even after it has finished; an unknown file cannot; a
// file already in flight is not double-requested.
func TestRequeueForDownload(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu        sync.Mutex
		requested []string
	)
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		requested = append(requested, e.FileName)
		mu.Unlock()
	}))

	stateFile := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 256,
		Name: "v1.0-accounts.0-256.kv",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateFile}},
	})
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(requested) == 1
	}, 2*time.Second, "initial DownloadRequested")

	// Download completes — file leaves the pending set.
	bus.Publish(DownloadComplete{FileName: stateFile.Name})

	// An unknown file cannot be re-queued.
	require.False(t, o.RequeueForDownload("v1.0-storage.0-256.kv"),
		"file with no peer claim is not re-fetchable")

	// A known, completed file is re-queued: a fresh DownloadRequested fires.
	require.True(t, o.RequeueForDownload(stateFile.Name))
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(requested) == 2
	}, 2*time.Second, "re-queued DownloadRequested")

	// The file is now in flight — re-queueing again returns true but does
	// not double-request.
	require.True(t, o.RequeueForDownload(stateFile.Name))
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	require.Equal(t, 2, len(requested), "in-flight re-queue must not double-request")
	mu.Unlock()
}
