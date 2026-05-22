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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// recordingStorage is an in-test flow.Storage implementation used to exercise
// the NewWithStorage path. The harness package has MockStorage with the same
// behaviour — this one is in-package so flow can test its own contract
// without introducing a harness import cycle.
type recordingStorage struct {
	mu       sync.Mutex
	inv      *snapshot.Inventory
	recorded []*snapshot.FileEntry
	failWith error
}

func (s *recordingStorage) Inventory() *snapshot.Inventory { return s.inv }

func (s *recordingStorage) RecordFile(e *snapshot.FileEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return s.failWith
	}
	s.recorded = append(s.recorded, e)
	s.inv.AddFile(e)
	return nil
}

func (s *recordingStorage) snapshotRecorded() []*snapshot.FileEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*snapshot.FileEntry, len(s.recorded))
	copy(out, s.recorded)
	return out
}

var _ Storage = (*recordingStorage)(nil)

func newBusForTest() event.EventBus { return event.NewEventBus(nil) }

func logger() log.Logger { return log.Root() }

// testDomain is the non-empty Domain flow tests use; the snapshot package
// exposes Domain as a typed string.
const testDomain snapshot.Domain = "accounts"

func peerEntry(name string, from, to uint64) *snapshot.FileEntry {
	return &snapshot.FileEntry{
		Name:     name,
		Domain:   testDomain,
		FromStep: from,
		ToStep:   to,
		Size:     1024,
	}
}

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

func TestNewWithStorageRecordFileOnDownloadComplete(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := peerEntry("v1.0-accounts.0-256.kv", 0, 256)
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entry},
		},
	})

	// Gap-fill publishes DownloadRequested; the download layer isn't
	// wired, so we simulate the completion directly.
	bus.Publish(DownloadComplete{
		FileName: entry.Name,
		Size:     entry.Size,
	})

	waitUntil(t, func() bool {
		return len(storage.snapshotRecorded()) == 1
	}, 2*time.Second, "RecordFile call")

	got := storage.snapshotRecorded()
	require.Len(t, got, 1)
	require.Equal(t, entry.Name, got[0].Name)
	require.Equal(t, snapshot.TrustVerified, got[0].Trust)
	require.True(t, got[0].Local)
}

func TestNewWithStorageRecordFileFailureKeepsPending(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory(), failWith: errors.New("disk full")}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entry := peerEntry("v1.0-accounts.0-256.kv", 0, 256)
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			testDomain: {entry},
		},
	})

	// Wait for the orchestrator to mark the entry as pending.
	waitUntil(t, func() bool {
		return o.PendingCount() == 1
	}, 2*time.Second, "pending count")

	bus.Publish(DownloadComplete{FileName: entry.Name, Size: entry.Size})
	// Give the handler a moment to run.
	time.Sleep(50 * time.Millisecond)

	require.Empty(t, storage.snapshotRecorded(), "RecordFile should not commit on failure")
	require.Equal(t, 1, o.PendingCount(), "pending must stay after RecordFile failure")
}
