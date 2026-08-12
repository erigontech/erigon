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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestOnDownloadComplete_RecordFileError_DecrementsStatePending pins
// the fix for the leg-M cycle 9 statePending leak: when RecordFile
// fails for a phase-1 file (e.g. downloader emits DownloadComplete for
// a zero-byte file that then fails content_not_empty validation), the
// orchestrator's original behaviour was to leave the increment in
// place — under the mistaken assumption that a retry would arrive.
// No retry mechanism exists, so statePending accumulated permanently
// and InitialStateReady never fired.
//
// Fix: treat RecordFile failure as a functional download failure —
// decrement statePending, evict pending, publish DownloadFailed. This
// unblocks the InitialStateReady gate; if the file is genuinely
// needed, a future PeerManifestReceived can re-request it.
func TestOnDownloadComplete_RecordFileError_DecrementsStatePending(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{
		inv:      snapshot.NewInventory(),
		failWith: errors.New("validation: content_not_empty: file not on disk"),
	}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var stateReadyFired atomic.Int32
	require.NoError(t, bus.Subscribe(func(e InitialStateReady) {
		stateReadyFired.Add(1)
	}))

	var downloadFailedFired atomic.Int32
	require.NoError(t, bus.Subscribe(func(e DownloadFailed) {
		downloadFailedFired.Add(1)
	}))

	stateFile := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 256,
		Name: "v1.0-accounts.0-256.kv",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateFile}},
	})

	// Wait for the DownloadRequested to have been issued (statePending==1 now).
	waitUntil(t, func() bool {
		return o.PendingCount() >= 1
	}, 2*time.Second, "state file to enter pending")

	// Simulate the "phantom complete" case: DownloadComplete fires for
	// the file but RecordFile rejects it. Pre-fix, statePending would
	// stay at 1 forever and InitialStateReady would never fire.
	bus.Publish(DownloadComplete{
		FileName: stateFile.Name,
		InfoHash: stateFile.TorrentHash,
		Size:     0,
	})

	waitUntil(t, func() bool {
		return downloadFailedFired.Load() >= 1
	}, 2*time.Second, "DownloadFailed to be published on RecordFile error")

	waitUntil(t, func() bool {
		return stateReadyFired.Load() >= 1
	}, 2*time.Second, "InitialStateReady to fire after DEC unblocks the gate")

	require.Equal(t, int32(1), stateReadyFired.Load(),
		"InitialStateReady must fire once statePending drops to zero via the RecordFile-error DEC path")
	require.Equal(t, 0, o.PendingCount(),
		"pending map must be drained when RecordFile fails, so a future manifest can re-request")
}

// TestOnDownloadFailed_DecrementsStatePending pins the same invariant
// on the direct DownloadFailed handler path. bus.go now publishes
// DownloadFailed for zero-byte / missing files; if onDownloadFailed
// didn't decrement statePending, the leak just moved sites.
func TestOnDownloadFailed_DecrementsStatePending(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var stateReadyFired atomic.Int32
	require.NoError(t, bus.Subscribe(func(e InitialStateReady) {
		stateReadyFired.Add(1)
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
		return o.PendingCount() >= 1
	}, 2*time.Second, "state file to enter pending")

	// Downloader reports the file failed to arrive (e.g. bus.go's
	// post-Stat check saw a zero-byte or missing file).
	bus.Publish(DownloadFailed{
		FileName: stateFile.Name,
		Reason:   "download produced zero-byte file",
	})

	waitUntil(t, func() bool {
		return stateReadyFired.Load() >= 1
	}, 2*time.Second, "InitialStateReady after DownloadFailed drain")

	require.Equal(t, 0, o.PendingCount(),
		"pending must drain on DownloadFailed")
}

// TestOnDownloadFailed_EvictsPhase1Files pins the companion invariant:
// a permanently-failed phase-1 file must be removed from phase1Files
// so tryFireInitialStateReady's LifecycleIndexed gate (Gate 3) doesn't
// block forever waiting for a file that will never appear on disk.
//
// Failure discovered on publisher's own bootstrap: v1.1-*-headers.seg
// files listed in preverified for hoodi are superseded and never
// download; before this eviction, Gate 3 saw phase1_total=273 but
// phase1_indexed=248 and InitialStateReady never fired.
func TestOnDownloadFailed_EvictsPhase1Files(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	stateFile := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 256,
		Name: "v1.0-accounts.0-256.kv",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateFile}},
	})

	waitUntil(t, func() bool {
		o.peerMu.RLock()
		defer o.peerMu.RUnlock()
		_, ok := o.phase1Files[stateFile.Name]
		return ok
	}, 2*time.Second, "phase1Files to record the state file")

	bus.Publish(DownloadFailed{
		FileName: stateFile.Name,
		Reason:   "download produced zero-byte file",
	})

	waitUntil(t, func() bool {
		o.peerMu.RLock()
		defer o.peerMu.RUnlock()
		_, ok := o.phase1Files[stateFile.Name]
		return !ok
	}, 2*time.Second, "phase1Files to evict the failed file")
}
