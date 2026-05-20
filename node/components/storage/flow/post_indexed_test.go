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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestPostIndexed_GatesOnLifecycleIndexed asserts the new post-Indexed
// gate added in (C) Step 1 holds InitialStateReady until every phase-1
// file has reached LifecycleIndexed AND the postIndexed callback has
// returned nil. This catches the 2026-05-18 race where the legacy gate
// (statePending == 0) fired ~155ms before block-header .idx files
// finished their Indexing transition, causing the downstream OpenFolder
// to lstat-fail on a not-yet-built .idx.
func TestPostIndexed_GatesOnLifecycleIndexed(t *testing.T) {
	bus := newBusForTest()
	inv := snapshot.NewInventory()
	storage := &recordingStorage{inv: inv}
	o := NewWithStorage(bus, storage, logger())

	var postIndexedCalls atomic.Int32
	require.NoError(t, o.SetPostIndexed(func(ctx context.Context) error {
		postIndexedCalls.Add(1)
		return nil
	}))

	var stateReadyFires atomic.Int32
	require.NoError(t, bus.Subscribe(func(e InitialStateReady) {
		stateReadyFires.Add(1)
	}))

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	stateFile := peerEntry("v1.0-accounts.0-256.kv", 0, 256)
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateFile}},
	})

	// Simulate the download landing. RecordFile sets Local=true so the
	// inventory derives LifecycleDownloaded. statePending drops to 0
	// (legacy gate met) — but the post-Indexed gate must hold because
	// the file is not yet at LifecycleIndexed.
	bus.Publish(DownloadComplete{
		FileName: stateFile.Name,
		Size:     stateFile.Size,
	})

	// Give the handler a beat to process; verify the gate holds.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(0), stateReadyFires.Load(),
		"InitialStateReady must wait for LifecycleIndexed; legacy gate (statePending==0) is no longer sufficient")
	require.Equal(t, int32(0), postIndexedCalls.Load(),
		"postIndexed must not be invoked until phase-1 files are Indexed")

	// Now advance the file to LifecycleIndexed. The inventory's
	// ChangeSet notification triggers the orchestrator's re-check.
	require.True(t, inv.AdvanceTo(stateFile.Name, snapshot.LifecycleIndexed),
		"AdvanceTo must succeed; file should exist at LifecycleDownloaded")

	// Expect postIndexed to be called, then InitialStateReady to fire.
	waitUntil(t, func() bool {
		return stateReadyFires.Load() == 1
	}, 2*time.Second, "InitialStateReady to fire after Indexed transition")

	require.Equal(t, int32(1), postIndexedCalls.Load(),
		"postIndexed must be called exactly once before InitialStateReady fires")
	require.Equal(t, int32(1), stateReadyFires.Load(),
		"InitialStateReady fires exactly once")
}

// TestPostIndexed_RetriesOnError asserts that if postIndexed returns an
// error, the orchestrator does NOT fire InitialStateReady and re-evaluates
// on the next inventory ChangeSet. This prevents a silent hang when the
// post-download bookkeeping (OpenFolder, FillDBFromSnapshots) genuinely
// fails — the operator should see the postIndexed error in the log AND
// the downstream consumers should keep waiting.
func TestPostIndexed_RetriesOnError(t *testing.T) {
	bus := newBusForTest()
	inv := snapshot.NewInventory()
	storage := &recordingStorage{inv: inv}
	o := NewWithStorage(bus, storage, logger())

	var fail atomic.Bool
	fail.Store(true)
	var postIndexedCalls atomic.Int32
	require.NoError(t, o.SetPostIndexed(func(ctx context.Context) error {
		postIndexedCalls.Add(1)
		if fail.Load() {
			return errPostIndexed
		}
		return nil
	}))

	var stateReadyFires atomic.Int32
	require.NoError(t, bus.Subscribe(func(e InitialStateReady) {
		stateReadyFires.Add(1)
	}))

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	stateFile := peerEntry("v1.0-accounts.0-256.kv", 0, 256)
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateFile}},
	})
	bus.Publish(DownloadComplete{FileName: stateFile.Name, Size: stateFile.Size})
	require.True(t, inv.AdvanceTo(stateFile.Name, snapshot.LifecycleIndexed))

	// postIndexed returns the failure first time. InitialStateReady does
	// not fire; the orchestrator waits for the next ChangeSet to retry.
	waitUntil(t, func() bool {
		return postIndexedCalls.Load() >= 1
	}, 2*time.Second, "first postIndexed attempt")

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), stateReadyFires.Load(),
		"InitialStateReady must not fire when postIndexed errors")

	// Flip the failure off; advance the file to Advertisable to trigger
	// another ChangeSet → re-check → postIndexed retry → success → fire.
	fail.Store(false)
	require.True(t, inv.AdvanceTo(stateFile.Name, snapshot.LifecycleAdvertisable))

	waitUntil(t, func() bool {
		return stateReadyFires.Load() == 1
	}, 2*time.Second, "InitialStateReady after postIndexed recovers")
}

var errPostIndexed = postIndexedTestErr{}

type postIndexedTestErr struct{}

func (postIndexedTestErr) Error() string { return "synthetic postIndexed failure" }
