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

// TestInitialDownloadsComplete_FiresAfterCaplinDrains pins the phase-2
// path: with a caplin file held behind InitialStateReady,
// InitialDownloadsComplete must NOT fire when state completes — caplin
// is still pending — and must fire once the caplin download drains.
func TestInitialDownloadsComplete_FiresAfterCaplinDrains(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu            sync.Mutex
		stateReady    int
		downloadsDone int
	)
	require.NoError(t, bus.Subscribe(func(InitialStateReady) {
		mu.Lock()
		stateReady++
		mu.Unlock()
	}))
	require.NoError(t, bus.Subscribe(func(InitialDownloadsComplete) {
		mu.Lock()
		downloadsDone++
		mu.Unlock()
	}))

	stateFile := &snapshot.FileEntry{
		Domain: testDomain, FromStep: 0, ToStep: 256,
		Name: "v1.0-accounts.0-256.kv",
	}
	caplinFile := &snapshot.FileEntry{
		Kind: snapshot.KindCaplin,
		Name: "caplin/v1.1-000000-000010-beaconblocks.seg",
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{testDomain: {stateFile}},
		Caplin:  []*snapshot.FileEntry{caplinFile},
	})

	// Completing the state file fires InitialStateReady and releases the
	// caplin request — but caplin is still in flight, so the initial
	// download set is NOT complete yet.
	bus.Publish(DownloadComplete{FileName: stateFile.Name})
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stateReady == 1
	}, 2*time.Second, "InitialStateReady after state completes")

	mu.Lock()
	require.Zero(t, downloadsDone,
		"InitialDownloadsComplete must not fire while caplin is still pending")
	mu.Unlock()

	// Caplin completes → pending drains → InitialDownloadsComplete fires.
	bus.Publish(DownloadComplete{FileName: caplinFile.Name})
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return downloadsDone == 1
	}, 2*time.Second, "InitialDownloadsComplete after caplin drains")
}

// TestInitialDownloadsComplete_FiresWhenNoCaplin pins the no-phase-2
// path: a manifest with only a state file drains entirely on the state
// DownloadComplete — InitialStateReady and InitialDownloadsComplete both
// fire, the latter from fireInitialStateReady's tail.
func TestInitialDownloadsComplete_FiresWhenNoCaplin(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var (
		mu            sync.Mutex
		downloadsDone int
	)
	require.NoError(t, bus.Subscribe(func(InitialDownloadsComplete) {
		mu.Lock()
		downloadsDone++
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
	bus.Publish(DownloadComplete{FileName: stateFile.Name})

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return downloadsDone == 1
	}, 2*time.Second, "InitialDownloadsComplete with no caplin")

	// Hold briefly and confirm it stays at exactly one — fire-once.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	require.Equal(t, 1, downloadsDone, "InitialDownloadsComplete must fire exactly once")
	mu.Unlock()
}
