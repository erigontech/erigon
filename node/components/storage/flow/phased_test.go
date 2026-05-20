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

// phaseObserver captures the sequence of DownloadRequested +
// InitialStateReady events in the order they arrive on the bus.
type phaseObserver struct {
	mu     sync.Mutex
	events []string // "state:<name>", "block:<name>", or "state-ready"
}

func (p *phaseObserver) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.events))
	copy(out, p.events)
	return out
}

// TestPhasedScheduling_StateBeforeBlocks asserts that block-file
// DownloadRequested events don't fire until the state phase completes.
// State domain files publish first, InitialStateReady fires after the
// last state-file DownloadComplete, and only then do block DownloadRequested
// events emerge.
func TestPhasedScheduling_StateBeforeBlocks(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	obs := &phaseObserver{}
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		obs.mu.Lock()
		if e.Domain != "" {
			obs.events = append(obs.events, "state:"+e.FileName)
		} else {
			obs.events = append(obs.events, "block:"+e.FileName)
		}
		obs.mu.Unlock()
	}))
	require.NoError(t, bus.Subscribe(func(e InitialStateReady) {
		obs.mu.Lock()
		obs.events = append(obs.events, "state-ready")
		obs.mu.Unlock()
	}))

	// Peer advertises one state-domain file and one block file. The
	// Phase semantics (post-2026-05-18 widening): phase-1 = state +
	// meta + salt + ALL block file types (headers + bodies +
	// transactions). Phase-2 = caplin only (Caplin has its own
	// BlockHeadersReady signal). This test exercises a caplin file as
	// the phase-2 entry queued behind state-ready, since blocks are
	// no longer phase-2.
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

	// Wait for the state DownloadRequested to have been observed.
	waitUntil(t, func() bool {
		s := obs.snapshot()
		return len(s) >= 1
	}, 2*time.Second, "state DownloadRequested")

	// At this moment, only state has fired — no caplin request, no state-ready.
	first := obs.snapshot()
	require.Contains(t, first, "state:v1.0-accounts.0-256.kv")
	require.NotContains(t, first, "state-ready", "state-ready must wait for state completion")
	require.NotContains(t, first, "block:"+caplinFile.Name,
		"caplin request must be held until state-ready fires")

	// Now complete the state download. That should drain state-pending,
	// fire InitialStateReady, and release the queued caplin request.
	bus.Publish(DownloadComplete{
		FileName: stateFile.Name,
		InfoHash: stateFile.TorrentHash,
		Size:     stateFile.Size,
	})

	waitUntil(t, func() bool {
		s := obs.snapshot()
		return len(s) >= 3 // state, state-ready, caplin
	}, 2*time.Second, "state-ready + caplin request to follow")

	got := obs.snapshot()
	stateIdx := indexOf(got, "state:v1.0-accounts.0-256.kv")
	readyIdx := indexOf(got, "state-ready")
	caplinIdx := indexOf(got, "block:"+caplinFile.Name)

	require.GreaterOrEqual(t, stateIdx, 0, "state request observed")
	require.Greater(t, readyIdx, stateIdx, "state-ready must follow state request")
	require.Greater(t, caplinIdx, readyIdx, "caplin request must follow state-ready")
}

// TestPhasedScheduling_FiresImmediatelyWhenNoStateGap asserts that when a
// peer's manifest produces no state-domain gap (because we already have
// all the state locally, or the peer only advertises blocks), the
// orchestrator fires InitialStateReady right after processing the
// manifest so the block queue drains immediately.
func TestPhasedScheduling_FiresImmediatelyWhenNoStateGap(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	obs := &phaseObserver{}
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		obs.mu.Lock()
		if e.Domain != "" {
			obs.events = append(obs.events, "state:"+e.FileName)
		} else {
			obs.events = append(obs.events, "block:"+e.FileName)
		}
		obs.mu.Unlock()
	}))
	require.NoError(t, bus.Subscribe(func(e InitialStateReady) {
		obs.mu.Lock()
		obs.events = append(obs.events, "state-ready")
		obs.mu.Unlock()
	}))

	// Peer advertises only caplin files — no state, no blocks. Caplin
	// is the only phase-2 category (post-2026-05-18: ALL block files
	// are phase 1). With nothing in phase 1, the no-state-gap path
	// fires state-ready immediately and the caplin queue drains.
	caplin1 := &snapshot.FileEntry{Kind: snapshot.KindCaplin, Name: "caplin/v1.1-000000-000010-beaconblocks.seg"}
	caplin2 := &snapshot.FileEntry{Kind: snapshot.KindCaplin, Name: "caplin/v1.1-000010-000020-beaconblocks.seg"}
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-1",
		Caplin: []*snapshot.FileEntry{caplin1, caplin2},
	})

	waitUntil(t, func() bool {
		s := obs.snapshot()
		return len(s) >= 3 // state-ready + two caplin requests
	}, 2*time.Second, "state-ready + caplin requests")

	got := obs.snapshot()
	readyIdx := indexOf(got, "state-ready")
	c1Idx := indexOf(got, "block:"+caplin1.Name)
	c2Idx := indexOf(got, "block:"+caplin2.Name)

	require.GreaterOrEqual(t, readyIdx, 0, "state-ready fires on no-state-gap manifest")
	require.Greater(t, c1Idx, readyIdx, "caplin request follows state-ready")
	require.Greater(t, c2Idx, readyIdx, "caplin request follows state-ready")
}

// TestPhasedScheduling_StateReadyFiresOnce asserts that InitialStateReady
// is published exactly once per orchestrator lifetime even when multiple
// peers publish manifests that each drain the state phase.
func TestPhasedScheduling_StateReadyFiresOnce(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	var readyCount int
	var mu sync.Mutex
	require.NoError(t, bus.Subscribe(func(InitialStateReady) {
		mu.Lock()
		readyCount++
		mu.Unlock()
	}))

	// Three manifests with no phase-1 gap — each would try to fire
	// ready. Caplin-only manifests have nothing in phase-1, so
	// statePending == 0 holds throughout; only the first one's fire
	// should publish.
	for i := 0; i < 3; i++ {
		bus.Publish(PeerManifestReceived{
			PeerID: "peer",
			Caplin: []*snapshot.FileEntry{
				{Kind: snapshot.KindCaplin, Name: "caplin/v1.1-000000-000010-beaconblocks.seg"},
			},
		})
	}

	// Give time for any stray fires.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, readyCount, "InitialStateReady must fire exactly once across manifests")
}

// indexOf returns the first index of needle in haystack, or -1 if absent.
func indexOf(haystack []string, needle string) int {
	for i, s := range haystack {
		if s == needle {
			return i
		}
	}
	return -1
}
