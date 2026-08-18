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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// captureRequests subscribes to DownloadRequested and returns a snapshot func + a mu-protected slice.
func captureRequests(t *testing.T, bus interface {
	Subscribe(any) error
}) (getAll func() []string) {
	t.Helper()
	var mu sync.Mutex
	var events []string
	require.NoError(t, bus.Subscribe(func(e DownloadRequested) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e.FileName)
	}))
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(events))
		copy(out, events)
		return out
	}
}

// P3-4: after canonical has settled, a peer manifest with a file NOT
// in canonical must NOT trigger a request. Split concern: cancel of
// already-requested files that fall off canonical is Phase 4.
func TestOrchestrator_CanonicalGate_PostSettleRejectsNonCanonicalFile(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)

	// Establish canonical with {A.kv} — both peers agree, only A.
	m := map[snapshot.Domain][]*snapshot.FileEntry{
		snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
	}
	bus.Publish(PeerManifestReceived{PeerID: "P1", Domains: m})
	bus.Publish(PeerManifestReceived{PeerID: "P2", Domains: m})
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "canonical settled at {A}")

	// Now record only NEW requests going forward — subscribe after settle.
	getAll := captureRequests(t, bus)

	// P1 re-sends its manifest but now WITH X.kv (gap file — P2 doesn't
	// advertise it). Canonical stays at {A} because P2's set hasn't
	// changed. X must NOT be requested.
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("X.kv", snapshot.DomainCommitment, 2, 3, 0xC1),
			},
		},
	})
	time.Sleep(100 * time.Millisecond)

	reqs := getAll()
	for _, name := range reqs {
		require.NotEqual(t, "X.kv", name,
			"X.kv must NOT be requested — canonical excludes it (only P1 advertises)")
	}
}

// P3-5: bootstrap (no manifests received yet) → additive fallback lets the
// first manifest's files be requested. Once quorum can form (any peer's
// manifest recorded), canonical takes over.
func TestOrchestrator_CanonicalGate_BootstrapAdditiveFallback(t *testing.T) {
	o, bus := canonicalTestOrch(t, 10*time.Millisecond)
	getAll := captureRequests(t, bus)

	// Single-peer manifest arrives — this IS canonical (trivial quorum).
	// File should be requested via canonical gate (not the fallback).
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool {
		_, ok := o.Canonical()["A.kv"]
		return ok
	}, 2*time.Second, "canonical populates with A.kv")
	waitUntil(t, func() bool {
		return slices.Contains(getAll(), "A.kv")
	}, 2*time.Second, "A.kv requested via canonical")
}

// P3-2: file in canonical + already local → NOT requested (haveLocally gate).
// The canonical gate stacks with the existing local-file gate — canonical
// says "yes we should have this" but haveLocally short-circuits.
func TestOrchestrator_CanonicalGate_LocalFileNotRerequested(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	getAll := captureRequests(t, bus)

	// Seed the inventory: A.kv is local. Peer advertises it too.
	local := &snapshot.FileEntry{
		Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 1,
		Name: "A.kv", Kind: snapshot.KindKV, Local: true,
	}
	require.NoError(t, o.storage.RecordFile(local))

	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool {
		_, ok := o.Canonical()["A.kv"]
		return ok
	}, 2*time.Second, "canonical contains A.kv")

	time.Sleep(50 * time.Millisecond)
	require.NotContains(t, getAll(), "A.kv",
		"local file must not be re-requested even if canonical lists it")
}

// P3-3: file in canonical + producing locally (G3+G4 IsProducing filter) →
// NOT requested even though canonical would allow it. Local production
// wins over network fetch (per G3+G4).
func TestOrchestrator_CanonicalGate_ProducingLocallyNotRequested(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	getAll := captureRequests(t, bus)

	inv := o.storage.Inventory()
	// Produce marker: RemoveFile marks as producing (per G3+G4).
	inv.AddFile(&snapshot.FileEntry{
		Domain: snapshot.DomainCommitment, FromStep: 0, ToStep: 1,
		Name: "A.kv", Kind: snapshot.KindKV, Local: true,
	})
	inv.RemoveFile("A.kv")
	require.True(t, inv.IsProducing("A.kv"))

	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool {
		_, ok := o.Canonical()["A.kv"]
		return ok
	}, 2*time.Second, "canonical contains A.kv")

	time.Sleep(50 * time.Millisecond)
	require.NotContains(t, getAll(), "A.kv",
		"producing-locally file must not be requested")
}
