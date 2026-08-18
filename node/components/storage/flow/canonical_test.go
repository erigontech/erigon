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

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// canonicalTestOrch builds an orchestrator wired for canonical tests: bus,
// empty inventory, tiny debounce for fast test convergence, running.
func canonicalTestOrch(t *testing.T, debounce time.Duration) (*Orchestrator, event.EventBus) {
	t.Helper()
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	o.SetCanonicalDebounce(debounce)
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })
	return o, bus
}

func fe(name string, dom snapshot.Domain, from, to uint64, hashSeed byte) *snapshot.FileEntry {
	var h [20]byte
	for i := range h {
		h[i] = hashSeed
	}
	return &snapshot.FileEntry{
		Domain: dom, FromStep: from, ToStep: to,
		Name: name, Kind: snapshot.KindKV,
		TorrentHash: h,
	}
}

// P2-1: zero trusted peers → canonical empty.
func TestOrchestrator_Canonical_EmptyWithNoPeers(t *testing.T) {
	o, _ := canonicalTestOrch(t, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	require.Empty(t, o.Canonical())
}

// P2-2: one trusted peer with manifest → canonical mirrors it (trivial
// quorum — intersection of one set is itself).
func TestOrchestrator_Canonical_SinglePeerMirrors(t *testing.T) {
	o, bus := canonicalTestOrch(t, 10*time.Millisecond)
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "canonical settles")
	c := o.Canonical()
	require.Len(t, c, 1)
	require.NotNil(t, c["A.kv"])
	require.Equal(t, byte(0xA1), c["A.kv"].TorrentHash[0])
}

// P2-3: two peers with identical manifests → canonical is that set.
func TestOrchestrator_Canonical_TwoPeersMatchingSetsUnion(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	m := map[snapshot.Domain][]*snapshot.FileEntry{
		snapshot.DomainCommitment: {
			fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
			fe("B.kv", snapshot.DomainCommitment, 1, 2, 0xB1),
		},
	}
	bus.Publish(PeerManifestReceived{PeerID: "P1", Domains: m})
	bus.Publish(PeerManifestReceived{PeerID: "P2", Domains: m})
	waitUntil(t, func() bool { return len(o.Canonical()) == 2 }, 2*time.Second, "canonical settles at 2")
	c := o.Canonical()
	require.Contains(t, c, "A.kv")
	require.Contains(t, c, "B.kv")
}

// P2-4: two peers, one advertises a superset → canonical is the intersection.
func TestOrchestrator_Canonical_TwoPeersIntersectionOnly(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("B.kv", snapshot.DomainCommitment, 1, 2, 0xB1),
				fe("X.kv", snapshot.DomainCommitment, 2, 3, 0xC1),
			},
		},
	})
	bus.Publish(PeerManifestReceived{
		PeerID: "P2",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("B.kv", snapshot.DomainCommitment, 1, 2, 0xB1),
			},
		},
	})
	waitUntil(t, func() bool {
		c := o.Canonical()
		return len(c) == 2 && c["A.kv"] != nil && c["B.kv"] != nil && c["X.kv"] == nil
	}, 2*time.Second, "canonical settles at intersection {A, B}")
}

// P2-5: two peers advertise same filename with DIFFERENT hashes → filename
// excluded from canonical (divergence rejection).
func TestOrchestrator_Canonical_DivergentHashExcluded(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("B.kv", snapshot.DomainCommitment, 1, 2, 0xB1),
			},
		},
	})
	bus.Publish(PeerManifestReceived{
		PeerID: "P2",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA2),
				fe("B.kv", snapshot.DomainCommitment, 1, 2, 0xB1),
			},
		},
	})
	waitUntil(t, func() bool {
		c := o.Canonical()
		return len(c) == 1 && c["B.kv"] != nil && c["A.kv"] == nil
	}, 2*time.Second, "A.kv divergence rejected; B.kv passes")
}

// P2-6: peer joins (was 1, now 2 with narrower set) → canonical shrinks;
// CanonicalChanged event fires with the removed file.
func TestOrchestrator_Canonical_PeerJoinShrinksAndFiresChanged(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	var (
		mu     sync.Mutex
		events []CanonicalChanged
	)
	require.NoError(t, bus.Subscribe(func(e CanonicalChanged) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))

	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("X.kv", snapshot.DomainCommitment, 2, 3, 0xC1),
			},
		},
	})
	waitUntil(t, func() bool { return len(o.Canonical()) == 2 }, 2*time.Second, "canonical = 2 with P1 alone")

	bus.Publish(PeerManifestReceived{
		PeerID: "P2",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "canonical shrinks to {A}")

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	require.Contains(t, last.Removed, "X.kv", "CanonicalChanged.Removed contains X")
}

// P2-7: peer departs → canonical grows to remaining peer's set; CanonicalChanged fires.
func TestOrchestrator_Canonical_PeerDepartsGrowsAndFires(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	var (
		mu     sync.Mutex
		events []CanonicalChanged
	)
	require.NoError(t, bus.Subscribe(func(e CanonicalChanged) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))

	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("X.kv", snapshot.DomainCommitment, 2, 3, 0xC1),
			},
		},
	})
	bus.Publish(PeerManifestReceived{
		PeerID: "P2",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "canonical = {A} with both peers")

	bus.Publish(PeerDeparted{PeerID: "P2"})
	waitUntil(t, func() bool { return len(o.Canonical()) == 2 }, 2*time.Second, "canonical grows to {A, X}")

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	require.Contains(t, last.Added, "X.kv", "CanonicalChanged.Added contains X after P2 departs")
}

// P2-8: debounced re-eval — N peer manifests within debounce → single recompute.
func TestOrchestrator_Canonical_DebouncedSingleRecompute(t *testing.T) {
	o, bus := canonicalTestOrch(t, 100*time.Millisecond)
	var (
		mu     sync.Mutex
		events []CanonicalChanged
	)
	require.NoError(t, bus.Subscribe(func(e CanonicalChanged) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))

	// Burst three manifests within the debounce window from three peers.
	// Debounce should batch into a single canonical recompute (one event
	// or zero, not three).
	for _, peerID := range []string{"P1", "P2", "P3"} {
		bus.Publish(PeerManifestReceived{
			PeerID: peerID,
			Domains: map[snapshot.Domain][]*snapshot.FileEntry{
				snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
			},
		})
	}
	// Wait past the debounce + a small margin.
	time.Sleep(200 * time.Millisecond)

	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "canonical settles at {A}")

	mu.Lock()
	defer mu.Unlock()
	require.LessOrEqual(t, len(events), 2,
		"debounced: burst of 3 manifests within window produces at most 2 CanonicalChanged events (first fire + final settle); got %d", len(events))
}

// P2-11: peer flap (drop then re-appear with same manifest) → canonical
// bounces via prior-canonical fallback. Recomputes cleanly.
func TestOrchestrator_Canonical_PeerFlapReSettles(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)

	m := map[snapshot.Domain][]*snapshot.FileEntry{
		snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
	}
	bus.Publish(PeerManifestReceived{PeerID: "P1", Domains: m})
	bus.Publish(PeerManifestReceived{PeerID: "P2", Domains: m})
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "settle {A}")

	bus.Publish(PeerDeparted{PeerID: "P2"})
	// P2 gone → canonical = P1's manifest still {A} (single-peer canonical).
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "still {A} with just P1")

	bus.Publish(PeerManifestReceived{PeerID: "P2", Domains: m})
	// P2 back with same manifest → canonical stays {A}.
	waitUntil(t, func() bool { return len(o.Canonical()) == 1 }, 2*time.Second, "re-settle {A}")
	require.Contains(t, o.Canonical(), "A.kv")
}
