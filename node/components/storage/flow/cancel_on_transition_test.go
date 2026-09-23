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

func captureSuperseded(t *testing.T, bus interface {
	Subscribe(any) error
}) func() []string {
	t.Helper()
	var mu sync.Mutex
	var events []string
	require.NoError(t, bus.Subscribe(func(e DownloadSuperseded) {
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

// P4-1: file in pending removed from canonical → DownloadSuperseded fires
// (Provider will call downloader.Delete on this signal in production).
func TestOrchestrator_CancelOnTransition_PendingRemovedFromCanonical_Fires(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	getReqs := captureRequests(t, bus)
	getCanc := captureSuperseded(t, bus)

	// P1 alone: canonical = P1's set. Requests fire additively.
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("X.kv", snapshot.DomainCommitment, 2, 3, 0xC1),
			},
		},
	})
	waitUntil(t, func() bool { return slices.Contains(getReqs(), "X.kv") }, 2*time.Second, "X.kv requested via P1 alone")
	waitUntil(t, func() bool { _, ok := o.Canonical()["X.kv"]; return ok }, 2*time.Second, "X in canonical")

	// P2 arrives without X → canonical shrinks; X is in pending.
	bus.Publish(PeerManifestReceived{
		PeerID: "P2",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { _, ok := o.Canonical()["X.kv"]; return !ok }, 2*time.Second, "X removed from canonical")

	// DownloadSuperseded fires for X.
	waitUntil(t, func() bool { return slices.Contains(getCanc(), "X.kv") }, 2*time.Second, "X.kv superseded event")
}

// P4-2: file NOT in pending removed from canonical → NO cancel fires.
func TestOrchestrator_CancelOnTransition_NonPendingRemoved_NoEvent(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)

	// Seed X.kv as local already — haveLocally gate prevents adding to pending.
	inv := o.storage.Inventory()
	_ = inv.AddFile(&snapshot.FileEntry{
		Domain: snapshot.DomainCommitment, FromStep: 2, ToStep: 3,
		Name: "X.kv", Kind: snapshot.KindKV, Local: true,
	})

	getCanc := captureSuperseded(t, bus)

	// P1 alone: canonical grows to include X, but X is local so pending stays empty for X.
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {
				fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1),
				fe("X.kv", snapshot.DomainCommitment, 2, 3, 0xC1),
			},
		},
	})
	waitUntil(t, func() bool { _, ok := o.Canonical()["X.kv"]; return ok }, 2*time.Second, "X in canonical")

	// P2 arrives without X → canonical drops X. Not-in-pending → no cancel.
	bus.Publish(PeerManifestReceived{
		PeerID: "P2",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { _, ok := o.Canonical()["X.kv"]; return !ok }, 2*time.Second, "X removed from canonical")

	time.Sleep(100 * time.Millisecond)
	require.NotContains(t, getCanc(), "X.kv",
		"X was never pending — no supersede signal should fire")
}

// P4-3: peer P departs, orphaning a pending file that was in canonical
// (single-peer trivial quorum) → DownloadSuperseded fires. Closes the
// current gap where onPeerDeparted retained pending entries forever.
func TestOrchestrator_CancelOnTransition_PeerDepartOrphansPending_Fires(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	getReqs := captureRequests(t, bus)
	getCanc := captureSuperseded(t, bus)

	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { return slices.Contains(getReqs(), "A.kv") }, 2*time.Second, "A.kv requested")
	waitUntil(t, func() bool { _, ok := o.Canonical()["A.kv"]; return ok }, 2*time.Second, "canonical has A")

	// P1 departs → canonical shrinks to empty (no peers left).
	bus.Publish(PeerDeparted{PeerID: "P1"})
	waitUntil(t, func() bool { return len(o.Canonical()) == 0 }, 2*time.Second, "canonical drops A")

	// Pending A must be cancelled.
	waitUntil(t, func() bool { return slices.Contains(getCanc(), "A.kv") }, 2*time.Second, "supersede event for A")
}

// P4-4: file cancelled, then peer re-advertises it → requestGapsFor picks
// it up again cleanly (no phantom-cancel state that permanently rejects).
func TestOrchestrator_CancelOnTransition_ReAdvertiseRerequests(t *testing.T) {
	o, bus := canonicalTestOrch(t, 20*time.Millisecond)
	getReqs := captureRequests(t, bus)
	getCanc := captureSuperseded(t, bus)

	// P1 sends A; get requested + in canonical.
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { return slices.Contains(getReqs(), "A.kv") }, 2*time.Second, "A requested")

	// P1 sends empty manifest → canonical drops A → cancel fires.
	bus.Publish(PeerManifestReceived{PeerID: "P1"})
	waitUntil(t, func() bool { return slices.Contains(getCanc(), "A.kv") }, 2*time.Second, "A cancelled")

	initialReqCount := len(getReqs())

	// P1 re-sends A → canonical rebuilds → A must be re-requested.
	bus.Publish(PeerManifestReceived{
		PeerID: "P1",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {fe("A.kv", snapshot.DomainCommitment, 0, 1, 0xA1)},
		},
	})
	waitUntil(t, func() bool { return len(getReqs()) > initialReqCount }, 2*time.Second, "A re-requested after cancel")
	waitUntil(t, func() bool { _, ok := o.Canonical()["A.kv"]; return ok }, 2*time.Second, "canonical restored to include A")
	require.Contains(t, o.Canonical(), "A.kv")
}
