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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Orchestrator captures the CURRENT view every trusted peer advertises
// so downstream code can compute canonical / delta / cancel. Peer
// manifests are self-replacing snapshots (each carries the peer's
// current state, not a diff).
func TestOrchestrator_PeerManifestState_RecordsOnReceive(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entries := []*snapshot.FileEntry{
		{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311,
			Name: "v2.2-commitment.310-311.kv", Kind: snapshot.KindKV},
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{snapshot.DomainCommitment: entries},
	})

	waitUntil(t, func() bool {
		return o.PeerManifestFiles("peer-A") != nil
	}, 2*time.Second, "peer manifest state recorded")

	files := o.PeerManifestFiles("peer-A")
	require.Len(t, files, 1)
	require.Contains(t, files, "v2.2-commitment.310-311.kv")
}

// A subsequent manifest from the same peer REPLACES the prior recorded
// view — this is the semantic each peer's chain.v2 publication carries
// (peer advertises its CURRENT state, not a diff).
func TestOrchestrator_PeerManifestState_ReplacesOnSecondReceive(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	first := []*snapshot.FileEntry{
		{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311,
			Name: "v2.2-commitment.310-311.kv", Kind: snapshot.KindKV},
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{snapshot.DomainCommitment: first},
	})
	waitUntil(t, func() bool { return o.PeerManifestFiles("peer-A") != nil }, 2*time.Second, "first recorded")

	second := []*snapshot.FileEntry{
		{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 312,
			Name: "v2.2-commitment.310-312.kv", Kind: snapshot.KindKV},
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{snapshot.DomainCommitment: second},
	})

	waitUntil(t, func() bool {
		files := o.PeerManifestFiles("peer-A")
		return len(files) == 1 && files["v2.2-commitment.310-312.kv"] != nil
	}, 2*time.Second, "second replaces first")

	files := o.PeerManifestFiles("peer-A")
	require.Len(t, files, 1)
	require.NotContains(t, files, "v2.2-commitment.310-311.kv",
		"old file must be gone after replacement")
	require.Contains(t, files, "v2.2-commitment.310-312.kv")
}

// PeerDeparted drops the peer's recorded manifest state.
func TestOrchestrator_PeerManifestState_ClearedOnPeerDeparted(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	entries := []*snapshot.FileEntry{
		{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311,
			Name: "v2.2-commitment.310-311.kv", Kind: snapshot.KindKV},
	}
	bus.Publish(PeerManifestReceived{
		PeerID:  "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{snapshot.DomainCommitment: entries},
	})
	waitUntil(t, func() bool { return o.PeerManifestFiles("peer-A") != nil }, 2*time.Second, "recorded")

	bus.Publish(PeerDeparted{PeerID: "peer-A"})
	waitUntil(t, func() bool { return o.PeerManifestFiles("peer-A") == nil }, 2*time.Second, "cleared")

	require.Nil(t, o.PeerManifestFiles("peer-A"))
}

// A manifest spanning multiple kinds (Domains + Blocks + Caplin + Meta
// + Salt) records every file across every kind — Phase 2's canonical
// computation must see the full union.
func TestOrchestrator_PeerManifestState_MultiKindManifestFullyCaptured(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainAccounts: {{Domain: snapshot.DomainAccounts, FromStep: 0, ToStep: 256,
				Name: "v1.0-accounts.0-256.kv", Kind: snapshot.KindKV}},
		},
		Blocks: []*snapshot.FileEntry{
			{FromStep: 0, ToStep: 500, Name: "v1.0-000000-000500-headers.seg"},
		},
		Caplin: []*snapshot.FileEntry{
			{FromStep: 0, ToStep: 500, Name: "v1.0-000000-000500-beaconblocks.seg", Kind: snapshot.KindCaplin},
		},
		Meta: []*snapshot.FileEntry{
			{Name: "erigondb.toml", Kind: snapshot.KindMeta},
		},
		Salt: []*snapshot.FileEntry{
			{Name: "salt-blocks.txt", Kind: snapshot.KindSalt},
		},
	})

	waitUntil(t, func() bool {
		files := o.PeerManifestFiles("peer-A")
		return len(files) == 5
	}, 2*time.Second, "all 5 kinds captured")

	files := o.PeerManifestFiles("peer-A")
	require.Len(t, files, 5)
	for _, name := range []string{
		"v1.0-accounts.0-256.kv",
		"v1.0-000000-000500-headers.seg",
		"v1.0-000000-000500-beaconblocks.seg",
		"erigondb.toml",
		"salt-blocks.txt",
	} {
		require.Contains(t, files, name, "kind coverage: %s missing", name)
	}
}

// An empty manifest from a peer records an empty state (not "peer
// never sent anything") — the peer is on record as advertising nothing.
// Phase 2's canonical then intersects to empty, correctly.
func TestOrchestrator_PeerManifestState_EmptyManifestRecordedAsEmpty(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	bus.Publish(PeerManifestReceived{PeerID: "peer-A"})

	waitUntil(t, func() bool {
		return o.PeerManifestFiles("peer-A") != nil
	}, 2*time.Second, "empty manifest recorded")

	files := o.PeerManifestFiles("peer-A")
	require.NotNil(t, files, "state exists")
	require.Empty(t, files, "empty manifest yields empty file set")
}

// Untrusted peer's manifest must NOT be captured — trust filter gates at
// the source. Otherwise an untrusted peer could pollute the canonical
// intersection with unauthenticated file claims.
func TestOrchestrator_PeerManifestState_UntrustedPeerNotCaptured(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.SetTrust(rejectAllTrust{}))
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-hostile",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311,
				Name: "v2.2-commitment.310-311.kv", Kind: snapshot.KindKV}},
		},
	})
	// Give the async handler a chance to run.
	time.Sleep(200 * time.Millisecond)

	require.Nil(t, o.PeerManifestFiles("peer-hostile"),
		"untrusted peer's manifest must not enter per-peer state")
}

type rejectAllTrust struct{}

func (rejectAllTrust) Trusted(peerID string) bool { return false }

// Multiple peers each get their own recorded state, isolated.
func TestOrchestrator_PeerManifestState_TwoPeersIsolated(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())
	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	bus.Publish(PeerManifestReceived{
		PeerID: "peer-A",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {{Domain: snapshot.DomainCommitment, FromStep: 310, ToStep: 311,
				Name: "v2.2-commitment.310-311.kv", Kind: snapshot.KindKV}},
		},
	})
	bus.Publish(PeerManifestReceived{
		PeerID: "peer-B",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainCommitment: {{Domain: snapshot.DomainCommitment, FromStep: 311, ToStep: 312,
				Name: "v2.2-commitment.311-312.kv", Kind: snapshot.KindKV}},
		},
	})

	waitUntil(t, func() bool {
		return o.PeerManifestFiles("peer-A") != nil && o.PeerManifestFiles("peer-B") != nil
	}, 2*time.Second, "both peers recorded")

	filesA := o.PeerManifestFiles("peer-A")
	filesB := o.PeerManifestFiles("peer-B")
	require.Len(t, filesA, 1)
	require.Len(t, filesB, 1)
	require.Contains(t, filesA, "v2.2-commitment.310-311.kv")
	require.Contains(t, filesB, "v2.2-commitment.311-312.kv")
}
